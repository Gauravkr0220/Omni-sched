#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "kronos/example_tasks.hpp"
#include "kronos/task_engine.hpp"

namespace kronos {
namespace {

using namespace std::chrono_literals;

class InstantTask final : public ITask {
 public:
  explicit InstantTask(std::string output = "done")
      : output_(std::move(output)) {}

  SliceResult run_slice(const TaskContext&) override {
    return SliceResult::completed(output_, 1);
  }

 private:
  std::string output_;
};

class ThrowingTask final : public ITask {
 public:
  SliceResult run_slice(const TaskContext&) override {
    throw std::runtime_error("intentional failure");
  }
};

class GateTask final : public ITask {
 public:
  GateTask(std::promise<void>& started, std::shared_future<void> release)
      : started_(started), release_(std::move(release)) {}

  SliceResult run_slice(const TaskContext&) override {
    started_.set_value();
    release_.wait();
    return SliceResult::completed("gate released", 1);
  }

 private:
  std::promise<void>& started_;
  std::shared_future<void> release_;
};

class RecordingTask final : public ITask {
 public:
  RecordingTask(int marker, std::vector<int>& order, std::mutex& mutex)
      : marker_(marker), order_(order), mutex_(mutex) {}

  SliceResult run_slice(const TaskContext&) override {
    {
      std::lock_guard lock(mutex_);
      order_.push_back(marker_);
    }
    return SliceResult::completed("recorded", 1);
  }

 private:
  int marker_;
  std::vector<int>& order_;
  std::mutex& mutex_;
};

bool wait_for_state(TaskEngine& engine, JobId id, JobState desired,
                    std::chrono::milliseconds timeout = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto job = engine.get_job(id);
    if (job && job->state == desired) {
      return true;
    }
    std::this_thread::sleep_for(1ms);
  }
  return false;
}

TEST(TaskEngineTest, CompletesTaskAndPublishesSnapshotAndFuture) {
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});
  auto handle = engine.submit(std::make_unique<InstantTask>("answer"),
                              {.name = "instant",
                               .priority = 3,
                               .estimated_work_units = 1});

  const auto result = handle.result().get();
  const auto snapshot = engine.get_job(handle.id());
  engine.shutdown();

  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(result.state, JobState::Completed);
  EXPECT_EQ(result.message, "answer");
  EXPECT_EQ(snapshot->name, "instant");
  EXPECT_EQ(snapshot->priority, 3);
  EXPECT_EQ(snapshot->work_units_consumed, 1U);
  EXPECT_NE(snapshot->started_at, std::chrono::steady_clock::time_point{});
  EXPECT_NE(snapshot->finished_at, std::chrono::steady_clock::time_point{});
}

TEST(TaskEngineTest, ConvertsTaskExceptionIntoFailedResult) {
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});
  auto handle = engine.submit(std::make_unique<ThrowingTask>());

  const auto result = handle.result().get();
  engine.shutdown();

  EXPECT_EQ(result.state, JobState::Failed);
  EXPECT_EQ(result.message, "intentional failure");
}

TEST(TaskEngineTest, RejectsInvalidSubmission) {
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});

  EXPECT_THROW((void)engine.submit(nullptr), std::invalid_argument);
  EXPECT_THROW((void)engine.submit(std::make_unique<InstantTask>(),
                                   {.estimated_work_units = 0}),
               std::invalid_argument);
  engine.shutdown();
  EXPECT_THROW((void)engine.submit(std::make_unique<InstantTask>()),
               std::logic_error);
}

TEST(TaskEngineTest, CancelsQueuedTaskWithoutExecutingIt) {
  std::promise<void> started;
  std::promise<void> release;
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});
  auto gate = engine.submit(std::make_unique<GateTask>(
      started, release.get_future().share()));
  started.get_future().wait();

  std::vector<int> order;
  std::mutex order_mutex;
  auto pending = engine.submit(
      std::make_unique<RecordingTask>(7, order, order_mutex),
      {.name = "pending", .estimated_work_units = 1});

  EXPECT_TRUE(engine.cancel(pending.id()));
  release.set_value();
  EXPECT_EQ(gate.result().get().state, JobState::Completed);
  EXPECT_EQ(pending.result().get().state, JobState::Cancelled);
  engine.shutdown();

  EXPECT_TRUE(order.empty());
  EXPECT_FALSE(engine.cancel(pending.id()));
}

TEST(TaskEngineTest, CooperativelyCancelsRunningTask) {
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});
  auto handle = engine.submit(
      std::make_unique<StepTask>(1'000, 2ms),
      {.name = "long-running", .estimated_work_units = 1'000});
  ASSERT_TRUE(wait_for_state(engine, handle.id(), JobState::Running));

  EXPECT_TRUE(engine.cancel(handle.id()));
  ASSERT_EQ(handle.result().wait_for(2s), std::future_status::ready);
  EXPECT_EQ(handle.result().get().state, JobState::Cancelled);
  engine.shutdown();
}

TEST(TaskEngineTest, RuntimeSwitchReordersOnlyQueuedJobs) {
  std::promise<void> started;
  std::promise<void> release;
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});
  auto gate = engine.submit(std::make_unique<GateTask>(
      started, release.get_future().share()));
  started.get_future().wait();

  std::vector<int> order;
  std::mutex order_mutex;
  auto low = engine.submit(
      std::make_unique<RecordingTask>(1, order, order_mutex),
      {.name = "low", .priority = 1, .estimated_work_units = 1});
  auto high = engine.submit(
      std::make_unique<RecordingTask>(10, order, order_mutex),
      {.name = "high", .priority = 10, .estimated_work_units = 1});
  auto middle = engine.submit(
      std::make_unique<RecordingTask>(5, order, order_mutex),
      {.name = "middle", .priority = 5, .estimated_work_units = 1});

  engine.set_scheduler({SchedulerKind::Priority, 100});
  EXPECT_EQ(engine.scheduler_config().kind, SchedulerKind::Priority);
  release.set_value();
  engine.shutdown(ShutdownMode::Drain);

  EXPECT_EQ(gate.result().get().state, JobState::Completed);
  EXPECT_EQ(high.result().get().state, JobState::Completed);
  EXPECT_EQ(middle.result().get().state, JobState::Completed);
  EXPECT_EQ(low.result().get().state, JobState::Completed);
  EXPECT_EQ(order, (std::vector<int>{10, 5, 1}));
}

TEST(TaskEngineTest, DrainShutdownCompletesRoundRobinWork) {
  TaskEngine engine({2, {SchedulerKind::RoundRobin, 7}, nullptr});
  auto first = engine.submit(
      std::make_unique<StepTask>(50, 0ms),
      {.name = "first", .estimated_work_units = 50});
  auto second = engine.submit(
      std::make_unique<StepTask>(40, 0ms),
      {.name = "second", .estimated_work_units = 40});

  engine.shutdown(ShutdownMode::Drain);

  EXPECT_EQ(first.result().get().state, JobState::Completed);
  EXPECT_EQ(second.result().get().state, JobState::Completed);
  EXPECT_FALSE(engine.is_accepting());
}

TEST(TaskEngineTest, CancelPendingShutdownKeepsRunningJobAndCancelsQueue) {
  std::promise<void> started;
  std::promise<void> release;
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, nullptr});
  auto running = engine.submit(std::make_unique<GateTask>(
      started, release.get_future().share()));
  started.get_future().wait();
  auto pending = engine.submit(std::make_unique<InstantTask>());

  auto shutdown = std::async(std::launch::async, [&engine] {
    engine.shutdown(ShutdownMode::CancelPending);
  });
  const auto pending_status = pending.result().wait_for(2s);
  EXPECT_EQ(pending_status, std::future_status::ready);
  if (pending_status == std::future_status::ready) {
    EXPECT_EQ(pending.result().get().state, JobState::Cancelled);
  }
  release.set_value();
  ASSERT_EQ(shutdown.wait_for(2s), std::future_status::ready);

  EXPECT_EQ(running.result().get().state, JobState::Completed);
}

TEST(TaskEngineTest, ConcurrentShutdownCallsAreSerialized) {
  TaskEngine engine({2, {SchedulerKind::RoundRobin, 5}, nullptr});
  auto job = engine.submit(
      std::make_unique<StepTask>(100, 0ms),
      {.name = "sliced", .estimated_work_units = 100});

  auto first = std::async(std::launch::async, [&engine] {
    engine.shutdown(ShutdownMode::Drain);
  });
  auto second = std::async(std::launch::async, [&engine] {
    engine.shutdown(ShutdownMode::Drain);
  });

  EXPECT_EQ(first.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(second.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(job.result().get().state, JobState::Completed);
}

TEST(TaskEngineTest, ConcurrentProducersDoNotLoseOrDuplicateJobs) {
  TaskEngine engine({4, {SchedulerKind::Fifo, 100}, nullptr});
  std::vector<JobHandle> handles;
  std::mutex handles_mutex;
  std::vector<std::jthread> producers;

  for (int producer = 0; producer < 4; ++producer) {
    producers.emplace_back([&] {
      for (int job = 0; job < 25; ++job) {
        auto handle = engine.submit(std::make_unique<InstantTask>());
        std::lock_guard lock(handles_mutex);
        handles.push_back(std::move(handle));
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  engine.shutdown(ShutdownMode::Drain);

  ASSERT_EQ(handles.size(), 100U);
  std::set<JobId> ids;
  for (const auto& handle : handles) {
    EXPECT_EQ(handle.result().get().state, JobState::Completed);
    ids.insert(handle.id());
  }
  EXPECT_EQ(ids.size(), handles.size());
  EXPECT_EQ(engine.snapshot().size(), handles.size());
}

TEST(TaskEngineTest, ResolvesAutomaticWorkerCountToAtLeastOne) {
  TaskEngine engine;
  EXPECT_GE(engine.worker_count(), 1U);
  engine.shutdown();
}

}  // namespace
}  // namespace kronos
