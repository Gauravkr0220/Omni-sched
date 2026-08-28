#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "kronos/example_tasks.hpp"
#include "kronos/task_factory.hpp"
#include "kronos/wal.hpp"
#include "test_support.hpp"

namespace kronos {
namespace {

TEST(WalManagerTest, RoundTripsChecksummedSubmissionAndTransitions) {
  test::TemporaryDirectory temporary{"wal-roundtrip"};
  WalManager wal(temporary.path() / "jobs.wal");
  wal.append_submission(
      {42, 7, "name|with\ntext", 9, 100, {"prime", "101"}});
  wal.append_transition({42, JobState::Running, 0, {}});
  wal.append_transition({42, JobState::Ready, 25, "yielded|once"});
  wal.append_transition({42, JobState::Completed, 100, "finished\ncleanly"});

  const auto report = wal.recover();

  ASSERT_EQ(report.jobs.size(), 1U);
  const auto& recovered = report.jobs.front();
  EXPECT_EQ(recovered.submission.id, 42U);
  EXPECT_EQ(recovered.submission.submission_sequence, 7U);
  EXPECT_EQ(recovered.submission.name, "name|with\ntext");
  EXPECT_EQ(recovered.submission.task.type, "prime");
  EXPECT_EQ(recovered.submission.task.payload, "101");
  EXPECT_EQ(recovered.state, JobState::Completed);
  EXPECT_EQ(recovered.work_units_consumed, 100U);
  EXPECT_EQ(recovered.message, "finished\ncleanly");
  EXPECT_FALSE(report.ignored_incomplete_tail);
}

TEST(WalManagerTest, IgnoresOnlyAnIncompleteFinalRecord) {
  test::TemporaryDirectory temporary{"wal-tail"};
  const auto path = temporary.path() / "jobs.wal";
  {
    WalManager wal(path);
    wal.append_submission({1, 0, "prime", 0, 10, {"prime", "11"}});
  }
  {
    std::ofstream output(path, std::ios::app | std::ios::binary);
    output << "K1|T|incomplete";
  }

  WalManager wal(path);
  const auto report = wal.recover();
  wal.append_transition({1, JobState::Completed, 10, "repaired"});
  const auto repaired_report = wal.recover();

  ASSERT_EQ(report.jobs.size(), 1U);
  EXPECT_EQ(report.jobs.front().state, JobState::Ready);
  EXPECT_TRUE(report.ignored_incomplete_tail);
  ASSERT_EQ(repaired_report.jobs.size(), 1U);
  EXPECT_EQ(repaired_report.jobs.front().state, JobState::Completed);
  EXPECT_FALSE(repaired_report.ignored_incomplete_tail);
}

TEST(WalManagerTest, RejectsCorruptionInsideTheLog) {
  test::TemporaryDirectory temporary{"wal-corrupt"};
  const auto path = temporary.path() / "jobs.wal";
  {
    WalManager wal(path);
    wal.append_submission({1, 0, "prime", 0, 10, {"prime", "11"}});
  }
  {
    std::ofstream output(path, std::ios::app | std::ios::binary);
    output << "corrupt-record\n";
  }

  WalManager wal(path);
  EXPECT_THROW((void)wal.recover(), std::runtime_error);
}

TEST(WalManagerTest, SerializesConcurrentWriters) {
  test::TemporaryDirectory temporary{"wal-concurrent"};
  WalManager wal(temporary.path() / "jobs.wal");
  std::vector<std::jthread> writers;
  for (std::uint64_t thread = 0; thread < 4; ++thread) {
    writers.emplace_back([&, thread] {
      for (std::uint64_t index = 0; index < 10; ++index) {
        const auto id = thread * 10 + index + 1;
        wal.append_submission(
            {id, id, "job", 0, 1, {"prime", "2"}});
        wal.append_transition({id, JobState::Completed, 1, "done"});
      }
    });
  }
  for (auto& writer : writers) {
    writer.join();
  }

  const auto report = wal.recover();
  ASSERT_EQ(report.jobs.size(), 40U);
  for (const auto& job : report.jobs) {
    EXPECT_EQ(job.state, JobState::Completed);
  }
}

TEST(WalIntegrationTest, EnginePersistsTerminalResult) {
  test::TemporaryDirectory temporary{"wal-engine"};
  auto wal = std::make_shared<WalManager>(temporary.path() / "jobs.wal");
  TaskEngine engine({1, {SchedulerKind::RoundRobin, 2}, wal});
  auto created = TaskFactory::create_for_submission("step", {"5", "1"}, 4);
  auto handle = engine.submit(std::move(created.task), created.options);

  EXPECT_EQ(handle.result().get().state, JobState::Completed);
  engine.shutdown();
  const auto report = wal->recover();

  ASSERT_EQ(report.jobs.size(), 1U);
  EXPECT_EQ(report.jobs.front().state, JobState::Completed);
  EXPECT_EQ(report.jobs.front().submission.priority, 4);
  EXPECT_EQ(report.jobs.front().work_units_consumed, 5U);
}

TEST(WalIntegrationTest, RestoresOriginalIdAndAdvancesNewIds) {
  test::TemporaryDirectory temporary{"wal-restore"};
  auto wal = std::make_shared<WalManager>(temporary.path() / "jobs.wal");
  JobSubmissionRecord recovered_submission{
      50, 80, "step-3", 2, 3, {"step", "3,1"}};
  wal->append_submission(recovered_submission);
  wal->append_transition({50, JobState::Running, 1, {}});

  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, wal});
  auto restored = engine.restore(TaskFactory::restore(recovered_submission.task),
                                 recovered_submission);
  EXPECT_EQ(restored.id(), 50U);
  EXPECT_EQ(restored.result().get().state, JobState::Completed);

  auto next_task = TaskFactory::create_for_submission("prime", {"10"}, 0);
  auto next = engine.submit(std::move(next_task.task), next_task.options);
  EXPECT_EQ(next.id(), 51U);
  EXPECT_EQ(next.result().get().state, JobState::Completed);
  engine.shutdown();
}

TEST(WalIntegrationTest, JournaledEngineRequiresDurableTaskSpec) {
  test::TemporaryDirectory temporary{"wal-required"};
  auto wal = std::make_shared<WalManager>(temporary.path() / "jobs.wal");
  TaskEngine engine({1, {SchedulerKind::Fifo, 100}, wal});

  EXPECT_THROW((void)engine.submit(std::make_unique<PrimeCountTask>(10),
                                   {.name = "missing-spec",
                                    .priority = 0,
                                    .estimated_work_units = 9,
                                    .durable_spec = std::nullopt}),
               std::invalid_argument);
  engine.shutdown();
}

}  // namespace
}  // namespace kronos
