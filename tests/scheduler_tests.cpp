#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

#include "kronos/scheduler.hpp"

namespace kronos {
namespace {

SchedulerEntry entry(JobId id, int priority, WorkUnits estimate,
                     std::uint64_t sequence) {
  return {id, priority, estimate, sequence};
}

std::vector<JobId> pop_all(IScheduler& scheduler) {
  std::vector<JobId> ids;
  while (auto next = scheduler.pop()) {
    ids.push_back(next->id);
  }
  return ids;
}

TEST(FifoSchedulerTest, PreservesSubmissionOrder) {
  FifoScheduler scheduler;
  scheduler.push(entry(3, 0, 1, 2));
  scheduler.push(entry(1, 0, 1, 0));
  scheduler.push(entry(2, 0, 1, 1));

  EXPECT_EQ(pop_all(scheduler), (std::vector<JobId>{3, 1, 2}));
  EXPECT_TRUE(scheduler.empty());
  EXPECT_EQ(scheduler.slice_budget(), unlimited_work);
}

TEST(PrioritySchedulerTest, HigherPriorityWinsAndTiesAreStable) {
  PriorityScheduler scheduler;
  scheduler.push(entry(1, 2, 100, 0));
  scheduler.push(entry(2, 9, 100, 1));
  scheduler.push(entry(3, 9, 100, 2));
  scheduler.push(entry(4, -1, 100, 3));

  EXPECT_EQ(pop_all(scheduler), (std::vector<JobId>{2, 3, 1, 4}));
}

TEST(SjfSchedulerTest, ShorterEstimateWinsAndTiesAreStable) {
  SjfScheduler scheduler;
  scheduler.push(entry(1, 0, 80, 0));
  scheduler.push(entry(2, 0, 10, 1));
  scheduler.push(entry(3, 0, 10, 2));
  scheduler.push(entry(4, 0, 40, 3));

  EXPECT_EQ(pop_all(scheduler), (std::vector<JobId>{2, 3, 4, 1}));
}

TEST(RoundRobinSchedulerTest, RotatesRequeuedEntriesAtConfiguredQuantum) {
  RoundRobinScheduler scheduler(25);
  scheduler.push(entry(1, 0, 10, 0));
  scheduler.push(entry(2, 0, 10, 1));
  scheduler.push(entry(3, 0, 10, 2));

  auto first = scheduler.pop();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->id, 1U);
  scheduler.push(*first);

  EXPECT_EQ(pop_all(scheduler), (std::vector<JobId>{2, 3, 1}));
  EXPECT_EQ(scheduler.slice_budget(), 25U);
}

TEST(RoundRobinSchedulerTest, RejectsZeroQuantum) {
  EXPECT_THROW((void)RoundRobinScheduler(0), std::invalid_argument);
}

TEST(SchedulerFactoryTest, BuildsEveryStrategy) {
  EXPECT_EQ(make_scheduler({SchedulerKind::Fifo, 1})->kind(),
            SchedulerKind::Fifo);
  EXPECT_EQ(make_scheduler({SchedulerKind::Priority, 1})->kind(),
            SchedulerKind::Priority);
  EXPECT_EQ(make_scheduler({SchedulerKind::ShortestJobFirst, 1})->kind(),
            SchedulerKind::ShortestJobFirst);
  EXPECT_EQ(make_scheduler({SchedulerKind::RoundRobin, 7})->slice_budget(),
            7U);
}

}  // namespace
}  // namespace kronos

