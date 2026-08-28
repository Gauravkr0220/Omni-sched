#include <gtest/gtest.h>

#include <stop_token>

#include "kronos/example_tasks.hpp"

namespace kronos {
namespace {

TEST(PrimeCountTaskTest, CompletesAcrossMultipleSlices) {
  PrimeCountTask task(20);
  std::stop_source cancellation;

  EXPECT_EQ(task.run_slice({cancellation.get_token(), 5}).status,
            SliceStatus::Yielded);
  EXPECT_EQ(task.run_slice({cancellation.get_token(), 5}).status,
            SliceStatus::Yielded);
  EXPECT_EQ(task.run_slice({cancellation.get_token(), 5}).status,
            SliceStatus::Yielded);
  const auto final = task.run_slice({cancellation.get_token(), 5});

  EXPECT_EQ(final.status, SliceStatus::Completed);
  EXPECT_EQ(final.message, "primes found: 8");
}

TEST(PrimeCountTaskTest, HonorsCancellationBeforeWork) {
  PrimeCountTask task(100);
  std::stop_source cancellation;
  cancellation.request_stop();

  const auto result =
      task.run_slice({cancellation.get_token(), unlimited_work});

  EXPECT_EQ(result.status, SliceStatus::Cancelled);
  EXPECT_EQ(result.work_units_consumed, 0U);
}

TEST(StepTaskTest, ReportsCompletedWork) {
  StepTask task(3, std::chrono::milliseconds{0});
  std::stop_source cancellation;

  EXPECT_EQ(task.run_slice({cancellation.get_token(), 2}).status,
            SliceStatus::Yielded);
  const auto result = task.run_slice({cancellation.get_token(), 2});

  EXPECT_EQ(result.status, SliceStatus::Completed);
  EXPECT_EQ(result.work_units_consumed, 1U);
  EXPECT_EQ(result.message, "steps completed: 3");
}

}  // namespace
}  // namespace kronos
