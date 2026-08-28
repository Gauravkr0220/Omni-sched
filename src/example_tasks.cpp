#include "kronos/example_tasks.hpp"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <sstream>

namespace kronos {
namespace {

bool is_prime(std::uint64_t value) {
  if (value < 2) {
    return false;
  }
  for (std::uint64_t divisor = 2; divisor <= value / divisor; ++divisor) {
    if (value % divisor == 0) {
      return false;
    }
  }
  return true;
}

WorkUnits effective_budget(WorkUnits requested, WorkUnits remaining) {
  if (requested == unlimited_work) {
    return remaining;
  }
  return std::min(requested, remaining);
}

}  // namespace

PrimeCountTask::PrimeCountTask(std::uint64_t upper_bound)
    : upper_bound_(upper_bound) {}

SliceResult PrimeCountTask::run_slice(const TaskContext& context) {
  if (context.cancellation.stop_requested()) {
    return SliceResult::cancelled("prime-count task cancelled");
  }
  if (candidate_ > upper_bound_) {
    return SliceResult::completed("primes found: " +
                                  std::to_string(prime_count_));
  }

  const auto remaining = upper_bound_ - candidate_ + 1;
  const auto budget = effective_budget(context.max_work_units, remaining);
  WorkUnits consumed = 0;

  while (consumed < budget && candidate_ <= upper_bound_) {
    if (context.cancellation.stop_requested()) {
      return SliceResult::cancelled("prime-count task cancelled", consumed);
    }
    if (is_prime(candidate_)) {
      ++prime_count_;
    }
    ++candidate_;
    ++consumed;
  }

  if (candidate_ > upper_bound_) {
    return SliceResult::completed("primes found: " +
                                      std::to_string(prime_count_),
                                  consumed);
  }
  return SliceResult::yielded(consumed);
}

StepTask::StepTask(WorkUnits steps,
                   std::chrono::milliseconds delay_per_step)
    : total_steps_(steps), delay_per_step_(delay_per_step) {}

SliceResult StepTask::run_slice(const TaskContext& context) {
  if (context.cancellation.stop_requested()) {
    return SliceResult::cancelled("step task cancelled");
  }
  if (completed_steps_ >= total_steps_) {
    return SliceResult::completed("steps completed: " +
                                  std::to_string(completed_steps_));
  }

  const auto remaining = total_steps_ - completed_steps_;
  const auto budget = effective_budget(context.max_work_units, remaining);
  WorkUnits consumed = 0;

  std::condition_variable_any cancellation_wait;
  std::mutex wait_mutex;
  while (consumed < budget && completed_steps_ < total_steps_) {
    if (delay_per_step_.count() > 0) {
      std::unique_lock lock(wait_mutex);
      cancellation_wait.wait_for(lock, context.cancellation, delay_per_step_,
                                 [] { return false; });
    }
    if (context.cancellation.stop_requested()) {
      return SliceResult::cancelled("step task cancelled", consumed);
    }
    ++completed_steps_;
    ++consumed;
  }

  if (completed_steps_ == total_steps_) {
    return SliceResult::completed("steps completed: " +
                                      std::to_string(completed_steps_),
                                  consumed);
  }
  return SliceResult::yielded(consumed);
}

}  // namespace kronos

