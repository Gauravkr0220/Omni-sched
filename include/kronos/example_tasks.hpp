#pragma once

#include <chrono>
#include <cstdint>

#include "kronos/task.hpp"

namespace kronos {

class PrimeCountTask final : public ITask {
 public:
  explicit PrimeCountTask(std::uint64_t upper_bound);
  [[nodiscard]] SliceResult run_slice(const TaskContext& context) override;

 private:
  std::uint64_t upper_bound_;
  std::uint64_t candidate_{2};
  std::uint64_t prime_count_{0};
};

class StepTask final : public ITask {
 public:
  StepTask(WorkUnits steps, std::chrono::milliseconds delay_per_step);
  [[nodiscard]] SliceResult run_slice(const TaskContext& context) override;

 private:
  WorkUnits total_steps_;
  WorkUnits completed_steps_{0};
  std::chrono::milliseconds delay_per_step_;
};

}  // namespace kronos
