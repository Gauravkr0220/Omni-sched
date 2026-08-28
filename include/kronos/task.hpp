#pragma once

#include <stop_token>

#include "kronos/types.hpp"

namespace kronos {

struct TaskContext {
  // Tasks should poll this token at a frequency appropriate to their latency
  // requirements. Kronos never interrupts task code asynchronously.
  std::stop_token cancellation;
  // A task-defined work budget. Round Robin supplies its configured quantum.
  WorkUnits max_work_units{unlimited_work};
};

// Stateful, cooperatively resumable unit of work. Implementations must return
// Yielded when more work remains after consuming the supplied slice budget.
class ITask {
 public:
  virtual ~ITask() = default;

  ITask(const ITask&) = delete;
  ITask& operator=(const ITask&) = delete;
  ITask(ITask&&) = delete;
  ITask& operator=(ITask&&) = delete;

  [[nodiscard]] virtual SliceResult run_slice(const TaskContext& context) = 0;

 protected:
  ITask() = default;
};

}  // namespace kronos
