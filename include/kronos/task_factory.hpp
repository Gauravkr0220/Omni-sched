#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "kronos/task_engine.hpp"

namespace kronos {

struct CreatedTask {
  std::unique_ptr<ITask> task;
  SubmissionOptions options;
};

// Creates the built-in tasks used by the daemon and reconstructs them from WAL
// specifications. Supported types are "prime" and "step".
class TaskFactory {
 public:
  [[nodiscard]] static CreatedTask create_for_submission(
      std::string_view type, const std::vector<std::string>& arguments,
      int priority);
  [[nodiscard]] static std::unique_ptr<ITask> restore(
      const DurableTaskSpec& specification);
};

}  // namespace kronos

