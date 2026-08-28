#pragma once

#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <optional>
#include <vector>

#include "kronos/journal.hpp"
#include "kronos/scheduler.hpp"
#include "kronos/task.hpp"

namespace kronos {

struct EngineConfig {
  std::size_t worker_count{0};
  SchedulerConfig scheduler{};
  std::shared_ptr<IJobJournal> journal;
};

class JobHandle {
 public:
  JobHandle() = default;
  JobHandle(JobId id, std::shared_future<JobResult> result);

  [[nodiscard]] JobId id() const noexcept;
  [[nodiscard]] bool valid() const noexcept;
  [[nodiscard]] const std::shared_future<JobResult>& result() const noexcept;

 private:
  JobId id_{0};
  std::shared_future<JobResult> result_;
};

// Owns submitted tasks, a fixed worker pool, the scheduler, and job state.
// Destruction performs a cancel-pending shutdown and joins all workers.
class TaskEngine {
 public:
  explicit TaskEngine(EngineConfig config = {});
  ~TaskEngine();

  TaskEngine(const TaskEngine&) = delete;
  TaskEngine& operator=(const TaskEngine&) = delete;
  TaskEngine(TaskEngine&&) = delete;
  TaskEngine& operator=(TaskEngine&&) = delete;

  [[nodiscard]] JobHandle submit(std::unique_ptr<ITask> task,
                                 SubmissionOptions options = {});
  // Restores a previously journaled non-terminal job with its original ID.
  [[nodiscard]] JobHandle restore(std::unique_ptr<ITask> task,
                                  const JobSubmissionRecord& submission);
  // Ensures future generated IDs and sequences follow all recovered history,
  // including terminal jobs that are not restored into the active registry.
  void reserve_recovered_identity(const JobSubmissionRecord& submission);
  // Returns false when the ID is unknown or the job is already terminal.
  [[nodiscard]] bool cancel(JobId id);
  [[nodiscard]] std::optional<JobSnapshot> get_job(JobId id) const;
  [[nodiscard]] std::vector<JobSnapshot> snapshot() const;

  void set_scheduler(SchedulerConfig config);
  [[nodiscard]] SchedulerConfig scheduler_config() const;
  [[nodiscard]] std::size_t worker_count() const noexcept;

  // Drain completes all accepted work. CancelPending cancels queued work but
  // lets slices that were already running reach their natural outcome.
  void shutdown(ShutdownMode mode = ShutdownMode::Drain);
  [[nodiscard]] bool is_accepting() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace kronos
