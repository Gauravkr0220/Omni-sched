#include "kronos/task_engine.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

namespace kronos {
namespace {

using Clock = std::chrono::steady_clock;

struct Dispatch {
  SchedulerEntry entry;
  WorkUnits budget{unlimited_work};
};

class TaskQueue {
 public:
  explicit TaskQueue(SchedulerConfig config)
      : scheduler_(make_scheduler(config)), config_(config) {}

  bool push_external(SchedulerEntry entry) {
    {
      std::lock_guard lock(mutex_);
      if (closed_) {
        return false;
      }
      scheduler_->push(entry);
    }
    available_.notify_one();
    return true;
  }

  void requeue(SchedulerEntry entry) {
    {
      std::lock_guard lock(mutex_);
      scheduler_->push(entry);
    }
    available_.notify_one();
  }

  std::optional<Dispatch> pop(std::stop_token stop_token) {
    std::unique_lock lock(mutex_);
    available_.wait(lock, stop_token,
                    [this] { return closed_ || !scheduler_->empty(); });

    if (scheduler_->empty()) {
      return std::nullopt;
    }

    auto entry = scheduler_->pop();
    if (!entry) {
      return std::nullopt;
    }
    return Dispatch{*entry, scheduler_->slice_budget()};
  }

  void set_scheduler(SchedulerConfig config) {
    auto replacement = make_scheduler(config);
    {
      std::lock_guard lock(mutex_);
      if (closed_) {
        throw std::logic_error("cannot change scheduler after shutdown");
      }
      auto entries = scheduler_->drain();
      for (auto& entry : entries) {
        replacement->push(entry);
      }
      scheduler_ = std::move(replacement);
      config_ = config;
    }
    available_.notify_all();
  }

  [[nodiscard]] SchedulerConfig config() const {
    std::lock_guard lock(mutex_);
    return config_;
  }

  std::vector<SchedulerEntry> close_and_drain_pending(bool drain_pending) {
    std::vector<SchedulerEntry> entries;
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
      if (drain_pending) {
        entries = scheduler_->drain();
      }
    }
    available_.notify_all();
    return entries;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable_any available_;
  std::unique_ptr<IScheduler> scheduler_;
  SchedulerConfig config_;
  bool closed_{false};
};

struct JobRecord {
  JobRecord(JobSnapshot initial_snapshot, SchedulerEntry scheduler_entry,
            std::unique_ptr<ITask> submitted_task)
      : snapshot(std::move(initial_snapshot)),
        entry(scheduler_entry),
        task(std::move(submitted_task)),
        future(promise.get_future().share()) {}

  JobSnapshot snapshot;
  SchedulerEntry entry;
  std::unique_ptr<ITask> task;
  std::stop_source cancellation;
  std::promise<JobResult> promise;
  std::shared_future<JobResult> future;
  bool result_published{false};
};

std::size_t resolved_worker_count(std::size_t requested) {
  if (requested != 0) {
    return requested;
  }
  const auto detected = std::thread::hardware_concurrency();
  return detected == 0 ? 1 : static_cast<std::size_t>(detected);
}

}  // namespace

JobHandle::JobHandle(JobId id, std::shared_future<JobResult> result)
    : id_(id), result_(std::move(result)) {}

JobId JobHandle::id() const noexcept { return id_; }

bool JobHandle::valid() const noexcept { return result_.valid(); }

const std::shared_future<JobResult>& JobHandle::result() const noexcept {
  return result_;
}

class TaskEngine::Impl {
 public:
  explicit Impl(EngineConfig config)
      : queue_(config.scheduler),
        worker_count_(resolved_worker_count(config.worker_count)),
        journal_(std::move(config.journal)) {
    workers_.reserve(worker_count_);
    for (std::size_t index = 0; index < worker_count_; ++index) {
      workers_.emplace_back([this](std::stop_token stop_token) {
        worker_loop(stop_token);
      });
    }
  }

  ~Impl() {
    try {
      shutdown(ShutdownMode::CancelPending);
    } catch (...) {
      // Destructors must not throw. Journal failures are already reflected in
      // the affected in-memory job results.
    }
  }

  JobHandle submit(std::unique_ptr<ITask> task, SubmissionOptions options) {
    if (!task) {
      throw std::invalid_argument("cannot submit a null task");
    }
    if (options.estimated_work_units == 0) {
      throw std::invalid_argument(
          "estimated work units must be greater than zero");
    }
    if (!accepting_.load(std::memory_order_acquire)) {
      throw std::logic_error("task engine is not accepting submissions");
    }

    const auto id = next_job_id_.fetch_add(1, std::memory_order_relaxed);
    const auto sequence =
        next_sequence_.fetch_add(1, std::memory_order_relaxed);
    const auto now = Clock::now();

    JobSnapshot initial;
    initial.id = id;
    initial.name = options.name.empty() ? "task" : std::move(options.name);
    initial.state = JobState::Ready;
    initial.priority = options.priority;
    initial.estimated_work_units = options.estimated_work_units;
    initial.submission_sequence = sequence;
    initial.submitted_at = now;

    SchedulerEntry entry{id, options.priority, options.estimated_work_units,
                         sequence};
    if (journal_) {
      if (!options.durable_spec || options.durable_spec->type.empty()) {
        throw std::invalid_argument(
            "journaled submissions require a durable task specification");
      }
      journal_->append_submission(JobSubmissionRecord{
          id, sequence, initial.name, options.priority,
          options.estimated_work_units, *options.durable_spec});
    }
    auto record = std::make_shared<JobRecord>(initial, entry, std::move(task));
    const auto handle = JobHandle{id, record->future};

    {
      std::unique_lock lock(registry_mutex_);
      records_.emplace(id, record);
    }

    if (!queue_.push_external(entry)) {
      std::unique_lock lock(registry_mutex_);
      finish_locked(*record, JobState::Cancelled,
                    "submission raced with engine shutdown");
    }
    return handle;
  }

  JobHandle restore(std::unique_ptr<ITask> task,
                    const JobSubmissionRecord& submission) {
    if (!task) {
      throw std::invalid_argument("cannot restore a null task");
    }
    if (submission.id == 0 || submission.estimated_work_units == 0 ||
        submission.task.type.empty()) {
      throw std::invalid_argument("invalid recovered submission");
    }
    if (!accepting_.load(std::memory_order_acquire)) {
      throw std::logic_error("task engine is not accepting recovered jobs");
    }

    JobSnapshot initial;
    initial.id = submission.id;
    initial.name = submission.name;
    initial.state = JobState::Ready;
    initial.priority = submission.priority;
    initial.estimated_work_units = submission.estimated_work_units;
    initial.submission_sequence = submission.submission_sequence;
    initial.submitted_at = Clock::now();
    initial.message = "recovered after restart";

    SchedulerEntry entry{submission.id, submission.priority,
                         submission.estimated_work_units,
                         submission.submission_sequence};
    auto record = std::make_shared<JobRecord>(initial, entry, std::move(task));
    const auto handle = JobHandle{submission.id, record->future};

    if (journal_) {
      journal_->append_transition(JobTransitionRecord{
          submission.id, JobState::Ready, 0, "recovered after restart"});
    }
    {
      std::unique_lock lock(registry_mutex_);
      if (!records_.emplace(submission.id, record).second) {
        throw std::invalid_argument("recovered job ID already exists");
      }
    }
    advance_counter(next_job_id_, submission.id + 1);
    advance_counter(next_sequence_, submission.submission_sequence + 1);

    if (!queue_.push_external(entry)) {
      std::unique_lock lock(registry_mutex_);
      finish_locked(*record, JobState::Cancelled,
                    "recovery raced with engine shutdown");
    }
    return handle;
  }

  void reserve_recovered_identity(const JobSubmissionRecord& submission) {
    if (submission.id == 0) {
      throw std::invalid_argument("recovered job ID cannot be zero");
    }
    advance_counter(next_job_id_, submission.id + 1);
    advance_counter(next_sequence_, submission.submission_sequence + 1);
  }

  bool cancel(JobId id) {
    std::shared_ptr<JobRecord> record;
    {
      std::shared_lock lock(registry_mutex_);
      const auto found = records_.find(id);
      if (found == records_.end()) {
        return false;
      }
      record = found->second;
    }

    std::unique_lock lock(registry_mutex_);
    if (is_terminal(record->snapshot.state)) {
      return false;
    }

    record->cancellation.request_stop();
    if (record->snapshot.state == JobState::Ready ||
        record->snapshot.state == JobState::New) {
      finish_locked(*record, JobState::Cancelled, "cancelled before execution");
    }
    return true;
  }

  std::optional<JobSnapshot> get_job(JobId id) const {
    std::shared_lock lock(registry_mutex_);
    const auto found = records_.find(id);
    if (found == records_.end()) {
      return std::nullopt;
    }
    return found->second->snapshot;
  }

  std::vector<JobSnapshot> snapshot() const {
    std::vector<JobSnapshot> snapshots;
    {
      std::shared_lock lock(registry_mutex_);
      snapshots.reserve(records_.size());
      for (const auto& [id, record] : records_) {
        (void)id;
        snapshots.push_back(record->snapshot);
      }
    }
    std::sort(snapshots.begin(), snapshots.end(),
              [](const JobSnapshot& lhs, const JobSnapshot& rhs) {
                return lhs.submission_sequence < rhs.submission_sequence;
              });
    return snapshots;
  }

  void set_scheduler(SchedulerConfig config) {
    if (!accepting_.load(std::memory_order_acquire)) {
      throw std::logic_error("cannot change scheduler after shutdown");
    }
    queue_.set_scheduler(config);
  }

  SchedulerConfig scheduler_config() const { return queue_.config(); }

  std::size_t worker_count() const noexcept { return worker_count_; }

  void shutdown(ShutdownMode mode) {
    std::lock_guard shutdown_lock(shutdown_mutex_);
    if (workers_.empty()) {
      return;
    }

    accepting_.store(false, std::memory_order_release);

    const bool cancel_pending = mode == ShutdownMode::CancelPending;
    auto pending = queue_.close_and_drain_pending(cancel_pending);
    if (cancel_pending) {
      std::unique_lock lock(registry_mutex_);
      for (const auto& entry : pending) {
        const auto found = records_.find(entry.id);
        if (found == records_.end()) {
          continue;
        }
        auto& record = *found->second;
        if (!is_terminal(record.snapshot.state)) {
          record.cancellation.request_stop();
          finish_locked(record, JobState::Cancelled,
                        "cancelled during engine shutdown");
        }
      }
    }
    join_workers();
  }

  bool is_accepting() const noexcept {
    return accepting_.load(std::memory_order_acquire);
  }

 private:
  void join_workers() {
    for (auto& worker : workers_) {
      if (worker.joinable() && worker.get_id() != std::this_thread::get_id()) {
        worker.join();
      }
    }
    workers_.clear();
  }

  std::shared_ptr<JobRecord> find_record(JobId id) const {
    std::shared_lock lock(registry_mutex_);
    const auto found = records_.find(id);
    return found == records_.end() ? nullptr : found->second;
  }

  void worker_loop(std::stop_token worker_stop) {
    while (true) {
      auto dispatch = queue_.pop(worker_stop);
      if (!dispatch) {
        return;
      }

      auto record = find_record(dispatch->entry.id);
      if (!record) {
        continue;
      }

      {
        std::unique_lock lock(registry_mutex_);
        if (record->snapshot.state != JobState::Ready) {
          continue;
        }
        if (!persist_transition_locked(*record, JobState::Running, {})) {
          continue;
        }
        record->snapshot.state = JobState::Running;
        if (record->snapshot.started_at == Clock::time_point{}) {
          record->snapshot.started_at = Clock::now();
        }
      }

      SliceResult result;
      try {
        result = record->task->run_slice(
            TaskContext{record->cancellation.get_token(), dispatch->budget});
      } catch (const std::exception& error) {
        result = SliceResult::failed(error.what());
      } catch (...) {
        result = SliceResult::failed("task threw an unknown exception");
      }

      bool should_requeue = false;
      {
        std::unique_lock lock(registry_mutex_);
        record->snapshot.work_units_consumed += result.work_units_consumed;

        switch (result.status) {
          case SliceStatus::Yielded:
            if (record->cancellation.stop_requested()) {
              finish_locked(*record, JobState::Cancelled,
                            result.message.empty() ? "cancellation requested"
                                                   : std::move(result.message));
            } else {
              if (!persist_transition_locked(*record, JobState::Ready,
                                             result.message)) {
                break;
              }
              record->snapshot.state = JobState::Ready;
              record->snapshot.message = std::move(result.message);
              should_requeue = true;
            }
            break;
          case SliceStatus::Completed:
            finish_locked(*record, JobState::Completed,
                          std::move(result.message));
            break;
          case SliceStatus::Failed:
            finish_locked(*record, JobState::Failed, std::move(result.message));
            break;
          case SliceStatus::Cancelled:
            finish_locked(*record, JobState::Cancelled,
                          std::move(result.message));
            break;
        }
      }

      if (should_requeue) {
        queue_.requeue(record->entry);
      }
    }
  }

  bool persist_transition_locked(JobRecord& record, JobState state,
                                 const std::string& message) {
    if (!journal_) {
      return true;
    }
    try {
      journal_->append_transition(JobTransitionRecord{
          record.snapshot.id, state, record.snapshot.work_units_consumed,
          message});
      return true;
    } catch (const std::exception& error) {
      fail_without_journal_locked(
          record, "write-ahead log failure: " + std::string{error.what()});
      return false;
    }
  }

  void finish_locked(JobRecord& record, JobState state, std::string message) {
    if (record.result_published) {
      return;
    }
    if (!persist_transition_locked(record, state, message)) {
      return;
    }
    record.snapshot.state = state;
    record.snapshot.message = std::move(message);
    record.snapshot.finished_at = Clock::now();
    record.result_published = true;
    record.promise.set_value(JobResult{record.snapshot.id, state,
                                       record.snapshot.message,
                                       record.snapshot.work_units_consumed});
  }

  static void fail_without_journal_locked(JobRecord& record,
                                          std::string message) {
    if (record.result_published) {
      return;
    }
    record.snapshot.state = JobState::Failed;
    record.snapshot.message = std::move(message);
    record.snapshot.finished_at = Clock::now();
    record.result_published = true;
    record.promise.set_value(JobResult{
        record.snapshot.id, JobState::Failed, record.snapshot.message,
        record.snapshot.work_units_consumed});
  }

  template <typename Integer>
  static void advance_counter(std::atomic<Integer>& counter, Integer desired) {
    auto current = counter.load(std::memory_order_relaxed);
    while (current < desired &&
           !counter.compare_exchange_weak(current, desired,
                                          std::memory_order_relaxed)) {
    }
  }

  TaskQueue queue_;
  const std::size_t worker_count_;
  std::vector<std::jthread> workers_;

  mutable std::shared_mutex registry_mutex_;
  std::unordered_map<JobId, std::shared_ptr<JobRecord>> records_;

  std::atomic<JobId> next_job_id_{1};
  std::atomic<std::uint64_t> next_sequence_{0};
  std::atomic<bool> accepting_{true};
  std::mutex shutdown_mutex_;
  std::shared_ptr<IJobJournal> journal_;
};

TaskEngine::TaskEngine(EngineConfig config)
    : impl_(std::make_unique<Impl>(config)) {}

TaskEngine::~TaskEngine() = default;

JobHandle TaskEngine::submit(std::unique_ptr<ITask> task,
                             SubmissionOptions options) {
  return impl_->submit(std::move(task), std::move(options));
}

JobHandle TaskEngine::restore(std::unique_ptr<ITask> task,
                              const JobSubmissionRecord& submission) {
  return impl_->restore(std::move(task), submission);
}

void TaskEngine::reserve_recovered_identity(
    const JobSubmissionRecord& submission) {
  impl_->reserve_recovered_identity(submission);
}

bool TaskEngine::cancel(JobId id) { return impl_->cancel(id); }

std::optional<JobSnapshot> TaskEngine::get_job(JobId id) const {
  return impl_->get_job(id);
}

std::vector<JobSnapshot> TaskEngine::snapshot() const {
  return impl_->snapshot();
}

void TaskEngine::set_scheduler(SchedulerConfig config) {
  impl_->set_scheduler(config);
}

SchedulerConfig TaskEngine::scheduler_config() const {
  return impl_->scheduler_config();
}

std::size_t TaskEngine::worker_count() const noexcept {
  return impl_->worker_count();
}

void TaskEngine::shutdown(ShutdownMode mode) { impl_->shutdown(mode); }

bool TaskEngine::is_accepting() const noexcept { return impl_->is_accepting(); }

}  // namespace kronos
