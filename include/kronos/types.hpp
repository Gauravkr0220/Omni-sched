#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

namespace kronos {

using JobId = std::uint64_t;
using WorkUnits = std::uint64_t;

inline constexpr WorkUnits unlimited_work =
    std::numeric_limits<WorkUnits>::max();

enum class JobState {
  New,
  Ready,
  Running,
  Completed,
  Failed,
  Cancelled,
};

enum class SliceStatus {
  Yielded,
  Completed,
  Failed,
  Cancelled,
};

enum class SchedulerKind {
  Fifo,
  Priority,
  ShortestJobFirst,
  RoundRobin,
};

enum class ShutdownMode {
  Drain,
  CancelPending,
};

struct SchedulerConfig {
  SchedulerKind kind{SchedulerKind::Fifo};
  WorkUnits quantum{100};
};

// Describes how a submitted task can be reconstructed after a process crash.
// Payload interpretation belongs to the task factory, not the execution core.
struct DurableTaskSpec {
  std::string type;
  std::string payload;
};

struct SubmissionOptions {
  std::string name{"task"};
  int priority{0};
  WorkUnits estimated_work_units{1};
  std::optional<DurableTaskSpec> durable_spec;
};

struct SliceResult {
  SliceStatus status{SliceStatus::Completed};
  WorkUnits work_units_consumed{0};
  std::string message;

  [[nodiscard]] static SliceResult yielded(WorkUnits consumed = 0);
  [[nodiscard]] static SliceResult completed(std::string output = {},
                                             WorkUnits consumed = 0);
  [[nodiscard]] static SliceResult failed(std::string error,
                                          WorkUnits consumed = 0);
  [[nodiscard]] static SliceResult cancelled(std::string reason = {},
                                             WorkUnits consumed = 0);
};

struct JobResult {
  JobId id{0};
  JobState state{JobState::Failed};
  std::string message;
  WorkUnits work_units_consumed{0};
};

struct JobSnapshot {
  JobId id{0};
  std::string name;
  JobState state{JobState::New};
  int priority{0};
  WorkUnits estimated_work_units{0};
  WorkUnits work_units_consumed{0};
  std::uint64_t submission_sequence{0};
  std::chrono::steady_clock::time_point submitted_at;
  std::chrono::steady_clock::time_point started_at;
  std::chrono::steady_clock::time_point finished_at;
  std::string message;
};

[[nodiscard]] constexpr bool is_terminal(JobState state) noexcept {
  return state == JobState::Completed || state == JobState::Failed ||
         state == JobState::Cancelled;
}

[[nodiscard]] std::string_view to_string(JobState state) noexcept;
[[nodiscard]] std::string_view to_string(SchedulerKind kind) noexcept;

}  // namespace kronos
