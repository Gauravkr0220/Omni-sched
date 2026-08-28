#include "kronos/types.hpp"

#include <utility>

namespace kronos {

SliceResult SliceResult::yielded(WorkUnits consumed) {
  return {SliceStatus::Yielded, consumed, {}};
}

SliceResult SliceResult::completed(std::string output, WorkUnits consumed) {
  return {SliceStatus::Completed, consumed, std::move(output)};
}

SliceResult SliceResult::failed(std::string error, WorkUnits consumed) {
  return {SliceStatus::Failed, consumed, std::move(error)};
}

SliceResult SliceResult::cancelled(std::string reason, WorkUnits consumed) {
  return {SliceStatus::Cancelled, consumed, std::move(reason)};
}

std::string_view to_string(JobState state) noexcept {
  switch (state) {
    case JobState::New:
      return "NEW";
    case JobState::Ready:
      return "READY";
    case JobState::Running:
      return "RUNNING";
    case JobState::Completed:
      return "COMPLETED";
    case JobState::Failed:
      return "FAILED";
    case JobState::Cancelled:
      return "CANCELLED";
  }
  return "UNKNOWN";
}

std::string_view to_string(SchedulerKind kind) noexcept {
  switch (kind) {
    case SchedulerKind::Fifo:
      return "fifo";
    case SchedulerKind::Priority:
      return "priority";
    case SchedulerKind::ShortestJobFirst:
      return "sjf";
    case SchedulerKind::RoundRobin:
      return "round-robin";
  }
  return "unknown";
}

}  // namespace kronos

