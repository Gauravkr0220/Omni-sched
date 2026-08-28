#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "kronos/types.hpp"

namespace kronos {

struct JobSubmissionRecord {
  JobId id{0};
  std::uint64_t submission_sequence{0};
  std::string name;
  int priority{0};
  WorkUnits estimated_work_units{1};
  DurableTaskSpec task;
};

struct JobTransitionRecord {
  JobId id{0};
  JobState state{JobState::Ready};
  WorkUnits work_units_consumed{0};
  std::string message;
};

// The engine calls the journal before publishing the corresponding in-memory
// state change. Implementations may throw to prevent a non-durable transition.
class IJobJournal {
 public:
  virtual ~IJobJournal() = default;

  virtual void append_submission(const JobSubmissionRecord& record) = 0;
  virtual void append_transition(const JobTransitionRecord& record) = 0;
};

}  // namespace kronos

