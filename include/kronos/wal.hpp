#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "kronos/journal.hpp"

namespace kronos {

struct RecoveredJob {
  JobSubmissionRecord submission;
  JobState state{JobState::Ready};
  WorkUnits work_units_consumed{0};
  std::string message;
};

struct RecoveryReport {
  std::vector<RecoveredJob> jobs;
  bool ignored_incomplete_tail{false};
};

// Append-only, checksummed POSIX write-ahead log. Every append is followed by
// fsync so a successful method return means the transition reached stable
// storage according to the host operating system.
class WalManager final : public IJobJournal {
 public:
  explicit WalManager(std::filesystem::path path);
  ~WalManager() override;

  WalManager(const WalManager&) = delete;
  WalManager& operator=(const WalManager&) = delete;
  WalManager(WalManager&&) = delete;
  WalManager& operator=(WalManager&&) = delete;

  void append_submission(const JobSubmissionRecord& record) override;
  void append_transition(const JobTransitionRecord& record) override;

  [[nodiscard]] RecoveryReport recover() const;
  [[nodiscard]] const std::filesystem::path& path() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace kronos
