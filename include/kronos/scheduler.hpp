#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "kronos/types.hpp"

namespace kronos {

struct SchedulerEntry {
  JobId id{0};
  int priority{0};
  WorkUnits estimated_work_units{1};
  std::uint64_t submission_sequence{0};
};

// Non-thread-safe scheduling strategy. TaskEngine provides synchronization
// around implementations of this interface.
class IScheduler {
 public:
  virtual ~IScheduler() = default;

  virtual void push(SchedulerEntry entry) = 0;
  [[nodiscard]] virtual std::optional<SchedulerEntry> pop() = 0;
  [[nodiscard]] virtual std::vector<SchedulerEntry> drain() = 0;
  [[nodiscard]] virtual std::size_t size() const noexcept = 0;
  [[nodiscard]] virtual bool empty() const noexcept = 0;
  [[nodiscard]] virtual WorkUnits slice_budget() const noexcept = 0;
  [[nodiscard]] virtual SchedulerKind kind() const noexcept = 0;
};

class FifoScheduler final : public IScheduler {
 public:
  FifoScheduler();
  ~FifoScheduler() override;
  FifoScheduler(FifoScheduler&&) noexcept;
  FifoScheduler& operator=(FifoScheduler&&) noexcept;

  void push(SchedulerEntry entry) override;
  [[nodiscard]] std::optional<SchedulerEntry> pop() override;
  [[nodiscard]] std::vector<SchedulerEntry> drain() override;
  [[nodiscard]] std::size_t size() const noexcept override;
  [[nodiscard]] bool empty() const noexcept override;
  [[nodiscard]] WorkUnits slice_budget() const noexcept override;
  [[nodiscard]] SchedulerKind kind() const noexcept override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class PriorityScheduler final : public IScheduler {
 public:
  PriorityScheduler();
  ~PriorityScheduler() override;
  PriorityScheduler(PriorityScheduler&&) noexcept;
  PriorityScheduler& operator=(PriorityScheduler&&) noexcept;

  void push(SchedulerEntry entry) override;
  [[nodiscard]] std::optional<SchedulerEntry> pop() override;
  [[nodiscard]] std::vector<SchedulerEntry> drain() override;
  [[nodiscard]] std::size_t size() const noexcept override;
  [[nodiscard]] bool empty() const noexcept override;
  [[nodiscard]] WorkUnits slice_budget() const noexcept override;
  [[nodiscard]] SchedulerKind kind() const noexcept override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class SjfScheduler final : public IScheduler {
 public:
  SjfScheduler();
  ~SjfScheduler() override;
  SjfScheduler(SjfScheduler&&) noexcept;
  SjfScheduler& operator=(SjfScheduler&&) noexcept;

  void push(SchedulerEntry entry) override;
  [[nodiscard]] std::optional<SchedulerEntry> pop() override;
  [[nodiscard]] std::vector<SchedulerEntry> drain() override;
  [[nodiscard]] std::size_t size() const noexcept override;
  [[nodiscard]] bool empty() const noexcept override;
  [[nodiscard]] WorkUnits slice_budget() const noexcept override;
  [[nodiscard]] SchedulerKind kind() const noexcept override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class RoundRobinScheduler final : public IScheduler {
 public:
  // The quantum is measured in task-defined work units and must be non-zero.
  explicit RoundRobinScheduler(WorkUnits quantum);
  ~RoundRobinScheduler() override;
  RoundRobinScheduler(RoundRobinScheduler&&) noexcept;
  RoundRobinScheduler& operator=(RoundRobinScheduler&&) noexcept;

  void push(SchedulerEntry entry) override;
  [[nodiscard]] std::optional<SchedulerEntry> pop() override;
  [[nodiscard]] std::vector<SchedulerEntry> drain() override;
  [[nodiscard]] std::size_t size() const noexcept override;
  [[nodiscard]] bool empty() const noexcept override;
  [[nodiscard]] WorkUnits slice_budget() const noexcept override;
  [[nodiscard]] SchedulerKind kind() const noexcept override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::unique_ptr<IScheduler> make_scheduler(
    SchedulerConfig config);

}  // namespace kronos
