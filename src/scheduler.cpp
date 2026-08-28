#include "kronos/scheduler.hpp"

#include <deque>
#include <queue>
#include <stdexcept>
#include <utility>

namespace kronos {
namespace {

template <typename Container>
std::vector<SchedulerEntry> drain_container(Container& container) {
  std::vector<SchedulerEntry> entries;
  entries.reserve(container.size());
  while (!container.empty()) {
    entries.push_back(container.front());
    container.pop_front();
  }
  return entries;
}

template <typename Queue>
std::optional<SchedulerEntry> pop_priority_queue(Queue& queue) {
  if (queue.empty()) {
    return std::nullopt;
  }
  auto entry = queue.top();
  queue.pop();
  return entry;
}

template <typename Queue>
std::vector<SchedulerEntry> drain_priority_queue(Queue& queue) {
  std::vector<SchedulerEntry> entries;
  entries.reserve(queue.size());
  while (!queue.empty()) {
    entries.push_back(queue.top());
    queue.pop();
  }
  return entries;
}

struct PriorityCompare {
  bool operator()(const SchedulerEntry& lhs,
                  const SchedulerEntry& rhs) const noexcept {
    if (lhs.priority != rhs.priority) {
      return lhs.priority < rhs.priority;
    }
    return lhs.submission_sequence > rhs.submission_sequence;
  }
};

struct SjfCompare {
  bool operator()(const SchedulerEntry& lhs,
                  const SchedulerEntry& rhs) const noexcept {
    if (lhs.estimated_work_units != rhs.estimated_work_units) {
      return lhs.estimated_work_units > rhs.estimated_work_units;
    }
    return lhs.submission_sequence > rhs.submission_sequence;
  }
};

}  // namespace

struct FifoScheduler::Impl {
  std::deque<SchedulerEntry> entries;
};

FifoScheduler::FifoScheduler() : impl_(std::make_unique<Impl>()) {}
FifoScheduler::~FifoScheduler() = default;
FifoScheduler::FifoScheduler(FifoScheduler&&) noexcept = default;
FifoScheduler& FifoScheduler::operator=(FifoScheduler&&) noexcept = default;

void FifoScheduler::push(SchedulerEntry entry) {
  impl_->entries.push_back(entry);
}

std::optional<SchedulerEntry> FifoScheduler::pop() {
  if (impl_->entries.empty()) {
    return std::nullopt;
  }
  auto entry = impl_->entries.front();
  impl_->entries.pop_front();
  return entry;
}

std::vector<SchedulerEntry> FifoScheduler::drain() {
  return drain_container(impl_->entries);
}

std::size_t FifoScheduler::size() const noexcept {
  return impl_->entries.size();
}

bool FifoScheduler::empty() const noexcept { return impl_->entries.empty(); }

WorkUnits FifoScheduler::slice_budget() const noexcept {
  return unlimited_work;
}

SchedulerKind FifoScheduler::kind() const noexcept {
  return SchedulerKind::Fifo;
}

struct PriorityScheduler::Impl {
  std::priority_queue<SchedulerEntry, std::vector<SchedulerEntry>,
                      PriorityCompare>
      entries;
};

PriorityScheduler::PriorityScheduler() : impl_(std::make_unique<Impl>()) {}
PriorityScheduler::~PriorityScheduler() = default;
PriorityScheduler::PriorityScheduler(PriorityScheduler&&) noexcept = default;
PriorityScheduler& PriorityScheduler::operator=(PriorityScheduler&&) noexcept =
    default;

void PriorityScheduler::push(SchedulerEntry entry) {
  impl_->entries.push(entry);
}

std::optional<SchedulerEntry> PriorityScheduler::pop() {
  return pop_priority_queue(impl_->entries);
}

std::vector<SchedulerEntry> PriorityScheduler::drain() {
  return drain_priority_queue(impl_->entries);
}

std::size_t PriorityScheduler::size() const noexcept {
  return impl_->entries.size();
}

bool PriorityScheduler::empty() const noexcept {
  return impl_->entries.empty();
}

WorkUnits PriorityScheduler::slice_budget() const noexcept {
  return unlimited_work;
}

SchedulerKind PriorityScheduler::kind() const noexcept {
  return SchedulerKind::Priority;
}

struct SjfScheduler::Impl {
  std::priority_queue<SchedulerEntry, std::vector<SchedulerEntry>, SjfCompare>
      entries;
};

SjfScheduler::SjfScheduler() : impl_(std::make_unique<Impl>()) {}
SjfScheduler::~SjfScheduler() = default;
SjfScheduler::SjfScheduler(SjfScheduler&&) noexcept = default;
SjfScheduler& SjfScheduler::operator=(SjfScheduler&&) noexcept = default;

void SjfScheduler::push(SchedulerEntry entry) { impl_->entries.push(entry); }

std::optional<SchedulerEntry> SjfScheduler::pop() {
  return pop_priority_queue(impl_->entries);
}

std::vector<SchedulerEntry> SjfScheduler::drain() {
  return drain_priority_queue(impl_->entries);
}

std::size_t SjfScheduler::size() const noexcept {
  return impl_->entries.size();
}

bool SjfScheduler::empty() const noexcept { return impl_->entries.empty(); }

WorkUnits SjfScheduler::slice_budget() const noexcept {
  return unlimited_work;
}

SchedulerKind SjfScheduler::kind() const noexcept {
  return SchedulerKind::ShortestJobFirst;
}

struct RoundRobinScheduler::Impl {
  explicit Impl(WorkUnits configured_quantum) : quantum(configured_quantum) {}

  std::deque<SchedulerEntry> entries;
  WorkUnits quantum;
};

RoundRobinScheduler::RoundRobinScheduler(WorkUnits quantum)
    : impl_(std::make_unique<Impl>(quantum)) {
  if (quantum == 0) {
    throw std::invalid_argument("round-robin quantum must be greater than zero");
  }
}

RoundRobinScheduler::~RoundRobinScheduler() = default;
RoundRobinScheduler::RoundRobinScheduler(RoundRobinScheduler&&) noexcept =
    default;
RoundRobinScheduler& RoundRobinScheduler::operator=(
    RoundRobinScheduler&&) noexcept = default;

void RoundRobinScheduler::push(SchedulerEntry entry) {
  impl_->entries.push_back(entry);
}

std::optional<SchedulerEntry> RoundRobinScheduler::pop() {
  if (impl_->entries.empty()) {
    return std::nullopt;
  }
  auto entry = impl_->entries.front();
  impl_->entries.pop_front();
  return entry;
}

std::vector<SchedulerEntry> RoundRobinScheduler::drain() {
  return drain_container(impl_->entries);
}

std::size_t RoundRobinScheduler::size() const noexcept {
  return impl_->entries.size();
}

bool RoundRobinScheduler::empty() const noexcept {
  return impl_->entries.empty();
}

WorkUnits RoundRobinScheduler::slice_budget() const noexcept {
  return impl_->quantum;
}

SchedulerKind RoundRobinScheduler::kind() const noexcept {
  return SchedulerKind::RoundRobin;
}

std::unique_ptr<IScheduler> make_scheduler(SchedulerConfig config) {
  switch (config.kind) {
    case SchedulerKind::Fifo:
      return std::make_unique<FifoScheduler>();
    case SchedulerKind::Priority:
      return std::make_unique<PriorityScheduler>();
    case SchedulerKind::ShortestJobFirst:
      return std::make_unique<SjfScheduler>();
    case SchedulerKind::RoundRobin:
      return std::make_unique<RoundRobinScheduler>(config.quantum);
  }
  throw std::invalid_argument("unknown scheduler kind");
}

}  // namespace kronos
