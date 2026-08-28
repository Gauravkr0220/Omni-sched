#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "kronos/task.hpp"
#include "kronos/task_engine.hpp"
#include "kronos/types.hpp"
#include "kronos/wal.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct BenchmarkConfig {
  std::size_t scheduler_jobs{30'000};
  std::size_t cpu_jobs{64};
  kronos::WorkUnits cpu_operations{1'000'000};
  std::size_t wal_jobs{500};
  std::size_t samples{5};
  std::size_t scheduler_workers{0};
};

class ImmediateTask final : public kronos::ITask {
 public:
  [[nodiscard]] kronos::SliceResult run_slice(
      const kronos::TaskContext& context) override {
    if (context.cancellation.stop_requested()) {
      return kronos::SliceResult::cancelled("benchmark task cancelled");
    }
    return kronos::SliceResult::completed({}, 1);
  }
};

class CpuTask final : public kronos::ITask {
 public:
  explicit CpuTask(kronos::WorkUnits operations) : operations_(operations) {}

  [[nodiscard]] kronos::SliceResult run_slice(
      const kronos::TaskContext& context) override {
    std::uint64_t value = 0x9E3779B97F4A7C15ULL ^ operations_;
    for (kronos::WorkUnits index = 0; index < operations_; ++index) {
      if ((index & 0xFFFU) == 0 && context.cancellation.stop_requested()) {
        return kronos::SliceResult::cancelled("CPU benchmark cancelled",
                                               index);
      }
      value ^= value << 13U;
      value ^= value >> 7U;
      value ^= value << 17U;
      value += index + 0xD1B54A32D192ED03ULL;
    }
    return kronos::SliceResult::completed(std::to_string(value), operations_);
  }

 private:
  kronos::WorkUnits operations_;
};

class SleepTask final : public kronos::ITask {
 public:
  explicit SleepTask(std::chrono::milliseconds duration)
      : duration_(duration) {}

  [[nodiscard]] kronos::SliceResult run_slice(
      const kronos::TaskContext& context) override {
    if (context.cancellation.stop_requested()) {
      return kronos::SliceResult::cancelled("sleep benchmark cancelled");
    }
    std::this_thread::sleep_for(duration_);
    return kronos::SliceResult::completed({}, 1);
  }

 private:
  std::chrono::milliseconds duration_;
};

class GateTask final : public kronos::ITask {
 public:
  GateTask(std::shared_future<void> release,
           std::shared_ptr<std::promise<void>> started)
      : release_(std::move(release)), started_(std::move(started)) {}

  [[nodiscard]] kronos::SliceResult run_slice(
      const kronos::TaskContext&) override {
    started_->set_value();
    release_.wait();
    return kronos::SliceResult::completed({}, 1);
  }

 private:
  std::shared_future<void> release_;
  std::shared_ptr<std::promise<void>> started_;
};

class TemporaryWal {
 public:
  TemporaryWal() {
    const auto stamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           Clock::now().time_since_epoch())
                           .count();
    path_ = std::filesystem::temp_directory_path() /
            ("kronos-benchmark-" + std::to_string(stamp) + ".wal");
  }

  ~TemporaryWal() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  TemporaryWal(const TemporaryWal&) = delete;
  TemporaryWal& operator=(const TemporaryWal&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

 private:
  std::filesystem::path path_;
};

[[nodiscard]] std::size_t detected_workers() {
  const auto detected = std::thread::hardware_concurrency();
  return detected == 0 ? 1 : static_cast<std::size_t>(detected);
}

[[nodiscard]] double milliseconds(Clock::duration duration) {
  return std::chrono::duration<double, std::milli>(duration).count();
}

[[nodiscard]] double median(std::vector<double> values) {
  if (values.empty()) {
    throw std::invalid_argument("cannot calculate an empty median");
  }
  std::sort(values.begin(), values.end());
  const auto middle = values.size() / 2;
  if (values.size() % 2 == 0) {
    return (values[middle - 1] + values[middle]) / 2.0;
  }
  return values[middle];
}

[[nodiscard]] std::size_t parse_size(std::string_view value,
                                     std::string_view option) {
  std::size_t position = 0;
  const auto parsed = std::stoull(std::string{value}, &position);
  if (position != value.size() || parsed == 0) {
    throw std::invalid_argument(std::string{option} +
                                " must be a positive integer");
  }
  return static_cast<std::size_t>(parsed);
}

[[nodiscard]] BenchmarkConfig parse_arguments(int argc, char** argv) {
  BenchmarkConfig config;
  for (int index = 1; index < argc; ++index) {
    const std::string_view option{argv[index]};
    if (option == "--help") {
      std::cout
          << "Usage: kronos_benchmark [options]\n"
          << "  --scheduler-jobs N   Short jobs per scheduler sample\n"
          << "  --cpu-jobs N         CPU-bound jobs per scaling sample\n"
          << "  --cpu-operations N   Deterministic operations per CPU job\n"
          << "  --wal-jobs N         Durable jobs written and recovered\n"
          << "  --samples N          Median sample count\n"
          << "  --workers N          Workers used for scheduler samples\n";
      std::exit(0);
    }
    if (index + 1 >= argc) {
      throw std::invalid_argument("missing value for " + std::string{option});
    }
    const std::string_view value{argv[++index]};
    if (option == "--scheduler-jobs") {
      config.scheduler_jobs = parse_size(value, option);
    } else if (option == "--cpu-jobs") {
      config.cpu_jobs = parse_size(value, option);
    } else if (option == "--cpu-operations") {
      config.cpu_operations = parse_size(value, option);
    } else if (option == "--wal-jobs") {
      config.wal_jobs = parse_size(value, option);
    } else if (option == "--samples") {
      config.samples = parse_size(value, option);
    } else if (option == "--workers") {
      config.scheduler_workers = parse_size(value, option);
    } else {
      throw std::invalid_argument("unknown option: " + std::string{option});
    }
  }
  return config;
}

void wait_for_results(const std::vector<kronos::JobHandle>& handles) {
  for (const auto& handle : handles) {
    const auto result = handle.result().get();
    if (result.state != kronos::JobState::Completed) {
      throw std::runtime_error("benchmark job did not complete");
    }
  }
}

[[nodiscard]] double run_scheduler_sample(kronos::SchedulerKind kind,
                                          std::size_t workers,
                                          std::size_t job_count) {
  kronos::TaskEngine engine({
      .worker_count = workers,
      .scheduler = {kind, 1},
      .journal = nullptr,
  });
  std::this_thread::sleep_for(std::chrono::milliseconds{20});

  std::vector<kronos::JobHandle> handles;
  handles.reserve(job_count);
  const auto started = Clock::now();
  for (std::size_t index = 0; index < job_count; ++index) {
    handles.push_back(engine.submit(
        std::make_unique<ImmediateTask>(),
        {.name = "benchmark-short",
         .priority = static_cast<int>(index % 11),
         .estimated_work_units = 1 + (index % 97),
         .durable_spec = std::nullopt}));
  }
  wait_for_results(handles);
  const auto finished = Clock::now();
  engine.shutdown(kronos::ShutdownMode::Drain);
  return milliseconds(finished - started);
}

[[nodiscard]] double run_cpu_sample(std::size_t workers,
                                    std::size_t job_count,
                                    kronos::WorkUnits operations) {
  kronos::TaskEngine engine({
      .worker_count = workers,
      .scheduler = {kronos::SchedulerKind::Fifo, 1},
      .journal = nullptr,
  });
  std::this_thread::sleep_for(std::chrono::milliseconds{20});

  std::vector<kronos::JobHandle> handles;
  handles.reserve(job_count);
  const auto started = Clock::now();
  for (std::size_t index = 0; index < job_count; ++index) {
    handles.push_back(engine.submit(
        std::make_unique<CpuTask>(operations),
        {.name = "benchmark-cpu",
         .priority = 0,
         .estimated_work_units = operations,
         .durable_spec = std::nullopt}));
  }
  wait_for_results(handles);
  const auto finished = Clock::now();
  engine.shutdown(kronos::ShutdownMode::Drain);
  return milliseconds(finished - started);
}

[[nodiscard]] double run_wait_time_sample(kronos::SchedulerKind kind) {
  constexpr std::size_t long_jobs = 10;
  constexpr std::size_t short_jobs = 30;
  constexpr auto long_duration = std::chrono::milliseconds{20};
  constexpr auto short_duration = std::chrono::milliseconds{1};

  kronos::TaskEngine engine({
      .worker_count = 1,
      .scheduler = {kind, 1},
      .journal = nullptr,
  });
  auto release = std::make_shared<std::promise<void>>();
  auto started = std::make_shared<std::promise<void>>();
  auto gate = engine.submit(
      std::make_unique<GateTask>(release->get_future().share(), started),
      {.name = "benchmark-gate", .estimated_work_units = 1});
  started->get_future().wait();

  std::vector<kronos::JobHandle> handles;
  handles.reserve(long_jobs + short_jobs);
  for (std::size_t index = 0; index < long_jobs; ++index) {
    handles.push_back(engine.submit(
        std::make_unique<SleepTask>(long_duration),
        {.name = "benchmark-long",
         .priority = 0,
         .estimated_work_units =
             static_cast<kronos::WorkUnits>(long_duration.count())}));
  }
  for (std::size_t index = 0; index < short_jobs; ++index) {
    handles.push_back(engine.submit(
        std::make_unique<SleepTask>(short_duration),
        {.name = "benchmark-short",
         .priority = 0,
         .estimated_work_units =
             static_cast<kronos::WorkUnits>(short_duration.count())}));
  }

  release->set_value();
  wait_for_results(handles);
  (void)gate.result().get();

  double total_wait_ms = 0.0;
  for (const auto& handle : handles) {
    const auto snapshot = engine.get_job(handle.id());
    if (!snapshot || snapshot->started_at == Clock::time_point{}) {
      throw std::runtime_error("benchmark job has no start timestamp");
    }
    total_wait_ms +=
        milliseconds(snapshot->started_at - snapshot->submitted_at);
  }
  engine.shutdown(kronos::ShutdownMode::Drain);
  return total_wait_ms / static_cast<double>(handles.size());
}

[[nodiscard]] std::vector<std::size_t> worker_counts(
    std::size_t logical_cpus) {
  std::vector<std::size_t> counts{1};
  for (const std::size_t candidate : {2U, 4U, 8U, 16U}) {
    if (candidate <= logical_cpus) {
      counts.push_back(candidate);
    }
  }
  if (counts.back() != logical_cpus) {
    counts.push_back(logical_cpus);
  }
  return counts;
}

void benchmark_schedulers(const BenchmarkConfig& config,
                          std::size_t workers) {
  const std::vector<kronos::SchedulerKind> policies{
      kronos::SchedulerKind::Fifo, kronos::SchedulerKind::Priority,
      kronos::SchedulerKind::ShortestJobFirst,
      kronos::SchedulerKind::RoundRobin};

  (void)run_scheduler_sample(kronos::SchedulerKind::Fifo, workers,
                             std::min<std::size_t>(config.scheduler_jobs, 1000));
  for (const auto policy : policies) {
    std::vector<double> samples;
    samples.reserve(config.samples);
    for (std::size_t sample = 0; sample < config.samples; ++sample) {
      samples.push_back(
          run_scheduler_sample(policy, workers, config.scheduler_jobs));
    }
    const auto median_ms = median(std::move(samples));
    const auto throughput =
        static_cast<double>(config.scheduler_jobs) * 1000.0 / median_ms;
    std::cout << "scheduler policy=" << kronos::to_string(policy)
              << " workers=" << workers << " jobs=" << config.scheduler_jobs
              << " median_ms=" << median_ms
              << " throughput_jobs_per_second=" << throughput << '\n';
  }
}

void benchmark_cpu_scaling(const BenchmarkConfig& config,
                           std::size_t logical_cpus) {
  double baseline_ms = 0.0;
  for (const auto workers : worker_counts(logical_cpus)) {
    std::vector<double> samples;
    samples.reserve(config.samples);
    for (std::size_t sample = 0; sample < config.samples; ++sample) {
      samples.push_back(
          run_cpu_sample(workers, config.cpu_jobs, config.cpu_operations));
    }
    const auto median_ms = median(std::move(samples));
    if (workers == 1) {
      baseline_ms = median_ms;
    }
    const auto operations = static_cast<double>(config.cpu_jobs) *
                            static_cast<double>(config.cpu_operations);
    std::cout << "cpu workers=" << workers << " jobs=" << config.cpu_jobs
              << " operations_per_job=" << config.cpu_operations
              << " median_ms=" << median_ms
              << " throughput_operations_per_second="
              << operations * 1000.0 / median_ms
              << " speedup_vs_one_worker=" << baseline_ms / median_ms << '\n';
  }
}

void benchmark_wait_time(const BenchmarkConfig& config) {
  std::vector<double> fifo_samples;
  std::vector<double> sjf_samples;
  fifo_samples.reserve(config.samples);
  sjf_samples.reserve(config.samples);
  for (std::size_t sample = 0; sample < config.samples; ++sample) {
    fifo_samples.push_back(run_wait_time_sample(kronos::SchedulerKind::Fifo));
    sjf_samples.push_back(
        run_wait_time_sample(kronos::SchedulerKind::ShortestJobFirst));
  }
  const auto fifo_ms = median(std::move(fifo_samples));
  const auto sjf_ms = median(std::move(sjf_samples));
  const auto reduction = (fifo_ms - sjf_ms) * 100.0 / fifo_ms;
  std::cout << "wait_time workers=1 jobs=40 long_jobs=10 long_ms=20"
            << " short_jobs=30 short_ms=1 fifo_mean_wait_ms=" << fifo_ms
            << " sjf_mean_wait_ms=" << sjf_ms
            << " sjf_reduction_percent=" << reduction << '\n';
}

void benchmark_wal(const BenchmarkConfig& config) {
  TemporaryWal temporary;
  double append_ms = 0.0;
  std::uintmax_t file_size = 0;
  std::vector<double> recovery_samples;
  std::size_t recovered_jobs = 0;
  std::size_t restartable_jobs = 0;
  {
    kronos::WalManager wal(temporary.path());
    const auto append_started = Clock::now();
    for (std::size_t index = 0; index < config.wal_jobs; ++index) {
      const auto id = static_cast<kronos::JobId>(index + 1);
      wal.append_submission({id,
                             index,
                             "benchmark-job",
                             static_cast<int>(index % 11),
                             100,
                             {"step", "100:0"}});
      wal.append_transition({id, kronos::JobState::Running, 0, {}});
      if (index % 2 == 0) {
        wal.append_transition(
            {id, kronos::JobState::Completed, 100, "benchmark complete"});
      } else {
        wal.append_transition(
            {id, kronos::JobState::Ready, 50, "benchmark yielded"});
      }
    }
    append_ms = milliseconds(Clock::now() - append_started);
    file_size = std::filesystem::file_size(temporary.path());

    recovery_samples.reserve(config.samples);
    for (std::size_t sample = 0; sample < config.samples; ++sample) {
      const auto recovery_started = Clock::now();
      const auto report = wal.recover();
      recovery_samples.push_back(milliseconds(Clock::now() - recovery_started));
      recovered_jobs = report.jobs.size();
      restartable_jobs = static_cast<std::size_t>(std::count_if(
          report.jobs.begin(), report.jobs.end(), [](const auto& job) {
            return job.state == kronos::JobState::Ready ||
                   job.state == kronos::JobState::Running;
          }));
    }
  }

  const auto record_count = config.wal_jobs * 3;
  const auto recovery_ms = median(std::move(recovery_samples));
  std::cout << "durability jobs=" << config.wal_jobs
            << " records=" << record_count << " file_bytes=" << file_size
            << " durable_append_ms=" << append_ms
            << " append_records_per_second="
            << static_cast<double>(record_count) * 1000.0 / append_ms
            << " recovery_median_ms=" << recovery_ms
            << " recovery_records_per_second="
            << static_cast<double>(record_count) * 1000.0 / recovery_ms
            << " recovered_jobs=" << recovered_jobs
            << " restartable_jobs=" << restartable_jobs << '\n';
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto config = parse_arguments(argc, argv);
    const auto logical_cpus = detected_workers();
    const auto scheduler_workers =
        config.scheduler_workers == 0
            ? std::min<std::size_t>(logical_cpus, 8)
            : config.scheduler_workers;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "kronos_benchmark version=1 build="
#ifdef NDEBUG
              << "release"
#else
              << "debug"
#endif
              << " logical_cpus=" << logical_cpus
              << " samples=" << config.samples << '\n';
#ifndef NDEBUG
    std::cout << "warning benchmark results from debug builds are not suitable "
                 "for performance claims\n";
#endif

    benchmark_schedulers(config, scheduler_workers);
    benchmark_cpu_scaling(config, logical_cpus);
    benchmark_wait_time(config);
    benchmark_wal(config);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "benchmark error: " << error.what() << '\n';
    return 1;
  }
}
