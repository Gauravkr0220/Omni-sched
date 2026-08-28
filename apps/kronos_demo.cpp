#include <chrono>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "kronos/example_tasks.hpp"
#include "kronos/task_engine.hpp"

namespace {

using namespace std::chrono_literals;

struct DemoOptions {
  kronos::SchedulerKind scheduler{kronos::SchedulerKind::RoundRobin};
  std::size_t workers{4};
  kronos::WorkUnits quantum{250};
  std::size_t jobs{8};
  bool switch_to_priority{false};
};

void print_usage(std::string_view program) {
  std::cout
      << "Usage: " << program
      << " [--scheduler fifo|priority|sjf|rr] [--workers N]"
         " [--quantum N] [--jobs N] [--switch-to-priority]\n";
}

std::size_t parse_size(std::string_view text, std::string_view option) {
  std::size_t parsed = 0;
  const auto value = std::stoull(std::string{text}, &parsed);
  if (parsed != text.size() || value == 0) {
    throw std::invalid_argument(std::string{option} +
                                " expects a positive integer");
  }
  return static_cast<std::size_t>(value);
}

kronos::SchedulerKind parse_scheduler(std::string_view value) {
  if (value == "fifo") {
    return kronos::SchedulerKind::Fifo;
  }
  if (value == "priority") {
    return kronos::SchedulerKind::Priority;
  }
  if (value == "sjf") {
    return kronos::SchedulerKind::ShortestJobFirst;
  }
  if (value == "rr" || value == "round-robin") {
    return kronos::SchedulerKind::RoundRobin;
  }
  throw std::invalid_argument("unknown scheduler: " + std::string{value});
}

DemoOptions parse_options(int argc, char** argv) {
  DemoOptions options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    auto require_value = [&](std::string_view option) -> std::string_view {
      if (index + 1 >= argc) {
        throw std::invalid_argument(std::string{option} + " requires a value");
      }
      return argv[++index];
    };

    if (argument == "--scheduler") {
      options.scheduler = parse_scheduler(require_value(argument));
    } else if (argument == "--workers") {
      options.workers = parse_size(require_value(argument), argument);
    } else if (argument == "--quantum") {
      options.quantum = parse_size(require_value(argument), argument);
    } else if (argument == "--jobs") {
      options.jobs = parse_size(require_value(argument), argument);
    } else if (argument == "--switch-to-priority") {
      options.switch_to_priority = true;
    } else if (argument == "--help" || argument == "-h") {
      print_usage(argv[0]);
      std::exit(EXIT_SUCCESS);
    } else {
      throw std::invalid_argument("unknown option: " + std::string{argument});
    }
  }
  return options;
}

void print_snapshot(const std::vector<kronos::JobSnapshot>& jobs) {
  std::cout << "\n"
            << std::left << std::setw(5) << "ID" << std::setw(18) << "NAME"
            << std::setw(12) << "STATE" << std::setw(10) << "PRIORITY"
            << "WORK\n";
  for (const auto& job : jobs) {
    std::cout << std::left << std::setw(5) << job.id << std::setw(18)
              << job.name << std::setw(12) << kronos::to_string(job.state)
              << std::setw(10) << job.priority << job.work_units_consumed << '/'
              << job.estimated_work_units << '\n';
  }
}

bool all_terminal(const std::vector<kronos::JobSnapshot>& jobs) {
  for (const auto& job : jobs) {
    if (!kronos::is_terminal(job.state)) {
      return false;
    }
  }
  return !jobs.empty();
}

int run_demo(const DemoOptions& options) {
  kronos::TaskEngine engine({
      options.workers,
      {options.scheduler, options.quantum},
      nullptr,
  });

  std::cout << "Kronos foundation demo\n"
            << "workers=" << engine.worker_count()
            << " scheduler=" << kronos::to_string(options.scheduler)
            << " quantum=" << options.quantum << "\n";

  std::vector<kronos::JobHandle> handles;
  handles.reserve(options.jobs);
  for (std::size_t index = 0; index < options.jobs; ++index) {
    const auto upper_bound = 12'000U + static_cast<unsigned>(index) * 1'000U;
    handles.push_back(engine.submit(
        std::make_unique<kronos::PrimeCountTask>(upper_bound),
        {.name = "prime-" + std::to_string(index + 1),
         .priority = static_cast<int>((index % 4) + 1),
         .estimated_work_units = upper_bound - 1}));
  }

  bool switched = false;
  while (true) {
    auto jobs = engine.snapshot();
    print_snapshot(jobs);
    if (all_terminal(jobs)) {
      break;
    }
    if (options.switch_to_priority && !switched) {
      std::this_thread::sleep_for(20ms);
      engine.set_scheduler({kronos::SchedulerKind::Priority, options.quantum});
      std::cout << "\nScheduler switched safely to priority.\n";
      switched = true;
    }
    std::this_thread::sleep_for(50ms);
  }

  engine.shutdown(kronos::ShutdownMode::Drain);
  std::size_t failures = 0;
  std::cout << "\nFinal results:\n";
  for (const auto& handle : handles) {
    const auto result = handle.result().get();
    std::cout << "job " << result.id << ": " << kronos::to_string(result.state)
              << " - " << result.message << '\n';
    failures += result.state == kronos::JobState::Completed ? 0U : 1U;
  }
  return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return run_demo(parse_options(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << '\n';
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }
}
