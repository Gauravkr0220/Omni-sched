#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "kronos/fifo.hpp"
#include "kronos/task_factory.hpp"
#include "kronos/wal.hpp"

namespace {

using namespace std::chrono_literals;

volatile std::sig_atomic_t signal_requested = 0;

void handle_signal(int) { signal_requested = 1; }

struct DaemonOptions {
  std::filesystem::path ipc_directory{kronos::default_ipc_directory()};
  std::optional<std::filesystem::path> wal_path;
  std::size_t workers{0};
  kronos::SchedulerConfig scheduler{kronos::SchedulerKind::Fifo, 100};
};

template <typename Integer>
Integer parse_integer(std::string_view value, std::string_view label) {
  Integer parsed{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument(std::string{label} + " must be an integer");
  }
  return parsed;
}

std::size_t parse_positive_size(std::string_view value,
                                std::string_view label) {
  const auto parsed = parse_integer<std::uint64_t>(value, label);
  if (parsed == 0) {
    throw std::invalid_argument(std::string{label} +
                                " must be greater than zero");
  }
  return static_cast<std::size_t>(parsed);
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

void print_usage(std::string_view program) {
  std::cout << "Usage: " << program
            << " [--ipc-dir PATH] [--wal PATH] [--workers N]"
               " [--scheduler fifo|priority|sjf|rr] [--quantum N]\n";
}

DaemonOptions parse_options(int argc, char** argv) {
  DaemonOptions options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    auto require_value = [&](std::string_view option) -> std::string_view {
      if (index + 1 >= argc) {
        throw std::invalid_argument(std::string{option} + " requires a value");
      }
      return argv[++index];
    };

    if (argument == "--ipc-dir") {
      options.ipc_directory = require_value(argument);
    } else if (argument == "--wal") {
      options.wal_path = std::filesystem::path{require_value(argument)};
    } else if (argument == "--workers") {
      options.workers = parse_positive_size(require_value(argument), argument);
    } else if (argument == "--scheduler") {
      options.scheduler.kind = parse_scheduler(require_value(argument));
    } else if (argument == "--quantum") {
      options.scheduler.quantum =
          parse_positive_size(require_value(argument), argument);
    } else if (argument == "--help" || argument == "-h") {
      print_usage(argv[0]);
      std::exit(EXIT_SUCCESS);
    } else {
      throw std::invalid_argument("unknown option: " + std::string{argument});
    }
  }
  if (!options.wal_path) {
    options.wal_path = options.ipc_directory / "kronos.wal";
  }
  return options;
}

std::string format_snapshots(const std::vector<kronos::JobSnapshot>& jobs,
                             bool terminal) {
  std::ostringstream output;
  std::size_t count = 0;
  output << std::left << std::setw(6) << "ID" << std::setw(20) << "NAME"
         << std::setw(12) << "STATE" << std::setw(10) << "PRIORITY"
         << "WORK\n";
  for (const auto& job : jobs) {
    if (kronos::is_terminal(job.state) != terminal) {
      continue;
    }
    ++count;
    output << std::left << std::setw(6) << job.id << std::setw(20) << job.name
           << std::setw(12) << kronos::to_string(job.state) << std::setw(10)
           << job.priority << job.work_units_consumed << '/'
           << job.estimated_work_units << '\n';
  }
  return count == 0 ? (terminal ? "No completed jobs." : "No active jobs.")
                    : output.str();
}

std::string format_history(const kronos::RecoveryReport& report) {
  std::ostringstream output;
  std::size_t count = 0;
  output << std::left << std::setw(6) << "ID" << std::setw(20) << "NAME"
         << std::setw(12) << "STATE" << "RESULT\n";
  for (const auto& job : report.jobs) {
    if (!kronos::is_terminal(job.state)) {
      continue;
    }
    ++count;
    output << std::left << std::setw(6) << job.submission.id << std::setw(20)
           << job.submission.name << std::setw(12)
           << kronos::to_string(job.state) << job.message << '\n';
  }
  return count == 0 ? "No completed jobs." : output.str();
}

class Daemon {
 public:
  explicit Daemon(const DaemonOptions& options)
      : server_(options.ipc_directory),
        wal_(std::make_shared<kronos::WalManager>(*options.wal_path)),
        engine_({options.workers, options.scheduler, wal_}) {
    const auto recovery = wal_->recover();
    if (recovery.ignored_incomplete_tail) {
      std::cerr << "warning: ignored incomplete final WAL record\n";
    }
    for (const auto& job : recovery.jobs) {
      engine_.reserve_recovered_identity(job.submission);
    }
    for (const auto& job : recovery.jobs) {
      if (kronos::is_terminal(job.state)) {
        continue;
      }
      try {
        auto handle = engine_.restore(
            kronos::TaskFactory::restore(job.submission.task), job.submission);
        observe(handle);
        ++recovered_count_;
      } catch (const std::exception& error) {
        wal_->append_transition({job.submission.id, kronos::JobState::Failed,
                                 job.work_units_consumed,
                                 "recovery failed: " +
                                     std::string{error.what()}});
      }
    }
  }

  int run() {
    std::cout << "Kronos engine ready\n"
              << "IPC: " << server_.paths().directory << '\n'
              << "WAL: " << wal_->path() << '\n'
              << "Recovered jobs: " << recovered_count_ << '\n'
              << std::flush;

    while (!stop_requested_ && signal_requested == 0) {
      try {
        auto request = server_.receive(200ms);
        if (request) {
          handle_request(*request);
        }
      } catch (const std::exception& error) {
        std::cerr << "IPC error: " << error.what() << '\n';
      }
    }

    engine_.shutdown(kronos::ShutdownMode::Drain);
    for (auto& observer : observers_) {
      observer.join();
    }
    return EXIT_SUCCESS;
  }

 private:
  void observe(kronos::JobHandle handle) {
    observers_.emplace_back([this, handle = std::move(handle)](
                                std::stop_token stop_token) mutable {
      while (!stop_token.stop_requested() &&
             handle.result().wait_for(100ms) != std::future_status::ready) {
      }
      if (stop_token.stop_requested()) {
        return;
      }
      const auto result = handle.result().get();
      try {
        server_.send({0,
                      "EVENT",
                      {"job " + std::to_string(result.id) + " " +
                       std::string{kronos::to_string(result.state)} + ": " +
                       result.message}});
      } catch (const std::exception& error) {
        std::cerr << "notification error: " << error.what() << '\n';
      }
    });
  }

  void respond(std::uint64_t request_id, std::string kind,
               std::string message) {
    server_.send({request_id, std::move(kind), {std::move(message)}});
  }

  void handle_request(const kronos::ProtocolMessage& request) {
    if (request.kind != "REQUEST" || request.fields.empty()) {
      respond(request.request_id, "ERROR", "invalid request");
      return;
    }

    try {
      const auto& command = request.fields[0];
      if (command == "submit") {
        handle_submit(request);
      } else if (command == "ps") {
        if (request.fields.size() != 1) {
          throw std::invalid_argument("usage: ps");
        }
        respond(request.request_id, "OK",
                format_snapshots(engine_.snapshot(), false));
      } else if (command == "history") {
        if (request.fields.size() != 1) {
          throw std::invalid_argument("usage: history");
        }
        respond(request.request_id, "OK", format_history(wal_->recover()));
      } else if (command == "kill") {
        if (request.fields.size() != 2) {
          throw std::invalid_argument("usage: kill <job-id>");
        }
        const auto id = parse_integer<kronos::JobId>(request.fields[1], "job ID");
        const auto job = engine_.get_job(id);
        const bool cancelled = engine_.cancel(id);
        respond(request.request_id, cancelled ? "OK" : "ERROR",
                cancelled ? "cancellation requested for job " +
                                std::to_string(id)
                          : (job ? "job is already terminal" : "job not found"));
      } else if (command == "scheduler") {
        handle_scheduler(request);
      } else if (command == "ping") {
        respond(request.request_id, "OK", "pong");
      } else if (command == "help") {
        respond(request.request_id, "OK", help_text());
      } else if (command == "shutdown") {
        if (request.fields.size() != 1) {
          throw std::invalid_argument("usage: shutdown");
        }
        respond(request.request_id, "OK", "engine shutdown started");
        stop_requested_ = true;
      } else {
        throw std::invalid_argument("unknown command: " + command);
      }
    } catch (const std::exception& error) {
      respond(request.request_id, "ERROR", error.what());
    }
  }

  void handle_submit(const kronos::ProtocolMessage& request) {
    if (request.fields.size() < 3) {
      throw std::invalid_argument(
          "usage: submit prime <upper-bound> [priority] | "
          "submit step <steps> <delay-ms> [priority]");
    }

    const auto& type = request.fields[1];
    const std::size_t required_arguments = type == "prime" ? 1U : 2U;
    if (request.fields.size() != required_arguments + 2 &&
        request.fields.size() != required_arguments + 3) {
      throw std::invalid_argument(
          "usage: submit prime <upper-bound> [priority] | "
          "submit step <steps> <delay-ms> [priority]");
    }

    int priority = 0;
    if (request.fields.size() == required_arguments + 3) {
      priority = parse_integer<int>(request.fields.back(), "priority");
    }
    std::vector<std::string> arguments;
    arguments.reserve(required_arguments);
    for (std::size_t index = 0; index < required_arguments; ++index) {
      arguments.push_back(request.fields[index + 2]);
    }

    auto created =
        kronos::TaskFactory::create_for_submission(type, arguments, priority);
    auto handle = engine_.submit(std::move(created.task), created.options);
    const auto id = handle.id();
    observe(std::move(handle));
    respond(request.request_id, "OK", "submitted job " + std::to_string(id));
  }

  void handle_scheduler(const kronos::ProtocolMessage& request) {
    if (request.fields.size() < 2 || request.fields.size() > 3) {
      throw std::invalid_argument(
          "usage: scheduler <fifo|priority|sjf|rr> [quantum]");
    }
    auto config = engine_.scheduler_config();
    config.kind = parse_scheduler(request.fields[1]);
    if (request.fields.size() == 3) {
      config.quantum = parse_positive_size(request.fields[2], "quantum");
    }
    engine_.set_scheduler(config);
    respond(request.request_id, "OK",
            "scheduler changed to " +
                std::string{kronos::to_string(config.kind)});
  }

  static std::string help_text() {
    return "submit prime <upper-bound> [priority]\n"
           "submit step <steps> <delay-ms> [priority]\n"
           "ps\nkill <job-id>\nhistory\n"
           "scheduler <fifo|priority|sjf|rr> [quantum]\n"
           "ping\nshutdown\nquit";
  }

  kronos::FifoServer server_;
  std::shared_ptr<kronos::WalManager> wal_;
  kronos::TaskEngine engine_;
  std::vector<std::jthread> observers_;
  std::size_t recovered_count_{0};
  bool stop_requested_{false};
};

}  // namespace

int main(int argc, char** argv) {
  try {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    return Daemon(parse_options(argc, argv)).run();
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << '\n';
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }
}
