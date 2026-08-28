#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kronos/fifo.hpp"
#include "kronos/protocol.hpp"

namespace {

using namespace std::chrono_literals;

volatile std::sig_atomic_t signal_requested = 0;

void handle_signal(int) { signal_requested = 1; }

struct ShellOptions {
  std::filesystem::path ipc_directory{kronos::default_ipc_directory()};
  std::optional<std::string> command;
};

void print_usage(std::string_view program) {
  std::cout << "Usage: " << program
            << " [--ipc-dir PATH] [--command \"COMMAND\"]\n";
}

ShellOptions parse_options(int argc, char** argv) {
  ShellOptions options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    if (argument == "--help" || argument == "-h") {
      print_usage(argv[0]);
      std::exit(EXIT_SUCCESS);
    }
    if (index + 1 >= argc) {
      throw std::invalid_argument(std::string{argument} + " requires a value");
    }
    if (argument == "--ipc-dir") {
      options.ipc_directory = argv[++index];
    } else if (argument == "--command") {
      options.command = argv[++index];
    } else {
      throw std::invalid_argument("unknown option: " + std::string{argument});
    }
  }
  return options;
}

class ShellSession {
 public:
  explicit ShellSession(const std::filesystem::path& directory)
      : transport_(directory),
        listener_([this](std::stop_token stop_token) { listen(stop_token); }) {}

  ~ShellSession() {
    listener_.request_stop();
    listener_.join();
  }

  kronos::ProtocolMessage execute(const std::vector<std::string>& tokens) {
    if (tokens.empty()) {
      throw std::invalid_argument("empty command");
    }
    const auto request_id = next_request_id_.fetch_add(1);
    transport_.send({request_id, "REQUEST", tokens});

    std::unique_lock lock(response_mutex_);
    const bool received = response_ready_.wait_for(lock, 10s, [&] {
      return responses_.contains(request_id) || listener_error_.has_value();
    });
    if (!received) {
      throw std::runtime_error("timed out waiting for engine response");
    }
    if (listener_error_) {
      throw std::runtime_error(*listener_error_);
    }
    auto response = std::move(responses_.at(request_id));
    responses_.erase(request_id);
    return response;
  }

  void print(std::string_view text) {
    std::lock_guard output_lock(output_mutex_);
    std::cout << text << std::flush;
  }

  void print_error(std::string_view text) {
    std::lock_guard output_lock(output_mutex_);
    std::cerr << text << std::flush;
  }

 private:
  void listen(std::stop_token stop_token) {
    try {
      while (!stop_token.stop_requested()) {
        auto message = transport_.receive(200ms);
        if (!message) {
          continue;
        }
        if (message->kind == "EVENT") {
          print("\n[event] " +
                (message->fields.empty() ? std::string{"job update"}
                                         : message->fields.front()) +
                "\n");
          continue;
        }
        {
          std::lock_guard lock(response_mutex_);
          responses_[message->request_id] = std::move(*message);
        }
        response_ready_.notify_all();
      }
    } catch (const std::exception& error) {
      {
        std::lock_guard lock(response_mutex_);
        listener_error_ = "IPC listener stopped: " + std::string{error.what()};
      }
      response_ready_.notify_all();
    }
  }

  kronos::FifoClient transport_;
  std::atomic<std::uint64_t> next_request_id_{1};
  std::mutex response_mutex_;
  std::condition_variable response_ready_;
  std::unordered_map<std::uint64_t, kronos::ProtocolMessage> responses_;
  std::optional<std::string> listener_error_;
  std::mutex output_mutex_;
  std::jthread listener_;
};

int run_command(ShellSession& session, std::string_view command) {
  auto tokens = kronos::tokenize_command(command);
  if (tokens.empty()) {
    return EXIT_SUCCESS;
  }
  if (tokens.front() == "quit" || tokens.front() == "exit") {
    return EXIT_SUCCESS;
  }
  const auto response = session.execute(tokens);
  if (!response.fields.empty()) {
    session.print(response.fields.front() + "\n");
  }
  return response.kind == "OK" ? EXIT_SUCCESS : EXIT_FAILURE;
}

int run_shell(const ShellOptions& options) {
  ShellSession session(options.ipc_directory);
  if (options.command) {
    return run_command(session, *options.command);
  }

  session.print("Kronos shell connected. Type 'help' for commands.\n");
  std::string line;
  while (signal_requested == 0) {
    session.print("kronos> ");
    if (!std::getline(std::cin, line)) {
      break;
    }
    try {
      const int status = run_command(session, line);
      const auto tokens = kronos::tokenize_command(line);
      if (!tokens.empty() &&
          (tokens.front() == "quit" || tokens.front() == "exit" ||
           tokens.front() == "shutdown")) {
        break;
      }
      (void)status;
    } catch (const std::exception& error) {
      session.print_error("error: " + std::string{error.what()} + "\n");
    }
  }
  return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGPIPE, SIG_IGN);
    return run_shell(parse_options(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << '\n';
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }
}
