#include "kronos/fifo.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <fcntl.h>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kronos {
namespace {

constexpr std::size_t max_message_size = 1024U * 1024U;

void close_descriptor(int& descriptor) noexcept {
  if (descriptor >= 0) {
    ::close(descriptor);
    descriptor = -1;
  }
}

void validate_fifo(const std::filesystem::path& path) {
  struct stat information {};
  if (::lstat(path.c_str(), &information) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "IPC FIFO does not exist: " + path.string());
  }
  if (!S_ISFIFO(information.st_mode)) {
    throw std::runtime_error("IPC path is not a FIFO: " + path.string());
  }
  if (information.st_uid != ::getuid()) {
    throw std::runtime_error("IPC FIFO is owned by another user: " +
                             path.string());
  }
}

void create_fifo(const std::filesystem::path& path) {
  struct stat information {};
  if (::lstat(path.c_str(), &information) == 0) {
    if (!S_ISFIFO(information.st_mode) || information.st_uid != ::getuid()) {
      throw std::runtime_error("refusing to replace unsafe IPC path: " +
                               path.string());
    }
    if (::unlink(path.c_str()) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "failed to replace stale FIFO");
    }
  } else if (errno != ENOENT) {
    throw std::system_error(errno, std::generic_category(),
                            "failed to inspect FIFO path");
  }

  if (::mkfifo(path.c_str(), S_IRUSR | S_IWUSR) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "failed to create FIFO");
  }
}

int open_fifo(const std::filesystem::path& path, int flags) {
  const int descriptor = ::open(path.c_str(), flags | O_NONBLOCK | O_CLOEXEC);
  if (descriptor < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "failed to open FIFO: " + path.string());
  }
  return descriptor;
}

std::optional<std::string> receive_line(int descriptor, std::string& buffer,
                                        std::chrono::milliseconds timeout) {
  const auto extract_line = [&buffer]() -> std::optional<std::string> {
    const auto newline = buffer.find('\n');
    if (newline == std::string::npos) {
      return std::nullopt;
    }
    std::string line = buffer.substr(0, newline + 1);
    buffer.erase(0, newline + 1);
    return line;
  };

  if (auto line = extract_line()) {
    return line;
  }

  struct pollfd event {
    descriptor, POLLIN, 0
  };
  const auto milliseconds = timeout.count();
  const int bounded_timeout =
      milliseconds > static_cast<decltype(milliseconds)>(INT_MAX)
          ? INT_MAX
          : static_cast<int>(milliseconds);
  int poll_result;
  do {
    poll_result = ::poll(&event, 1, bounded_timeout);
  } while (poll_result < 0 && errno == EINTR);
  if (poll_result == 0) {
    return std::nullopt;
  }
  if (poll_result < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "failed while polling FIFO");
  }
  if ((event.revents & (POLLERR | POLLNVAL)) != 0) {
    throw std::runtime_error("IPC FIFO became unavailable");
  }

  std::array<char, 4096> chunk{};
  while (true) {
    const auto count = ::read(descriptor, chunk.data(), chunk.size());
    if (count > 0) {
      buffer.append(chunk.data(), static_cast<std::size_t>(count));
      if (buffer.size() > max_message_size) {
        throw std::runtime_error("IPC message exceeded the size limit");
      }
      if (auto line = extract_line()) {
        return line;
      }
      continue;
    }
    if (count == 0) {
      return std::nullopt;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return std::nullopt;
    }
    throw std::system_error(errno, std::generic_category(),
                            "failed to read FIFO");
  }
}

void send_line(int descriptor, std::mutex& mutex, const std::string& message) {
  if (message.size() > max_message_size) {
    throw std::invalid_argument("IPC message exceeded the size limit");
  }
  std::lock_guard lock(mutex);
  const char* cursor = message.data();
  std::size_t remaining = message.size();
  while (remaining > 0) {
    const auto written = ::write(descriptor, cursor, remaining);
    if (written > 0) {
      cursor += written;
      remaining -= static_cast<std::size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      struct pollfd event {
        descriptor, POLLOUT, 0
      };
      int poll_result;
      do {
        poll_result = ::poll(&event, 1, 2000);
      } while (poll_result < 0 && errno == EINTR);
      if (poll_result > 0) {
        continue;
      }
      if (poll_result == 0) {
        throw std::runtime_error("timed out writing to IPC FIFO");
      }
    }
    throw std::system_error(errno, std::generic_category(),
                            "failed to write FIFO");
  }
}

void prepare_directory(const std::filesystem::path& directory) {
  std::filesystem::create_directories(directory);
  struct stat information {};
  if (::lstat(directory.c_str(), &information) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "failed to inspect IPC directory");
  }
  if (!S_ISDIR(information.st_mode) || S_ISLNK(information.st_mode) ||
      information.st_uid != ::getuid()) {
    throw std::runtime_error("IPC directory is not a safe user-owned directory");
  }
  if (::chmod(directory.c_str(), S_IRWXU) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "failed to secure IPC directory");
  }
}

class Transport {
 public:
  Transport(FifoPaths configured_paths, int read_descriptor,
            int write_descriptor)
      : paths(std::move(configured_paths)),
        read_fd(read_descriptor),
        write_fd(write_descriptor) {}

  ~Transport() {
    close_descriptor(read_fd);
    close_descriptor(write_fd);
  }

  void send(const ProtocolMessage& message) {
    send_line(write_fd, write_mutex, encode_message(message));
  }

  std::optional<ProtocolMessage> receive(std::chrono::milliseconds timeout) {
    auto line = receive_line(read_fd, read_buffer, timeout);
    return line ? std::optional<ProtocolMessage>{decode_message(*line)}
                : std::nullopt;
  }

  FifoPaths paths;
  int read_fd{-1};
  int write_fd{-1};
  std::mutex write_mutex;
  std::string read_buffer;
};

}  // namespace

std::filesystem::path default_ipc_directory() {
  return std::filesystem::temp_directory_path() /
         ("kronos-" + std::to_string(::getuid()));
}

FifoPaths fifo_paths(const std::filesystem::path& directory) {
  return {directory, directory / "requests.fifo", directory / "responses.fifo"};
}

class FifoServer::Impl {
 public:
  explicit Impl(std::filesystem::path directory) {
    auto paths = fifo_paths(directory);
    prepare_directory(paths.directory);
    const auto lock_path = paths.directory / ".engine.lock";
    lock_fd_ = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW,
                      S_IRUSR | S_IWUSR);
    if (lock_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "failed to open engine lock");
    }
    struct stat lock_information {};
    if (::fstat(lock_fd_, &lock_information) != 0 ||
        !S_ISREG(lock_information.st_mode) ||
        lock_information.st_uid != ::getuid()) {
      close_descriptor(lock_fd_);
      throw std::runtime_error("engine lock is not a safe user-owned file");
    }
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
      close_descriptor(lock_fd_);
      throw std::runtime_error(
          "another Kronos engine is already using this IPC directory");
    }

    int request = -1;
    int response = -1;
    bool request_created = false;
    bool response_created = false;
    try {
      create_fifo(paths.requests);
      request_created = true;
      create_fifo(paths.responses);
      response_created = true;
      request = open_fifo(paths.requests, O_RDWR);
      response = open_fifo(paths.responses, O_RDWR);
      transport_ = std::make_unique<Transport>(paths, request, response);
      request = -1;
      response = -1;
    } catch (...) {
      close_descriptor(request);
      close_descriptor(response);
      if (request_created) {
        ::unlink(paths.requests.c_str());
      }
      if (response_created) {
        ::unlink(paths.responses.c_str());
      }
      ::flock(lock_fd_, LOCK_UN);
      close_descriptor(lock_fd_);
      throw;
    }
  }

  ~Impl() {
    if (transport_) {
      const auto paths = transport_->paths;
      transport_.reset();
      ::unlink(paths.requests.c_str());
      ::unlink(paths.responses.c_str());
    }
    if (lock_fd_ >= 0) {
      ::flock(lock_fd_, LOCK_UN);
      close_descriptor(lock_fd_);
    }
  }

  std::unique_ptr<Transport> transport_;
  int lock_fd_{-1};
};

FifoServer::FifoServer(std::filesystem::path directory)
    : impl_(std::make_unique<Impl>(std::move(directory))) {}

FifoServer::~FifoServer() = default;

void FifoServer::send(const ProtocolMessage& message) {
  impl_->transport_->send(message);
}

std::optional<ProtocolMessage> FifoServer::receive(
    std::chrono::milliseconds timeout) {
  return impl_->transport_->receive(timeout);
}

const FifoPaths& FifoServer::paths() const noexcept {
  return impl_->transport_->paths;
}

class FifoClient::Impl {
 public:
  explicit Impl(std::filesystem::path directory) {
    auto paths = fifo_paths(directory);
    validate_fifo(paths.requests);
    validate_fifo(paths.responses);
    const int request = open_fifo(paths.requests, O_WRONLY);
    try {
      const int response = open_fifo(paths.responses, O_RDONLY);
      transport_ =
          std::make_unique<Transport>(paths, response, request);
    } catch (...) {
      int request_to_close = request;
      close_descriptor(request_to_close);
      throw;
    }
  }

  std::unique_ptr<Transport> transport_;
};

FifoClient::FifoClient(std::filesystem::path directory)
    : impl_(std::make_unique<Impl>(std::move(directory))) {}

FifoClient::~FifoClient() = default;

void FifoClient::send(const ProtocolMessage& message) {
  impl_->transport_->send(message);
}

std::optional<ProtocolMessage> FifoClient::receive(
    std::chrono::milliseconds timeout) {
  return impl_->transport_->receive(timeout);
}

const FifoPaths& FifoClient::paths() const noexcept {
  return impl_->transport_->paths;
}

}  // namespace kronos
