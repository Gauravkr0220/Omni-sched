#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>

#include "kronos/protocol.hpp"

namespace kronos {

struct FifoPaths {
  std::filesystem::path directory;
  std::filesystem::path requests;
  std::filesystem::path responses;
};

[[nodiscard]] std::filesystem::path default_ipc_directory();
[[nodiscard]] FifoPaths fifo_paths(const std::filesystem::path& directory);

class FifoServer {
 public:
  explicit FifoServer(std::filesystem::path directory);
  ~FifoServer();

  FifoServer(const FifoServer&) = delete;
  FifoServer& operator=(const FifoServer&) = delete;

  void send(const ProtocolMessage& message);
  [[nodiscard]] std::optional<ProtocolMessage> receive(
      std::chrono::milliseconds timeout);
  [[nodiscard]] const FifoPaths& paths() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class FifoClient {
 public:
  explicit FifoClient(std::filesystem::path directory);
  ~FifoClient();

  FifoClient(const FifoClient&) = delete;
  FifoClient& operator=(const FifoClient&) = delete;

  void send(const ProtocolMessage& message);
  [[nodiscard]] std::optional<ProtocolMessage> receive(
      std::chrono::milliseconds timeout);
  [[nodiscard]] const FifoPaths& paths() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace kronos

