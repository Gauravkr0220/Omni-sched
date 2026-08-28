#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kronos {

struct ProtocolMessage {
  std::uint64_t request_id{0};
  std::string kind;
  std::vector<std::string> fields;
};

[[nodiscard]] std::string encode_message(const ProtocolMessage& message);
[[nodiscard]] ProtocolMessage decode_message(std::string_view encoded_line);
[[nodiscard]] std::vector<std::string> tokenize_command(
    std::string_view command_line);

}  // namespace kronos

