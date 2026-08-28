#include "kronos/protocol.hpp"

#include <charconv>
#include <cctype>
#include <stdexcept>

namespace kronos {
namespace {

constexpr std::string_view protocol_version = "KIPC1";

std::string hex_encode(std::string_view value) {
  constexpr char digits[] = "0123456789ABCDEF";
  std::string encoded;
  encoded.reserve(value.size() * 2);
  for (const unsigned char byte : value) {
    encoded.push_back(digits[byte >> 4U]);
    encoded.push_back(digits[byte & 0x0FU]);
  }
  return encoded;
}

int hex_value(char value) {
  if (value >= '0' && value <= '9') {
    return value - '0';
  }
  if (value >= 'A' && value <= 'F') {
    return value - 'A' + 10;
  }
  if (value >= 'a' && value <= 'f') {
    return value - 'a' + 10;
  }
  return -1;
}

std::string hex_decode(std::string_view value) {
  if (value.size() % 2 != 0) {
    throw std::invalid_argument("protocol field has odd-length hex encoding");
  }
  std::string decoded;
  decoded.reserve(value.size() / 2);
  for (std::size_t index = 0; index < value.size(); index += 2) {
    const int high = hex_value(value[index]);
    const int low = hex_value(value[index + 1]);
    if (high < 0 || low < 0) {
      throw std::invalid_argument("protocol field has invalid hex encoding");
    }
    decoded.push_back(static_cast<char>((high << 4) | low));
  }
  return decoded;
}

std::vector<std::string_view> split_tabs(std::string_view value) {
  std::vector<std::string_view> fields;
  std::size_t begin = 0;
  while (true) {
    const auto end = value.find('\t', begin);
    if (end == std::string_view::npos) {
      fields.push_back(value.substr(begin));
      return fields;
    }
    fields.push_back(value.substr(begin, end - begin));
    begin = end + 1;
  }
}

}  // namespace

std::string encode_message(const ProtocolMessage& message) {
  if (message.kind.empty() || message.kind.find_first_of("\t\r\n") !=
                                  std::string::npos) {
    throw std::invalid_argument("protocol message kind is invalid");
  }
  std::string encoded = std::string{protocol_version} + '\t' +
                        std::to_string(message.request_id) + '\t' + message.kind;
  for (const auto& field : message.fields) {
    encoded += '\t';
    encoded += hex_encode(field);
  }
  encoded += '\n';
  return encoded;
}

ProtocolMessage decode_message(std::string_view encoded_line) {
  if (!encoded_line.empty() && encoded_line.back() == '\n') {
    encoded_line.remove_suffix(1);
  }
  if (!encoded_line.empty() && encoded_line.back() == '\r') {
    encoded_line.remove_suffix(1);
  }
  const auto fields = split_tabs(encoded_line);
  if (fields.size() < 3 || fields[0] != protocol_version || fields[2].empty()) {
    throw std::invalid_argument("invalid IPC protocol message");
  }

  ProtocolMessage message;
  const auto [end, error] = std::from_chars(
      fields[1].data(), fields[1].data() + fields[1].size(), message.request_id);
  if (error != std::errc{} || end != fields[1].data() + fields[1].size()) {
    throw std::invalid_argument("invalid IPC request ID");
  }
  message.kind = fields[2];
  message.fields.reserve(fields.size() - 3);
  for (std::size_t index = 3; index < fields.size(); ++index) {
    message.fields.push_back(hex_decode(fields[index]));
  }
  return message;
}

std::vector<std::string> tokenize_command(std::string_view command_line) {
  std::vector<std::string> tokens;
  std::string current;
  char quote = '\0';
  bool escaped = false;

  for (const char value : command_line) {
    if (escaped) {
      current.push_back(value);
      escaped = false;
      continue;
    }
    if (value == '\\') {
      escaped = true;
      continue;
    }
    if (quote != '\0') {
      if (value == quote) {
        quote = '\0';
      } else {
        current.push_back(value);
      }
      continue;
    }
    if (value == '\'' || value == '"') {
      quote = value;
      continue;
    }
    if (std::isspace(static_cast<unsigned char>(value)) != 0) {
      if (!current.empty()) {
        tokens.push_back(std::move(current));
        current.clear();
      }
      continue;
    }
    current.push_back(value);
  }

  if (escaped || quote != '\0') {
    throw std::invalid_argument("unfinished escape or quote in command");
  }
  if (!current.empty()) {
    tokens.push_back(std::move(current));
  }
  return tokens;
}

}  // namespace kronos
