#include "kronos/wal.hpp"

#include <algorithm>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <unordered_map>
#include <unistd.h>

namespace kronos {
namespace {

std::uint32_t crc32(std::string_view value) {
  std::uint32_t checksum = 0xFFFFFFFFU;
  for (const unsigned char byte : value) {
    checksum ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      const auto mask = static_cast<std::uint32_t>(
          -static_cast<std::int32_t>(checksum & 1U));
      checksum = (checksum >> 1U) ^ (0xEDB88320U & mask);
    }
  }
  return checksum ^ 0xFFFFFFFFU;
}

std::string checksum_text(std::string_view value) {
  std::ostringstream output;
  output << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
         << crc32(value);
  return output.str();
}

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
    throw std::runtime_error("WAL contains an invalid hex field");
  }
  std::string decoded;
  decoded.reserve(value.size() / 2);
  for (std::size_t index = 0; index < value.size(); index += 2) {
    const int high = hex_value(value[index]);
    const int low = hex_value(value[index + 1]);
    if (high < 0 || low < 0) {
      throw std::runtime_error("WAL contains an invalid hex field");
    }
    decoded.push_back(static_cast<char>((high << 4) | low));
  }
  return decoded;
}

std::vector<std::string_view> split(std::string_view line) {
  std::vector<std::string_view> fields;
  std::size_t begin = 0;
  while (true) {
    const auto end = line.find('|', begin);
    if (end == std::string_view::npos) {
      fields.push_back(line.substr(begin));
      return fields;
    }
    fields.push_back(line.substr(begin, end - begin));
    begin = end + 1;
  }
}

template <typename Integer>
Integer parse_integer(std::string_view value, std::string_view field_name) {
  Integer parsed{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::runtime_error("WAL contains an invalid " +
                             std::string{field_name});
  }
  return parsed;
}

JobState parse_state(std::string_view state) {
  if (state == "NEW") {
    return JobState::New;
  }
  if (state == "READY") {
    return JobState::Ready;
  }
  if (state == "RUNNING") {
    return JobState::Running;
  }
  if (state == "COMPLETED") {
    return JobState::Completed;
  }
  if (state == "FAILED") {
    return JobState::Failed;
  }
  if (state == "CANCELLED") {
    return JobState::Cancelled;
  }
  throw std::runtime_error("WAL contains an unknown job state");
}

std::int64_t current_unix_milliseconds() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void validate_checksum(std::string_view line,
                       const std::vector<std::string_view>& fields) {
  if (fields.empty()) {
    throw std::runtime_error("WAL record has no fields");
  }
  const auto separator = line.rfind('|');
  if (separator == std::string_view::npos ||
      checksum_text(line.substr(0, separator)) != fields.back()) {
    throw std::runtime_error("WAL checksum mismatch");
  }
}

std::string with_checksum(std::string prefix) {
  prefix += '|';
  prefix += checksum_text(prefix.substr(0, prefix.size() - 1));
  prefix += '\n';
  return prefix;
}

}  // namespace

class WalManager::Impl {
 public:
  explicit Impl(std::filesystem::path configured_path)
      : path_(std::move(configured_path)) {
    if (path_.empty()) {
      throw std::invalid_argument("WAL path cannot be empty");
    }
    if (path_.has_parent_path()) {
      std::filesystem::create_directories(path_.parent_path());
    }
    descriptor_ = ::open(path_.c_str(), O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC,
                         S_IRUSR | S_IWUSR);
    if (descriptor_ < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "failed to open WAL");
    }
  }

  ~Impl() {
    if (descriptor_ >= 0) {
      ::close(descriptor_);
    }
  }

  void append_submission(const JobSubmissionRecord& record) {
    if (record.id == 0 || record.estimated_work_units == 0 ||
        record.task.type.empty()) {
      throw std::invalid_argument("invalid durable submission record");
    }

    std::ostringstream prefix;
    prefix << "K1|S|" << current_unix_milliseconds() << '|' << record.id << '|'
           << record.submission_sequence << '|' << record.priority << '|'
           << record.estimated_work_units << '|' << hex_encode(record.name) << '|'
           << hex_encode(record.task.type) << '|' << hex_encode(record.task.payload);
    append(with_checksum(prefix.str()));
  }

  void append_transition(const JobTransitionRecord& record) {
    if (record.id == 0) {
      throw std::invalid_argument("invalid durable transition record");
    }

    std::ostringstream prefix;
    prefix << "K1|T|" << current_unix_milliseconds() << '|' << record.id << '|'
           << to_string(record.state) << '|' << record.work_units_consumed << '|'
           << hex_encode(record.message);
    append(with_checksum(prefix.str()));
  }

  RecoveryReport recover() const {
    std::lock_guard lock(mutex_);
    std::ifstream input(path_, std::ios::binary);
    if (!input) {
      throw std::runtime_error("failed to read WAL: " + path_.string());
    }
    std::string contents((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());

    RecoveryReport report;
    if (!contents.empty() && contents.back() != '\n') {
      report.ignored_incomplete_tail = true;
      const auto final_newline = contents.rfind('\n');
      const auto valid_size =
          final_newline == std::string::npos ? 0 : final_newline + 1;
      if (::ftruncate(descriptor_, static_cast<off_t>(valid_size)) != 0 ||
          ::fsync(descriptor_) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "failed to repair incomplete WAL tail");
      }
      contents.resize(valid_size);
    }

    std::unordered_map<JobId, RecoveredJob> recovered;
    std::size_t line_number = 0;
    std::size_t begin = 0;
    while (begin < contents.size()) {
      const auto end = contents.find('\n', begin);
      ++line_number;
      const std::string_view line{contents.data() + begin, end - begin};
      begin = end + 1;
      if (line.empty()) {
        throw std::runtime_error("empty WAL record at line " +
                                 std::to_string(line_number));
      }

      try {
        const auto fields = split(line);
        validate_checksum(line, fields);
        if (fields.size() < 2 || fields[0] != "K1") {
          throw std::runtime_error("unsupported WAL record version");
        }

        if (fields[1] == "S") {
          if (fields.size() != 11) {
            throw std::runtime_error("invalid submission record field count");
          }
          (void)parse_integer<std::int64_t>(fields[2], "timestamp");
          JobSubmissionRecord submission;
          submission.id = parse_integer<JobId>(fields[3], "job ID");
          submission.submission_sequence =
              parse_integer<std::uint64_t>(fields[4], "submission sequence");
          submission.priority = parse_integer<int>(fields[5], "priority");
          submission.estimated_work_units =
              parse_integer<WorkUnits>(fields[6], "work estimate");
          submission.name = hex_decode(fields[7]);
          submission.task.type = hex_decode(fields[8]);
          submission.task.payload = hex_decode(fields[9]);
          if (submission.id == 0 || submission.estimated_work_units == 0 ||
              submission.task.type.empty()) {
            throw std::runtime_error("invalid submission record values");
          }
          const auto [position, inserted] = recovered.emplace(
              submission.id,
              RecoveredJob{submission, JobState::Ready, 0, {}});
          (void)position;
          if (!inserted) {
            throw std::runtime_error("duplicate job submission");
          }
        } else if (fields[1] == "T") {
          if (fields.size() != 8) {
            throw std::runtime_error("invalid transition record field count");
          }
          (void)parse_integer<std::int64_t>(fields[2], "timestamp");
          const auto id = parse_integer<JobId>(fields[3], "job ID");
          const auto found = recovered.find(id);
          if (found == recovered.end()) {
            throw std::runtime_error("transition precedes submission");
          }
          found->second.state = parse_state(fields[4]);
          found->second.work_units_consumed =
              parse_integer<WorkUnits>(fields[5], "consumed work");
          found->second.message = hex_decode(fields[6]);
        } else {
          throw std::runtime_error("unknown WAL record type");
        }
      } catch (const std::exception& error) {
        throw std::runtime_error("invalid WAL record at line " +
                                 std::to_string(line_number) + ": " +
                                 error.what());
      }
    }

    report.jobs.reserve(recovered.size());
    for (auto& [id, job] : recovered) {
      (void)id;
      report.jobs.push_back(std::move(job));
    }
    std::sort(report.jobs.begin(), report.jobs.end(),
              [](const RecoveredJob& lhs, const RecoveredJob& rhs) {
                return lhs.submission.submission_sequence <
                       rhs.submission.submission_sequence;
              });
    return report;
  }

  const std::filesystem::path& path() const noexcept { return path_; }

 private:
  void append(const std::string& record) {
    std::lock_guard lock(mutex_);
    const char* cursor = record.data();
    std::size_t remaining = record.size();
    while (remaining > 0) {
      const auto written = ::write(descriptor_, cursor, remaining);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(),
                                "failed to append WAL record");
      }
      cursor += written;
      remaining -= static_cast<std::size_t>(written);
    }
    if (::fsync(descriptor_) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "failed to fsync WAL");
    }
  }

  std::filesystem::path path_;
  int descriptor_{-1};
  mutable std::mutex mutex_;
};

WalManager::WalManager(std::filesystem::path path)
    : impl_(std::make_unique<Impl>(std::move(path))) {}

WalManager::~WalManager() = default;

void WalManager::append_submission(const JobSubmissionRecord& record) {
  impl_->append_submission(record);
}

void WalManager::append_transition(const JobTransitionRecord& record) {
  impl_->append_transition(record);
}

RecoveryReport WalManager::recover() const { return impl_->recover(); }

const std::filesystem::path& WalManager::path() const noexcept {
  return impl_->path();
}

}  // namespace kronos
