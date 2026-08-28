#include "kronos/task_factory.hpp"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "kronos/example_tasks.hpp"

namespace kronos {
namespace {

template <typename Integer>
Integer parse_positive(std::string_view value, std::string_view label) {
  Integer parsed{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size() ||
      parsed == 0) {
    throw std::invalid_argument(std::string{label} +
                                " must be a positive integer");
  }
  return parsed;
}

template <typename Integer>
Integer parse_nonnegative(std::string_view value, std::string_view label) {
  Integer parsed{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument(std::string{label} +
                                " must be a non-negative integer");
  }
  return parsed;
}

std::vector<std::string> split_payload(std::string_view payload) {
  std::vector<std::string> fields;
  std::size_t begin = 0;
  while (true) {
    const auto end = payload.find(',', begin);
    if (end == std::string_view::npos) {
      fields.emplace_back(payload.substr(begin));
      return fields;
    }
    fields.emplace_back(payload.substr(begin, end - begin));
    begin = end + 1;
  }
}

CreatedTask make_prime(std::string_view upper_bound_text, int priority) {
  const auto upper_bound =
      parse_positive<std::uint64_t>(upper_bound_text, "upper bound");
  const auto estimate = upper_bound > 1 ? upper_bound - 1 : 1;
  DurableTaskSpec specification{"prime", std::to_string(upper_bound)};
  return {std::make_unique<PrimeCountTask>(upper_bound),
          {.name = "prime-" + std::to_string(upper_bound),
           .priority = priority,
           .estimated_work_units = estimate,
           .durable_spec = specification}};
}

CreatedTask make_step(std::string_view steps_text,
                      std::string_view delay_text, int priority) {
  const auto steps = parse_positive<WorkUnits>(steps_text, "step count");
  const auto delay =
      parse_nonnegative<std::uint64_t>(delay_text, "delay milliseconds");
  if (delay > static_cast<std::uint64_t>(
                  std::chrono::milliseconds::max().count())) {
    throw std::invalid_argument("delay milliseconds is too large");
  }
  DurableTaskSpec specification{
      "step", std::to_string(steps) + ',' + std::to_string(delay)};
  return {std::make_unique<StepTask>(
              steps, std::chrono::milliseconds{static_cast<std::int64_t>(delay)}),
          {.name = "step-" + std::to_string(steps),
           .priority = priority,
           .estimated_work_units = steps,
           .durable_spec = specification}};
}

}  // namespace

CreatedTask TaskFactory::create_for_submission(
    std::string_view type, const std::vector<std::string>& arguments,
    int priority) {
  if (type == "prime") {
    if (arguments.size() != 1) {
      throw std::invalid_argument("usage: submit prime <upper-bound> [priority]");
    }
    return make_prime(arguments[0], priority);
  }
  if (type == "step") {
    if (arguments.size() != 2) {
      throw std::invalid_argument(
          "usage: submit step <steps> <delay-ms> [priority]");
    }
    return make_step(arguments[0], arguments[1], priority);
  }
  throw std::invalid_argument("unknown task type: " + std::string{type});
}

std::unique_ptr<ITask> TaskFactory::restore(
    const DurableTaskSpec& specification) {
  if (specification.type == "prime") {
    return make_prime(specification.payload, 0).task;
  }
  if (specification.type == "step") {
    const auto fields = split_payload(specification.payload);
    if (fields.size() != 2) {
      throw std::invalid_argument("invalid durable step task payload");
    }
    return make_step(fields[0], fields[1], 0).task;
  }
  throw std::invalid_argument("cannot restore unknown task type: " +
                              specification.type);
}

}  // namespace kronos
