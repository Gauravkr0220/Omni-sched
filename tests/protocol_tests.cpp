#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "kronos/protocol.hpp"
#include "kronos/task_factory.hpp"

namespace kronos {
namespace {

TEST(ProtocolTest, RoundTripsArbitraryFieldText) {
  const ProtocolMessage original{17,
                                 "OK",
                                 {"line one\nline two", "tabs\tand|pipes", ""}};

  const auto decoded = decode_message(encode_message(original));

  EXPECT_EQ(decoded.request_id, original.request_id);
  EXPECT_EQ(decoded.kind, original.kind);
  EXPECT_EQ(decoded.fields, original.fields);
}

TEST(ProtocolTest, RejectsMalformedMessages) {
  EXPECT_THROW((void)decode_message("wrong\t1\tOK\n"),
               std::invalid_argument);
  EXPECT_THROW((void)decode_message("KIPC1\tnot-a-number\tOK\n"),
               std::invalid_argument);
  EXPECT_THROW((void)decode_message("KIPC1\t1\tOK\tX\n"),
               std::invalid_argument);
}

TEST(CommandTokenizerTest, SupportsQuotesAndEscapes) {
  EXPECT_EQ(tokenize_command("submit step '20' \"5\" 3"),
            (std::vector<std::string>{"submit", "step", "20", "5", "3"}));
  EXPECT_EQ(tokenize_command("one two\\ three"),
            (std::vector<std::string>{"one", "two three"}));
  EXPECT_THROW((void)tokenize_command("'unfinished"), std::invalid_argument);
}

TEST(TaskFactoryTest, BuildsAndRestoresSupportedTasks) {
  auto prime = TaskFactory::create_for_submission("prime", {"100"}, 7);
  ASSERT_TRUE(prime.task);
  ASSERT_TRUE(prime.options.durable_spec.has_value());
  EXPECT_EQ(prime.options.durable_spec->type, "prime");
  EXPECT_EQ(prime.options.priority, 7);
  EXPECT_TRUE(TaskFactory::restore(*prime.options.durable_spec));

  auto step = TaskFactory::create_for_submission("step", {"5", "1"}, -2);
  ASSERT_TRUE(step.options.durable_spec.has_value());
  EXPECT_EQ(step.options.durable_spec->payload, "5,1");
  EXPECT_TRUE(TaskFactory::restore(*step.options.durable_spec));
}

TEST(TaskFactoryTest, RejectsUnknownOrMalformedTasks) {
  EXPECT_THROW((void)TaskFactory::create_for_submission("unknown", {}, 0),
               std::invalid_argument);
  EXPECT_THROW((void)TaskFactory::restore({"step", "bad"}),
               std::invalid_argument);
}

}  // namespace
}  // namespace kronos

