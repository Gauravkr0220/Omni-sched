#include <gtest/gtest.h>

#include <chrono>
#include <fstream>

#include "kronos/fifo.hpp"
#include "test_support.hpp"

namespace kronos {
namespace {

using namespace std::chrono_literals;

TEST(FifoTest, ExchangesFramedMessagesInBothDirections) {
  test::TemporaryDirectory temporary{"fifo-roundtrip"};
  FifoServer server(temporary.path());
  FifoClient client(temporary.path());

  client.send({9, "REQUEST", {"ps"}});
  const auto request = server.receive(1s);
  ASSERT_TRUE(request.has_value());
  EXPECT_EQ(request->request_id, 9U);
  EXPECT_EQ(request->fields, (std::vector<std::string>{"ps"}));

  server.send({9, "OK", {"No active jobs."}});
  const auto response = client.receive(1s);
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->kind, "OK");
  EXPECT_EQ(response->fields.front(), "No active jobs.");
}

TEST(FifoTest, RefusesToReplaceARegularFile) {
  test::TemporaryDirectory temporary{"fifo-safety"};
  const auto paths = fifo_paths(temporary.path());
  {
    std::ofstream regular_file(paths.requests);
    regular_file << "do not replace";
  }

  EXPECT_THROW((void)FifoServer(temporary.path()), std::runtime_error);
  EXPECT_TRUE(std::filesystem::is_regular_file(paths.requests));
}

TEST(FifoTest, PreservesAnUnsafeResponsePathWhenSetupFails) {
  test::TemporaryDirectory temporary{"fifo-response-safety"};
  const auto paths = fifo_paths(temporary.path());
  {
    std::ofstream regular_file(paths.responses);
    regular_file << "preserve me";
  }

  EXPECT_THROW((void)FifoServer(temporary.path()), std::runtime_error);
  EXPECT_TRUE(std::filesystem::is_regular_file(paths.responses));
  EXPECT_FALSE(std::filesystem::exists(paths.requests));
}

TEST(FifoTest, PreventsTwoServersUsingTheSameDirectory) {
  test::TemporaryDirectory temporary{"fifo-lock"};
  FifoServer first(temporary.path());

  EXPECT_THROW((void)FifoServer(temporary.path()), std::runtime_error);
}

}  // namespace
}  // namespace kronos
