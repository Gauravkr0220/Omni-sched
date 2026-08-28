cmake_minimum_required(VERSION 3.24)

project(
  Kronos
  VERSION 0.1.0
  DESCRIPTION "A concurrent C++20 task execution engine"
  LANGUAGES CXX)

include(CTest)
include(FetchContent)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(KRONOS_BUILD_BENCHMARKS "Build the repeatable Kronos benchmark harness" ON)

set(KRONOS_SANITIZER "none" CACHE STRING
    "Sanitizer configuration: none, address, undefined, address-undefined, thread")
set_property(CACHE KRONOS_SANITIZER PROPERTY STRINGS
             none address undefined address-undefined thread)

find_package(Threads REQUIRED)

function(kronos_enable_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive-)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Werror)
  endif()
endfunction()

function(kronos_enable_sanitizer target)
  if(KRONOS_SANITIZER STREQUAL "none")
    return()
  endif()
  if(MSVC)
    message(FATAL_ERROR "The configured sanitizer is not supported by this project on MSVC")
  endif()

  if(KRONOS_SANITIZER STREQUAL "address")
    set(flags -fsanitize=address -fno-omit-frame-pointer)
  elseif(KRONOS_SANITIZER STREQUAL "undefined")
    set(flags -fsanitize=undefined -fno-omit-frame-pointer)
  elseif(KRONOS_SANITIZER STREQUAL "address-undefined")
    set(flags -fsanitize=address,undefined -fno-omit-frame-pointer)
  elseif(KRONOS_SANITIZER STREQUAL "thread")
    set(flags -fsanitize=thread -fno-omit-frame-pointer)
  else()
    message(FATAL_ERROR "Unknown KRONOS_SANITIZER value: ${KRONOS_SANITIZER}")
  endif()

  target_compile_options(${target} PRIVATE ${flags})
  target_link_options(${target} PRIVATE ${flags})
endfunction()

add_library(kronos_core
  src/example_tasks.cpp
  src/fifo.cpp
  src/protocol.cpp
  src/scheduler.cpp
  src/task_factory.cpp
  src/task_engine.cpp
  src/types.cpp
  src/wal.cpp)
add_library(Kronos::core ALIAS kronos_core)

target_compile_features(kronos_core PUBLIC cxx_std_20)
target_include_directories(kronos_core
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(kronos_core PUBLIC Threads::Threads)
kronos_enable_warnings(kronos_core)
kronos_enable_sanitizer(kronos_core)

add_executable(kronos_demo apps/kronos_demo.cpp)
target_link_libraries(kronos_demo PRIVATE Kronos::core)
kronos_enable_warnings(kronos_demo)
kronos_enable_sanitizer(kronos_demo)

add_executable(kronos_engine apps/kronos_engine.cpp)
target_link_libraries(kronos_engine PRIVATE Kronos::core)
kronos_enable_warnings(kronos_engine)
kronos_enable_sanitizer(kronos_engine)

add_executable(kronos_shell apps/kronos_shell.cpp)
target_link_libraries(kronos_shell PRIVATE Kronos::core)
kronos_enable_warnings(kronos_shell)
kronos_enable_sanitizer(kronos_shell)

if(KRONOS_BUILD_BENCHMARKS)
  add_executable(kronos_benchmark benchmarks/kronos_benchmark.cpp)
  target_link_libraries(kronos_benchmark PRIVATE Kronos::core)
  kronos_enable_warnings(kronos_benchmark)
  kronos_enable_sanitizer(kronos_benchmark)
endif()

if(BUILD_TESTING)
  find_package(GTest CONFIG QUIET)
  if(NOT TARGET GTest::gtest_main)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()

  add_executable(kronos_tests
    tests/engine_tests.cpp
    tests/example_task_tests.cpp
    tests/fifo_tests.cpp
    tests/protocol_tests.cpp
    tests/scheduler_tests.cpp
    tests/wal_tests.cpp)
  target_link_libraries(kronos_tests PRIVATE Kronos::core GTest::gtest_main)
  kronos_enable_warnings(kronos_tests)
  kronos_enable_sanitizer(kronos_tests)

  include(GoogleTest)
  gtest_discover_tests(kronos_tests DISCOVERY_TIMEOUT 30)

  if(UNIX)
    add_test(
      NAME KronosE2E.DaemonShellCrashRecovery
      COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/tests/e2e_test.sh
              $<TARGET_FILE:kronos_engine> $<TARGET_FILE:kronos_shell>)
    set_tests_properties(KronosE2E.DaemonShellCrashRecovery
                         PROPERTIES TIMEOUT 30)
  endif()
endif()
