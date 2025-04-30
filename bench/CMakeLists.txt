# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

cmake_minimum_required(VERSION 3.31)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../cmake/")

project(dbbench
  LANGUAGES CXX C
  DESCRIPTION "Benchmark to compare nimble performance with other embedded databases")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(DBBENCH_HEADERS
  "base.h"
  "driver.h"
  "config.h"
  "record.h"
  "histogram.h"
)

set(DBBENCH_FILES
  "driver_debug.cc"
  "config.cc"
  "record.cc"
  "histogram.cc"
  "main.cc"
)

# LMDB
option(ENABLE_LMDB "Enable LMDB" ON)
if(ENABLE_LMDB)
  include(ExternalProject)
  ExternalProject_Add(liblmdb
    GIT_REPOSITORY    https://github.com/LMDB/lmdb.git
    GIT_TAG           f20e41de09d97e4461946b7e26ec831d0c24fac7
    GIT_SHALLOW       ON
    SOURCE_DIR        ${PROJECT_BINARY_DIR}/_deps/liblmdb
    CONFIGURE_COMMAND ""
    BUILD_COMMAND     ${CMAKE_MAKE_PROGRAM} -C libraries/liblmdb/ liblmdb${CMAKE_STATIC_LIBRARY_SUFFIX}
    INSTALL_COMMAND   ""
    BUILD_IN_SOURCE   ON)

  add_library(liblmdb::liblmdb STATIC IMPORTED)
  add_dependencies(liblmdb::liblmdb liblmdb)
  include_directories(${PROJECT_BINARY_DIR}/_deps/liblmdb/libraries/liblmdb)
  set_target_properties(liblmdb::liblmdb PROPERTIES IMPORTED_LOCATION
      ${PROJECT_BINARY_DIR}/_deps/liblmdb/libraries/liblmdb/liblmdb${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

# RocksDB
option(ENABLE_ROCKSDB "Enable RocksDB" ON)
if(ENABLE_ROCKSDB)
  include(FetchContent)
  FetchContent_Declare(RocksDB SYSTEM
    GIT_REPOSITORY https://github.com/facebook/rocksdb.git
    GIT_TAG        4b2122578e475cb88aef4dcf152cccd5dbf51060 # version=10.2.0
  )

  set(WITH_TESTS OFF CACHE INTERNAL BOOL)
  set(WITH_ALL_TESTS OFF CACHE INTERNAL BOOL)
  set(WITH_GFLAGS OFF CACHE INTERNAL BOOL)
  set(WITH_TOOLS OFF CACHE INTERNAL BOOL)
  set(WITH_CORE_TOOLS OFF CACHE INTERNAL BOOL)
  set(WITH_TRACE_TOOLS OFF CACHE INTERNAL BOOL)
  set(WITH_BENCHMARK_TOOLS OFF CACHE INTERNAL BOOL)
  set(FAIL_ON_WARNINGS OFF CACHE INTERNAL BOOL)

  FetchContent_MakeAvailable(RocksDB)
endif()

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_package(Threads REQUIRED)

include(FindCLI11)
find_package(CLI11 REQUIRED)

include_directories("${PROJECT_SOURCE_DIR}")
include_directories("${PROJECT_BINARY_DIR}")

add_executable(bench
  ${DBBENCH_HEADERS} ${DBBENCH_FILES}
  "$<$<BOOL:ENABLE_LMDB>:driver_lmdb.cc>"
  "$<$<BOOL:ENABLE_ROCKSDB>:driver_rocksdb.cc>"
)
target_compile_definitions(bench PRIVATE
  "$<$<BOOL:ENABLE_LMDB>:-DHAVE_LMDB>"
  "$<$<BOOL:ENABLE_ROCKSDB>:-DHAVE_ROCKSDB>"
)
target_link_libraries(bench PUBLIC
  Threads::Threads CLI11::CLI11
  "$<$<BOOL:ENABLE_LMDB>:liblmdb::liblmdb>"
  "$<$<BOOL:ENABLE_ROCKSDB>:rocksdb>"
)
