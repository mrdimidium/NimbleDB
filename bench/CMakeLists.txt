# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

cmake_minimum_required(VERSION 3.31)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../cmake/")
include(FindCLI11)

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

set(DBBENCH_THIRDPARTY "")
add_custom_target(dbbench_deps)

# LMDB
option(ENABLE_LMDB "Enable LMDB" ON)
if(ENABLE_LMDB)
  set(LMDB_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/lmdb/libraries/liblmdb)

  if(${PROJECT_BINARY_DIR} STREQUAL ${PROJECT_SOURCE_DIR})
    add_custom_command(
      OUTPUT ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb/liblmdb${CMAKE_SHARED_LIBRARY_SUFFIX}
      COMMAND ${CMAKE_MAKE_PROGRAM} -j ${MAKE_NJOB} -C ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb liblmdb${CMAKE_SHARED_LIBRARY_SUFFIX}
      DEPENDS ${CMAKE_BINARY_DIR}/CMakeCache.txt)
  else()
    add_custom_command(
      OUTPUT ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb/liblmdb${CMAKE_SHARED_LIBRARY_SUFFIX}
      COMMAND ${CMAKE_COMMAND} -E make_directory ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb
      COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/lmdb/libraries/liblmdb ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb
      COMMAND ${CMAKE_MAKE_PROGRAM} -j ${MAKE_NJOB} -C ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb liblmdb${CMAKE_SHARED_LIBRARY_SUFFIX}
      DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt)
  endif()

  add_custom_target(liblmdb ALL
    DEPENDS ${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb/liblmdb${CMAKE_SHARED_LIBRARY_SUFFIX})

  list(APPEND DBBENCH_FILES "driver_lmdb.cc")
  list(APPEND DBBENCH_THIRDPARTY
    "${PROJECT_BINARY_DIR}/lmdb/libraries/liblmdb/liblmdb${CMAKE_SHARED_LIBRARY_SUFFIX}")

  include_directories(${LMDB_INCLUDE_DIRS})

  add_definitions(-DHAVE_LMDB)
  add_dependencies(dbbench_deps liblmdb)
endif()

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_package(Threads REQUIRED)

include_directories("${PROJECT_SOURCE_DIR}")
include_directories("${PROJECT_BINARY_DIR}")

add_executable(bench ${DBBENCH_FILES} ${DBBENCH_HEADERS})
add_dependencies(bench dbbench_deps)

target_link_libraries(bench
  PRIVATE CLI11::CLI11 Threads::Threads
  PUBLIC ${DBBENCH_THIRDPARTY} "${CMAKE_THREAD_LIBS_INIT}"
)
