# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

cmake_minimum_required(VERSION 3.31)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../cmake/")

project(cbench
  LANGUAGES CXX C
  DESCRIPTION "Benchmark to compare nimble performance with other embedded databases")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)

option(ENABLE_NIMBLEDB "Enable NimbleDB" ON)
if(ENABLE_NIMBLEDB)
  # This CMAakeLists.txt can be built independently or connected from the root.
  # If it is root then we specify nimble as a dependency,
  # otherwise the library is already available
  if(PROJECT_IS_TOP_LEVEL)
    add_subdirectory(
      ${CMAKE_CURRENT_SOURCE_DIR}/../
      ${CMAKE_CURRENT_BINARY_DIR}/_deps/nimbledb)
  endif()
endif()

# LMDB
option(ENABLE_LMDB "Enable LMDB" OFF)
if(ENABLE_LMDB)
  find_program(MAKE_PROGRAM NAMES gmake nmake make REQUIRED)

  set(TARGET liblmdb${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/liblmdb)
  set(SOURCE_SUBDIR libraries/liblmdb)

  include(ExternalProject)
  ExternalProject_Add(liblmdb
    GIT_REPOSITORY    https://github.com/LMDB/lmdb.git
    GIT_TAG           f20e41de09d97e4461946b7e26ec831d0c24fac7
    SOURCE_DIR        ${SOURCE_DIR}
    SOURCE_SUBDIR     ${SOURCE_SUBDIR}
    BUILD_BYPRODUCTS  ${SOURCE_DIR}/${SOURCE_SUBDIR}/${TARGET}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
      ${MAKE_PROGRAM} CC=${CMAKE_C_COMPILER} AR=${CMAKE_AR} ${TARGET}
    INSTALL_COMMAND   ""
    BUILD_IN_SOURCE   ON)

  add_library(liblmdb::liblmdb STATIC IMPORTED)
  add_dependencies(liblmdb::liblmdb liblmdb)
  include_directories(${SOURCE_DIR}/${SOURCE_SUBDIR})
  set_target_properties(liblmdb::liblmdb PROPERTIES
    IMPORTED_LOCATION ${SOURCE_DIR}/${SOURCE_SUBDIR}/${TARGET})
endif()

# RocksDB
option(ENABLE_ROCKSDB "Enable RocksDB" OFF)
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

# move benchmark runner to the library so that
# it is easy to test it independently of the drivers
add_library(cbenchlib STATIC
  "base.h" "base.cc"
  "cbench.h" "cbench.cc"
)
nimble_compile_warnings(cbenchlib)
target_include_directories(cbenchlib PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)
target_link_libraries(cbenchlib PUBLIC Threads::Threads CLI11::CLI11)

# The executable only links with the drivers and runs the benchmark library.
add_executable(cbench
  "main.cc" "driver_debug.cc"
  "$<$<BOOL:${ENABLE_NIMBLEDB}>:driver_nimbledb.cc>"
  "$<$<BOOL:${ENABLE_LMDB}>:driver_lmdb.cc>"
  "$<$<BOOL:${ENABLE_ROCKSDB}>:driver_rocksdb.cc>"
)
nimble_compile_warnings(cbench)
target_compile_definitions(cbench PRIVATE
  "$<$<BOOL:${ENABLE_NIMBLEDB}>:-DHAVE_NIMBLEDB>"
  "$<$<BOOL:${ENABLE_LMDB}>:-DHAVE_LMDB>"
  "$<$<BOOL:${ENABLE_ROCKSDB}>:-DHAVE_ROCKSDB>"
)
target_link_libraries(cbench PUBLIC cbenchlib
  "$<$<BOOL:${ENABLE_NIMBLEDB}>:nimbledb::nimbledb>"
  "$<$<BOOL:${ENABLE_LMDB}>:liblmdb::liblmdb>"
  "$<$<BOOL:${ENABLE_ROCKSDB}>:rocksdb>"
)
