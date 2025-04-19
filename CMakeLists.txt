# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

cmake_minimum_required(VERSION 3.31)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/")
include(GetVersion)
include(NimbleCompileWarnings)

get_nimbledb_version(nimbledb_VERSION)
project(nimbledb
  VERSION     ${nimbledb_VERSION}
  LANGUAGES   CXX
  DESCRIPTION "Robust ACID in-process key-value database for modern hardware")

set(CMAKE_CXX_STANDARD 20)  # 20 version required
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(NIMBLEDB_FILES "src/nimbledb.cc")
set(NIMBLEDB_TESTS "src/nimbledb_test.cc")

if(NOT CMAKE_BUILD_TYPE)
  if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
    set(DEFAULT_BUILD_TYPE "Debug")
    set(CMAKE_EXPORT_COMPILE_COMMANDS On)
  else()
    set(DEFAULT_BUILD_TYPE "RelWithDebInfo")
  endif()
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING
      "BUILD_TYPE is ${default_build_type}" FORCE)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()

# RTTI is enables in debug and disables it in release.
message(STATUS "Enabling RTTI in Debug builds only (default)")
if(MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_RELEASE} /GR")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /GR-")
else()
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_RELEASE} -frtti")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-rtti")
endif()

# Configure compiler warnings
set(NIMBLEDB_SANITIZER "" CACHE STRING
    "Build with one of the supported sanitizers: asan, tsan, ubsan")
if(NIMBLEDB_SANITIZER STREQUAL "asan")
  add_link_options(-fsanitize=address,leak,pointer-compare,pointer-subtract)
  add_compile_options(-fsanitize=address,leak,pointer-compare,pointer-subtract)
endif()
if(NIMBLEDB_SANITIZER STREQUAL "tsan")
  add_link_options(-fsanitize=thread -Wl,-pie)
  add_compile_options(-fsanitize=thread -fPIC)
endif()
if(NIMBLEDB_SANITIZER STREQUAL "ubsan")
  add_link_options(-fsanitize=undefined)
  add_compile_options(-fsanitize=undefined)
endif()

# Build main library
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)

option(BUILD_SHARED_LIBS "Build using shared libraries" ON)
if(BUILD_SHARED_LIBS)
  set(NIMBLEDB_LIB nimble-shared${ARTIFACT_SUFFIX})
else()
  set(NIMBLEDB_LIB nimble${ARTIFACT_SUFFIX})
endif()

add_library(${NIMBLEDB_LIB} ${NIMBLEDB_FILES})
nimble_compile_warnings(${NIMBLEDB_LIB})

target_include_directories(${NIMBLEDB_LIB} PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
target_link_libraries(${NIMBLEDB_LIB} PRIVATE ${THIRDPARTY_LIBS})

if(BUILD_SHARED_LIBS)
  target_compile_definitions(${NIMBLEDB_LIB}
    PUBLIC  "NIMBLEDB_SHARED"
    PRIVATE "NIMBLEDB_SHARED_EXPORTS")

  if(MSVC)
    set_target_properties(${NIMBLEDB_STATIC_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${NIMBLEDB_STATIC_LIB}.pdb")
    set_target_properties(${NIMBLEDB_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${NIMBLEDB_LIB}.pdb")
  else()
    set_target_properties(${NIMBLEDB_LIB} PROPERTIES LINKER_LANGUAGE CXX
                          VERSION ${nimbledb_VERSION}
                          SOVERSION ${nimbledb_VERSION_MAJOR}
                          OUTPUT_NAME "nimbledb${ARTIFACT_SUFFIX}")
  endif()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  option(NIMBLEDB_WITH_CLANG_TIDY "Check code with clang-tidy" ON)
else()
  option(NIMBLEDB_WITH_CLANG_TIDY "Check code with clang-tidy" OFF)
endif()
if(NIMBLEDB_WITH_CLANG_TIDY)
  find_program(CLANG_TIDY_PATH NAMES
      "clang-tidy-19" "clang-tidy-18" "clang-tidy-17" "clang-tidy")

  if(CLANG_TIDY_PATH)
    message(STATUS
            "Using clang-tidy: ${CLANG_TIDY_PATH}.
            The checks will be run during the build process.
            See the .clang-tidy file in the root to configure the checks.")

    set(USE_CLANG_TIDY ON)

    # clang-tidy requires assertions to guide the analysis
    # Note that NDEBUG is set implicitly by CMake for non-debug builds
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -UNDEBUG")

    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}")
  else()
    message(${RECONFIGURE_MESSAGE_LEVEL} "clang-tidy is not found")
  endif()
endif()

# Tests
option(NIMBLEDB_WITH_TESTS "build with tests" OFF)
if(NIMBLEDB_WITH_TESTS)
  include(GoogleTest)
  find_package(GTest REQUIRED)

  enable_testing()

  add_custom_target(nimbledb_tests COMMAND ${CMAKE_CTEST_COMMAND})

  foreach(sourcefile ${NIMBLEDB_TESTS})
    get_filename_component(exename ${sourcefile} NAME_WE)

    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    nimble_compile_warnings(${exename}${ARTIFACT_SUFFIX})

    set_target_properties(${exename}${ARTIFACT_SUFFIX} PROPERTIES
        EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
        OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX}
        ${NIMBLEDB_LIB} ${THIRDPARTY_LIBS} GTest::gtest_main)

    gtest_discover_tests(${exename} DISCOVERY_TIMEOUT 120)
    add_dependencies(nimbledb_tests ${exename}${ARTIFACT_SUFFIX})
  endforeach()
endif()
