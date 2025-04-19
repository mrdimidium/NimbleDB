# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

cmake_minimum_required(VERSION 3.15)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/")
include(GetVersion)

get_nimbledb_version(nimbledb_VERSION)
project(nimbledb
  VERSION ${nimbledb_VERSION}
  DESCRIPTION "Robust ACID in-process key-value database optimized for modern hardware"
  LANGUAGES CXX C ASM)

if(NOT CMAKE_BUILD_TYPE)
  if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
    set(DEFAULT_BUILD_TYPE "Debug")
    set(CMAKE_EXPORT_COMPILE_COMMANDS On)
  else()
    set(default_build_type "RelWithDebInfo")
  endif()
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "BUILD_TYPE is ${default_build_type}" FORCE)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()

set(CMAKE_CXX_STANDARD 20)  # 20 version required
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find dependencies
find_package(Threads REQUIRED)
list(APPEND THIRDPARTY_LIBS Threads::Threads)

# find_package(uring 2.0 REQUIRED)
# list(APPEND THIRDPARTY_LIBS uring::uring)

# Configure compiler warnings
if(MSVC)
  add_compile_options(/W4 /Zi /nologo /EHsc /GS /Gd /GF /fp:precise /Zc:wchar_t /Zc:forScope)
else()
  add_compile_options(-W -Wall -Wextra)
  add_compile_options(-Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-strict-aliasing -Wno-invalid-offsetof)
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-fno-omit-frame-pointer -momit-leaf-frame-pointer)
  endif()

  if(CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(-fno-builtin-memcmp)
  endif()
endif()

option(NIMBLEDB_FAIL_ON_WARNINGS "Treat compile warnings as errors" OFF)
if(NIMBLEDB_FAIL_ON_WARNINGS)
  if(MSVC)
    add_compile_options(/WX)
  else() # assume GCC or Clang
    add_compile_options(-Werror)
  endif()
endif()

option(NIMBLEDB_WITH_ASAN "build with ASAN" OFF)
if(NIMBLEDB_WITH_ASAN)
  add_link_options(-fsanitize=address)
  add_compile_options(-fsanitize=address)
endif()

option(NIMBLEDB_WITH_TSAN "build with TSAN" OFF)
if(NIMBLEDB_WITH_TSAN)
  add_link_options(-fsanitize=thread -Wl,-pie)
  add_compile_options(-fsanitize=thread -fPIC)
endif()

option(NIMBLEDB_WITH_UBSAN "build with UBSAN" OFF)
if(NIMBLEDB_WITH_UBSAN)
  add_link_options(-fsanitize=undefined)
  add_compile_options(-fsanitize=undefined)
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

# Build main library
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)

set(SOURCES "src/nimbledb.cc")

set(NIMBLEDB_STATIC_LIB nimble${ARTIFACT_SUFFIX})
set(NIMBLEDB_SHARED_LIB nimble-shared${ARTIFACT_SUFFIX})

add_library(${NIMBLEDB_STATIC_LIB} STATIC ${SOURCES})
target_include_directories(${NIMBLEDB_STATIC_LIB} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
target_link_libraries(${NIMBLEDB_STATIC_LIB} PRIVATE ${THIRDPARTY_LIBS})

if(WIN32)
  option(NIMBLEDB_BUILD_SHARED "Build shared versions of the NimbleDB libraries" OFF)
else()
  option(NIMBLEDB_BUILD_SHARED "Build shared versions of the NimbleDB libraries" ON)
endif()
if(NIMBLEDB_BUILD_SHARED)
  add_library(${NIMBLEDB_SHARED_LIB} SHARED ${SOURCES})
  target_include_directories(${NIMBLEDB_SHARED_LIB} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
  target_link_libraries(${NIMBLEDB_SHARED_LIB} PRIVATE ${THIRDPARTY_LIBS})

  set_target_properties(${NIMBLEDB_SHARED_LIB} PROPERTIES COMPILE_DEFINITIONS "NIMBLEDB_SHARED;NIMBLEDB_SHARED_EXPORTS")

  if(MSVC)
    set_target_properties(${NIMBLEDB_STATIC_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${NIMBLEDB_STATIC_LIB}.pdb")
    set_target_properties(${NIMBLEDB_SHARED_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${NIMBLEDB_SHARED_LIB}.pdb")
  else()
    set_target_properties(${NIMBLEDB_SHARED_LIB} PROPERTIES LINKER_LANGUAGE CXX
                          VERSION ${nimbledb_VERSION}
                          SOVERSION ${nimbledb_VERSION_MAJOR}
                          OUTPUT_NAME "nimbledb${ARTIFACT_SUFFIX}")
  endif()
endif()

if(NIMBLEDB_BUILD_SHARED)
  set(NIMBLEDB_LIB ${NIMBLEDB_SHARED_LIB})
else()
  set(NIMBLEDB_LIB ${NIMBLEDB_STATIC_LIB})
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  option(NIMBLEDB_WITH_CLANG_TIDY "Check code with clang-tidy" ON)
else()
  option(NIMBLEDB_WITH_CLANG_TIDY "Check code with clang-tidy" OFF)
endif()
if(NIMBLEDB_WITH_CLANG_TIDY)
  find_program(CLANG_TIDY_PATH NAMES "clang-tidy-19" "clang-tidy-18" "clang-tidy-17" "clang-tidy")

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

# Installation and packaging
if(NOT WIN32)
  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
      # Change default installation prefix on Linux to /usr
      set(CMAKE_INSTALL_PREFIX /usr CACHE PATH "Install path prefix, prepended onto install directories." FORCE)
    endif()
  endif()

  include(GNUInstallDirs)
  include(CMakePackageConfigHelpers)

  install(DIRECTORY include/nimbledb COMPONENT devel DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  install(TARGETS ${NIMBLEDB_STATIC_LIB}
          EXPORT NimbleDBTargets
          COMPONENT devel
          ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
          INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

  if(NIMBLEDB_BUILD_SHARED)
    install(TARGETS ${NIMBLEDB_SHARED_LIB}
            EXPORT NimbleDBTargets
            COMPONENT runtime
            ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
            RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
            LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
            INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  endif()
endif()

# Tests
option(NIMBLEDB_WITH_TESTS "build with tests" OFF)
if(NIMBLEDB_WITH_TESTS)
  include(GoogleTest)
  find_package(GTest REQUIRED)

  set(TESTS "src/nimbledb_test.cc")

  enable_testing()

  add_custom_target(nimbledb_tests COMMAND ${CMAKE_CTEST_COMMAND})

  foreach(sourcefile ${TESTS})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${exename}${ARTIFACT_SUFFIX} PROPERTIES
        EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
        OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${THIRDPARTY_LIBS} ${NIMBLEDB_LIB} GTest::gtest_main)

    gtest_discover_tests(${exename} DISCOVERY_TIMEOUT 120)
    add_dependencies(nimbledb_tests ${exename}${ARTIFACT_SUFFIX})
  endforeach()
endif()
