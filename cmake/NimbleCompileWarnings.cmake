# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

option(NIMBLEDB_WITH_CLANG_TIDY "Check code with clang-tidy" OFF)
if(NIMBLEDB_WITH_CLANG_TIDY)
  if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
    find_program(CLANG_TIDY_PATH NAMES "clang-tidy-20" "clang-tidy")

    if(CLANG_TIDY_PATH)
      message(STATUS "Using clang-tidy: ${CLANG_TIDY_PATH}.
        The checks will be run during the build process.
        See the .clang-tidy file in the root to configure the checks.")

      set(USE_CLANG_TIDY ON)

      # clang-tidy requires assertions to guide the analysis
      # Note that NDEBUG is set implicitly by CMake for non-debug builds
      add_definitions(-UNDEBUG)

      # We do not install the check globally,
      # but enable it on our targets to avoid warnings on dependencies
      # set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
    else()
      message(${RECONFIGURE_MESSAGE_LEVEL} "clang-tidy is not found")
    endif()
  else()
    message(WARNING "clang-tidy is only supported when built with the clang")
  endif()
endif()

option(NIMBLEDB_FAIL_ON_WARNINGS "Treat compile warnings as errors" OFF)

function(nimble_compile_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE
        /W4 /Zi /nologo /EHsc /GS /Gd /GF /fp:precise
        /Zc:wchar_t /Zc:forScope)
  else()
    target_compile_options(${target} PRIVATE
        -W -Wall -Wextra
        -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable
        -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers
        -Wno-strict-aliasing -Wno-invalid-offsetof)
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
      target_compile_options(${target} PRIVATE
          -fno-omit-frame-pointer -momit-leaf-frame-pointer)
    endif()

    if(${CMAKE_CXX_COMPILER_ID} MATCHES "GNU")
      target_compile_options(${target} PRIVATE -fno-builtin-memcmp)
    endif()
  endif()

  if(USE_CLANG_TIDY)
    set_target_properties(${target}
        PROPERTIES CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
  endif()

  if(NIMBLEDB_FAIL_ON_WARNINGS)
    if(MSVC)
      target_compile_options(${target} PRIVATE /WX)
    else() # assume GCC or Clang
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
endfunction()
