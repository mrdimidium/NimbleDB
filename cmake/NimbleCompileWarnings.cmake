# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

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

    if(CMAKE_COMPILER_IS_GNUCXX)
      target_compile_options(${target} PRIVATE -fno-builtin-memcmp)
    endif()
  endif()

  if(NIMBLEDB_FAIL_ON_WARNINGS)
    if(MSVC)
      target_compile_options(${target} PRIVATE /WX)
    else() # assume GCC or Clang
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
endfunction()
