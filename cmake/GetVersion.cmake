# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

function(get_nimbledb_version version_var)
  file(READ "${CMAKE_CURRENT_SOURCE_DIR}/include/nimbledb/version.h" version_header_file)

  foreach(component MAJOR MINOR PATCH)
    string(REGEX MATCH "#define NIMBLEDB_${component} ([0-9]+)" _ ${version_header_file})
    set(NIMBLEDB_VERSION_${component} ${CMAKE_MATCH_1})
  endforeach()

  set(${version_var} "${NIMBLEDB_VERSION_MAJOR}.${NIMBLEDB_VERSION_MINOR}.${NIMBLEDB_VERSION_PATCH}" PARENT_SCOPE)
endfunction()
