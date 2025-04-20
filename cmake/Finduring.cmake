# Copyright 2025 Nikolay Govorov. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at LICENSE file in the root.

find_package(PkgConfig REQUIRED)
pkg_search_module(uring liburing)

find_path(URING_INCLUDE_DIR NAMES liburing.h HINTS ${URING_PC_INCLUDEDIR} ${URING_PC_INCLUDE_DIRS})
mark_as_advanced(URING_INCLUDE_DIR)

find_library(URING_LIBRARIES NAMES uring HINTS ${URING_PC_LIBDIR} ${URING_PC_LIBRARY_DIRS})
mark_as_advanced(URING_LIBRARIES)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uring
  VERSION_VAR URING_PC_VERSION
  REQUIRED_VARS URING_LIBRARIES URING_INCLUDE_DIR)

if(URING_FOUND AND NOT TARGET uring::uring)
  add_library(uring::uring UNKNOWN IMPORTED)
  set_target_properties(uring::uring PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${URING_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION ${URING_LIBRARIES})
endif ()
