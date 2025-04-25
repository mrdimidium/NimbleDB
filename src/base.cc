// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/base.h"

#include <array>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <string>

namespace {

#if !(defined(_WIN32) && (defined(__MINGW32__) || defined(_MSC_VER)))

// XSI-compatible strerror_r
[[maybe_unused]] std::string invoke_strerror_r(
    int (*strerror_r)(int, char*, size_t), int err, char* buf, size_t blen) {
  const int r = strerror_r(err, buf, blen);

  // OSX/FreeBSD use EINVAL and Linux uses -1 so just check for non-zero
  if (r != 0) {
    return std::format("Unknown error {} (strerror_r failed with error {})",
                       err, errno);
  }

  return buf;
}

// GNU strerror_r
[[maybe_unused]] std::string invoke_strerror_r(
    char* (*strerror_r)(int, char*, size_t), int err, char* buf, size_t blen) {
  return strerror_r(err, buf, blen);
}

#endif  // !(defined(NIMBLEDB_OS_WINDOWS) && (defined(__MINGW32__) ||
        // defined(_MSC_VER)))

}  // namespace

namespace NIMBLEDB_NAMESPACE {

// Based on folly/string.cpp:
// https://github.com/facebook/folly/blob/58b6c5/folly/String.cpp#L494
//
// There are two variants of `strerror_r` function, one returns `int`, and
// another returns `char*`. Selecting proper version using preprocessor macros
// portably is extremely hard.
//
// For example, on Android function signature depends on `__USE_GNU` and
// `__ANDROID_API__` macros (https://git.io/fjBBE).
//
// We are using C++ overloading trick: we pass a pointer of
// `strerror_r` to `invoke_strerror_r` function, and C++ compiler
// selects proper function.
std::string Status::ErrnoToString(int err) {
  thread_local static std::array<char, 1024> buf{'\0'};

  // https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man3/strerror_r.3.html
  // http://www.kernel.org/doc/man-pages/online/pages/man3/strerror.3.html
#if defined(_WIN32) && (defined(__MINGW32__) || defined(_MSC_VER))
  // mingw64 has no strerror_r, but Windows has strerror_s, which C11 added as
  // well. So maybe we should use this across all platforms (together with
  // strerrorlen_s). Note strerror_r and _s have swapped args.
  int r = strerror_s(buf.data(), buf.size(), err);
  if (r != 0) {
    return std::format("Unknown error {} (strerror_r failed with error {})",
                       err, errno);
  }

  return buf.data();
#else
  // NOLINTNEXTLINE(misc-include-cleaner), it's a clinag-tidy bug
  return invoke_strerror_r(strerror_r, err, buf.data(), buf.size());
#endif
}

Status::Status(Code _code, const std::string& msg, const std::string& msg2,
               Severity sev)
    : code_(_code), severity_(sev) {
  assert(_code != kMaxCode);

  if (!msg2.empty()) {
    state_ = msg + ": " + msg2;
  } else {
    state_ = msg;
  }
}

std::string Status::ToString() const {
  MarkChecked();

  std::string result;
  switch (code_) {
    case kOk:
      return "OK";
    case kNoMemory:
      return "Out of memory";
    default: {
      // This should not happen since `code_` should be a valid non-`kMaxCode`
      // member of the `Code` enum. The above switch-statement should have had a
      // case assigning `type` to a corresponding string.
      assert(false);
      result = std::format("Unknown code({}): ", static_cast<int>(code()));
    }
  }

  return result + (!state_.empty() ? state_ : "(empty message)");
}

}  // namespace NIMBLEDB_NAMESPACE
