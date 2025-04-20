// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/nimbledb.h"

#include <array>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <format>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace {

#if !(defined(NIMBLEDB_OS_WINDOWS) && \
      (defined(__MINGW32__) || defined(_MSC_VER)))

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

namespace nimbledb {

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
// So we are using C++ overloading trick: we pass a pointer of
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
  // Using any strerror_r
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

class DB::DBImpl {
 public:
  explicit DBImpl(std::string filename) : filename_(std::move(filename)) {}

  bool Search(const std::string& key, std::string& value) {
    if (map_.contains(key)) {
      value = map_[key];
      return true;
    }

    return false;
  }

  void Insert(const std::string& key, const std::string& value) {
    map_[key] = value;
  }

 private:
  std::string filename_;
  std::map<std::string, std::string> map_;
};

// static

Status DB::Open(std::string_view filename, const Options& options,
                std::shared_ptr<DB>* dbptr) {
  std::ignore = options;

  if (auto* ptr = new (std::nothrow) DB(filename)) {
    dbptr->reset(ptr);
  } else {
    return Status::NoMemory();
  }

  return Status::Ok();
}

Status DB::Close() { return Status::Ok(); }

DB::DB(std::string_view filename) : filename_(filename) {}

DB::~DB() = default;

void DB::Get(std::string_view key,
             const Callback<std::optional<std::string>>& callback) {
  if (!map_.contains(std::string(key))) {
    callback(Status::Ok(), std::nullopt);
    return;
  }

  callback(Status::Ok(), map_[std::string(key)]);
}

void DB::Put(std::string_view key, std::string_view value,
             const Callback<bool /* rewritten */>& callback) {
  const bool rewritten = map_.contains(std::string(key));
  map_[std::string(key)] = value;
  callback(Status::Ok(), rewritten);
}

void DB::Delete(std::string_view key,
                const Callback<bool /* found */>& callback) {
  const bool found = map_.contains(std::string(key));
  map_.erase(std::string(key));
  callback(Status::Ok(), found);
}

}  // namespace nimbledb
