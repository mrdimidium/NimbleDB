// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BASE_H_
#define NIMBLEDB_BASE_H_

#include <functional>
#include <span>
#include <string>

#ifndef NDEBUG
  #define NIMBLEDB_ASSERT_STATUS_CHECKED
#endif

#ifdef NIMBLEDB_ASSERT_STATUS_CHECKED
  #include <format>
  #include <iostream>

  #if __cpp_lib_stacktrace >= 202011L
    #include <stacktrace>
  #endif
#endif

#if defined(__linux__)
  // Developer:     Open source
  // Processors:    x86, x86-64, POWER, etc.
  // Distributions: Centos, Debian, Fedora, OpenSUSE, RedHat, Ubuntu
  #define NIMBLEDB_OS_LINUX
#endif

#if defined(__APPLE__) && defined(__MACH__)
  // Developer:     Apple and open source
  // Processors:    x86, x86-64, ARM
  // Distributions: OSX, iOS, Darwin
  #define NIMBLEDB_OS_DARWIN
#endif

#if defined(_WIN64) || defined(_WIN32)
  // Developer:     Microsoft
  // Processors:    x86, x86-64
  // Distributions: Windows XP, Vista, 7, 8, 10
  #define NIMBLEDB_OS_WINDOWS
#endif

#if defined(_AIX)
  // Developer:     IBM
  // Processors:    POWER
  // Distributions: AIX
  #define NIMBLEDB_OS_AIX
#endif

#if defined(__hpux)
  // Developer:     Hewlett-Packard
  // Processors:    Itanium
  // Distributions: HP-UX
  #define NIMBLEDB_OS_HPUX
#endif

#if defined(__sun) && defined(__SVR4)
  // Developer:     Oracle and open source
  // Processors:    x86, x86-64, SPARC
  // Distributions: Oracle Solaris, Open Indiana
  #define NIMBLEDB_OS_SOLARIS
#endif

#if defined(NIMBLEDB_SHARED)
  #if defined(WIN32) && !defined(__MINGW32__)
    #if defined(NIMBLEDB_SHARED_EXPORTS)
      #define NIMBLEDB_EXPORT __declspec(dllexport)
    #else
      #define NIMBLEDB_EXPORT __declspec(dllimport)
    #endif
  #else
    #if defined(NIMBLEDB_SHARED_EXPORTS)
      #define NIMBLEDB_EXPORT __attribute__((visibility("default")))
    #else
      #define NIMBLEDB_EXPORT
    #endif
  #endif
#else
  #define NIMBLEDB_EXPORT
#endif

// For testing purposes
#if NIMBLEDB_NAMESPACE == 42
  #undef NIBMLEDB_NAMESPACE
#endif

#ifndef NIMBLEDB_NAMESPACE
  #define NIMBLEDB_NAMESPACE nimbledb
#endif

namespace NIMBLEDB_NAMESPACE {

// A Status wrap the result of an operation.
// It may indicate success or an error with an associated error message.
class NIMBLEDB_EXPORT Status {
 public:
  Status() = default;  // Create a success status.
  ~Status() {
#ifdef NIMBLEDB_ASSERT_STATUS_CHECKED
    if (!checked_) {
      std::cout << std::format("Failed to check Status {}\n",
                               static_cast<void*>(this));
  #if __cpp_lib_stacktrace >= 202011L
      std::cout << std::stacktrace::current();
  #endif
      std::abort();
    }
#endif  // NIMBLEDB_ASSERT_STATUS_CHECKED
  }

  // Copy the specified status.
  Status(const Status& s);
  Status& operator=(const Status& s);
  Status(Status&& s) noexcept;
  Status& operator=(Status&& s) noexcept;
  bool operator==(const Status& rhs) const;
  bool operator!=(const Status& rhs) const;

  // In case of intentionally swallowing an error, user must explicitly call
  // this function. That way we are easily able to search the code to find where
  // error swallowing occurs.
  void PermitUncheckedError() const { MarkChecked(); }

  void MustCheck() const {
#ifdef NIMBLEDB_ASSERT_STATUS_CHECKED
    checked_ = false;
#endif  // NIMBLEDB_ASSERT_STATUS_CHECKED
  }

  // Thread-safe version of POSIX strerror()
  static std::string ErrnoToString(int err = errno);

  enum Code : unsigned char {
    kOk = 0,
    kNoMemory = 1,
    kIOError = 2,
    kCorruptedDatafile = 3,
    kMaxCode = kCorruptedDatafile + 1,
  };
  [[nodiscard]] Code code() const {
    MarkChecked();
    return code_;
  }

  enum Severity : unsigned char {
    kNoError = 0,
    kSoftError = 1,
    kHardError = 2,
    kFatalError = 3,
    kUnrecoverableError = 4,
  };
  [[nodiscard]] Severity severity() const {
    MarkChecked();
    return severity_;
  }

  Status(const Status& s, Severity sev);

  Status(Code _code, Severity _sev, const std::string& msg)
      : Status(_code, msg, "", _sev) {}

  // Returns a C style string indicating the message of the Status
  [[nodiscard]] const std::string& state() const {
    MarkChecked();
    return state_;
  }

  static Status Ok() { return {}; }
  static Status NoMemory(const std::string& msg = "",
                         const std::string& msg2 = "") {
    return {kNoMemory, msg, msg2};
  }
  static Status IOError(const std::string& msg = "",
                        const std::string& msg2 = "") {
    return {kIOError, msg, msg2};
  }
  static Status CorruptedDatafile(const std::string& msg = "",
                                  const std::string& msg2 = "") {
    return {kCorruptedDatafile, msg, msg2};
  }

  [[nodiscard]] bool IsOk() const { return code() == kOk; }
  [[nodiscard]] bool IsOOM() const { return code() == kNoMemory; }
  [[nodiscard]] bool IsIOError() const { return code() == kIOError; }
  [[nodiscard]] bool IsCorruptedDatafile() const {
    return code() == kCorruptedDatafile;
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  [[nodiscard]] std::string ToString() const;

 protected:
  Code code_ = kOk;
  Severity severity_ = kNoError;

  std::string state_;

#ifdef NIMBLEDB_ASSERT_STATUS_CHECKED
  mutable bool checked_ = false;
#endif  // NIMBLEDB_ASSERT_STATUS_CHECKED

  explicit Status(Code _code) : code_(_code) {}

  Status(Code _code, const std::string& msg, const std::string& msg2,
         Severity sev = kNoError);

  void MarkChecked() const {
#ifdef NIMBLEDB_ASSERT_STATUS_CHECKED
    checked_ = true;
#endif  // NIMBLEDB_ASSERT_STATUS_CHECKED
  }
};

inline Status::Status(const Status& s)
    : code_(s.code_), severity_(s.severity_), state_(s.state_) {
  s.MarkChecked();  // NOLINT(cert-oop58-cpp)
}

inline Status::Status(const Status& s, Severity sev)
    : code_(s.code_), severity_(sev), state_(s.state_) {
  s.MarkChecked();
}

inline Status& Status::operator=(const Status& s) {
  if (this != &s) {
    s.MarkChecked();  // NOLINT(cert-oop58-cpp)
    MustCheck();

    code_ = s.code_;
    severity_ = s.severity_;
    state_ = s.state_;
  }
  return *this;
}

inline Status::Status(Status&& s) noexcept : Status() {
  s.MarkChecked();
  *this = std::move(s);
}

inline Status& Status::operator=(Status&& s) noexcept {
  if (this != &s) {
    s.MarkChecked();
    MustCheck();

    code_ = s.code_;
    s.code_ = kOk;

    severity_ = s.severity_;
    s.severity_ = kNoError;

    state_ = std::move(s.state_);
  }
  return *this;
}

inline bool Status::operator==(const Status& rhs) const {
  MarkChecked();
  rhs.MarkChecked();
  return (code_ == rhs.code_);
}

inline bool Status::operator!=(const Status& rhs) const {
  MarkChecked();
  rhs.MarkChecked();
  return !(*this == rhs);
}

template <typename... Args>
using Callback = std::function<void(Status, Args...)>;

using RWBuffer = std::span<std::byte>;
using ROBuffer = std::span<const std::byte>;

}  // namespace NIMBLEDB_NAMESPACE

#endif  // NIMBLEDB_BASE_H_
