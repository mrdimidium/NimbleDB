// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_NIMBLEDB_H_
#define NIMBLEDB_NIMBLEDB_H_

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

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

namespace nimbledb {

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

// Cross-platform asynchronous file interface.
//
// This class is not thread-safe and doesn't own the read/write buffers.
// You must keep the buffers valid from the method call until they return from
// the callback.
class NIMBLEDB_EXPORT File {
 public:
  struct DeviceAttrs {
    size_t sector_size;
  };

  // Cross-Platform options for opening/creating a file.
  // Some flags may be ignored depending on the system.
  struct Flags {
    // Access modes for read, write or both.
    bool read = true, write = true;

    bool excl = false, creat = false;
    bool trunc = false, append = false;

    bool cloexec = false;
    bool direct = false;

    [[nodiscard]] int GetMask() const;
  };

  enum class DirectIO : uint8_t { kRequired, kOptional, kDisabled };
  enum class SyncMode : uint8_t { kFull, kNormal, kDataOnly };

  explicit File(std::string_view filename, int fd = -1)
      : filename_(filename), fd_(fd) {}

  ~File() {
    if (!closed_) {
      std::ignore = Close();
    }
  }

  File(const File&) = delete;
  File(File&&) = delete;
  File& operator=(const File&) = delete;
  File& operator=(File&&) = delete;

  Status GetFileSize(int64_t* size_ptr) const;
  Status GetDeviceAttrs(DeviceAttrs* attrs_ptr) const;

  void Read(RWBuffer buffer, off_t offset, const Callback<>& callback) const;
  void Write(ROBuffer buffer, off_t offset, const Callback<>& callback) const;

  void Sync(SyncMode mode, const Callback<>& callback) const;
  void Truncate(int64_t size, const Callback<>& callback) const;

  Status Close();

 protected:
  const std::string filename_;

  bool closed_ = false;
  const int fd_ = -1;

  static size_t BufferLimit(size_t buffer_len) {
#if defined(NIMBLEDB_OS_LINUX)
    // Linux limits how much may be written in a `pwrite()/pread()` call, which
    // is `0x7ffff000` on both 64-bit and 32-bit systems, due to using a signed
    // C int as the return value, as well as stuffing the errno codes into the
    // last `4096` values
    constexpr size_t kLimit = 0x7ffff000;
#elif defined(NIMBLEDB_OS_DARWIN)
    // Darwin can write `0x7fffffff` bytes, more than that returns `EINVAL`
    constexpr size_t kLimit = std::numeric_limits<int32_t>::max();
#else
    // The corresponding POSIX limit
    constexpr size_t kLimit = std::numeric_limits<size_t>::max();
#endif

    return std::min(kLimit, buffer_len);
  }
};

// Interface between the NimbleDB and the underlying operating system.
//
// All methods accept a callback so that the implementation can
// rely on the async kernel API (e.g. io_uring or kqueue), but the moment
// of calling the callback isn't defined (for a naive implementation it can
// happen immediately with the thread blocking).
//
// This class does not implement any caching, you should build your own page
// cache higher up.
class NIMBLEDB_EXPORT OS {
 public:
  OS(OS&&) = delete;
  OS(const OS&) = delete;
  OS& operator=(OS&&) = delete;
  OS& operator=(const OS&) = delete;

  virtual ~OS();

  static Status Create(std::unique_ptr<OS>* ioptr);

  // Pass all queued submissions to the kernel and peek for completions.
  Status Tick() {  // NOLINT(*-convert-member-functions-to-static)
    // no-op, stil unimplemented
    return Status::Ok();
  }

  Status Close() {  // NOLINT(*-convert-member-functions-to-static)
    // no-op, stil unimplemented
    closed_ = true;
    return Status::Ok();
  }

  Status OpenDatafile(std::string_view file_path, File::Flags flags,
                      std::unique_ptr<File>* file_ptr);

 protected:
  explicit OS();

  bool closed_ = false;
};

struct NIMBLEDB_EXPORT Options {};

class NIMBLEDB_EXPORT DB {
 public:
  // No copying & moving allowed
  DB(DB&) = delete;
  DB(DB&&) = delete;
  DB& operator=(DB&&) = delete;
  void operator=(const DB&) = delete;

  virtual ~DB();

  // Open database, return shared_ptr to DB instance
  static Status Open(std::string_view filename, const Options& options,
                     std::shared_ptr<DB>* dbptr);

  // Synchronizes any unfinished state to disk and graceful close database.
  // Use this method instead of the implicit destructor to handle errors.
  Status Close();

  // Find key in database, return std::nullopt if not found
  void Get(std::string_view key,
           const Callback<std::optional<std::string>>& callback);

  // Add key to database, overrite if key exists
  void Put(std::string_view key, std::string_view value,
           const Callback<bool /* rewritten */>& callback);

  // Delete key from database. Returns succes if key not found.
  void Delete(std::string_view key, const Callback<bool /* found */>& callback);

  // For debug purposes, return a graphical representation of the tree
  void DebugRenderBTree(std::ostream& in);

 protected:
  struct BTreeNode;
  struct BTreeNodeKey;
  struct BTreeNodeVal;

  using NodeId = int64_t;
  using NodeType = enum : uint8_t { kInterior, kLeaf };

  DB(Options options, std::unique_ptr<OS> os, std::unique_ptr<File> datafile);

  void NodeSplit(const std::shared_ptr<BTreeNode>& x, NodeId child_id);
  void NodeInsert(NodeId node_id, std::string_view k, std::string_view v);

  auto AddNode(NodeType page_type) -> std::shared_ptr<BTreeNode>;
  auto GetNode(NodeId id) -> std::shared_ptr<BTreeNode>;
  Status Sync();

  bool closed_ = false;

  const Options options_;

  std::unique_ptr<OS> os_ = nullptr;
  std::unique_ptr<File> datafile_ = nullptr;

  NodeId pages_ = 0;
  NodeId root_id_ = 0;
  std::map<NodeId, std::shared_ptr<BTreeNode>> nodes_;
};

}  // namespace nimbledb

#endif  // NIMBLEDB_NIMBLEDB_H_
