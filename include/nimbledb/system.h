// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_SYSTEM_H_
#define NIMBLEDB_SYSTEM_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "nimbledb/base.h"

namespace NIMBLEDB_NAMESPACE {

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

}  // namespace NIMBLEDB_NAMESPACE

#endif  // NIMBLEDB_SYSTEM_H_
