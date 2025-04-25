// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/system.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "nimbledb/base.h"

#if defined(NIMBLEDB_OS_WINDOWS)
  #include <io.h>
  #include <windows.h>

  // Use `open` instead of `_open` on Windows
  // https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warning-level-3-c4996
  #pragma warning(disable : 4996)
#else
  #include <sys/types.h>
  #include <unistd.h>
#endif

namespace NIMBLEDB_NAMESPACE {

int File::Flags::GetMask() const {
  unsigned mask = 0;

  // NOLINTBEGIN(*-braces-around-statements)
  if (read && write) {
    mask |= O_RDWR;
  } else {
    if (read) mask |= O_RDONLY;
    if (write) mask |= O_WRONLY;
  }

  if (excl) mask |= O_EXCL;
  if (creat) mask |= O_CREAT;
  if (trunc) mask |= O_TRUNC;
  if (append) mask |= O_APPEND;

#if defined(NIMBLEDB_OS_WINDOWS)
  if (cloexec) mask |= O_NOINHERIT;
#else
  if (cloexec) mask |= O_CLOEXEC;
#endif

  // For LINUX, the O_DIRECT flag has to be included.
  //
  // For MacOS O_DIRECT isn't available. Instead, fcntl(fd, F_NOCACHE, 1)
  // looks to be the canonical solution where fd is the file descriptor of the
  // file.
  //
  // For Windows, there is a flag called FILE_FLAG_NO_BUFFERING as the
  // counterpart in Windows of O_DIRECT.
#if defined(NIMBLEDB_OS_WINDOWS)
  if (direct) mask |= FILE_FLAG_NO_BUFFERING;
#elif defined(NIMBLEDB_OS_LINUX)
  if (direct) mask |= O_DIRECT;
#endif
  // NOLINTEND(*-braces-around-statements)

  assert((void("Flags must include access modes"), read || write));

  assert((void("Mutually exclusive flags: `creat`, `excl`"), !creat || !excl));

  assert(
      (void("Mutually exclusive flags: `trunc`, `append`"), !trunc || !append));

  return static_cast<int>(mask);
}

Status File::GetFileSize(int64_t* size) const {
  assert(!closed_);

  struct stat stat_buf{};
  if (fstat(fd_, &stat_buf) != 0) {
    return Status::IOError("Couldn't get datafile size",
                           Status::ErrnoToString());
  }
  *size = stat_buf.st_size;

  return Status::Ok();
}

Status File::Close() {
  closed_ = true;

  if (!closed_) {
    if (const int rc = close(fd_); rc != 0) {
      return Status::IOError("couldn't close file", Status::ErrnoToString());
    }
  }

  return Status::Ok();
}

void File::Read(RWBuffer buffer, off_t offset,
                const Callback<>& callback) const {
  assert(!closed_);

  auto new_offset = lseek(fd_, offset, SEEK_SET);
  if (new_offset < 0) {
    callback(Status::IOError("couldn't lseek to file position",
                             Status::ErrnoToString()));
    return;
  }

  auto bytes = read(fd_, buffer.data(), buffer.size());
  if (bytes < 0) {
    callback(
        Status::IOError("couldn't read from file", Status::ErrnoToString()));
    return;
  }

  if (static_cast<size_t>(bytes) != buffer.size()) {
    callback(Status::IOError("couldn't read all data"));
    return;
  }

  callback(Status::Ok());
}

void File::Write(ROBuffer buffer, off_t offset,
                 const Callback<>& callback) const {
  assert(!closed_);

  auto new_offset = lseek(fd_, offset, SEEK_SET);
  if (new_offset < 0) {
    callback(Status::IOError("couldn't lseek to file position",
                             Status::ErrnoToString()));
    return;
  }

  auto bytes = write(fd_, buffer.data(), buffer.size());
  if (bytes < 0) {
    callback(
        Status::IOError("couldn't write to file", Status::ErrnoToString()));
    return;
  }

  if (static_cast<size_t>(bytes) != buffer.size()) {
    callback(Status::IOError("couldn't write all data"));
    return;
  }

  callback(Status::Ok());
}

void File::Sync(SyncMode mode, const Callback<>& callback) const {
  assert(!closed_);

  int rc = -1;
  switch (mode) {
    case SyncMode::kFull:
#ifdef F_FULLFSYNC
      // If the FULLFSYNC failed, fall back to attempting an fsync().
      // It shouldn't be possible for fullfsync to fail on the local
      // file system (on OSX), so failure indicates that FULLFSYNC
      // isn't supported for this file system. So, attempt an fsync
      // and (for now) ignore the overhead of a superfluous fcntl call.
      if (fcntl(fd_, F_FULLFSYNC, 0) == 0) {
        break;
      }
      [[fallthrough]];
#endif

    case SyncMode::kNormal:
#if defined(NIMBLEDB_OS_WINDOWS)
      rc = _commit(fd_);
#else
      rc = fsync(fd_);
#endif
      break;

    case SyncMode::kDataOnly:
#if defined(NIMBLEDB_OS_DARWIN)
      // fdatasync() on HFS+ doesn't yet flush the file size if it changed
      // correctly so currently we default to the macro that redefines fdatasync
      // to fsync
      rc = fsync(fd_);
#elif defined(NIMBLEDB_OS_WINDOWS)
      // It would be better to use FLUSH_FLAGS_FILE_DATA_SYNC_ONLY for this
      rc = _commit(fd_);
#else
      rc = fdatasync(fd_);
#endif
      break;
  }

  if (rc < 0) {
    callback(Status::IOError("couldn't fsync file", Status::ErrnoToString()));
    return;
  }

  callback(Status::Ok());
}

// static
Status OS::Create(std::unique_ptr<OS>* ioptr) {
  OS* ptr = new (std::nothrow) OS;
  if (ptr == nullptr) {
    return Status::NoMemory();
  }

  ioptr->reset(ptr);
  return Status::Ok();
}

OS::OS() = default;
OS::~OS() {
  if (!closed_) {
    std::ignore = Close().state();
  }
}

Status OS::OpenDatafile(std::string_view file_path, File::Flags flags,
                        std::unique_ptr<File>* file_ptr) {
  const int fd = open(std::string(file_path).c_str(), flags.GetMask(), 0644);
  if (fd < 0) {
    return Status::IOError("couldn't open file", Status::ErrnoToString());
  }

#if defined(NIMBLEDB_OS_DARWIN)
  if (flags.direct) {
    int result = fcntl(fd, F_NOCACHE, 1);
    if (result < 0) {
      return Status::IOError("failed to enable direct io for file",
                             Status::ErrnoToString());
    }
  }
#endif

  *file_ptr = std::make_unique<File>(file_path, fd);
  return Status::Ok();
}

}  // namespace NIMBLEDB_NAMESPACE
