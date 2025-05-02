// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BENCH_DRIVER_H_
#define NIMBLEDB_BENCH_DRIVER_H_

#include <pthread.h>
#include <strings.h>

#include <cassert>
#include <cstring>
#include <format>
#include <string>
#include <string_view>

#include "base.h"
#include "config.h"

// Specifies a key and value pair to write or read in the driver.
// The driver itself does not own the write memory and only reads/copies to it.
struct Record {
  std::span<char> key;
  std::span<char> value;
};

// Driver is a universal interface to different databases.
// To add support for a new database, create a `dirver_<dbname>.cc` file and
// implement the interface. See `driver_debug.cc` for reference.
class Driver {
 public:
  using Context = void *;

  // Returns human-readable driver name for logs and config.
  // Prefer lowercase for name.
  [[nodiscard]] virtual std::string_view GetName() const = 0;

  // Opens a connection to the database, called only once for all threads.
  virtual Result Open(Config *config, const std::string &datadir) = 0;

  // Closes the connection to the database,
  // called only once at the very end.
  virtual Result Close() = 0;

  // Creates an opaque context for a each thread
  // Access to contexts is not synchronized, if the database requires
  // synchronization it is the driver's responsibility.
  virtual Context ThreadNew() = 0;

  // Clears the context of a specific thread.
  virtual void ThreadDispose(Context) = 0;

  // Execute a block of data operations in the obvious order:
  // Begin->Next->..->Next->Done() Begin and End are needed to prepare
  // transactions and complex scenarios such as block recording if the database
  // supports it.
  virtual Result Begin(Context, BenchType) = 0;
  virtual Result Next(Context, BenchType, Record *kv) = 0;
  virtual Result Done(Context, BenchType) = 0;

  // Returns a list of supported driver names.
  // If the driver was excluded during build, this method will also exclude it.
  static std::string Supported();

  // Returns a pointer to the driver singleton if available, or nullptr if no
  // such driver exists. It's thread safe.
  static Driver *GetDriverFor(std::string_view name);
};
static_assert(std::is_abstract<Driver>(),
              "Driver must be clear abstract class");

Driver *driver_debug();

#if HAVE_NIMBLEDB
Driver *driver_nimbledb();
#endif

#if HAVE_LMDB
Driver *driver_lmdb();
#endif

#if HAVE_ROCKSDB
Driver *driver_rocksdb();
#endif

inline Driver *Driver::GetDriverFor(std::string_view name) {
  if (name == driver_debug()->GetName()) {
    return driver_debug();
  }

#if HAVE_NIMBLEDB
  if (name == driver_nimbledb()->GetName()) {
    return driver_nimbledb();
  }
#endif

#if HAVE_LMDB
  if (name == driver_lmdb()->GetName()) {
    return driver_lmdb();
  }
#endif

#if HAVE_ROCKSDB
  if (name == driver_rocksdb()->GetName()) {
    return driver_rocksdb();
  }
#endif

  return nullptr;
}

inline std::string Driver::Supported() {
  size_t len = 0;
  std::string list;

  const auto setup_driver = [&](const std::string_view name) {
    list += std::format("{}{}", (len > 0) ? ", " : "", name);
    len += 1;
  };

  setup_driver(driver_debug()->GetName());

#if HAVE_NIMBLEDB
  setup_driver(driver_nimbledb()->GetName());
#endif

#if HAVE_LMDB
  setup_driver(driver_lmdb()->GetName());
#endif

#if HAVE_ROCKSDB
  setup_driver(driver_rocksdb()->GetName());
#endif

  return list;
}

#endif  // NIMBLEDB_BENCH_DRIVER_H_
