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

#include "config.h"
#include "record.h"

class Driver {
 public:
  using Context = void *;

  [[nodiscard]] virtual std::string_view GetName() const = 0;

  virtual int Open(Config *config, const std::string &datadir) = 0;
  virtual int Close() = 0;

  virtual Context ThreadNew() = 0;
  virtual void ThreadDispose(Context) = 0;

  virtual int Begin(Context, BenchType) = 0;
  virtual int Next(Context, BenchType, Record *kv) = 0;
  virtual int Done(Context, BenchType) = 0;

  static std::string Supported();
  static Driver *GetDriverFor(std::string_view name);
};

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
