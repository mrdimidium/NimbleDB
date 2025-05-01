// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BENCH_CONFIG_H_
#define NIMBLEDB_BENCH_CONFIG_H_

#include <pthread.h>
#include <strings.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <set>
#include <string>
#include <string_view>

enum BenchType : uint8_t {
  kTypeSet,
  kTypeGet,
  kTypeDelete,
  kTypeIterate,
  kTypeBatch,
  kTypeCrud,
  kTypeMaxCode,
};
std::string_view to_string(BenchType b);
BenchType BenchTypeFromString(const std::string_view &name);

enum BenchSyncMode : uint8_t {
  kModeSync,
  kModeLazy,
  kModeNoSync,
};
std::string_view to_string(BenchSyncMode syncmode);
std::optional<BenchSyncMode> BenchSyncModeFromString(const std::string &str);

enum BenchWalMode : uint8_t { kWalDefault, kWalEnabled, kWalDisabled };
std::string_view to_string(BenchWalMode walmode);
std::optional<BenchWalMode> BenchWalModeFromString(const std::string &str);

// clang-format off
const size_t kBenchMaskRead = 0U
  | 1ULL << kTypeBatch
  | 1ULL << kTypeCrud
  | 1ULL << kTypeGet
  | 1ULL << kTypeIterate;
const size_t kBenchMaskWrite = 0U
  | 1ULL << kTypeBatch
  | 1ULL << kTypeCrud
  | 1ULL << kTypeDelete
  | 1ULL << kTypeSet;
const size_t kBenchMask2Keyspace = 0U
  | 1ULL << kTypeBatch
  | 1ULL << kTypeCrud;
// clang-format on

struct Config {
  explicit Config(std::string supported) : supported_(std::move(supported)) {}

  std::string driver_name;
  std::string dirname = "./_cdb_tmp";
  std::set<BenchType> benchmarks{kTypeGet, kTypeSet};

  uintmax_t count = 1000000;
  size_t key_size = 16, value_size = 32;

  BenchWalMode walmode = kWalDefault;
  BenchSyncMode syncmode = kModeLazy;

  size_t rthr = sysconf(_SC_NPROCESSORS_ONLN),
         wthr = sysconf(_SC_NPROCESSORS_ONLN);

  int kvseed = 42;
  int nrepeat = 1;
  int batch_length = 500;

  bool binary = false;
  bool separate = false;
  bool ignore_keynotfound = false;
  bool continuous_completing = false;

  void Print() const;

 private:
  std::string supported_;
};

#endif  // NIMBLEDB_BENCH_CONFIG_H_
