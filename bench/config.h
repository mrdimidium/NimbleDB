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
  IA_SET,
  IA_GET,
  IA_DELETE,
  IA_ITERATE,
  IA_BATCH,
  IA_CRUD,
  IA_MAX,
};
std::string_view BenchTypeToString(BenchType b);
BenchType BenchTypeFromString(const std::string_view &name);

enum BenchSyncMode : uint8_t {
  IA_SYNC,
  IA_LAZY,
  IA_NOSYNC,
};
std::string_view BenchSyncModeToString(BenchSyncMode syncmode);
std::optional<BenchSyncMode> BenchSyncModeFromString(const std::string &str);

enum BenchWalMode : uint8_t { IA_WAL_INDEF, IA_WAL_ON, IA_WAL_OFF };
std::string_view BenchWalModeToString(BenchWalMode walmode);
std::optional<BenchWalMode> BenchWalModeFromString(const std::string &str);

// clang-format off
const size_t kBenchMaskRead = 0U
  | 1ULL << IA_BATCH
  | 1ULL << IA_CRUD
  | 1ULL << IA_GET
  | 1ULL << IA_ITERATE;
const size_t kBenchMaskWrite = 0U
  | 1ULL << IA_BATCH
  | 1ULL << IA_CRUD
  | 1ULL << IA_DELETE
  | 1ULL << IA_SET;
const size_t kBenchMask2Keyspace = 0U
  | 1ULL << IA_BATCH
  | 1ULL << IA_CRUD;
// clang-format on

struct Config {
  explicit Config(std::string supported) : supported_(std::move(supported)) {}

  std::string driver_name;
  std::string dirname = "./_dbbench.tmp";
  std::set<BenchType> benchmarks{IA_GET, IA_SET};

  uintmax_t count = 1000000;
  size_t key_size = 16, value_size = 32;

  BenchWalMode walmode = IA_WAL_INDEF;
  BenchSyncMode syncmode = IA_LAZY;

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
