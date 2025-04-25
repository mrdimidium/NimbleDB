// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BENCH_RECORD_H_
#define NIMBLEDB_BENCH_RECORD_H_

#include <fcntl.h>
#include <ftw.h>
#include <linux/limits.h>
#include <pthread.h>
#include <strings.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <span>

struct Record {
  std::span<char> key, value;
};

struct RecordGen {
  RecordGen(unsigned kspace, unsigned ksector, unsigned vsize, unsigned vage);

  int Get(Record *p, bool key_only);

  static int Setup(bool printable, unsigned ksize, unsigned nspaces,
                   unsigned nsectors, uintmax_t period, size_t seed);

  uint64_t base_, serial_;
  unsigned vsize_, vage_, pair_bytes_;

  std::unique_ptr<char[]> buf_;  // NOLINT(*-avoid-c-arrays)
};

struct RecordPool {
  RecordPool(RecordGen *gen, unsigned pool_size);

  int Pull(Record *p);

 private:
  RecordGen *gen_ = nullptr;
  char *pos_ = nullptr, *end_ = nullptr;

  std::unique_ptr<char[]> buf_;  // NOLINT(*-avoid-c-arrays)
};

#endif  // NIMBLEDB_BENCH_RECORD_H_
