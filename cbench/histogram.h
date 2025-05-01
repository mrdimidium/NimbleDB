// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BENCH_HISTOGRAM_H_
#define NIMBLEDB_BENCH_HISTOGRAM_H_

#include <unistd.h>

#include <atomic>
#include <map>
#include <mutex>
#include <type_traits>

#include "base.h"
#include "config.h"

constexpr size_t kHistogramCount = 167;

#define LINE_12_100(M)                                                      \
  M * 12, (M) * 14, (M) * 16, (M) * 18, (M) * 20, (M) * 25, (M) * 30,       \
      (M) * 35, (M) * 40, (M) * 45, (M) * 50, (M) * 60, (M) * 70, (M) * 80, \
      (M) * 90, (M) * 100

constexpr std::array<uintmax_t, kHistogramCount> kBuckets = {
    9,
    LINE_12_100(1ULL),
    LINE_12_100(10ULL),
    LINE_12_100(100ULL),
    LINE_12_100(US),
    LINE_12_100(US * 10),
    LINE_12_100(US * 100),
    LINE_12_100(MS),
    LINE_12_100(MS * 10),
    LINE_12_100(MS * 100),
    LINE_12_100(S),
    S * 5 * 60,
    S * 30 * 60,
    S * 3600 * 4,
    S * 3600 * 8,
    S * 3600 * 24,
    ~0ULL,
};

#undef LINE_12_100

struct Stats {
  uintmax_t n, volume_sum;
  uintmax_t latency_sum_ns, latency_sum_square;
};

class Bucket;
class Histogram;

class Bucket {
 public:
  explicit Bucket(Histogram *registry, bool is_worker = true);
  ~Bucket();

  Bucket(const Bucket &) = delete;
  Bucket(Bucket &&) = default;
  Bucket &operator=(const Bucket &) = delete;
  Bucket &operator=(Bucket &&) = default;

  void Reset(BenchType bench);
  void Add(Time t0, size_t volume);

 private:
  friend Histogram;

  Histogram *registry_;

  bool enabled_ = false, is_worker_ = false;
  BenchType bench_ = kTypeMaxCode;

  int merge_evo_ = 0;

  Time min_ = 0, max_ = 0;
  Time whole_min_ = ~0ULL, whole_max_ = ~0ULL;
  Time checkpoint_ns_ = 0, begin_ns_ = 0, end_ns_ = 0;

  Stats last_{}, acc_{};
  std::array<uintmax_t, kHistogramCount> buckets_{};
};

class Histogram {
 public:
  explicit Histogram(const std::set<BenchType> &benchmarks);

  int Summarize(Time now = 0);
  void Merge(Bucket *src);

  void Print();

 private:
  friend Bucket;

  void MergeLocked(Bucket &src, Time now);

  std::mutex mu_;

  Time starting_point_ = 0;
  Time checkpoint_ns_ = 0;

  int64_t enable_mask_ = 0;
  std::atomic_int merge_evo_ = 0;
  std::atomic_int workers_active_ = 0, workers_merged_ = 0;

  std::map<BenchType, Bucket> per_bench_;
};

#endif  // NIMBLEDB_BENCH_HISTOGRAM_H_
