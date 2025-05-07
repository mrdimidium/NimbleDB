// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_CBENCH_CBENCH_H_
#define NIMBLEDB_CBENCH_CBENCH_H_

#include <ftw.h>
#include <sys/resource.h>

#include <atomic>
#include <map>
#include <memory>

#include "base.h"

constexpr size_t kHistogramCount = 167;

constexpr size_t kSeedBoxSize = 2048;
const std::array<char, 64 /*2 + 10 + 26 + 26  */> alphabet = {
    '@', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
    'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
    'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_',
};

class Keyer {
 public:
  struct Options {
    bool binary;
    size_t count;
    size_t key_size, value_size;
    size_t spaces_count, sectors_count;
  };

  // A pool of pre-computed records.
  class Batch {
   public:
    Result Load(Record *p);

   private:
    friend Keyer;

    Batch(Keyer *gen, size_t pool_size);

    Keyer *gen_ = nullptr;
    char *pos_ = nullptr, *end_ = nullptr;

    std::unique_ptr<char[]> buf_;  // NOLINT(*-avoid-c-arrays)
  };

  static void Init(unsigned seed);

  Keyer(size_t kspace, size_t ksector, Options options);

  int Get(Record *p, bool key_only);
  Batch GetBatch(size_t batch_size) { return {this, batch_size}; }

 private:
  [[nodiscard]] size_t record_bytes() const;

  char *Fill(uint64_t *point, char *dst, size_t length) const;
  char *RecordPair(size_t vsize, uint64_t point, char *dst) const;

  // Maps x to y one-to-one. You can think of this as the hash function for a
  // number without collision (since the cardinality of the input and output
  // sets is equal). More about:
  // https://en.wikipedia.org/wiki/Injective_function
  [[nodiscard]] uint64_t Injection(uint64_t x) const;

  Options options_;
  size_t width_, base_, serial_;

  std::unique_ptr<char[]> buf_;  // NOLINT(*-avoid-c-arrays)

  static std::array<uint16_t, kSeedBoxSize> seed_box;
};

struct Stats {
  uintmax_t n, volume_sum;
  uintmax_t latency_sum_ns, latency_sum_square;
};

struct Usage {
  int64_t ram, disk;

  int64_t iops_read;
  int64_t iops_write;
  int64_t iops_page;

  int64_t cpu_user_ns;
  int64_t cpu_kernel_ns;

  static std::optional<Usage> Load(const std::string &datadir);
  static void PrintUsage(const Usage &start, const Usage &fihish);
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

  std::atomic_int merge_evo_ = 0;
  std::atomic_int workers_active_ = 0, workers_merged_ = 0;

  std::map<BenchType, Bucket> per_bench_;
};

class Worker {
 public:
  explicit Worker(size_t id, BenchTypeMask benchmask_mask, size_t key_space,
                  size_t key_sequence, const Keyer::Options &keyer_options,
                  Config *config, Driver *driver, Histogram *histograms,
                  const std::atomic_bool &failed);
  ~Worker();

  Worker(const Worker &) = delete;
  Worker(Worker &&) = delete;
  Worker &operator=(const Worker &) = delete;
  Worker &operator=(Worker &&) = delete;

  int FulFil();

 private:
  Result EvalCrud(Record *a, Record *b);
  Result EvalBenchmarkGST(BenchType bench);
  Result EvalBenchmarkCrud();
  Result EvalBenchmarkBatch(size_t &i);
  Result EvalBenchmarkIterate(size_t &i);

  void LogKeyNotFound(const char *op, Record *k) {
    Log("error: key {} not found ({}, {:#d}, {}+{})", k->key.data(), op, id_,
        key_space_, key_sequence_);
  }

  static std::atomic_int workers_count_, doers_done_;

  size_t id_ = 0, key_space_ = 0, key_sequence_ = 0;
  BenchTypeMask bench_mask_ = 0;

  const std::atomic_bool &g_failed_;

  Config *config_ = nullptr;
  Driver *driver_ = nullptr;

  Bucket hg_;
  Histogram *histograms_ = nullptr;

  Driver::Context ctx_{};

  std::unique_ptr<Keyer> gen_a_ = nullptr;
  std::unique_ptr<Keyer> gen_b_ = nullptr;
};

#endif  // NIMBLEDB_CBENCH_CBENCH_H_
