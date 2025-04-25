// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <fcntl.h>
#include <ftw.h>
#include <histogram.h>
#include <linux/limits.h>
#include <pthread.h>
#include <strings.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "base.h"

namespace {

constexpr size_t kIintervalStat = S;
constexpr size_t kIntervalMerge = (S / 100);

std::string snpf_val(long double val, std::string_view unit = "") {
  constexpr auto dec =
      std::to_array({'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y', 'R', 'Q'});
  constexpr auto inc =
      std::to_array({'m', 'u', 'n', 'p', 'f', 'a', 'z', 'y', 'r', 'q'});

  char suffix = ' ';

  for (const auto *it = dec.begin(); val > 995 && it != dec.end(); ++it) {
    val *= 1e-3;
    suffix = *it;
  }
  for (const auto *it = inc.begin(); val < 1 && it != inc.end(); ++it) {
    val *= 1e3;
    suffix = *it;
  }

  return suffix == ' ' ? std::format("{:0.3f}{}", val, unit)
                       : std::format("{:0.3f}{}{}", val, suffix, unit);
}

std::string snpf_lat(Time ns) {
  return snpf_val(static_cast<long double>(ns) / S, "s");
}

}  // namespace

Bucket::Bucket(Histogram *registry, bool is_worker)
    : is_worker_(is_worker),
      registry_(registry),
      merge_evo_(registry_->merge_evo_) {
  if (is_worker) {
    registry_->workers_active_ += 1;
  }
}

Bucket::~Bucket() {
  if (is_worker_) {
    if (merge_evo_ == registry_->merge_evo_ + 1) {
      registry_->workers_merged_ -= 1;
    }

    registry_->workers_active_ -= 1;
  }
}

void Bucket::Reset(BenchType bench) {
  int merge_evo = merge_evo;

  enabled_ = true;
  bench_ = bench;

  min_ = ~0ULL;
  whole_min_ = ~0ULL;
  checkpoint_ns_ = begin_ns_ = end_ns_ = GetTimeNow();
  merge_evo_ = merge_evo;
}

void Bucket::Add(Time t0, size_t volume) {
  Time now = GetTimeNow();
  Time latency = now - t0;

  if (begin_ns_ == 0) {
    begin_ns_ = t0;
  }

  end_ns_ = now;
  acc_.latency_sum_ns += latency;
  acc_.latency_sum_square += latency * latency;
  acc_.n++;
  acc_.volume_sum += volume;

  min_ = std::min(min_, latency);
  max_ = std::max(max_, latency);

  size_t begin = 0;
  size_t medium = 0;
  size_t finish = kHistogramCount - 1;

  while (true) {
    medium = begin / 2 + finish / 2;

    if (medium == begin) {
      for (medium = finish; medium > begin; medium--) {
        if (kBuckets[medium - 1] < latency) {
          break;
        }
      }

      break;
    }

    if (kBuckets[medium - 1] < latency) {
      begin = medium;
    } else {
      finish = medium;
    }
  }

  buckets_[medium]++;

  if (merge_evo_ != registry_->merge_evo_ ||
      now - checkpoint_ns_ < kIntervalMerge) {
    return;
  }

  if (registry_->mu_.try_lock()) {
    registry_->MergeLocked(*this, now);
    registry_->mu_.unlock();

    checkpoint_ns_ = now;
    min_ = ~0ULL;
    max_ = 0;
    last_ = acc_;

    for (size_t i = 0; i < kHistogramCount; i++) {
      buckets_[i] = 0;
    }
  }
}

Histogram::Histogram(const std::set<BenchType> &benchmarks) {
  starting_point_ = GetTimeNow();
  checkpoint_ns_ = starting_point_;

  for (size_t i = IA_SET; i < IA_MAX; ++i) {
    auto bench = static_cast<BenchType>(i);
    auto [it, _] = per_bench_.emplace(bench, Bucket(this, false));

    if (benchmarks.contains(bench)) {
      it->second.Reset(bench);
    }
  }
}

int Histogram::Summarize(Time now) {
  if (now == 0) {
    now = GetTimeNow();
  }

  if (now - checkpoint_ns_ < kIintervalStat) {
    return -1;
  }

  if (workers_active_ > ++workers_merged_) {
    return 0;
  }

  if (workers_active_ != workers_merged_) {
    Fatal(__FUNCTION__);
  }

  if (checkpoint_ns_ == starting_point_) {
    std::string line = "     time";

    for (auto &[_, h] : per_bench_) {
      if (h.enabled_) {
        line += std::format(
            " | {:5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}", "bench",
            "rps", "min", "avg", "rms", "max", "vol", "#N");
      }
    }

    Log("{}", line);
  }

  long double timepoint = static_cast<long double>(now - starting_point_) / S;
  std::string line = std::format("{:9.3f}", timepoint);

  Time wall_ns = now - checkpoint_ns_;
  auto wall = static_cast<long double>(wall_ns) / S;
  checkpoint_ns_ = now;

  for (auto &[_, h] : per_bench_) {
    if (!h.enabled_) {
      continue;
    }

    std::string name{BenchTypeToString(h.bench_)};
    uintmax_t n = h.acc_.n - h.last_.n;
    uintmax_t vol = h.acc_.volume_sum - h.last_.volume_sum;

    line += std::format(" | {:>5}:", name);
    if (n != 0) {
      Time rms = static_cast<Time>(
          sqrt(static_cast<double>(h.acc_.latency_sum_square -
                                   h.last_.latency_sum_square) /
               static_cast<double>(n)));
      Time avg = (h.acc_.latency_sum_ns - h.last_.latency_sum_ns) / n;
      auto rps = static_cast<long double>(n) / wall;
      auto bps = static_cast<long double>(vol) / wall;

      line += std::format("{:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
                          snpf_val(rps, ""), snpf_lat(h.min_), snpf_lat(avg),
                          snpf_lat(rms), snpf_lat(h.max_), snpf_val(bps, "bps"),
                          snpf_val(h.acc_.n, ""));
    } else {
      line += std::format("{:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
                          "-", "-", "-", "-", "-", "-", "-");
    }

    h.whole_min_ = std::min(h.whole_min_, h.min_);
    h.min_ = ~0ULL;

    h.whole_max_ = std::max(h.whole_max_, h.max_);
    h.max_ = 0;

    h.last_ = h.acc_;
  }

  Log("{}", line);

  assert(workers_active_ == workers_merged_);
  workers_merged_ = 0;
  merge_evo_ += 1;
  return 1;
}

void Histogram::MergeLocked(Bucket &src, Time now) {
  auto &dst = per_bench_.at(src.bench_);

  if (dst.enabled_ && src.acc_.n != src.last_.n) {
    dst.acc_.latency_sum_ns +=
        src.acc_.latency_sum_ns - src.last_.latency_sum_ns;
    dst.acc_.latency_sum_square +=
        src.acc_.latency_sum_square - src.last_.latency_sum_square;
    dst.acc_.volume_sum += src.acc_.volume_sum - src.last_.volume_sum;
    dst.acc_.n += src.acc_.n - src.last_.n;

    int i;
    for (i = 0; i < kHistogramCount; i++) {
      dst.buckets_[i] += src.buckets_[i];
    }

    if (dst.begin_ns_ == 0 || dst.begin_ns_ > src.begin_ns_) {
      dst.begin_ns_ = src.begin_ns_;
    }
    dst.end_ns_ = std::max(dst.end_ns_, src.end_ns_);
    dst.min_ = std::min(dst.min_, src.min_);
    dst.max_ = std::max(dst.max_, src.max_);

    if (src.merge_evo_ == src.registry_->merge_evo_ &&
        src.registry_->Summarize(now) >= 0) {
      src.merge_evo_ += 1;
    }
  }
}

void Histogram::Merge(Bucket *src) {
  std::lock_guard<std::mutex> guard(mu_);
  MergeLocked(*src, GetTimeNow());
}

void Histogram::Print() {
  for (auto &[_, h] : per_bench_) {
    if (!h.enabled_ || h.acc_.n == 0) {
      continue;
    }

    std::string name{BenchTypeToString(h.bench_)};
    Log("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "
        "{}({})",
        name, h.acc_.n);
    Log("[ {:>9}  {:>9} ] {:>13} {:>8} {:>10}", "ltn_from", "ltn_to",
        "ops_count", "%", "p%");
    Log("----------------------------------------------------------");

    size_t total_count = 0;
    for (size_t i = 0; i < kHistogramCount; i++) {
      if (h.buckets_[i] == 0) {
        continue;
      }

      total_count += h.buckets_[i];
      auto factor = 1e2 / static_cast<long double>(h.acc_.n);

      auto ltn_from = snpf_lat(i > 0 ? kBuckets[i - 1] : 0);
      auto ltn_to = snpf_lat(kBuckets[i] - 1);
      auto ops_count = h.buckets_[i];
      auto percent = factor * ops_count;
      auto percentile = factor * total_count;

      Log("[ {:>9}, {:>9} ] {:13} {:7.2f}% {:9.4f}%", ltn_from, ltn_to,
          ops_count, percent, percentile);
    }
    Log("----------------------------------------------------------");

    Log("total:       {:>9}  {:13}", snpf_lat(h.acc_.latency_sum_ns),
        total_count);
    Log("min latency: {:>9}/op", snpf_lat(h.whole_min_));
    Log("avg latency: {:>9}/op", snpf_lat(h.acc_.latency_sum_ns / h.acc_.n));
    Log("rms latency: {:>9}/op",
        snpf_lat(sqrt(h.acc_.latency_sum_square / h.acc_.n)));
    Log("max latency: {:>9}/op", snpf_lat(h.whole_max_));

    auto wall = static_cast<double>(h.end_ns_ - h.begin_ns_) / S;
    Log(" throughput: {:>7}ops/s", snpf_val(h.acc_.n / wall));
  }
}
