// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "cbench.h"

#include <endian.h>
#include <ftw.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <format>
#include <memory>
#include <mutex>
#include <numbers>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "base.h"

namespace {

// Create a bitmask where the first n bytes are included.
constexpr uint64_t Bitmask(uint64_t n) { return ~UINT64_C(0) >> (64 - n); }

// Aligns a `n` to a `bytes` boundary, 8 byte aligned by default.
constexpr uint64_t Align(uint64_t n, size_t bytes = sizeof(uint64_t)) {
  return (n + bytes - 1) & ~(bytes - 1);
}

constexpr uint64_t Remix4Tail(uint64_t point) {
  /* fast and dirty remix */
  return point ^ (((point << 47ULL) | (point >> 17ULL)) +
                  UINTMAX_C(7015912586649315971));
}

}  // namespace

std::array<uint16_t, kSeedBoxSize> Keyer::seed_box{};

Result Keyer::Batch::Load(Record *p) {
  if (std::cmp_less(end_ - pos_, gen_->record_bytes())) {
    return Result::kUnexpectedError;
  }

  p->key = std::span(pos_, gen_->options_.key_size);
  pos_ += !gen_->options_.binary ? p->key.size() + 1 : Align(p->key.size());

  p->value = std::span<char>();
  if (gen_->options_.value_size > 0) {
    p->value = std::span(pos_, gen_->options_.value_size);
    pos_ +=
        !gen_->options_.binary ? p->value.size() + 1 : Align(p->value.size());
  }

  return Result::kOk;
}

Keyer::Batch::Batch(Keyer *gen, size_t pool_size) {
  assert(pool_size < 1 || pool_size > INT_MAX / 2);

  auto bytes = gen->record_bytes() * pool_size;
  buf_.reset(new char[bytes]);

  char *dst = buf_.get();
  for (size_t i = 0; i < pool_size; ++i) {
    dst = gen_->RecordPair(gen->options_.value_size, gen->base_ + gen->serial_,
                           dst);
    gen->serial_ = (gen->serial_ + 1) % gen_->options_.count;
  }

  gen_ = gen;
  pos_ = buf_.get();
  end_ = dst;

  assert(dst == buf_.get() + bytes);
}

void Keyer::Init(unsigned int seed) {
  if (seed == 0) {
    seed = time(nullptr);
  }

  std::ranlux48_base random{seed};
  std::uniform_int_distribution<uint16_t> uniform_dist(0, UINT16_MAX);

  for (auto &i : seed_box) {
    i = uniform_dist(random);
  }
}

Keyer::Keyer(size_t kspace, size_t ksector, Options options)
    : options_(options) {
  auto maxkey = options_.count * options_.spaces_count;
  assert(maxkey >= 2);

  size_t bits = 0;
  if (maxkey < Bitmask(16)) {
    bits = 16;
  } else if (maxkey < Bitmask(24)) {
    bits = 24;
  } else if (maxkey < Bitmask(32)) {
    bits = 32;
  } else if (maxkey < Bitmask(40)) {
    bits = 40;
  } else if (maxkey < Bitmask(48)) {
    bits = 48;
  } else if (maxkey < Bitmask(56)) {
    bits = 56;
  } else if (maxkey < UINT64_MAX) {
    bits = 64;
  } else {
    auto required = ceil(log(static_cast<double>(maxkey)) / std::numbers::ln2);
    Fatal(
        "key-gen: {} sector of {} items is too huge, unable provide by "
        "{}-bit arithmetics, at least {} required",
        options_.sectors_count, options_.count, sizeof(uintmax_t),
        ceil(required));
  }

  const long double bytes4maxkey =
      log(static_cast<double>(Bitmask(bits))) /
      log(!options_.binary ? alphabet.size() : 256);
  if (bytes4maxkey > options_.key_size) {
    Fatal(
        "record-gen: key-length {} is insufficient for {} sectors of {} {} "
        "items, at least {} required",
        options_.key_size, options_.sectors_count,
        options_.binary ? "binary" : "printable", options_.count,
        ceill(bytes4maxkey));
  }

  Log("key-gen: using {} bits, up to {} keys", bits, maxkey);

  width_ = bits / 8;

  buf_.reset(new char[record_bytes()]);

  base_ = kspace * options_.count;
  serial_ = 0;
  if (ksector != 0) {
    serial_ = options_.count * ksector / options_.sectors_count;
    serial_ %= options_.count;
  }
}

int Keyer::Get(Record *p, bool key_only) {
  p->key = std::span(buf_.get(), options_.key_size);

  if (key_only) {
    p->value = std::span<char>();
  } else {
    if (!options_.binary) {
      p->value =
          std::span(buf_.get() + (options_.key_size + 1), options_.value_size);
    } else {
      p->value =
          std::span(buf_.get() + Align(options_.key_size), options_.value_size);
    }
  }

  const uint64_t point = base_ + serial_;
  serial_ = (serial_ + 1) % options_.count;

  char *end = RecordPair(p->value.size(), point, buf_.get());
  assert(end == buf_.get() + record_bytes());
  std::ignore = end;

  return 0;
}

[[nodiscard]] size_t Keyer::record_bytes() const {
  return !options_.binary
             ? options_.key_size + 1 +
                   ((options_.value_size > 0) ? options_.value_size + 1 : 0)
             : Align(options_.key_size) + Align(options_.value_size);
}

char *Keyer::Fill(uint64_t *point, char *dst, size_t length) const {
#ifdef DEBUG_KEY_GENERATION
  if (!options_.binary) {
    dst += snprintf(dst, options_.key_size + 1, "%0*" PRIu64, options_.key_size,
                    *point) +
           1;
  } else {
    dst += snprintf(dst, Align(options_.key_size), "%0*" PRIu64,
                    static_cast<int>(Align(options_.key_size)), *point);
  }
#else
  size_t left = width_ * 8;

  if (!options_.binary) {
    static_assert(alphabet.size() == 64);

    uint64_t acc = *point;
    for (;;) {
      *dst++ = alphabet[acc & 63U];
      if (--length == 0) {
        break;
      }
      acc >>= 6U;
      left -= 6;
      if (left < 6) {
        acc = *point = Remix4Tail(*point + acc);
        left = width_ * 8;
      }
    }
    *dst++ = 0;
  } else {
    auto *p = reinterpret_cast<uint64_t *>(dst);
    for (;;) {
      *p++ = htole64(*point);
      length -= 8;
      if (length <= 0) {
        break;
      }
      do {
        *point = Remix4Tail(*point);
        left += left;
      } while (left < 64);
    }
    dst = reinterpret_cast<char *>(p);
  }
#endif

  return dst;
}

char *Keyer::RecordPair(size_t vsize, uint64_t point, char *dst) const {
  point = Injection(point);
  dst = Fill(&point, dst, options_.key_size);

  if (vsize != 0) {
    point = Remix4Tail(point);
    dst = Fill(&point, dst, vsize);
  }

  return dst;
}

// Maps x to y one-to-one. You can think of this as the hash function for a
// number without collision (since the cardinality of the input and output
// sets is equal). More about:
// https://en.wikipedia.org/wiki/Injective_function
uint64_t Keyer::Injection(uint64_t x) const {
  // magic 'fractal' prime, it has enough one-bits and prime by mod
  // 2^{8,16,24,32,40,48,56,64}
  x += UINTMAX_C(10042331536242289283);

  // stirs lower bits
  x ^= seed_box[x & (seed_box.size() - 1)];

  // These "magic" prime numbers were found and verified with a bit of brute
  // force.
  if (width_ == 1) {
    uint8_t y = x;
    y ^= y >> 1U;  // NOLINT(*-signed-bitwise)
    y *= 113U;
    y ^= y << 2U;  // NOLINT(*-signed-bitwise)
    return y;
  }
  if (width_ == 2) {
    uint16_t y = x;
    y ^= y >> 1U;  // NOLINT(*-signed-bitwise)
    y *= 25693U;
    y ^= y << 7U;  // NOLINT(*-signed-bitwise)
    return y;
  }
  if (width_ == 3) {
    const uint32_t m = Bitmask(24);
    uint32_t y = x & m;
    y ^= y >> 1UL;
    y *= 5537317UL;
    y ^= y << 12UL;
    return y & m;
  }
  if (width_ == 4) {
    uint32_t y = x;
    y ^= y >> 1UL;
    y *= 1923730889UL;
    y ^= y << 15U;
    return y;
  }
  if (width_ == 5) {
    const uint64_t m = Bitmask(40);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(274992889273);
    y ^= y << 13UL;
    return y & m;
  }
  if (width_ == 6) {
    const uint64_t m = Bitmask(48);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(70375646670269);
    y ^= y << 15ULL;
    return y & m;
  }
  if (width_ == 7) {
    const uint64_t m = Bitmask(56);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(23022548244171181);
    y ^= y << 4ULL;
    return y & m;
  }
  if (width_ == 8) {
    uint64_t y = x;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(4613509448041658233);
    y ^= y << 25ULL;
    return y;
  }

  Unreachable();
}

// static
std::optional<Usage> Usage::Load(const std::string &datadir) {
  rusage libc_usage{};

  if (getrusage(RUSAGE_SELF, &libc_usage) != 0) {
    return std::nullopt;
  }

  static thread_local int64_t diskusage = 0;
  const auto ftw_diskspace = [](const char *, const struct stat *sb, int) {
    diskusage += sb->st_size;
    return 0;
  };

  // NOLINTBEGIN(concurrency-mt-unsafe)
  diskusage = 0;
  if (!datadir.empty() && ftw(datadir.c_str(), ftw_diskspace, 42) != 0) {
    Log("error: ", strerror(errno));
    return std::nullopt;
  }
  // NOLINTEND(concurrency-mt-unsafe)

  return Usage{
      .ram = libc_usage.ru_maxrss,
      .disk = diskusage,

      .iops_read = libc_usage.ru_inblock,
      .iops_write = libc_usage.ru_oublock,
      .iops_page = libc_usage.ru_majflt,

      .cpu_user_ns = (libc_usage.ru_utime.tv_sec * 1'000'000'000) +
                     (libc_usage.ru_utime.tv_usec * 1000),
      .cpu_kernel_ns = (libc_usage.ru_stime.tv_sec * 1'000'000'000) +
                       (libc_usage.ru_stime.tv_usec * 1000),
  };
}

// static
void Usage::PrintUsage(const Usage &start, const Usage &fihish) {
  printf("\n>>>>>>>>>>>>>>>>>>>>> resources summary <<<<<<<<<<<<<<<<<<<<<\n");

  printf("iops: read %ld, write %ld, page %ld\n",
         fihish.iops_read - start.iops_read,
         fihish.iops_write - start.iops_write,
         fihish.iops_page - start.iops_page);

  printf("cpu: user %f, system %f\n",
         static_cast<double>(fihish.cpu_user_ns - start.cpu_user_ns) / S,
         static_cast<double>(fihish.cpu_kernel_ns - start.cpu_kernel_ns) / S);

  const double mb = 1UL << 20UL;
  printf("space: disk %f, ram %f\n",
         static_cast<double>(fihish.disk - start.disk) / mb,
         static_cast<double>(fihish.ram - start.ram) / mb);
}

namespace {

constexpr size_t kIintervalStat = S;
constexpr size_t kIntervalMerge = (S / 100);

constexpr std::array<uintmax_t, kHistogramCount> kBuckets = {
#define LINE_12_100(M)                                                      \
  M * 12, (M) * 14, (M) * 16, (M) * 18, (M) * 20, (M) * 25, (M) * 30,       \
      (M) * 35, (M) * 40, (M) * 45, (M) * 50, (M) * 60, (M) * 70, (M) * 80, \
      (M) * 90, (M) * 100

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

#undef LINE_12_100
};

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
    : registry_(registry),
      is_worker_(is_worker),
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
  const int merge_evo = merge_evo_;

  enabled_ = true;
  bench_ = bench;

  min_ = ~0ULL;
  whole_min_ = ~0ULL;
  checkpoint_ns_ = begin_ns_ = end_ns_ = GetTimeNow();
  merge_evo_ = merge_evo;
}

void Bucket::Add(Time t0, size_t volume) {
  const Time now = GetTimeNow();
  const Time latency = now - t0;

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

  for (size_t i = kTypeSet; i < kTypeMaxCode; ++i) {
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
    Fatal("[{}]: not all workers finished: active={:d}, merged={}",
          __FUNCTION__, workers_active_.load(), workers_merged_.load());
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

  auto timepoint = static_cast<long double>(now - starting_point_) / S;
  std::string line = std::format("{:9.3f}", timepoint);

  const Time wall_ns = now - checkpoint_ns_;
  auto wall = static_cast<long double>(wall_ns) / S;
  checkpoint_ns_ = now;

  for (auto &[_, h] : per_bench_) {
    if (!h.enabled_) {
      continue;
    }

    std::string name{to_string(h.bench_)};
    const uintmax_t n = h.acc_.n - h.last_.n;
    const uintmax_t vol = h.acc_.volume_sum - h.last_.volume_sum;

    line += std::format(" | {:>5}:", name);
    if (n != 0) {
      auto rms = static_cast<Time>(
          sqrtl(static_cast<long double>(h.acc_.latency_sum_square -
                                         h.last_.latency_sum_square) /
                n));
      const Time avg = (h.acc_.latency_sum_ns - h.last_.latency_sum_ns) / n;
      const auto rps = static_cast<long double>(n) / wall;
      const auto bps = static_cast<long double>(vol) / wall;

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

    for (size_t i = 0; i < kHistogramCount; i++) {
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
  const std::lock_guard<std::mutex> guard(mu_);
  MergeLocked(*src, GetTimeNow());
}

void Histogram::Print() {
  for (auto &[_, h] : per_bench_) {
    if (!h.enabled_ || h.acc_.n == 0) {
      continue;
    }

    std::string name{to_string(h.bench_)};
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
        snpf_lat(static_cast<Time>(sqrtl(
            static_cast<long double>(h.acc_.latency_sum_square) / h.acc_.n))));
    Log("max latency: {:>9}/op", snpf_lat(h.whole_max_));

    auto wall = static_cast<long double>(h.end_ns_ - h.begin_ns_) / S;
    Log(" throughput: {:>7}ops/s",
        snpf_val(static_cast<long double>(h.acc_.n) / wall));
  }
}

std::atomic_int Worker::workers_count_ = 0;
std::atomic_int Worker::doers_done_ = 0;

Worker::Worker(size_t id, BenchTypeMask benchmask_mask, size_t key_space,
               size_t key_sequence, const Keyer::Options &keyer_options,
               Config *config, Driver *driver, Histogram *histograms,
               const std::atomic_bool &failed)
    : id_(id),
      key_space_(key_space),
      key_sequence_(key_sequence),
      bench_mask_(benchmask_mask),
      g_failed_(failed),
      config_(config),
      driver_(driver),
      hg_(histograms),
      histograms_(histograms) {
  if (benchmask_mask <= 0) {
    Fatal("error: There is no tasks for the worker: {}", benchmask_mask);
  }

  workers_count_ += 1;

  std::string line;
  for (size_t bench = kTypeSet; bench < kTypeMaxCode; ++bench) {
    if ((benchmask_mask & (1U << bench)) != 0) {
      if (!line.empty()) {
        line += ", ";
      }
      line += to_string(static_cast<BenchType>(bench));
    }
  }

  gen_a_ = std::make_unique<Keyer>(key_space_, key_sequence_, keyer_options);

  if ((benchmask_mask & kBenchMask2Keyspace) != 0) {
    gen_b_ =
        std::make_unique<Keyer>(key_space_ + 1, key_sequence_, keyer_options);

    Log("worker.{}: {}, key-space {} and {}, key-sequence {}", id_, line,
        key_space, key_space + 1, key_sequence);
  } else {
    Log("worker.{}: {}, key-space {}, key-sequence {}", id_, line, key_space,
        key_sequence);
  }
}

Worker::~Worker() { workers_count_ -= 1; }

int Worker::FulFil() {
  if (ctx_ == nullptr) {
    ctx_ = driver_->ThreadNew();
  }
  if (ctx_ == nullptr) {
    return -1;
  }

  size_t count = 0;
  while (count < config_->nrepeat ||
         (config_->continuous_completing && doers_done_ < workers_count_)) {
    Result rc = Result::kOk;

    for (size_t it = kTypeSet; rc == Result::kOk && it < kTypeMaxCode; it++) {
      if ((bench_mask_ & (1U << it)) == 0) {
        continue;
      }

      auto bench = static_cast<BenchType>(it);

      hg_.Reset(bench);

      for (size_t i = 0; rc == Result::kOk && i < config_->count;) {
        switch (it) {
          case kTypeSet:
          case kTypeDelete:
          case kTypeGet:
            rc = EvalBenchmarkGST(bench);
            ++i;
            break;
          case kTypeCrud:
            rc = EvalBenchmarkCrud();
            ++i;
            break;
          case kTypeBatch:
            rc = EvalBenchmarkBatch(i);
            break;
          case kTypeIterate:
            rc = EvalBenchmarkIterate(i);
            break;
          default:
            Fatal("1");
            Unreachable();
            rc = Result::kUnexpectedError;
        }
      }

      histograms_->Merge(&hg_);
    }

    if (++count == config_->nrepeat) {
      doers_done_ += 1;
    }

    if (!rc || g_failed_) {
      break;
    }
  }

  driver_->ThreadDispose(ctx_);
  ctx_ = nullptr;

  return 0;
}

Result Worker::EvalCrud(Record *a, Record *b) {
  if (auto rc = driver_->Next(ctx_, kTypeSet, b); !rc) {
    return rc;
  }

  if (auto rc = driver_->Next(ctx_, kTypeSet, a); !rc) {
    return rc;
  }

  if (auto rc = driver_->Next(ctx_, kTypeDelete, b); !rc) {
    if (rc == Result::kNotFound) {
      LogKeyNotFound("crud.del", b);
      if (!config_->ignore_keynotfound) {
        return Result::kNotFound;
      }
    } else {
      return rc;
    }
  }

  if (auto rc = driver_->Next(ctx_, kTypeGet, a); !rc) {
    if (rc == Result::kNotFound) {
      LogKeyNotFound("crud.get", a);
      if (!config_->ignore_keynotfound) {
        return Result::kNotFound;
      }
    } else {
      return rc;
    }
  }

  return Result::kOk;
}

Result Worker::EvalBenchmarkGST(BenchType bench) {
  Result rc = Result::kOk;
  Result rc2 = Result::kOk;
  Record a;

  if (gen_a_->Get(&a, bench != kTypeSet) != 0) {
    return Result::kUnexpectedError;
  }

  const Time t0 = GetTimeNow();
  rc = driver_->Begin(ctx_, bench);
  if (!rc) {
    rc = driver_->Next(ctx_, bench, &a);
  }
  rc2 = driver_->Done(ctx_, bench);

  hg_.Add(t0,
          bench == kTypeDelete ? a.key.size() : a.key.size() + a.value.size());

  if (rc == Result::kNotFound) {
    const std::string name{to_string(bench)};
    LogKeyNotFound(name.c_str(), &a);
    if (config_->ignore_keynotfound) {
      rc = Result::kOk;
    }
  }
  if (!rc) {
    rc = rc2;
  }
  if (!rc) {
    return rc;
  }

  return Result::kOk;
}

Result Worker::EvalBenchmarkCrud() {
  Record a;
  Record b;

  if (gen_a_->Get(&a, false) != 0 || gen_b_->Get(&b, false) != 0) {
    return Result::kUnexpectedError;
  }

  const Time t0 = GetTimeNow();
  auto rc = driver_->Begin(ctx_, kTypeCrud);
  if (rc == Result::kOk) {
    rc = EvalCrud(&a, &b);
  }
  if (rc == Result::kOk) {
    rc = driver_->Done(ctx_, kTypeCrud);
  }
  hg_.Add(t0, a.key.size() + a.value.size() + b.key.size() + b.value.size() +
                  a.key.size() + b.key.size() + b.value.size());
  if (!rc) {
    return rc;
  }

  return rc;
}

Result Worker::EvalBenchmarkBatch(size_t &i) {
  Record a;
  Record b;

  auto pool_a = gen_a_->GetBatch(config_->batch_length);
  auto pool_b = gen_b_->GetBatch(config_->batch_length);

  auto t0 = GetTimeNow();
  auto rc = driver_->Begin(ctx_, kTypeBatch);
  for (size_t j = 0; j < config_->batch_length; ++j) {
    if (!pool_a.Load(&a) || !pool_b.Load(&b)) {
      return Result::kUnexpectedError;
    }
    rc = Worker::EvalCrud(&a, &b);
    if (!rc || ++i == config_->count) {
      break;
    }
  }
  if (rc == Result::kOk) {
    rc = driver_->Done(ctx_, kTypeBatch);
  }

  auto record_size =
      a.key.size() + a.value.size() + b.key.size() + b.value.size();
  hg_.Add(t0, record_size * config_->batch_length);

  return rc;
}

Result Worker::EvalBenchmarkIterate(size_t &i) {
  Record a;
  Time t0 = GetTimeNow();
  auto rc = driver_->Begin(ctx_, kTypeIterate);
  while (rc == Result::kOk) {
    a.key = std::span<char>();
    a.value = std::span<char>();
    rc = driver_->Next(ctx_, kTypeIterate, &a);
    hg_.Add(t0, a.key.size() + a.value.size());
    if (++i == config_->count) {
      break;
    }
    t0 = GetTimeNow();
  }
  if (rc == Result::kNotFound) {
    rc = Result::kOk;
  }
  if (rc == Result::kOk) {
    rc = driver_->Done(ctx_, kTypeIterate);
  }
  return rc;
}
