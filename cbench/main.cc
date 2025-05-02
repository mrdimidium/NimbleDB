// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

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

#include <CLI/CLI.hpp>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <format>
#include <memory>
#include <optional>

#include "base.h"
#include "config.h"
#include "driver.h"
#include "histogram.h"
#include "record.h"

namespace {

struct Usage {
  int64_t ram, disk;

  int64_t iops_read;
  int64_t iops_write;
  int64_t iops_page;

  int64_t cpu_user_ns;
  int64_t cpu_kernel_ns;

  static std::optional<Usage> Load(const std::string &datadir);
};

thread_local int64_t diskusage;

int ftw_diskspace(const char *fpath, const struct stat *sb, int typeflag) {
  (void)fpath;
  (void)typeflag;
  diskusage += sb->st_size;
  return 0;
}

std::optional<Usage> Usage::Load(const std::string &datadir) {
  rusage libc_usage{};

  if (getrusage(RUSAGE_SELF, &libc_usage) != 0) {
    return std::nullopt;
  }

  diskusage = 0;
  // NOLINTBEGIN(concurrency-mt-unsafe)
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

void PrintUsage(const Usage &start, const Usage &fihish) {
  printf("\n>>>>>>>>>>>>>>>>>>>>>>> resources usage <<<<<<<<<<<<<<<<<<<<<<<\n");

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

class Worker {
 public:
  explicit Worker(size_t id, size_t benchmask_mask, size_t key_space,
                  size_t key_sequence, const Keyer::Options &keyer_options,
                  Config *config, Driver *driver, Histogram *histograms,
                  const std::atomic_bool &failed)
      : id_(id),
        key_space_(key_space),
        key_sequence_(key_sequence),
        benchmask_(benchmask_mask),
        hg_(histograms),
        g_failed_(failed),
        config_(config),
        driver_(driver),
        histograms_(histograms) {
    if (benchmask_mask <= 0) {
      Fatal("error: There is no tasks for the worker");
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

  ~Worker() { workers_count_ -= 1; }

  Worker(const Worker &) = delete;
  Worker(Worker &&) = delete;
  Worker &operator=(const Worker &) = delete;
  Worker &operator=(Worker &&) = delete;

  int FulFil() {
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
        if ((benchmask_ & (1U << it)) == 0) {
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

 private:
  Result EvalCrud(Record *a, Record *b) {
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

  Result EvalBenchmarkGST(BenchType bench) {
    Result rc = Result::kOk;
    Result rc2 = Result::kOk;
    Record a;

    if (gen_a_->Get(&a, bench != kTypeSet) != 0) {
      return Result::kUnexpectedError;
    }

    Time t0 = GetTimeNow();
    rc = driver_->Begin(ctx_, bench);
    if (!rc) {
      rc = driver_->Next(ctx_, bench, &a);
    }
    rc2 = driver_->Done(ctx_, bench);

    hg_.Add(t0, bench == kTypeDelete ? a.key.size()
                                     : a.key.size() + a.value.size());

    if (rc == Result::kNotFound) {
      std::string name{to_string(bench)};
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

  Result EvalBenchmarkCrud() {
    Record a;
    Record b;

    if (gen_a_->Get(&a, false) != 0 || gen_b_->Get(&b, false) != 0) {
      return Result::kUnexpectedError;
    }

    Time t0 = GetTimeNow();
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

  Result EvalBenchmarkBatch(size_t &i) {
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
      rc = EvalCrud(&a, &b);
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

  Result EvalBenchmarkIterate(size_t &i) {
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

  void LogKeyNotFound(const char *op, Record *k) {
    Log("error: key {} not found ({}, {:#d}, {}+{})", k->key.data(), op, id_,
        key_space_, key_sequence_);
  }

  static std::atomic_int workers_count_;
  static std::atomic_int doers_done_;

  size_t id_ = 0;
  size_t key_space_ = 0, key_sequence_ = 0;
  size_t benchmask_ = 0;
  Bucket hg_;

  const std::atomic_bool &g_failed_;

  Config *config_ = nullptr;
  Driver *driver_ = nullptr;
  Histogram *histograms_ = nullptr;

  Driver::Context ctx_{};

  std::unique_ptr<Keyer> gen_a_ = nullptr;
  std::unique_ptr<Keyer> gen_b_ = nullptr;
};

std::atomic_int Worker::workers_count_ = 0;
std::atomic_int Worker::doers_done_ = 0;

class Runner {
 public:
  Runner(const Runner &) = delete;
  Runner(Runner &&) = delete;
  Runner &operator=(const Runner &) = delete;
  Runner &operator=(Runner &&) = delete;

  [[nodiscard]] int Init(Config *config, Driver *driver, Histogram *histograms,
                         const std::string &datadir) {
    datadir_ = datadir;

    config_ = config;
    driver_ = driver;
    histograms_ = histograms;

    if (auto before_open = Usage::Load(datadir)) {
      before_open_ram_ = before_open->ram;
    } else {
      return -1;
    }

    if (!driver->Open(config_, datadir)) {
      return -1;
    }

    for (const auto &bench : config_->benchmarks) {
      if (bench == kTypeIterate || bench == kTypeGet) {
        set_rd |= 1UL << bench;
      } else {
        set_wr |= 1UL << bench;
      }
    }

    if ((set_rd | set_wr) == 0) {
      Log("error: there are no tasks for either reading or writing");
      return 0;
    }

    if (config_->rthr != 0 && set_rd == 0) {
      config_->rthr = 0;
    }
    if (config_->wthr != 0 && set_wr == 0) {
      config_->wthr = 0;
    }

    auto key_nsectors = std::max<size_t>({1, config_->rthr, config->wthr});
    auto key_nspaces = std::max<size_t>({1, config_->wthr});
    if ((set_wr & kBenchMask2Keyspace) != 0) {
      key_nspaces *= 2;
    }

    keyer_options_ = {.count = config_->count,
                      .key_size = config_->key_size,
                      .value_size = config_->value_size,
                      .spaces_count = key_nspaces,
                      .sectors_count = key_nsectors,
                      .binary = config_->binary};

    int rc = pthread_barrier_init(&barrier_start_, nullptr,
                                  config_->rthr + config_->wthr + 1);
    if (rc != 0) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      Fatal("error: start pthread_barrier_init {} ({})", strerror(errno),
            errno);
    }

    rc = pthread_barrier_init(&barrier_fihish_, nullptr,
                              config_->rthr + config_->wthr + 1);
    if (rc != 0) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      Log("error: finish pthread_barrier_init {} ({})", strerror(errno), errno);
      return rc;
    }

    return 0;
  }

  [[nodiscard]] int Run() {
    int nth = 0;
    int key_space = 0;

    if (int rc =
            RunWorkersPool(config_->rthr, &nth, &set_rd, set_rd, &key_space);
        rc != 0) {
      return -1;
    }

    if (int rc =
            RunWorkersPool(config_->wthr, &nth, &set_wr, set_wr, &key_space);
        rc != 0) {
      return -1;
    }

    Usage rusage_start{};
    Usage rusage_fihish{};

    if (auto it = Usage::Load(datadir_)) {
      rusage_start = *it;
    } else {
      return -1;
    }

    SyncStart();
    if ((set_wr | set_rd) != 0) {
      Worker worker(0, set_wr | set_rd, 0, 0, keyer_options_, config_, driver_,
                    histograms_, failed_);

      int rc = worker.FulFil();
      if (rc != 0) {
        failed_ = true;
      }
    }
    SyncFihish();

    if (auto it = Usage::Load(datadir_)) {
      rusage_fihish = *it;
    } else {
      return -1;
    }

    if (failed_) {
      return -1;
    }

    histograms_->Summarize();
    Log("complete.");
    histograms_->Print();

    rusage_start.ram = before_open_ram_;
    rusage_start.disk = 0;

    PrintUsage(rusage_start, rusage_fihish);

    return 0;
  }

  // Returns the runner instance, there is only one for all threads.
  static Runner &GetInstance() {
    static Runner instance;
    return instance;
  }

 private:
  explicit Runner() = default;
  ~Runner() {
    if (driver_ != nullptr) {
      driver_->Close();
    }
  }

  void SyncStart() {
#ifdef _POSIX_PRIORITY_SCHEDULING
    sched_yield();
#elif defined(__APPLE__) || defined(__MACH__)
    pthread_yield_np();
#else
    pthread_yield();
#endif

    int rc = pthread_barrier_wait(&barrier_start_);
    if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      Fatal("error: pthread_barrier_wait {} ({})", strerror(rc), rc);
    }
  }

  void SyncFihish() {
    int rc = pthread_barrier_wait(&barrier_fihish_);
    if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      Fatal("error: pthread_barrier_wait {} ({})", strerror(rc), rc);
    }
  }

  static void *WorkerThreadFunc(void *ptr) {
    auto *worker = static_cast<Worker *>(ptr);
    assert(worker != nullptr);

    GetInstance().SyncStart();
    int rc = worker->FulFil();
    GetInstance().SyncFihish();

    if (rc != 0) {
      GetInstance().failed_ = true;
    }

    delete worker;

    return nullptr;
  }

  [[nodiscard]] int RunWorkersPool(size_t count, int *nth, uint64_t *rotator,
                                   uint64_t set, int *key_space) const {
    for (size_t n = 0; n < count; n++) {
      assert(set != 0);

      if (*rotator == 0) {
        *rotator = set;
      }

      uint64_t mask = *rotator;
      if (config_->separate) {
        uint64_t order = kTypeSet;
        for (mask = 0; mask == 0; order = (order + 1) % kTypeMaxCode) {
          mask = *rotator & (1ULL << order);
        }
      }

      assert(mask != 0);
      if ((mask & kBenchMaskWrite) != 0) {
        *key_space += 1;
        if ((mask & kBenchMask2Keyspace) != 0) {
          *key_space += 1;
        }
      }

      *nth += 1;
      auto *worker = new Worker(*nth, mask, *key_space, *nth, keyer_options_,
                                config_, driver_, histograms_, failed_);

      pthread_t thread;
      int rc = pthread_create(&thread, nullptr, WorkerThreadFunc, worker);
      if (rc != 0) {
        return rc;
      }

      *rotator &= ~mask;
    }

    return 0;
  }

  std::string datadir_;

  Config *config_ = nullptr;
  Driver *driver_ = nullptr;
  Histogram *histograms_ = nullptr;

  uint64_t set_rd = 0;
  uint64_t set_wr = 0;

  Keyer::Options keyer_options_{};

  std::atomic_bool failed_ = false;
  int64_t before_open_ram_ = 0;

  pthread_barrier_t barrier_start_{}, barrier_fihish_{};
};

Config &ParseCLIArguments(Config &config, std::span<char *> args) {
  CLI::App app{"NumbleDB comparative benchmark"};

  app.add_option("-D,--database", config.driver_name)
      ->description("target database, choices: " + Driver::Supported())
      ->required(true);

  app.add_option_function<std::vector<std::string>>(
         "-B,--benchmark",
         [&](const std::vector<std::string> &items) {
           config.benchmarks.clear();
           for (const auto &it : items) {
             if (auto bench = BenchTypeFromString(it)) {
               config.benchmarks.insert(bench);
             } else {
               throw CLI::ValidationError("unknown benchmark name: " + it);
             }
           }
         })
      ->description(
          "load type, choices: set, get, delete, iterate, batch, crud")
      ->default_val(Join(config.benchmarks));

  app.add_option_function<std::string>(
         "-M,--sync-mode",
         [&](const std::string &str) {
           if (auto mode = BenchSyncModeFromString(str)) {
             config.syncmode = mode.value();
           } else {
             throw CLI::ValidationError("unknown syncmode: " + str);
           }
         })
      ->description("database sync mode, choices: sync, nosync, lazy")
      ->default_val(to_string(config.syncmode));

  app.add_option_function<std::string>(
         "-W,--wal-mode",
         [&](const std::string &str) {
           if (auto mode = BenchWalModeFromString(str)) {
             config.walmode = mode.value();
           } else {
             throw CLI::ValidationError("unknown walmode: " + str);
           }
         })
      ->description("database wal mode: indef, walon, waloff")
      ->default_val(to_string(config.walmode));

  app.add_option("-P,--dirname", config.dirname)
      ->description("dirname for temporaries files & reports")
      ->default_val(config.dirname);

  app.add_option("-n", config.count)
      ->description("number of operations")
      ->default_val(config.count);

  app.add_option("-k", config.key_size, "key size")
      ->default_val(config.key_size);
  app.add_option("-v", config.value_size, "value size")
      ->default_val(config.value_size);

  app.add_option("-r", config.rthr)
      ->description("number of read threads, `zero` to use single thread")
      ->default_val(config.rthr);
  app.add_option("-w", config.wthr)
      ->description("number of write threads, `zero` to use single thread")
      ->default_val(config.wthr);

  app.add_flag("--binary", config.binary, "generate binary (non ASCII) values")
      ->default_val(config.binary);
  app.add_flag("--continuous", config.continuous_completing,
               "continuous completing mode")
      ->default_val(config.continuous_completing);
  app.add_flag("--ignore-not-found", config.ignore_keynotfound,
               "ignore key-not-found error")
      ->default_val(config.ignore_keynotfound);

  try {
    app.parse(static_cast<int>(args.size()), args.data());
  } catch (const CLI::ParseError &e) {
    exit(app.exit(e));  // NOLINT(concurrency-mt-unsafe)
  }

  return config;
}

}  // namespace

int main(int argc, char *argv[]) {
  Config config{Driver::Supported()};
  ParseCLIArguments(config, std::span<char *>(argv, argc));
  config.Print();

  Keyer::Init(config.kvseed);

  auto *driver = Driver::GetDriverFor(config.driver_name);
  if (driver == nullptr) {
    Log("error: unknown database driver '{}'", config.driver_name);
    return EXIT_FAILURE;
  }

  Histogram histograms(config.benchmarks);

  auto datadir = std::format("{}/{}", config.dirname, driver->GetName());

  std::filesystem::create_directories(datadir);
  std::filesystem::permissions(config.dirname,
                               std::filesystem::perms::owner_all);

  auto &runner = Runner::GetInstance();

  if (int rc = runner.Init(&config, driver, &histograms, datadir); rc != 0) {
    return EXIT_FAILURE;
  }

  sync();

  if (auto rc = runner.Run(); rc != 0) {
    return EXIT_FAILURE;
  }
}
