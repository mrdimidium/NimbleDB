// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <ftw.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <CLI/CLI.hpp>
#include <atomic>
#include <cstdlib>
#include <format>
#include <latch>
#include <memory>
#include <optional>
#include <thread>

#include "base.h"
#include "cbench.h"
#include "driver.h"

namespace {

void PrintConfig(const Config &config);

Config ParseConfig(std::span<char *> args,
                   const std::string &supported_drivers);

}  // namespace

// NOLINTNEXTLINE(*-function-cognitive-complexity, bugprone-exception-escape)
int main(int argc, char *argv[]) {
  // parse benchmark configuration
  auto config = ParseConfig(std::span(argv, argc), Driver::Supported());
  PrintConfig(config);

  BenchTypeMask set_rd = 0;
  BenchTypeMask set_wr = 0;

  for (const auto &bench : config.benchmarks) {
    if (bench == kTypeIterate || bench == kTypeGet) {
      set_rd |= 1U << bench;
    } else {
      set_wr |= 1U << bench;
    }
  }

  if ((set_rd | set_wr) == 0) {
    Log("error: there are no tasks for either reading or writing");
    return EXIT_FAILURE;
  }

  if (set_rd == 0) {
    config.rthr = 0;
  }
  if (set_wr == 0) {
    config.wthr = 0;
  }

  auto key_nsectors = std::max<size_t>({1, config.rthr, config.wthr});
  auto key_nspaces = std::max<size_t>({1, config.wthr});
  if ((set_wr & kBenchMask2Keyspace) != 0) {
    key_nspaces *= 2;
  }

  Keyer::Init(config.kvseed);
  Keyer::Options keyer_options{.binary = config.binary,
                               .count = config.count,
                               .key_size = config.key_size,
                               .value_size = config.value_size,
                               .spaces_count = key_nspaces,
                               .sectors_count = key_nsectors};

  // find driver
  auto *driver = Driver::GetDriverFor(config.driver_name);
  if (driver == nullptr) {
    Log("error: unknown database driver '{}'", config.driver_name);
    return EXIT_FAILURE;
  }

  // prepare datadir
  auto datadir = std::format("{}/{}", config.dirname, driver->GetName());

  std::filesystem::create_directories(datadir);
  std::filesystem::permissions(config.dirname,
                               std::filesystem::perms::owner_all);

  // open database
  if (!driver->Open(&config, datadir)) {
    return EXIT_FAILURE;
  }

  // prepare statistics
  Usage rusage_before{};
  Usage rusage_start{};
  Usage rusage_fihish{};

  if (auto it = Usage::Load(datadir)) {
    rusage_before = *it;
  } else {
    return EXIT_FAILURE;
  }

  Histogram histograms(config.benchmarks);

  // finally launch the benchmark
  std::atomic_bool failed = false;
  std::latch l_start(static_cast<ptrdiff_t>(config.rthr + config.wthr + 1U));
  std::latch l_finish(static_cast<ptrdiff_t>(config.rthr + config.wthr + 1U));

  int nth = 0;
  int key_space = 0;

  const auto run_worker_thread = [&](BenchTypeMask mask) {
    auto worker =
        std::make_unique<Worker>(nth, mask, key_space, nth, keyer_options,
                                 &config, driver, &histograms, failed);

    std::thread([&, worker = std::move(worker)]() {
      l_start.arrive_and_wait();
      if (const int rc = worker->FulFil(); rc != 0) {
        failed = true;
      }
      l_finish.count_down();
    });
  };

  for (size_t i = 0; i < config.rthr; ++i, ++nth) {
    run_worker_thread(set_rd);
  }

  for (size_t i = 0; i < config.wthr; ++i, ++nth) {
    if ((set_wr & kBenchMaskWrite) != 0) {
      key_space += 1;
      if ((set_wr & kBenchMask2Keyspace) != 0) {
        key_space += 1;
      }
    }

    run_worker_thread(set_wr);
  }

  if (auto it = Usage::Load(datadir)) {
    rusage_start = *it;
  } else {
    return EXIT_FAILURE;
  }

  sync();

  if ((set_wr | set_rd) != 0) {
    Worker worker(0, set_wr | set_rd, 0, 0, keyer_options, &config, driver,
                  &histograms, failed);

    l_start.arrive_and_wait();

    if (const int rc = worker.FulFil(); rc != 0) {
      failed = true;
    }
  } else {
    l_start.count_down();
  }

  l_finish.arrive_and_wait();

  if (failed) {
    Log("error: benchmark finished with error");
    return EXIT_FAILURE;
  }

  sync();

  // print summary
  if (auto it = Usage::Load(datadir)) {
    rusage_fihish = *it;
  } else {
    return EXIT_FAILURE;
  }

  rusage_start.ram = rusage_before.ram;
  rusage_start.disk = 0;

  histograms.Summarize();
  Log("complete.");
  histograms.Print();

  Usage::PrintUsage(rusage_start, rusage_fihish);

  // try to close the driver carefully
  if (driver != nullptr) {
    driver->Close();
  }

  return EXIT_SUCCESS;
}

namespace {

template <typename T>
std::string Join(const T &container, const std::string &delimiter = ", ") {
  using std::to_string;

  std::string result;
  for (const auto &it : container) {
    if (!result.empty()) {
      result += delimiter;
    }
    result += to_string(it);
  }
  return result;
}

void PrintConfig(const Config &config) {
  Log("Configuration:");
  Log("\tdatabase   = {}", config.driver_name);
  Log("\tdirname    = {}", config.dirname);
  Log("\tbenchmarks = {}", Join(config.benchmarks));
  Log("");
  Log("\toperations = {}", config.count);
  Log("");
  Log("\tWAL mode   = {}", to_string(config.walmode));
  Log("\tsync mode  = {}", to_string(config.syncmode));
  Log("");
  Log("\tkey size   = {}", config.key_size);
  Log("\tvalue size = {}", config.value_size);
  Log("");
  Log("\tr-threads    = {}", config.rthr);
  Log("\tw-threads    = {}", config.wthr);
  Log("");
  Log("\tbinary                = {}", config.binary ? "yes" : "no");
  Log("\tignore not found      = {}", config.ignore_keynotfound ? "yes" : "no");
  Log("\tcontinuous completing = {}",
      config.continuous_completing ? "yes" : "no");
  Log("");
}

Config ParseConfig(std::span<char *> args,
                   const std::string &supported_drivers) {
  Config config{};

  CLI::App app{"NumbleDB comparative benchmark"};

  app.add_option("-D,--database", config.driver_name)
      ->description("target database, choices: " + supported_drivers)
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
