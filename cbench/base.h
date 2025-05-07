// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_CBENCH_BASE_H_
#define NIMBLEDB_CBENCH_BASE_H_

#include <unistd.h>

#include <cassert>
#include <format>
#include <iostream>
#include <ostream>
#include <set>

enum class Result : uint8_t {
  kOk = 0,
  kNotFound = 1,
  kSystemError = 2,
  kUnexpectedError = 3,
};

inline bool operator!(Result result) { return result != Result::kOk; }

template <typename... Args>
inline void Log(std::format_string<Args...> fmt, Args &&...args) {
  std::cout << std::vformat(fmt.get(), std::make_format_args(args...)) << "\n"
            << std::flush;
}

template <typename... Args>
inline void Fatal(std::format_string<Args...> fmt, Args &&...args) {
  std::cerr << "\n*** CBENCH fatal: "
            << std::vformat(fmt.get(), std::make_format_args(args...)) << "\n"
            << std::flush;
  std::abort();
}

[[noreturn]] inline void Unreachable() {
  // Uses compiler specific extensions if possible.
  // Even if no extension is used, undefined behavior is still raised by
  // an empty function body and the noreturn attribute.
#if defined(_MSC_VER) && !defined(__clang__)  // MSVC
  __assume(false);
#elif defined(__GNUC__) || defined(__clang__)
  __builtin_unreachable();
#else
  assert(0);
#endif
}

constexpr size_t US = 1000ULL;
constexpr size_t MS = 1000000ULL;
constexpr size_t S = 1000000000ULL;

using Time = uintmax_t;
inline Time GetTimeNow() noexcept {
#if defined(CLOCK_MONOTONIC_RAW)
  constexpr clockid_t clock_id = CLOCK_MONOTONIC_RAW;
#elif defined(CLOCK_MONOTONIC)
  constexpr clockid_t clock_id = CLOCK_MONOTONIC;
#else
  constexpr clockid_t clock_id = CLOCK_REALTIME;
#endif

  timespec ts{};
  const int rc = clock_gettime(clock_id, &ts);
  if (rc != 0) {
    Fatal(__FUNCTION__);
  }

  return (ts.tv_sec * S) + ts.tv_nsec;
}

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

using BenchTypeMask = uint8_t;

// clang-format off
constexpr BenchTypeMask kBenchMaskRead = 0U
  | 1U << kTypeBatch
  | 1U << kTypeCrud
  | 1U << kTypeGet
  | 1U << kTypeIterate;
constexpr BenchTypeMask kBenchMaskWrite = 0U
  | 1U << kTypeBatch
  | 1U << kTypeCrud
  | 1U << kTypeDelete
  | 1U << kTypeSet;
constexpr BenchTypeMask kBenchMask2Keyspace = 0U
  | 1U << kTypeBatch
  | 1U << kTypeCrud;
// clang-format on

struct Config {
  std::string driver_name;
  std::string dirname = "./_cbench.tmp";
  std::set<BenchType> benchmarks{kTypeGet, kTypeSet};

  uintmax_t count = 1000000;
  size_t key_size = 16, value_size = 32;

  BenchWalMode walmode = kWalDefault;
  BenchSyncMode syncmode = kModeLazy;

  size_t rthr = sysconf(_SC_NPROCESSORS_ONLN),
         wthr = sysconf(_SC_NPROCESSORS_ONLN);

  int kvseed = 0;
  size_t nrepeat = 1;
  size_t batch_length = 500;

  bool binary = false;
  bool ignore_keynotfound = false;
  bool continuous_completing = false;
};

// Specifies a key and value pair to write or read in the driver.
// The driver itself does not own the write memory and only reads/copies to it.
struct Record {
  std::span<char> key;
  std::span<char> value;
};

// Driver is a universal interface to different databases.
// To add support for a new database, create a `dirver_<dbname>.cc` file and
// implement the interface. See `driver_debug.cc` for reference.
class Driver {
 public:
  using Context = void *;

  Driver() = default;
  virtual ~Driver() = default;

  Driver(const Driver &) = delete;
  Driver(Driver &&) = delete;
  Driver &operator=(const Driver &) = delete;
  Driver &operator=(Driver &&) = delete;

  // Returns human-readable driver name for logs and config.
  // Prefer lowercase for name.
  [[nodiscard]] virtual std::string_view GetName() const = 0;

  // Opens a connection to the database, called only once for all threads.
  virtual Result Open(Config *config, const std::string &datadir) = 0;

  // Closes the connection to the database,
  // called only once at the very end.
  virtual Result Close() = 0;

  // Creates an opaque context for a each thread
  // Access to contexts is not synchronized, if the database requires
  // synchronization it is the driver's responsibility.
  virtual Context ThreadNew() = 0;

  // Clears the context of a specific thread.
  virtual void ThreadDispose(Context) = 0;

  // Execute a block of data operations in the obvious order:
  // Begin->Next->..->Next->Done() Begin and End are needed to prepare
  // transactions and complex scenarios such as block recording if the database
  // supports it.
  virtual Result Begin(Context, BenchType) = 0;
  virtual Result Next(Context, BenchType, Record *kv) = 0;
  virtual Result Done(Context, BenchType) = 0;

  // Returns a list of supported driver names.
  // If the driver was excluded during build, this method will also exclude it.
  static std::string Supported();

  // Returns a pointer to the driver singleton if available, or nullptr if no
  // such driver exists. It's thread safe.
  static Driver *GetDriverFor(std::string_view name);
};
static_assert(std::is_abstract<Driver>(),
              "Driver must be clear abstract class");

#endif  // NIMBLEDB_CBENCH_BASE_H_
