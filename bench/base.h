// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BENCH_BASE_H_
#define NIMBLEDB_BENCH_BASE_H_

#include <format>
#include <iostream>
#include <ostream>

template <typename... Args>
inline void Log(std::format_string<Args...> fmt, Args &&...args) {
  std::cout << std::vformat(fmt.get(), std::make_format_args(args...)) << "\n"
            << std::flush;
}

template <typename... Args>
inline void Fatal(std::format_string<Args...> fmt, Args &&...args) {
  std::cerr << "\n*** DBBENCH fatal: "
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
  int rc = clock_gettime(clock_id, &ts);
  if (rc != 0) {
    Fatal(__FUNCTION__);
  }

  return (ts.tv_sec * S) + ts.tv_nsec;
}

template <typename T>
std::string Join(T container, const std::string &delimiter = ", ") {
  using namespace std;  // NOLINT(*-build-using-namespace)

  std::string result;
  for (const auto &it : container) {
    if (!result.empty()) {
      result += delimiter;
    }
    result += to_string(it);
  }
  return result;
}

#endif  // NIMBLEDB_BENCH_BASE_H_
