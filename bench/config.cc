// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "config.h"

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
#include <string_view>

#include "base.h"

std::string_view BenchTypeToString(BenchType b) {
  switch (b) {
    case IA_SET:
      return "set";
    case IA_GET:
      return "get";
    case IA_DELETE:
      return "del";
    case IA_ITERATE:
      return "iter";
    case IA_BATCH:
      return "batch";
    case IA_CRUD:
      return "crud";
    case IA_MAX:
      Unreachable();
  }
  return "(unknown)";
}
BenchType BenchTypeFromString(const std::string_view &name) {
  if (name == "set") {
    return IA_SET;
  }
  if (name == "get") {
    return IA_GET;
  }
  if (name == "del" || name == "delete") {
    return IA_DELETE;
  }
  if (name == "iter" || name == "iterate") {
    return IA_ITERATE;
  }
  if (name == "batch") {
    return IA_BATCH;
  }
  if (name == "crud" || name == "transact") {
    return IA_CRUD;
  }
  return IA_MAX;
}

std::string_view BenchSyncModeToString(BenchSyncMode syncmode) {
  switch (syncmode) {
    case IA_SYNC:
      return "sync";
    case IA_LAZY:
      return "lazy";
    case IA_NOSYNC:
      return "nosync";
    default:
      return "???";
  }
}
std::optional<BenchSyncMode> BenchSyncModeFromString(const std::string &str) {
  if (str == BenchSyncModeToString(IA_SYNC)) {
    return IA_SYNC;
  }
  if (str == BenchSyncModeToString(IA_LAZY)) {
    return IA_LAZY;
  }
  if (str == BenchSyncModeToString(IA_NOSYNC)) {
    return IA_NOSYNC;
  }
  return std::nullopt;
}

std::string_view BenchWalModeToString(BenchWalMode walmode) {
  switch (walmode) {
    case IA_WAL_INDEF:
      return "indef";
    case IA_WAL_ON:
      return "walon";
    case IA_WAL_OFF:
      return "waloff";
    default:
      return "???";
  }
}
std::optional<BenchWalMode> BenchWalModeFromString(const std::string &str) {
  if (str == BenchWalModeToString(IA_WAL_INDEF)) {
    return IA_WAL_INDEF;
  }
  if (str == BenchWalModeToString(IA_WAL_ON)) {
    return IA_WAL_ON;
  }
  if (str == BenchWalModeToString(IA_WAL_OFF)) {
    return IA_WAL_OFF;
  }
  return std::nullopt;
}

void Config::Print() const {
  Log("Configuration:");
  Log("\tdatabase   = {}", driver_name);
  Log("\tdirname    = {}", dirname);
  Log("\tbenchmarks = {}", Join(benchmarks, BenchTypeToString));
  Log("");
  Log("\toperations = {}", count);
  Log("");
  Log("\tWAL mode   = {}", BenchWalModeToString(walmode));
  Log("\tsync mode  = {}", BenchSyncModeToString(syncmode));
  Log("");
  Log("\tkey size   = {}", key_size);
  Log("\tvalue size = {}", value_size);
  Log("");
  Log("\tr-threads    = {}", rthr);
  Log("\tw-threads    = {}", wthr);
  Log("");
  Log("\tbinary                = {}", binary ? "yes" : "no");
  Log("\tseparate              = {}", separate ? "yes" : "no");
  Log("\tignore not found      = {}", ignore_keynotfound ? "yes" : "no");
  Log("\tcontinuous completing = {}", continuous_completing ? "yes" : "no");
  Log("");
}
