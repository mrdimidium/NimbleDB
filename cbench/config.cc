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

std::string_view to_string(BenchType b) {
  switch (b) {
    case kTypeSet:
      return "set";
    case kTypeGet:
      return "get";
    case kTypeDelete:
      return "del";
    case kTypeIterate:
      return "iter";
    case kTypeBatch:
      return "batch";
    case kTypeCrud:
      return "crud";
    case kTypeMaxCode:
      Unreachable();
  }
  return "(unknown)";
}
BenchType BenchTypeFromString(const std::string_view &name) {
  if (name == "set") {
    return kTypeSet;
  }
  if (name == "get") {
    return kTypeGet;
  }
  if (name == "del" || name == "delete") {
    return kTypeDelete;
  }
  if (name == "iter" || name == "iterate") {
    return kTypeIterate;
  }
  if (name == "batch") {
    return kTypeBatch;
  }
  if (name == "crud" || name == "transact") {
    return kTypeCrud;
  }
  return kTypeMaxCode;
}

std::string_view to_string(BenchSyncMode syncmode) {
  switch (syncmode) {
    case kModeSync:
      return "sync";
    case kModeLazy:
      return "lazy";
    case kModeNoSync:
      return "nosync";
    default:
      return "???";
  }
}
std::optional<BenchSyncMode> BenchSyncModeFromString(const std::string &str) {
  if (str == to_string(kModeSync)) {
    return kModeSync;
  }
  if (str == to_string(kModeLazy)) {
    return kModeLazy;
  }
  if (str == to_string(kModeNoSync)) {
    return kModeNoSync;
  }
  return std::nullopt;
}

std::string_view to_string(BenchWalMode walmode) {
  switch (walmode) {
    case kWalDefault:
      return "indef";
    case kWalEnabled:
      return "walon";
    case kWalDisabled:
      return "waloff";
    default:
      return "???";
  }
}
std::optional<BenchWalMode> BenchWalModeFromString(const std::string &str) {
  if (str == to_string(kWalDefault)) {
    return kWalDefault;
  }
  if (str == to_string(kWalEnabled)) {
    return kWalEnabled;
  }
  if (str == to_string(kWalDisabled)) {
    return kWalDisabled;
  }
  return std::nullopt;
}

void Config::Print() const {
  Log("Configuration:");
  Log("\tdatabase   = {}", driver_name);
  Log("\tdirname    = {}", dirname);
  Log("\tbenchmarks = {}", Join(benchmarks));
  Log("");
  Log("\toperations = {}", count);
  Log("");
  Log("\tWAL mode   = {}", to_string(walmode));
  Log("\tsync mode  = {}", to_string(syncmode));
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
