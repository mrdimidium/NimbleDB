// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "base.h"

#include <cassert>
#include <optional>
#include <string>
#include <string_view>

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
