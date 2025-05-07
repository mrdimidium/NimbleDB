// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <cassert>
#include <cstring>

#include "base.h"
#include "driver.h"

struct DriverDebugContext {
  int debug;
};

class DriverDebug final : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "debug"; }

  Result Open(Config * /*config*/, const std::string &datadir) override;
  Result Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  Result Begin(Context ctx, BenchType step) override;
  Result Next(Context ctx, BenchType step, Record *kv) override;
  Result Done(Context ctx, BenchType step) override;
};

Result DriverDebug::Open(Config * /*config*/, const std::string &datadir) {
  Log("{}.open({})", GetName(), datadir);
  return Result::kOk;
}

Result DriverDebug::Close() {
  Log("{}.close()", GetName());
  return Result::kOk;
}

Driver::Context DriverDebug::ThreadNew() {
  auto *ctx = new DriverDebugContext;
  Log("{}.thread_new() = {:#x}", GetName(), reinterpret_cast<size_t>(ctx));
  return ctx;
}

void DriverDebug::ThreadDispose(Context ctx) {
  Log("{}.thread_dispose({:#x})", GetName(), reinterpret_cast<size_t>(ctx));
  delete static_cast<DriverDebugContext *>(ctx);
}

Result DriverDebug::Begin(Context ctx, BenchType step) {
  Log("{}.begin({:#x}, {})", GetName(), reinterpret_cast<size_t>(ctx),
      to_string(step));

  switch (step) {
    case kTypeSet:
    case kTypeBatch:
    case kTypeCrud:
    case kTypeDelete:
    case kTypeIterate:
    case kTypeGet:
      return Result::kOk;

    default:
      Unreachable();
  }

  return Result::kOk;
}

Result DriverDebug::Next(Context ctx, BenchType step, Record *kv) {
  switch (step) {
    case kTypeSet:
      Log("{}.next({:#x}, {}, {} -> {})", GetName(),
          reinterpret_cast<size_t>(ctx), to_string(step), kv->key.data(),
          kv->value.data());
      break;
    case kTypeGet:
    case kTypeDelete:
      Log("{}.next({:#x}, {}, {})", GetName(), reinterpret_cast<size_t>(ctx),
          to_string(step), kv->key.data());
      break;
    case kTypeIterate:
      Log("{}.next({:#x}, {})", GetName(), reinterpret_cast<size_t>(ctx),
          to_string(step));
      break;
    default:
      Unreachable();
  }

  return Result::kOk;
}

Result DriverDebug::Done(Context ctx, BenchType step) {
  Log("{}.done({:#x}, {})", GetName(), reinterpret_cast<size_t>(ctx),
      to_string(step));

  switch (step) {
    case kTypeSet:
    case kTypeBatch:
    case kTypeCrud:
    case kTypeDelete:
    case kTypeIterate:
    case kTypeGet:
      return Result::kOk;

    default:
      Unreachable();
  }

  return Result::kOk;
}

Driver *driver_debug() {
  static DriverDebug instance;
  return &instance;
}
