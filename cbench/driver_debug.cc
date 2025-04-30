// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <cassert>
#include <cstring>

#include "base.h"
#include "config.h"
#include "driver.h"

struct DriverDebugContext {
  int debug;
};

class DriverDebug : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "debug"; }

  int Open(Config * /*config*/, const std::string &datadir) override;
  int Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  int Begin(Context ctx, BenchType step) override;
  int Next(Context ctx, BenchType step, Record *kv) override;
  int Done(Context ctx, BenchType step) override;
};

int DriverDebug::Open(Config * /*config*/, const std::string &datadir) {
  Log("{}.open({})", GetName(), datadir);
  return 0;
}

int DriverDebug::Close() {
  Log("{}.close()", GetName());
  return 0;
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

int DriverDebug::Begin(Context ctx, BenchType step) {
  int rc;

  Log("{}.begin({:#x}, {})", GetName(), reinterpret_cast<size_t>(ctx),
      to_string(step));

  switch (step) {
    case IA_SET:
    case IA_BATCH:
    case IA_CRUD:
    case IA_DELETE:
    case IA_ITERATE:
    case IA_GET:
      rc = 0;
      break;

    default:
      assert(0);
      rc = -1;
  }

  return rc;
}
int DriverDebug::Next(Context ctx, BenchType step, Record *kv) {
  int rc = 0;

  switch (step) {
    case IA_SET:
      Log("{}.next({:#x}, {}, {} -> {})", GetName(),
          reinterpret_cast<size_t>(ctx), to_string(step), kv->key.data(),
          kv->value.data());
      break;
    case IA_GET:
    case IA_DELETE:
      Log("{}.next({:#x}, {}, {})", GetName(), reinterpret_cast<size_t>(ctx),
          to_string(step), kv->key.data());
      break;
    case IA_ITERATE:
      Log("{}.next({:#x}, {})", GetName(), reinterpret_cast<size_t>(ctx),
          to_string(step));
      break;
    default:
      assert(0);
      rc = -1;
  }

  return rc;
}
int DriverDebug::Done(Context ctx, BenchType step) {
  int rc;

  Log("{}.done({:#x}, {})", GetName(), reinterpret_cast<size_t>(ctx),
      to_string(step));

  switch (step) {
    case IA_SET:
    case IA_BATCH:
    case IA_CRUD:
    case IA_DELETE:
    case IA_ITERATE:
    case IA_GET:
      rc = 0;
      break;

    default:
      assert(0);
      rc = -1;
  }

  return rc;
}

Driver *driver_debug() {
  static DriverDebug instance;
  return &instance;
}
