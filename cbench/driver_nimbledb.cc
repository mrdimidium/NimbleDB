// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <cassert>

#include "config.h"
#include "driver.h"

struct DriverNimbleDBContext {};

class DriverNimbleDB : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "nimbledb"; }

  int Open(Config * /*config*/, const std::string &datadir) override;
  int Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  int Begin(Context ctx, BenchType step) override;
  int Next(Context ctx, BenchType step, Record *kv) override;
  int Done(Context ctx, BenchType step) override;
};

int DriverNimbleDB::Open(Config * /*config*/, const std::string & /*datadir*/) {
  return 0;
}

int DriverNimbleDB::Close() { return 0; }

Driver::Context DriverNimbleDB::ThreadNew() {
  auto *ctx = new DriverNimbleDBContext;
  return ctx;
}

void DriverNimbleDB::ThreadDispose(Context ctx) {
  delete static_cast<DriverNimbleDBContext *>(ctx);
}

int DriverNimbleDB::Begin(Context /*ctx*/, BenchType /*step*/) { return 0; }

int DriverNimbleDB::Next(Context /*ctx*/, BenchType /*step*/, Record * /*kv*/) {
  return 0;
}

int DriverNimbleDB::Done(Context /*ctx*/, BenchType /*step*/) { return 0; }

Driver *driver_nimbledb() {
  static DriverNimbleDB instance;
  return &instance;
}
