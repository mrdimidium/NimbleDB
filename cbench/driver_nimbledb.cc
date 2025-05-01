// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <nimbledb/db.h>

#include <cassert>

#include "base.h"
#include "config.h"
#include "driver.h"
#include "nimbledb/base.h"

struct DriverNimbleDBContext {};

class DriverNimbleDB : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "nimbledb"; }

  int Open(Config* /*config*/, const std::string& datadir) override;
  int Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  int Begin(Context ctx, BenchType step) override;
  int Next(Context ctx, BenchType step, Record* kv) override;
  int Done(Context ctx, BenchType step) override;

 private:
  Config* config_ = nullptr;

  std::shared_ptr<nimbledb::DB> db_ = nullptr;
};

int DriverNimbleDB::Open(Config* config, const std::string& datadir) {
  config_ = config;

  auto st = nimbledb::DB::Open(datadir + "/datafile.nmbl", {}, &db_);
  if (!st.IsOk()) {
    Fatal("error: {}, {}", __func__, st.ToString());
    return -1;
  }

  return 0;
}

int DriverNimbleDB::Close() {
  if (db_ != nullptr) {
    auto st = db_->Close();
    if (!st.IsOk()) {
      Fatal("error: {}, {}", __func__, st.ToString());
      return -1;
    }
  }

  return 0;
}

Driver::Context DriverNimbleDB::ThreadNew() {
  auto* ctx = new DriverNimbleDBContext;
  return ctx;
}

void DriverNimbleDB::ThreadDispose(Context ctx) {
  delete static_cast<DriverNimbleDBContext*>(ctx);
}

int DriverNimbleDB::Begin(Context /*ctx*/, BenchType /*step*/) { return 0; }

int DriverNimbleDB::Next(Context /*ctx*/, BenchType /*step*/, Record* /*kv*/) {
  return 0;
}

int DriverNimbleDB::Done(Context /*ctx*/, BenchType /*step*/) { return 0; }

Driver* driver_nimbledb() {
  static DriverNimbleDB instance;
  return &instance;
}
