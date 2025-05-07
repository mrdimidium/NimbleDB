// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <nimbledb/db.h>

#include <cassert>

#include "base.h"
#include "driver.h"
#include "nimbledb/base.h"

#ifndef HAVE_NIMBLEDB
  #error \
      "Driver implementation requires `HAVE_NIMBLEDB` variable that includes the definitions"
#endif  // !HAVE_NIMBLEDB

struct DriverNimbleDBContext {};

class DriverNimbleDB final : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "nimbledb"; }

  Result Open(Config* /*config*/, const std::string& datadir) override;
  Result Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  Result Begin(Context ctx, BenchType step) override;
  Result Next(Context ctx, BenchType step, Record* kv) override;
  Result Done(Context ctx, BenchType step) override;

 private:
  Config* config_ = nullptr;

  std::shared_ptr<nimbledb::DB> db_ = nullptr;
};

Result DriverNimbleDB::Open(Config* config, const std::string& datadir) {
  config_ = config;

  auto st = nimbledb::DB::Open(datadir + "/datafile.nmbl", {}, &db_);
  if (!st.IsOk()) {
    Log("error: {}, {}", __func__, st.ToString());
    return Result::kUnexpectedError;
  }

  return Result::kOk;
}

Result DriverNimbleDB::Close() {
  if (db_ != nullptr) {
    auto st = db_->Close();
    if (!st.IsOk()) {
      Log("error: {}, {}", __func__, st.ToString());
      return Result::kUnexpectedError;
    }
  }

  return Result::kOk;
}

Driver::Context DriverNimbleDB::ThreadNew() {
  auto* ctx = new DriverNimbleDBContext;
  return ctx;
}

void DriverNimbleDB::ThreadDispose(Context ctx) {
  delete static_cast<DriverNimbleDBContext*>(ctx);
}

Result DriverNimbleDB::Begin(Context /*ctx*/, BenchType /*step*/) {
  return Result::kOk;
}

Result DriverNimbleDB::Next(Context /*ctx*/, BenchType /*step*/,
                            Record* /*kv*/) {
  return Result::kOk;
}

Result DriverNimbleDB::Done(Context /*ctx*/, BenchType /*step*/) {
  return Result::kOk;
}

Driver* driver_nimbledb() {
  static DriverNimbleDB instance;
  return &instance;
}
