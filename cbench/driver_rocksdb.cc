// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <rocksdb/c.h>
#include <rocksdb/compression_type.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include <cassert>
#include <cstring>

#include "base.h"
#include "driver.h"

#ifndef HAVE_ROCKSDB
  #error \
      "Driver implementation requires `HAVE_ROCKSDB` variable that includes the definitions"
#endif  // !HAVE_ROCKSDB

namespace {

rocksdb::Slice ToSlice(std::span<char> span) {
  return {span.data(), span.size()};
}

}  // namespace

struct DriverRocksDBContext {
  rocksdb::Iterator *it = nullptr;
  rocksdb::WriteBatch *batch = nullptr;
};

class DriverRocksDB final : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "rocksdb"; }

  Result Open(Config * /*config*/, const std::string &datadir) override;
  Result Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  Result Begin(Context ctx, BenchType step) override;
  Result Next(Context ctx, BenchType step, Record *kv) override;
  Result Done(Context ctx, BenchType step) override;

 private:
  Config *config_ = nullptr;

  std::unique_ptr<rocksdb::DB> db = nullptr;

  rocksdb::Options opts;
  rocksdb::ReadOptions ropts;
  rocksdb::WriteOptions wopts;
};

Result DriverRocksDB::Open(Config *config, const std::string &datadir) {
  config_ = config;

  opts.compression = rocksdb::kNoCompression;
  opts.info_log = nullptr;
  opts.create_if_missing = true;
  ropts.fill_cache = false;

  /* LY: suggestions are welcome */
  switch (config_->syncmode) {
    case kModeSync:
      wopts.sync = true;
      opts.use_fsync = true;
      break;
    case kModeLazy:
    case kModeNoSync:
      wopts.sync = false;
      opts.use_fsync = false;
      break;
    default:
      Fatal("error: {}(): unsupported syncmode {}", __func__,
            to_string(config_->syncmode));
  }

  switch (config_->walmode) {
    case kWalDefault:
      break;
    case kWalEnabled:
      wopts.disableWAL = false;
      break;
    case kWalDisabled:
      wopts.disableWAL = true;
      break;
    default:
      Fatal("error: {}(): unsupported walmode {}", __func__,
            to_string(config_->walmode));
  }

  auto st = rocksdb::DB::Open(opts, datadir, &db);
  if (!st.ok()) {
    Log("error: {}, {}", __func__, st.ToString());
    return Result::kUnexpectedError;
  }

  return Result::kOk;
}

Result DriverRocksDB::Close() {
  if (db != nullptr) {
    auto st = db->Close();
    assert(st.ok());
  }
  return Result::kOk;
}

Driver::Context DriverRocksDB::ThreadNew() {
  auto *ctx = new DriverRocksDBContext;
  return ctx;
}

void DriverRocksDB::ThreadDispose(Context ctxptr) {
  auto *ctx = static_cast<DriverRocksDBContext *>(ctxptr);

  delete ctx->batch;
  delete ctx->it;
  delete ctx;
}

Result DriverRocksDB::Begin(Context ctxptr, BenchType step) {
  auto *ctx = static_cast<DriverRocksDBContext *>(ctxptr);

  switch (step) {
    case kTypeGet:
    case kTypeSet:
    case kTypeDelete:
      break;

    case kTypeIterate:
      ctx->it = db->NewIterator(ropts);
      if (ctx->it == nullptr) {
        Log("error: {}, {}, rocksdb_create_iterator() failed", __func__,
            to_string(step));
        return Result::kUnexpectedError;
      }
      ctx->it->SeekToFirst();
      break;

    case kTypeBatch:
    case kTypeCrud:
      ctx->batch = new rocksdb::WriteBatch;
      if (ctx->batch == nullptr) {
        Log("error: {}, {}, rocksdb_writebatch_create() failed", __func__,
            to_string(step));
        return Result::kUnexpectedError;
      }
      break;

    default:
      Unreachable();
  }

  return Result::kOk;
}

Result DriverRocksDB::Next(Context ctxptr, BenchType step, Record *kv) {
  auto *ctx = static_cast<DriverRocksDBContext *>(ctxptr);

  switch (step) {
    case kTypeSet: {
      rocksdb::Status st;
      if (ctx->batch != nullptr) {
        st = ctx->batch->Put(ToSlice(kv->key), ToSlice(kv->value));
      } else {
        st = db->Put(wopts, ToSlice(kv->key), ToSlice(kv->value));
      }
      if (!st.ok()) {
        Log("error: {}, {}, {}", __func__, to_string(step), st.ToString());
        return Result::kUnexpectedError;
      }
      break;
    }

    case kTypeDelete: {
      rocksdb::Status st;
      if (ctx->batch != nullptr) {
        st = ctx->batch->Delete(ToSlice(kv->key));
      } else {
        st = db->Delete(wopts, ToSlice(kv->key));
      }
      if (!st.ok()) {
        Log("error: {}, {}, {}", __func__, to_string(step), st.ToString());
        return Result::kUnexpectedError;
      }
      break;
    }

    case kTypeGet: {
      rocksdb::PinnableSlice value;
      auto st = db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(),
                        ToSlice(kv->key), &value);
      if (st.IsNotFound()) {
        if (ctx->batch == nullptr) /* TODO: rework to avoid */ {
          return Result::kNotFound;
        }
      }
      if (!st.ok()) {
        Log("error: {}, {}, {}", __func__, to_string(step), st.ToString());
        return Result::kUnexpectedError;
      }

      strncpy(kv->value.data(), value.data(), value.size());
      kv->value = std::span(kv->value.data(), value.size());
      break;
    }

    case kTypeIterate: {
      if (ctx->it->Valid()) {
        return Result::kNotFound;
      }

      auto key = ctx->it->key();

      strncpy(kv->key.data(), key.data(), key.size());
      kv->key = std::span(kv->key.data(), key.size());

      ctx->it->Next();
      break;
    }

    default:
      Unreachable();
  }

  return Result::kOk;
}

Result DriverRocksDB::Done(Context ctxptr, BenchType step) {
  auto *ctx = static_cast<DriverRocksDBContext *>(ctxptr);

  switch (step) {
    case kTypeGet:
    case kTypeSet:
    case kTypeDelete:
      break;

    case kTypeIterate:
      if (ctx->it != nullptr) {
        delete ctx->it;
        ctx->it = nullptr;
      }
      break;

    case kTypeCrud:
    case kTypeBatch:
      if (ctx->batch != nullptr) {
        auto st = db->Write(wopts, ctx->batch);
        if (!st.ok()) {
          Log("error: {}, {}, {}", __func__, to_string(step), st.ToString());
          return Result::kUnexpectedError;
        }
        delete ctx->batch;
        ctx->batch = nullptr;
      }
      break;

    default:
      Unreachable();
  }

  return Result::kOk;
}

Driver *driver_rocksdb() {
  static DriverRocksDB instance;
  return &instance;
}
