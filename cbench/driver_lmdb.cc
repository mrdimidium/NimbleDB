// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include <cassert>
#include <format>

#include "base.h"
#include "driver.h"
#include "lmdb.h"

#ifndef HAVE_LMDB
  #error \
      "Driver implementation requires `HAVE_LMDB` variable that includes the definitions"
#endif  // !HAVE_LMDB

namespace {

constexpr MDB_dbi INVALID_DBI = -1;

}  // namespace

struct DriverLmdbContext {
  MDB_txn *txn = nullptr;
  MDB_cursor *cursor = nullptr;
};

class DriverLmdb final : public Driver {
 public:
  [[nodiscard]] std::string_view GetName() const override { return "lmdb"; }

  Result Open(Config *config, const std::string &datadir) override;
  Result Close() override;

  Context ThreadNew() override;
  void ThreadDispose(Context ctx) override;

  Result Begin(Context ctx, BenchType step) override;
  Result Next(Context ctx, BenchType step, Record *kv) override;
  Result Done(Context ctx, BenchType step) override;

 private:
  Config *config_ = nullptr;

  MDB_env *env = nullptr;
  MDB_dbi dbi = INVALID_DBI;
};

Result DriverLmdb::Open(Config *config, const std::string &datadir) {
  config_ = config;

  unsigned modeflags;

  int rc = mdb_env_create(&env);
  if (rc != MDB_SUCCESS) {
    Log("error: {}, {} ({})", __func__, mdb_strerror(rc), rc);
    return Result::kUnexpectedError;
  }

  rc = mdb_env_set_mapsize(env, 4ULL * 1024 * 1024 * 1024);
  if (rc != MDB_SUCCESS) {
    Log("error: {}, {} ({})", __func__, mdb_strerror(rc), rc);
    return Result::kUnexpectedError;
  }

  // suggestions are welcome
  switch (config_->syncmode) {
    case kModeSync:
      modeflags = 0;
      break;
    case kModeLazy:
      modeflags = MDB_NOSYNC | MDB_NOMETASYNC;  // NOLINT(hicpp-signed-bitwise)
      break;
    case kModeNoSync:
      modeflags = MDB_WRITEMAP | MDB_MAPASYNC;  // NOLINT(hicpp-signed-bitwise)
      break;
    default:
      Fatal("error: {}(): unsupported syncmode {}", __func__,
            to_string(config_->syncmode));
  }

  switch (config_->walmode) {
    case kWalDefault:
    case kWalDisabled:
      break;
    case kWalEnabled:
    default:
      Fatal("error: {}(): unsupported walmode {}", __func__,
            to_string(config_->walmode));
  }

  // NOLINTNEXTLINE(hicpp-signed-bitwise)
  rc = mdb_env_open(env, datadir.c_str(), modeflags | MDB_NORDAHEAD, 0644);
  if (rc != MDB_SUCCESS) {
    Log("error: {}, {} ({})", __func__, mdb_strerror(rc), rc);
    return Result::kUnexpectedError;
  }

  return Result::kOk;
}

Result DriverLmdb::Close() {
  if (dbi != INVALID_DBI) {
    mdb_dbi_close(env, dbi);
  }

  if (env != nullptr) {
    mdb_env_close(env);
  }

  return Result::kOk;
}

Driver::Context DriverLmdb::ThreadNew() {
  if (dbi == INVALID_DBI) {
    MDB_txn *txn = nullptr;

    if (auto rc = mdb_txn_begin(env, nullptr, 0, &txn); rc != MDB_SUCCESS) {
      Log("error: {}, {} ({})", __func__, mdb_strerror(rc), rc);
      return nullptr;
    }

    int rc = mdb_dbi_open(txn, nullptr, 0, &dbi);
    mdb_txn_abort(txn);
    if (rc != MDB_SUCCESS) {
      Log("error: {}, {} ({})", __func__, mdb_strerror(rc), rc);
      return nullptr;
    }

    assert(dbi != INVALID_DBI);
  }

  return new DriverLmdbContext;
}

void DriverLmdb::ThreadDispose(Context ctxptr) {
  auto *ctx = static_cast<DriverLmdbContext *>(ctxptr);
  if (ctx->cursor != nullptr) {
    mdb_cursor_close(ctx->cursor);
    ctx->cursor = nullptr;
  }
  if (ctx->txn != nullptr) {
    mdb_txn_abort(ctx->txn);
    ctx->txn = nullptr;
  }
  delete ctx;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
Result DriverLmdb::Begin(Context ctxptr, BenchType step) {
  assert(dbi != INVALID_DBI);

  auto *ctx = static_cast<DriverLmdbContext *>(ctxptr);

  int rc = 0;
  switch (step) {
    case kTypeSet:
    case kTypeBatch:
    case kTypeCrud:
    case kTypeDelete: {
      if (ctx->cursor != nullptr) {
        // cursor could NOT be reused for read/write
        mdb_cursor_close(ctx->cursor);
        ctx->cursor = nullptr;
      }
      if (ctx->txn != nullptr) {
        // transaction could NOT be reused for read/write
        mdb_txn_abort(ctx->txn);
        ctx->txn = nullptr;
      }
      int rc = mdb_txn_begin(env, nullptr, 0, &ctx->txn);
      if (rc != MDB_SUCCESS) {
        Log("error: {}, {}, {} ({})", __func__, to_string(step),
            mdb_strerror(rc), rc);
        return Result::kUnexpectedError;
      }
      break;
    }

    case kTypeIterate:
    case kTypeGet: {
      if (ctx->txn != nullptr) {
        rc = mdb_txn_renew(ctx->txn);
        if (rc != MDB_SUCCESS) {
          mdb_txn_abort(ctx->txn);
          ctx->txn = nullptr;
        }
      }
      if (ctx->txn == nullptr) {
        auto rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &ctx->txn);
        if (rc != MDB_SUCCESS) {
          Log("error: {}, {}, {} ({})", __func__, to_string(step),
              mdb_strerror(rc), rc);
          return Result::kUnexpectedError;
        }
      }

      if (step == kTypeIterate) {
        if (ctx->cursor != nullptr) {
          rc = mdb_cursor_renew(ctx->txn, ctx->cursor);
          if (rc != MDB_SUCCESS) {
            mdb_cursor_close(ctx->cursor);
            ctx->cursor = nullptr;
          }
        }
        if (ctx->cursor == nullptr) {
          auto rc = mdb_cursor_open(ctx->txn, dbi, &ctx->cursor);
          if (rc != MDB_SUCCESS) {
            Log("error: {}, {}, {} ({})", __func__, to_string(step),
                mdb_strerror(rc), rc);
            return Result::kUnexpectedError;
          }
        }
      }
      break;
    }

    case kTypeMaxCode:
      Unreachable();
  }

  return Result::kOk;
}

Result DriverLmdb::Next(Context ctxptr, BenchType step, Record *kv) {
  auto *ctx = static_cast<DriverLmdbContext *>(ctxptr);

  MDB_val k;
  MDB_val v;

  switch (step) {
    case kTypeSet:
      k.mv_data = kv->key.data();
      k.mv_size = kv->key.size();
      v.mv_data = kv->value.data();
      v.mv_size = kv->value.size();
      if (auto rc = mdb_put(ctx->txn, dbi, &k, &v, 0); rc != MDB_SUCCESS) {
        Log("error: {}, {}, {} ({})", __func__, to_string(step),
            mdb_strerror(rc), rc);
        return Result::kUnexpectedError;
      }
      break;

    case kTypeDelete: {
      k.mv_data = kv->key.data();
      k.mv_size = kv->key.size();
      auto rc = mdb_del(ctx->txn, dbi, &k, nullptr);
      if (rc == MDB_NOTFOUND) {
        return Result::kNotFound;
      }
      if (rc != MDB_SUCCESS) {
        Log("error: {}, {}, {} ({})", __func__, to_string(step),
            mdb_strerror(rc), rc);
        return Result::kUnexpectedError;
      }
      break;
    }

    case kTypeIterate: {
      auto rc = mdb_cursor_get(ctx->cursor, &k, &v, MDB_NEXT);
      if (rc == MDB_NOTFOUND) {
        kv->key = std::span<char>();
        kv->value = std::span<char>();
        return Result::kNotFound;
      }
      if (rc != MDB_SUCCESS) {
        Log("error: {}, {}, {} ({})", __func__, to_string(step),
            mdb_strerror(rc), rc);
        return Result::kUnexpectedError;
      }
      kv->key = std::span(static_cast<char *>(k.mv_data), k.mv_size);
      kv->value = std::span(static_cast<char *>(v.mv_data), v.mv_size);
      break;
    }

    case kTypeGet: {
      k.mv_data = kv->key.data();
      k.mv_size = kv->key.size();
      auto rc = mdb_get(ctx->txn, dbi, &k, &v);
      if (rc == MDB_NOTFOUND) {
        return Result::kNotFound;
      }
      if (rc != MDB_SUCCESS) {
        Log("error: {}, {}, {} ({})", __func__, to_string(step),
            mdb_strerror(rc), rc);
        return Result::kUnexpectedError;
      }
      break;
    }

    default:
      Unreachable();
  }

  return Result::kOk;
}

Result DriverLmdb::Done(Context ctxptr, BenchType step) {
  auto *ctx = static_cast<DriverLmdbContext *>(ctxptr);

  switch (step) {
    case kTypeSet:
    case kTypeBatch:
    case kTypeCrud:
    case kTypeDelete:
      if (auto rc = mdb_txn_commit(ctx->txn); rc != MDB_SUCCESS) {
        mdb_txn_abort(ctx->txn);
        ctx->txn = nullptr;

        Log("error: {}, {}, {} ({})", __func__, to_string(step),
            mdb_strerror(rc), rc);
        return Result::kUnexpectedError;
      }
      ctx->txn = nullptr;
      break;

    case kTypeIterate:
    case kTypeGet:
      mdb_txn_reset(ctx->txn);
      break;

    default:
      Unreachable();
  }

  return Result::kOk;
}

Driver *driver_lmdb() {
  static DriverLmdb instance;
  return &instance;
}
