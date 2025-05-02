// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_BENCH_RECORD_H_
#define NIMBLEDB_BENCH_RECORD_H_

#include <memory>

#include "driver.h"

constexpr size_t kSeedBoxSize = 2048;

const std::array<char, 64 /*2 + 10 + 26 + 26  */> alphabet = {
    '@', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
    'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
    'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_',
};

class Keyer {
 public:
  // A pool of pre-computed records.
  class Batch {
   public:
    Result Load(Record *p) {
      if (end_ - pos_ < gen_->record_bytes()) {
        return Result::kUnexpectedError;
      }

      p->key = std::span(pos_, gen_->options_.key_size);
      pos_ += !gen_->options_.binary ? p->key.size() + 1 : Align(p->key.size());

      p->value = std::span<char>();
      if (gen_->options_.value_size > 0) {
        p->value = std::span(pos_, gen_->options_.value_size);
        pos_ += !gen_->options_.binary ? p->value.size() + 1
                                       : Align(p->value.size());
      }

      return Result::kOk;
    }

   private:
    friend Keyer;

    Batch(Keyer *gen, size_t pool_size) {
      assert(pool_size < 1 || pool_size > INT_MAX / 2);

      auto bytes = gen->record_bytes() * pool_size;
      buf_.reset(new char[bytes]);

      char *dst = buf_.get();
      for (size_t i = 0; i < pool_size; ++i) {
        dst = gen_->RecordPair(gen->options_.value_size,
                               gen->base_ + gen->serial_, dst);
        gen->serial_ = (gen->serial_ + 1) % gen_->options_.count;
      }

      gen_ = gen;
      pos_ = buf_.get();
      end_ = dst;

      assert(dst == buf_.get() + bytes);
    }

    Keyer *gen_ = nullptr;
    char *pos_ = nullptr, *end_ = nullptr;

    std::unique_ptr<char[]> buf_;  // NOLINT(*-avoid-c-arrays)
  };

  struct Options {
    size_t count;

    size_t key_size;
    size_t value_size;

    size_t spaces_count;
    size_t sectors_count;

    bool binary;
  };

  static void Init(unsigned seed);

  Keyer(size_t kspace, size_t ksector, Options options);

  int Get(Record *p, bool key_only);

  Batch GetBatch(size_t batch_size) { return {this, batch_size}; }

 private:
  char *RecordPair(size_t vsize, uint64_t point, char *dst) const;

  [[nodiscard]] size_t record_bytes() const {
    return !options_.binary
               ? options_.key_size + 1 +
                     ((options_.value_size > 0) ? options_.value_size + 1 : 0)
               : Align(options_.key_size) + Align(options_.value_size);
  }

  char *Fill(uint64_t *point, char *dst, size_t length) const;

  // Maps x to y one-to-one. You can think of this as the hash function for a
  // number without collision (since the cardinality of the input and output
  // sets is equal). More about:
  // https://en.wikipedia.org/wiki/Injective_function
  [[nodiscard]] uint64_t Injection(uint64_t x) const;

  static std::array<uint16_t, kSeedBoxSize> seed_box;

  Options options_;
  size_t width_, base_, serial_;

  std::unique_ptr<char[]> buf_;  // NOLINT(*-avoid-c-arrays)
};

#endif  // NIMBLEDB_BENCH_RECORD_H_
