// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "record.h"

#include <linux/limits.h>

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <random>
#include <tuple>

#include "base.h"

namespace {

constexpr uint64_t Remix4Tail(uint64_t point) {
  /* fast and dirty remix */
  return point ^ (((point << 47ULL) | (point >> 17ULL)) +
                  UINTMAX_C(7015912586649315971));
}

}  // namespace

std::array<uint16_t, kSeedBoxSize> Keyer::seed_box{};
// Maps x to y one-to-one. You can think of this as the hash function for a
// number without collision (since the cardinality of the input and output
// sets is equal). More about:
// https://en.wikipedia.org/wiki/Injective_function
uint64_t Keyer::Injection(uint64_t x) const {
  // magic 'fractal' prime, it has enough one-bits and prime by mod
  // 2^{8,16,24,32,40,48,56,64}
  x += UINTMAX_C(10042331536242289283);

  // stirs lower bits
  x ^= seed_box[x & (seed_box.size() - 1)];

  // These "magic" prime numbers were found and verified with a bit of brute
  // force.
  if (width_ == 1) {
    uint8_t y = x;
    y ^= y >> 1U;  // NOLINT(*-signed-bitwise)
    y *= 113U;
    y ^= y << 2U;  // NOLINT(*-signed-bitwise)
    return y;
  }
  if (width_ == 2) {
    uint16_t y = x;
    y ^= y >> 1U;  // NOLINT(*-signed-bitwise)
    y *= 25693U;
    y ^= y << 7U;  // NOLINT(*-signed-bitwise)
    return y;
  }
  if (width_ == 3) {
    const uint32_t m = Bitmask(24);
    uint32_t y = x & m;
    y ^= y >> 1UL;
    y *= 5537317UL;
    y ^= y << 12UL;
    return y & m;
  }
  if (width_ == 4) {
    uint32_t y = x;
    y ^= y >> 1UL;
    y *= 1923730889UL;
    y ^= y << 15U;
    return y;
  }
  if (width_ == 5) {
    const uint64_t m = Bitmask(40);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(274992889273);
    y ^= y << 13UL;
    return y & m;
  }
  if (width_ == 6) {
    const uint64_t m = Bitmask(48);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(70375646670269);
    y ^= y << 15ULL;
    return y & m;
  }
  if (width_ == 7) {
    const uint64_t m = Bitmask(56);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(23022548244171181);
    y ^= y << 4ULL;
    return y & m;
  }
  if (width_ == 8) {
    uint64_t y = x;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(4613509448041658233);
    y ^= y << 25ULL;
    return y;
  }

  Unreachable();
}

char *Keyer::Fill(uint64_t *point, char *dst, size_t length) const {
#ifdef DEBUG_KEY_GENERATION
  if (!options_.binary) {
    dst += snprintf(dst, options_.key_size + 1, "%0*" PRIu64, options_.key_size,
                    *point) +
           1;
  } else {
    dst += snprintf(dst, Align(options_.key_size), "%0*" PRIu64,
                    static_cast<int>(Align(options_.key_size)), *point);
  }
#else
  size_t left = width_ * 8;

  if (!options_.binary) {
    static_assert(alphabet.size() == 64);

    uint64_t acc = *point;
    for (;;) {
      *dst++ = alphabet[acc & 63U];
      if (--length == 0) {
        break;
      }
      acc >>= 6U;
      left -= 6;
      if (left < 6) {
        acc = *point = Remix4Tail(*point + acc);
        left = width_ * 8;
      }
    }
    *dst++ = 0;
  } else {
    auto *p = reinterpret_cast<uint64_t *>(dst);
    for (;;) {
      *p++ = htole64(*point);
      length -= 8;
      if (length <= 0) {
        break;
      }
      do {
        *point = Remix4Tail(*point);
        left += left;
      } while (left < 64);
    }
    dst = reinterpret_cast<char *>(p);
  }
#endif

  return dst;
}

char *Keyer::RecordPair(size_t vsize, uint64_t point, char *dst) const {
  point = Injection(point);
  dst = Fill(&point, dst, options_.key_size);

  if (vsize != 0) {
    point = Remix4Tail(point);
    dst = Fill(&point, dst, vsize);
  }

  return dst;
}

void Keyer::Init(unsigned int seed) {
  if (seed == 0) {
    seed = time(nullptr);
  }

  std::ranlux48_base random{seed};
  std::uniform_int_distribution<uint16_t> uniform_dist(0, UINT16_MAX);

  for (auto &i : seed_box) {
    i = uniform_dist(random);
  }
}

Keyer::Keyer(size_t kspace, size_t ksector, Options options)
    : options_(options) {
  auto maxkey = options_.count * options_.spaces_count;
  assert(maxkey >= 2);

  size_t bits;
  if (maxkey < Bitmask(16)) {
    bits = 16;
  } else if (maxkey < Bitmask(24)) {
    bits = 24;
  } else if (maxkey < Bitmask(32)) {
    bits = 32;
  } else if (maxkey < Bitmask(40)) {
    bits = 40;
  } else if (maxkey < Bitmask(48)) {
    bits = 48;
  } else if (maxkey < Bitmask(56)) {
    bits = 56;
  } else if (maxkey < UINT64_MAX) {
    bits = 64;
  } else {
    auto required = ceil(log(static_cast<double>(maxkey)) / std::numbers::ln2);
    Fatal(
        "key-gen: {} sector of {} items is too huge, unable provide by "
        "{}-bit arithmetics, at least {} required",
        options_.sectors_count, options_.count, sizeof(uintmax_t),
        ceil(required));
  }

  long double bytes4maxkey = log(static_cast<double>(Bitmask(bits))) /
                             log(!options_.binary ? alphabet.size() : 256);
  if (bytes4maxkey > options_.key_size) {
    Fatal(
        "record-gen: key-length {} is insufficient for {} sectors of {} {} "
        "items, at least {} required",
        options_.key_size, options_.sectors_count,
        options_.binary ? "binary" : "printable", options_.count,
        ceill(bytes4maxkey));
  }

  Log("key-gen: using {} bits, up to {} keys", bits, maxkey);

  width_ = bits / 8;

  buf_.reset(new char[record_bytes()]);

  base_ = kspace * options_.count;
  serial_ = 0;
  if (ksector != 0) {
    serial_ = options_.count * ksector / options_.sectors_count;
    serial_ %= options_.count;
  }
}

int Keyer::Get(Record *p, bool key_only) {
  p->key = std::span(buf_.get(), options_.key_size);

  if (key_only) {
    p->value = std::span<char>();
  } else {
    if (!options_.binary) {
      p->value =
          std::span(buf_.get() + (options_.key_size + 1), options_.value_size);
    } else {
      p->value =
          std::span(buf_.get() + Align(options_.key_size), options_.value_size);
    }
  }

  uint64_t point = base_ + serial_;
  serial_ = (serial_ + 1) % options_.count;

  char *end = RecordPair(p->value.size(), point, buf_.get());
  assert(end == buf_.get() + record_bytes());
  std::ignore = end;

  return 0;
}
