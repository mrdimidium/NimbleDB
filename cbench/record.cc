// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "record.h"

#include <fcntl.h>
#include <ftw.h>
#include <linux/limits.h>
#include <pthread.h>
#include <strings.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cinttypes>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <tuple>

#include "base.h"

#ifndef DEBUG_KEYGEN
  #define DEBUG_KEYGEN 0
#endif

namespace {

constexpr size_t kSboxSize = 2048;
std::array<uint16_t, kSboxSize> sbox{};

void kv_sbox_init(unsigned seed) {
  for (size_t i = 0; i < kSboxSize; ++i) {
    sbox[i] = i;
  }

  for (size_t i = 0; i < kSboxSize; ++i) {
    unsigned n = (rand_r(&seed) % 280583U) & (kSboxSize - 1);
    unsigned x = sbox[i] ^ sbox[n];
    sbox[i] ^= x;
    sbox[n] ^= x;
  }

  for (size_t i = 0; i < kSboxSize; ++i) {
    sbox[i] ^= i;
  }
}

constexpr size_t kAlphabetCardinality = 64;  // 2 + 10 + 26 + 26
const std::array<char, kAlphabetCardinality> alphabet = {
    '@', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
    'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
    'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_',
};

constexpr uint64_t Bitmask(uint64_t n) { return ~UINTMAX_C(0) >> (64 - (n)); }

constexpr uint64_t Align(uint64_t n) {
  return (n + sizeof(uint64_t) - 1) & ~(sizeof(uint64_t) - 1);
}

struct {
  bool debug, printable;
  unsigned ksize, width, nsectors;
  uint64_t period;
} globals = {
    .debug = DEBUG_KEYGEN,
};

// NOLINTBEGIN(*-signed-bitwise)
uint64_t Mod2nInjection(uint64_t x) {
  // magic 'fractal' prime, it has enough one-bits and prime by mod
  // 2^{8,16,24,32,40,48,56,64}
  x += UINTMAX_C(10042331536242289283);

  // stirs lower bits
  x ^= sbox[x & (kSboxSize - 1)];

  // LY: https://en.wikipedia.org/wiki/Injective_function
  // These "magic" prime numbers were found and verified with a bit of brute
  // force.
  if (globals.width == 1) {
    uint8_t y = x;
    y ^= y >> 1U;
    y *= 113U;
    y ^= y << 2U;
    return y;
  }
  if (globals.width == 2) {
    uint16_t y = x;
    y ^= y >> 1U;
    y *= 25693U;
    y ^= y << 7U;
    return y;
  }
  if (globals.width == 3) {
    const uint32_t m = Bitmask(24);
    uint32_t y = x & m;
    y ^= y >> 1UL;
    y *= 5537317UL;
    y ^= y << 12UL;
    return y & m;
  }
  if (globals.width == 4) {
    uint32_t y = x;
    y ^= y >> 1UL;
    y *= 1923730889UL;
    y ^= y << 15U;
    return y;
  }
  if (globals.width == 5) {
    const uint64_t m = Bitmask(40);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(274992889273);
    y ^= y << 13UL;
    return y & m;
  }
  if (globals.width == 6) {
    const uint64_t m = Bitmask(48);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(70375646670269);
    y ^= y << 15ULL;
    return y & m;
  }
  if (globals.width == 7) {
    const uint64_t m = Bitmask(56);
    uint64_t y = x & m;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(23022548244171181);
    y ^= y << 4ULL;
    return y & m;
  }
  if (globals.width == 8) {
    uint64_t y = x;
    y ^= y >> 1ULL;
    y *= UINTMAX_C(4613509448041658233);
    y ^= y << 25ULL;
    return y;
  }

  Unreachable();
}
// NOLINTEND(*-signed-bitwise)

size_t PairBytes(size_t vsize) {
  size_t bytes;
  if (globals.printable) {
    bytes = globals.ksize + ((vsize > 0) ? vsize + 2 : 1);
  } else {
    bytes = Align(globals.ksize) + Align(vsize);
  }

  return bytes;
}

uint64_t Remix4Tail(uint64_t point) {
  /* fast and dirty remix */
  return point ^ (((point << 47ULL) | (point >> 17ULL)) +
                  UINTMAX_C(7015912586649315971));
}

char *Fill(uint64_t *point, char *dst, unsigned length) {
  unsigned left = globals.width * 8;

  if (globals.printable) {
    static_assert(kAlphabetCardinality == 64);
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
        left = globals.width * 8;
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

  return dst;
}

char *RecordPair(unsigned vsize, unsigned vage, uint64_t point, char *dst) {
  if (!globals.debug) {
    point = Mod2nInjection(point);
    dst = Fill(&point, dst, globals.ksize);
  } else {
    if (globals.printable) {
      dst +=
          snprintf(dst, globals.ksize + 1, "%0*" PRIu64, globals.ksize, point) +
          1;
    } else {
      dst += snprintf(dst, Align(globals.ksize), "%0*" PRIu64,
                      static_cast<int>(Align(globals.ksize)), point);
    }
  }

  if (vsize != 0) {
    point = Remix4Tail(point + vage);
    dst = Fill(&point, dst, vsize);
  }

  return dst;
}

}  // namespace

int RecordGen::Setup(bool printable, unsigned ksize, unsigned nspaces,
                     unsigned nsectors, uintmax_t period, size_t seed) {
  auto maxkey = period * nspaces;
  if (maxkey < 2) {
    return -1;
  }

  uint64_t top;
  unsigned width;
  if (maxkey < Bitmask(16)) {
    width = 16 / 8;
    top = Bitmask(16);
  } else if (maxkey < Bitmask(24)) {
    width = 24 / 8;
    top = Bitmask(24);
  } else if (maxkey < Bitmask(32)) {
    width = 32 / 8;
    top = Bitmask(32);
  } else if (maxkey < Bitmask(40)) {
    width = 40 / 8;
    top = Bitmask(40);
  } else if (maxkey < Bitmask(48)) {
    width = 48 / 8;
    top = Bitmask(48);
  } else if (maxkey < Bitmask(56)) {
    width = 56 / 8;
    top = Bitmask(56);
  } else if (maxkey < UINT64_MAX) {
    width = 64 / 8;
    top = UINT64_MAX;
  } else {
    auto required = ceil(log(static_cast<double>(maxkey)) / std::numbers::ln2);
    Log("key-gen: {} sector of {} items is too huge, unable provide by "
        "{}-bit arithmetics, at least {} required",
        nsectors, period, sizeof(uintmax_t) * 8, ceil(required));
    return -1;
  }

  double bytes4maxkey = log(static_cast<double>(top)) /
                        log(printable ? kAlphabetCardinality : 256);
  if (bytes4maxkey > ksize) {
    Log("record-gen: key-length {} is insufficient for {} sectors of {} {} "
        "items, "
        "at least {} required",
        ksize, nsectors, printable ? "printable" : "binary", period,
        ceil(bytes4maxkey));
    return -1;
  }

  Log("key-gen: using {} bits, up to {} keys", width * 8, maxkey);
  globals.printable = printable;
  globals.ksize = ksize;
  globals.period = period;
  globals.width = width;
  globals.nsectors = nsectors;

  if (seed == 0) {
    seed = time(nullptr);
  }
  kv_sbox_init(seed);
  return 0;
}

RecordGen::RecordGen(unsigned kspace, unsigned ksector, unsigned vsize,
                     unsigned vage)
    : vsize_(vsize), vage_(vage) {
  pair_bytes_ = PairBytes(vsize);
  buf_.reset(new char[pair_bytes_]);

  base_ = kspace * globals.period;
  serial_ = 0;
  if (ksector != 0) {
    serial_ = globals.period * ksector / globals.nsectors;
    serial_ %= globals.period;
  }
}

int RecordGen::Get(Record *p, bool key_only) {
  p->key = std::span(buf_.get(), globals.ksize);
  p->value = std::span<char>();
  if (!key_only) {
    p->value =
        std::span(buf_.get() + (globals.printable ? p->key.size() + 1
                                                  : Align(p->key.size())),
                  vsize_);
  }

  uint64_t point = base_ + serial_;
  serial_ = (serial_ + 1) % globals.period;

  char *end = RecordPair(p->value.size(), vage_, point, buf_.get());
  assert(end == buf_.get() + PairBytes(p->value.size()));
  std::ignore = end;

  return 0;
}

RecordPool::RecordPool(RecordGen *gen, unsigned pool_size) {
  assert(pool_size < 1 || pool_size > INT_MAX / 2);

  auto bytes = gen->pair_bytes_ * pool_size;
  buf_.reset(new char[bytes]);

  char *dst = buf_.get();
  for (size_t i = 0; i < pool_size; ++i) {
    dst = RecordPair(gen->vsize_, gen->vage_, gen->base_ + gen->serial_, dst);
    gen->serial_ = (gen->serial_ + 1) % globals.period;
  }

  gen_ = gen;
  pos_ = buf_.get();
  end_ = dst;

  assert(dst == buf_.get() + bytes);
}

int RecordPool::Pull(Record *p) {
  if (end_ - pos_ < gen_->pair_bytes_) {
    return -1;
  }

  p->key = std::span(pos_, globals.ksize);
  pos_ += globals.printable ? p->key.size() + 1 : Align(p->key.size());

  p->value = std::span<char>();
  if (gen_->vsize_ > 0) {
    p->value = std::span(pos_, gen_->vsize_);
    pos_ += globals.printable ? p->value.size() + 1 : Align(p->value.size());
  }

  return 0;
}
