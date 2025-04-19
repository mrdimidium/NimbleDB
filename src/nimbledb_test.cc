// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/nimbledb.h"

#include <gtest/gtest.h>

#include <tuple>

// Demonstrate some basic assertions.
TEST(HelloTest, BasicAssertions) {
  std::ignore = nimbledb::DB();

  // Expect two strings not to be equal.
  EXPECT_STRNE("hello", "world");
  // Expect equality.
  EXPECT_EQ(7 * 6, 42);
}
