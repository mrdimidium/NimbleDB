// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/nimbledb.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

// NOLINTBEGIN(*-function-cognitive-complexity)
TEST(DBTest, BasicReadWrite) {
  std::shared_ptr<nimbledb::DB> db;
  auto status = nimbledb::DB::Open("_db_test_smoke.bin", {}, &db);
  ASSERT_TRUE(status.IsOk());

  db->Put("Mercury", "330.11",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Venus", "4_867.5",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Earth", "5_972.4",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Mars", "641.71",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Jupiter", "1_898_187",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Saturn", "568_317",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Uranus", "86_813",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  db->Put("Neptune", "102_413",
          [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
  // Wait, where is Pluto? So Pluto is not a planet.

  db->Get("Earth", [](const nimbledb::Status& st,
                      const std::optional<std::string>& value) {
    EXPECT_TRUE(st.IsOk());
    EXPECT_EQ(*value, "5_972.4");
  });

  status = db->Close();
  ASSERT_TRUE(status.IsOk());

  status = db->Close();
  ASSERT_TRUE(status.IsOk());
}
// NOLINTEND(*-function-cognitive-complexity)
