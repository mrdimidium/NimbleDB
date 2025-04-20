// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/nimbledb.h"

#include <gtest/gtest.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

namespace {

using OS = nimbledb::OS;
using File = nimbledb::File;
using Status = nimbledb::Status;

constexpr int kBufferSize = 20;
constexpr char const* kTestFilePath = "test_io_write_read_close";

}  // namespace

// NOLINTBEGIN(*-function-cognitive-complexity)
class OSTest : public ::testing::Test {
 public:
  void SetUp() override {
    src_buf_.fill(std::byte('a'));
    dst_buf_.fill(std::byte('b'));
  }

  void TearDown() override {
    // The file gets created below, either by createFile or openat.
    std::error_code rc;
    std::filesystem::remove(kTestFilePath, rc);
  }

  void Wait() const {}

  void OnReadCallback(Status&& status) {}

  std::unique_ptr<OS> os_ = nullptr;
  std::unique_ptr<File> file_ = nullptr;

  std::array<std::byte, kBufferSize> src_buf_{};
  std::array<std::byte, kBufferSize> dst_buf_{};
};

TEST_F(OSTest, CanOpenReadWrite) {
  bool readed_ = false;
  bool writed_ = false;

  const Status st1 = OS::Create(&os_);
  EXPECT_TRUE(st1.IsOk()) << st1.ToString();
  EXPECT_TRUE(os_ != nullptr);

  const File::Flags flags{.read = true, .write = true, .creat = true};

  // 1. Open datafile
  const Status st2 = os_->OpenDatafile(kTestFilePath, flags, &file_);
  EXPECT_TRUE(st2.IsOk()) << st2.ToString();
  EXPECT_TRUE(file_ != nullptr);

  // 2. Write to file
  file_->Write(src_buf_, 10, [&](const Status& st) {
    EXPECT_TRUE(st.IsOk()) << st.ToString();
    writed_ = true;
  });
  while (!writed_) {
    auto st = os_->Tick();
    ASSERT_TRUE(st.IsOk()) << st.ToString();
  }

  // 3. Read from file
  file_->Read(dst_buf_, 10, [&](const Status& st) {
    EXPECT_TRUE(st.IsOk()) << st.ToString();
    readed_ = true;
  });
  while (!readed_) {
    auto st = os_->Tick();
    ASSERT_TRUE(st.IsOk()) << st.ToString();
  }

  // 3. Close file
  auto st = file_->Close();
  ASSERT_TRUE(st.IsOk()) << st.ToString();

  EXPECT_TRUE(writed_);
  EXPECT_TRUE(readed_);
  EXPECT_EQ(dst_buf_, src_buf_);
}

class TestDB : public nimbledb::DB {};

TEST(DB, Smoke) {
  {
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

    status = db->Close();
    ASSERT_TRUE(status.IsOk());
  }

  {
    std::shared_ptr<nimbledb::DB> db;
    auto status = nimbledb::DB::Open("_db_test_smoke.bin", {}, &db);
    ASSERT_TRUE(status.IsOk());

    db->Get("Earth", [](const nimbledb::Status& st,
                        const std::optional<std::string>& value) {
      EXPECT_TRUE(st.IsOk());
      EXPECT_EQ(*value, "5_972.4");
    });

    status = db->Close();
    ASSERT_TRUE(status.IsOk());
  }
}

TEST(DB, SomeTreeLevels) {
  // It is convenient to use space names - there are quite a lot of them, the
  // names are human-readable, there is unicode symbols.
  constexpr std::array<std::string_view, 35 + 48 + 60 + 103>
      solar_system_objects = {
          // radii over 400 km
          "Sun", "Jupiter", "Saturn", "Uranus", "Neptune", "Earth", "Venus",
          "Mars", "Ganymede", "Titan", "Mercury", "Callisto", "Io (Jupiter I)",
          "Moon", "Europa (Jupiter Jupiter II)", "Triton", "Pluto", "Eris",
          "Haumea", "Titania", "Rhea", "Oberon", "Iapetus", "Makemake",
          "Gonggong", "Charon", "Umbriel", "Ariel", "Dione", "Quaoar", "Tethys",
          "Ceres", "Orcus", "Sedna", "Salacia",

          // From 200 to 399 km
          "2002 MS4", "2002 AW197", "Varda", "2013 FY27", "2003 AZ84",
          "2021 DR15", "Ixion", "2004 GV9", "2005 RN43", "Varuna", "2002 UX25",
          "2005 RM43", "Gǃkúnǁʼhòmdímà", "2014 UZ224", "2008 OG19",
          "2010 JO179", "Dysnomia", "2007 JJ43", "2014 EZ51", "2012 VP113",
          "2002 XW93", "2004 XR190", "2002 XV93", "2015 RR245", "2003 UZ413",
          "Vesta", "2003 VS2", "Pallas", "2004 TY364", "Enceladus",
          "2002 TC302", "2005 UQ513", "Miranda", "Dziewanna", "2005 TB190",
          "1999 DE9", "2003 FY128", "2002 VR128", "Vanth", "Hygiea",
          "2004 NT33", "Proteus", "2005 QU182", "Chaos", "2002 KX14",
          "2001 QF298", "Huya", "2004 PF115",

          // From 100 to 199 km
          "2004 UX10", "1998 VG44", "1993 SC", "Mimas", "1998 SN165",
          "2001 UR163", "1995 SM55", "2001 YH140", "2010 ER65", "Nereid",
          "1996 TL66", "2004 XA192", "2010 ET65", "2002 WC19", "2005 CA79",
          "Interamnia", "Ilmarë", "Europa 52", "Hiʻiaka", "2002 KW14",
          "1999 CD158", "2007 OC10", "2005 RR43", "Davida", "2002 TX300",
          "Actaea", "Sylvia", "2003 OP32", "Lempo", "Eunomia", "Hyperion",
          "Euphrosyne", "1998 SM165", "Cybele", "Chariklo", "Juno", "Hiisi",
          "Hektor", "Sila", "2007 RW10", "Altjira", "Nunam", "Bamberga",
          "Patientia", "Psyche", "Ceto", "Herculina", "S/2007 (148780) 1",
          "Hesperia", "Thisbe", "Doris", "Chiron", "Phoebe",
          "Satellite of (38628) Huya", "Fortuna", "Camilla", "Themis",
          "Amphitrite", "Egeria", "Iris",

          // From 50 to 99 km
          "Elektra", "Bienor", "Hebe", "Larissa", "Ursula", "S/2018 (532037) 1",
          "Eugenia", "Hermione", "Daphne", "Aurora", "Bertha", "Janus",
          "Teharonhiawako", "Aegle", "Galatea (Neptune VI)", "Phorcys", "Palma",
          "Metis", "Alauda", "Hilda", "Himalia", "Namaka", "Weywot", "Freia",
          "Amalthea", "Agamemnon", "Elpis", "Eleonora", "Nemesis", "Puck",
          "S/2015 (136472) 1", "Sycorax", "Io 85", "Minerva", "Alexandra",
          "Laetitia", "Nemausa", ", objectKalliope", "Despina", "Manwë",
          "Pales", "Parthenope", "Arethusa", "Pulcova", "Flora", "Ino",
          "Adeona", "Irene", "Melpomene", "Lamberta", "Aglaja", "Patroclus",
          "Julia", "Typhon", "Massalia", "Portia", "Emma", "Paha", "Lucina",
          "Sawiskera", "Achilles", "Panopaea", "Thule", "Borasisi", "Hestia",
          "Leto", "Undina", "Bellona", "Diana", "Anchises",
          "Bernardinelli-Bernstein", "Galatea 74", "Deiphobus", "Äneas",
          "Kleopatra", "Athamantis", "Diomedes", "Terpsichore", "Epimetheus",
          "Victoria", "Circe", "Leda", "Odysseus", "Alcathous", "Melete",
          "Mnemosyne", "Nestor", "Harmonia", "Leleākūhonua", "Euterpe",
          "Antilochus", "Thorondor", "Thalia", "Erato", "Astraea", "Pabu",
          "Eos", "Aegina", "Leukothea", "Menoetius", "Isis", "Klotho",
          "Troilus"};

  std::shared_ptr<nimbledb::DB> db;
  auto status = nimbledb::DB::Open("_db_test_sometreelevels.bin", {}, &db);
  ASSERT_TRUE(status.IsOk()) << status.ToString();

  int64_t index = 0;
  for (const auto& object : solar_system_objects) {
    db->Put(object, std::to_string(index),
            [](const nimbledb::Status& st, bool) { EXPECT_TRUE(st.IsOk()); });
    index += 1;
  }

  index = 0;
  for (const auto& object : solar_system_objects) {
    db->Get(object, [index, object](const nimbledb::Status& st,
                                    const std::optional<std::string>& value) {
      EXPECT_TRUE(st.IsOk());
      EXPECT_EQ(*value, std::to_string(index)) << object << ": " << index;
    });
    index += 1;
  }

  status = db->Close();
  ASSERT_TRUE(status.IsOk());
}
// NOLINTEND(*-function-cognitive-complexity)
