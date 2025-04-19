// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_NIMBLEDB_H_
#define NIMBLEDB_NIMBLEDB_H_

namespace nimbledb {

class DB {
 public:
  // No copying & moving allowed
  DB(DB &) = delete;
  DB(DB &&) = delete;
  DB &operator=(DB &&) = delete;
  void operator=(const DB &) = delete;

  explicit DB();
  virtual ~DB();
};

}  // namespace nimbledb

#endif  // NIMBLEDB_NIMBLEDB_H_
