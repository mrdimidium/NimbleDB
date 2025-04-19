// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_NIMBLEDB_H_
#define NIMBLEDB_NIMBLEDB_H_

#if defined(NIMBLEDB_SHARED)
  #if defined(WIN32) && !defined(__MINGW32__)
    #if defined(NIMBLEDB_SHARED_EXPORTS)
      #define NIMBLEDB_EXPORTS __declspec(dllexport)
    #else
      #define NIMBLEDB_EXPORTS __declspec(dllimport)
    #endif
  #else
    #if defined(NIMBLEDB_SHARED_EXPORTS)
      #define NIMBLEDB_EXPORTS __attribute__((visibility("default")))
    #else
      #define NIMBLEDB_EXPORTS
    #endif
  #endif
#else
  #define NIMBLEDB_EXPORTS
#endif

namespace nimbledb {

class NIMBLEDB_EXPORTS DB {
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
