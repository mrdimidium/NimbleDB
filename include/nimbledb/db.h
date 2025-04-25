// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#ifndef NIMBLEDB_NIMBLEDB_H_
#define NIMBLEDB_NIMBLEDB_H_

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "nimbledb/base.h"
#include "nimbledb/system.h"

namespace NIMBLEDB_NAMESPACE {

struct NIMBLEDB_EXPORT Options {};

class NIMBLEDB_EXPORT DB {
 public:
  // No copying & moving allowed
  DB(DB&) = delete;
  DB(DB&&) = delete;
  DB& operator=(DB&&) = delete;
  void operator=(const DB&) = delete;

  virtual ~DB();

  // Open database, return shared_ptr to DB instance
  static Status Open(std::string_view filename, const Options& options,
                     std::shared_ptr<DB>* dbptr);

  // Synchronizes any unfinished state to disk and graceful close database.
  // Use this method instead of the implicit destructor to handle errors.
  Status Close();

  // Find key in database, return std::nullopt if not found
  void Get(std::string_view key,
           const Callback<std::optional<std::string>>& callback);

  // Add key to database, overrite if key exists
  void Put(std::string_view key, std::string_view value,
           const Callback<bool /* rewritten */>& callback);

  // Delete key from database. Returns succes if key not found.
  void Delete(std::string_view key, const Callback<bool /* found */>& callback);

#ifndef NDEBUG
  // For debug purposes, return a graphical representation of the tree
  void DebugRenderBTree(std::ostream& in);
#endif  // !DEBUG

 protected:
  struct BTreeNode;
  struct BTreeNodeKey;
  struct BTreeNodeVal;

  using NodeId = int64_t;
  using NodeType = enum : uint8_t { kInterior, kLeaf };

  DB(Options options, std::unique_ptr<OS> os, std::unique_ptr<File> datafile);

  void NodeSplit(const std::shared_ptr<BTreeNode>& x, NodeId child_id);
  void NodeInsert(NodeId node_id, std::string_view k, std::string_view v);

  auto AddNode(NodeType page_type) -> std::shared_ptr<BTreeNode>;
  auto GetNode(NodeId id) -> std::shared_ptr<BTreeNode>;
  Status Sync();

  bool closed_ = false;

  const Options options_;

  std::unique_ptr<OS> os_ = nullptr;
  std::unique_ptr<File> datafile_ = nullptr;

  NodeId pages_ = 0;
  NodeId root_id_ = 0;
  std::map<NodeId, std::shared_ptr<BTreeNode>> nodes_;
};

}  // namespace NIMBLEDB_NAMESPACE

#endif  // NIMBLEDB_NIMBLEDB_H_
