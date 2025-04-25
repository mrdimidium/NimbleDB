// Copyright 2025 Nikolay Govorov. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may obtain a copy of the License at LICENSE file in the root.

#include "nimbledb/db.h"

#include <sys/types.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <queue>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "nimbledb/base.h"
#include "nimbledb/system.h"

namespace {

// The size of the b-tree page on disk. Must be a multiple of 4KB (the minimum
// data block size on most systems)
constexpr size_t btree_page_size = 1U << 16U;  // 64KB

// B-tree cardinality
constexpr size_t btree_page_keys = 48;

// Key & value max size in bytes
constexpr size_t btree_maxsize_key = 64;
constexpr size_t btree_maxsize_value = 512;

}  // namespace

namespace NIMBLEDB_NAMESPACE {

// NOLINTBEGIN(*-avoid-c-arrays)
struct alignas(8) DB::BTreeNodeKey {
  alignas(8) size_t size;
  alignas(8) char bytes[btree_maxsize_key];

  static std::string ToString(const BTreeNodeKey& x) {
    return {&(x.bytes)[0], x.size};
  }

  static int Compare(const BTreeNodeKey& lhs, std::string_view rhs) {
    const int result = std::memcmp(&(lhs.bytes[0]), rhs.data(),
                                   std::min(lhs.size, rhs.size()));
    if (result == 0) {
      if (lhs.size < rhs.size()) {
        return -1;
      }
      if (lhs.size > rhs.size()) {
        return 1;
      }
    }
    return result;
  }

  static int Compare(const BTreeNodeKey& lhs, const BTreeNodeKey& rhs) {
    const int result = std::memcmp(&(lhs.bytes[0]), &(rhs.bytes[0]),
                                   std::min(lhs.size, rhs.size));
    if (result == 0) {
      if (lhs.size < rhs.size) {
        return -1;
      }
      if (lhs.size > rhs.size) {
        return 1;
      }
    }
    return result;
  }

  static void Copy(BTreeNodeKey& dest, std::string_view src) {
    dest.size = src.size();
    std::memcpy(&(dest.bytes)[0], src.data(),
                std::min(btree_maxsize_key, src.size()));
  }
  static void Copy(BTreeNodeKey& dest, const BTreeNodeKey& src) {
    dest.size = src.size;
    std::memcpy(&(dest.bytes[0]), &(src.bytes[0]), src.size);
  }
};

struct alignas(8) DB::BTreeNodeVal {
  alignas(8) size_t size;
  alignas(8) char bytes[btree_maxsize_value];

  static std::string ToString(const BTreeNodeVal& x) {
    return {&(x.bytes)[0], x.size};
  }

  static void Copy(BTreeNodeVal& dest, std::string_view src) {
    dest.size = src.size();
    std::memcpy(&(dest.bytes)[0], src.data(),
                std::min(btree_maxsize_value, src.size()));
  }
  static void Copy(BTreeNodeVal& dest, const BTreeNodeVal& src) {
    dest.size = src.size;
    std::memcpy(&(dest.bytes[0]), &(src.bytes[0]), src.size);
  }
};

struct alignas(128) DB::BTreeNode {
  alignas(8) NodeId id;
  alignas(8) NodeType page_type;

  alignas(8) int64_t size;
  alignas(8) BTreeNodeKey keys[(2 * btree_page_keys) - 1];
  alignas(8) BTreeNodeVal vals[(2 * btree_page_keys) - 1];

  alignas(8) NodeId children[2 * btree_page_keys];
};
// NOLINTEND(*-avoid-c-arrays)

// static
Status DB::Open(std::string_view filename, const Options& options,
                std::shared_ptr<DB>* dbptr) {
  std::unique_ptr<OS> os;
  if (auto st = OS::Create(&os); !st.IsOk()) {
    return st;
  }

  std::unique_ptr<File> datafile;
  const File::Flags flags{
      .read = true, .write = true, .creat = true, .append = true};
  if (auto st = os->OpenDatafile(filename, flags, &datafile); !st.IsOk()) {
    return st;
  }

  int64_t filesize;
  if (auto st = datafile->GetFileSize(&filesize); !st.IsOk()) {
    return st;
  }

  auto* db = new (std::nothrow) DB(options, std::move(os), std::move(datafile));
  if (db == nullptr) {
    return Status::NoMemory();
  }
  dbptr->reset(db);

  if (auto div = filesize % static_cast<int64_t>(btree_page_size); div != 0) {
    return Status::CorruptedDatafile(
        "data file size is not a multiple of page size",
        std::format("{} bytes", div));
  }

  db->pages_ = filesize / static_cast<int64_t>(btree_page_size);

  return Status::Ok();
}

DB::DB(Options options, std::unique_ptr<OS> os, std::unique_ptr<File> datafile)
    : options_(options), os_(std::move(os)), datafile_(std::move(datafile)) {
  static_assert(sizeof(DB::BTreeNode) <= btree_page_size);
  static_assert(std::is_trivial_v<BTreeNode> &&
                std::is_standard_layout_v<BTreeNode>);
  static_assert(std::is_trivial_v<BTreeNodeKey> &&
                std::is_standard_layout_v<BTreeNodeKey>);
  static_assert(std::is_trivial_v<BTreeNodeVal> &&
                std::is_standard_layout_v<BTreeNodeVal>);
}

DB::~DB() {
  if (!closed_) {
    std::ignore = Close();
  }
}

Status DB::Close() {
  closed_ = true;

  if (auto st = Sync(); !st.IsOk()) {
    return st;
  }

  if (auto st = datafile_->Close(); !st.IsOk()) {
    return st;
  }

  if (auto st = os_->Close(); !st.IsOk()) {
    return st;
  }

  return Status::Ok();
}

void DB::Get(
    std::string_view key,
    const std::function<void(Status, std::optional<std::string>)>& callback) {
  if (pages_ == 0) {
    callback(Status::Ok(), std::nullopt);
    return;
  }

  std::optional<NodeId> node_id = root_id_;

  while (node_id != std::nullopt) {
    const auto node = GetNode(*node_id);
    node_id = std::nullopt;

    if (BTreeNodeKey::Compare(node->keys[0], key) > 0) {
      if (node->page_type == kLeaf) {
        break;
      }
      node_id = static_cast<int64_t>(node->children[0]);
      continue;
    }

    if (BTreeNodeKey::Compare(node->keys[node->size - 1], key) < 0) {
      if (node->page_type == kLeaf) {
        break;
      }
      node_id = static_cast<int64_t>(node->children[node->size]);
      continue;
    }

    for (int64_t i = 0; i < node->size; ++i) {
      const int cmp = BTreeNodeKey::Compare(node->keys[i], key);

      if (cmp == 0) {
        callback(Status::Ok(), BTreeNodeVal::ToString(node->vals[i]));
        return;
      }

      if (cmp > 0) {
        if (node->page_type == kLeaf) {
          callback(Status::Ok(), std::nullopt);
          return;
        }

        node_id = static_cast<int64_t>(node->children[i]);
      }
    }
  }

  callback(Status::Ok(), std::nullopt);
}

void DB::Put(std::string_view key, std::string_view value,
             const std::function<void(Status, bool rewritten)>& callback) {
  std::shared_ptr<BTreeNode> root;
  if (nodes_.empty()) {
    root = AddNode(kLeaf);
    root->size = 0;

    root_id_ = root->id;
  } else {
    root = GetNode(root_id_);
  }

  if (root->size == 2 * btree_page_keys - 1) {
    auto new_root = AddNode(kInterior);
    new_root->size = 0;
    new_root->children[0] = root->id;
    root_id_ = new_root->id;

    NodeSplit(new_root, 0);
  }

  NodeInsert(root_id_, key, value);

  callback(Status::Ok(), false);
}

void DB::Delete(std::string_view key,
                const std::function<void(Status, bool found)>& callback) {
  // not implemented
  std::ignore = key;
  std::ignore = callback;
}

std::shared_ptr<DB::BTreeNode> DB::AddNode(NodeType page_type) {
  auto* buffer = new std::byte[btree_page_size];
  auto node = std::shared_ptr<BTreeNode>(
      new (buffer) BTreeNode(), [](BTreeNode* rawptr) {
        auto* bptr = reinterpret_cast<std::byte*>(rawptr);
        delete[] bptr;
      });
  node->id = pages_;
  node->size = 0;
  node->page_type = page_type;

  auto [it, success] = nodes_.emplace(pages_, std::move(node));
  assert(success);

  pages_ += 1;
  return it->second;
}

auto DB::GetNode(NodeId id) -> std::shared_ptr<BTreeNode> {
  if (nodes_.contains(id)) {
    return nodes_.at(id);
  }

  auto* buffer = new std::byte[btree_page_size];

  datafile_->Read(std::span(buffer, btree_page_size),
                  static_cast<off_t>(id * btree_page_size),
                  [](const Status& st) {
                    if (!st.IsOk()) {
                      std::cerr << st.ToString();
                      std::abort();
                    }
                  });

  // Cast bytes to packaged struct
  const std::shared_ptr<BTreeNode> ptr(
      reinterpret_cast<BTreeNode*>(buffer), [](BTreeNode* rawptr) {
        auto* bptr = reinterpret_cast<std::byte*>(rawptr);
        delete[] bptr;
      });

  return nodes_[id] = ptr;
}

Status DB::Sync() {
  for (const auto& [id, node] : nodes_) {
    const auto* buffer = reinterpret_cast<const std::byte*>(node.get());

    datafile_->Write(std::span(buffer, btree_page_size),
                     static_cast<off_t>(id * btree_page_size),
                     [](const Status& st) {
                       if (!st.IsOk()) {
                         std::cerr << st.ToString();
                         std::abort();
                       }
                     });

    datafile_->Sync(File::SyncMode::kNormal, [](const Status& st) {
      if (!st.IsOk()) {
        std::cerr << st.ToString();
        std::abort();
      }
    });
  }

  return Status::Ok();
}

// NOLINTBEGIN(misc-no-recursion)
void DB::NodeInsert(NodeId node_id, std::string_view k, std::string_view v) {
  auto node = GetNode(node_id);
  assert(std::cmp_less(node->size, (btree_page_keys * 2) - 1));

  int64_t i = node->size - 1;

  switch (node->page_type) {
    case kLeaf:
      for (; i >= 0 && BTreeNodeKey::Compare(node->keys[i], k) > 0; --i) {
        BTreeNodeKey::Copy(node->keys[i + 1], node->keys[i]);
        BTreeNodeVal::Copy(node->vals[i + 1], node->vals[i]);
      }
      i += 1;

      BTreeNodeKey::Copy(node->keys[i], k);
      BTreeNodeVal::Copy(node->vals[i], v);
      node->size += 1;

      // Fsync(node);
      break;

    case kInterior:
      for (; i >= 0 && BTreeNodeKey::Compare(node->keys[i], k) > 0; --i) {
      }
      i += 1;

      auto child = GetNode(node->children[i]);

      if (child->size == 2 * btree_page_keys - 1) {
        NodeSplit(node, i);
        if (BTreeNodeKey::Compare(node->keys[i], k) < 0) {
          i += 1;
        }
      }

      NodeInsert(node->children[i], k, v);

      break;
  }
}
// NOLINTEND(misc-no-recursion)

void DB::NodeSplit(const std::shared_ptr<BTreeNode>& x, NodeId child_id) {
  auto y = GetNode(x->children[child_id]);

  auto z = AddNode(y->page_type);
  z->size = btree_page_keys - 1;

  for (size_t j = 0; j < btree_page_keys - 1; ++j) {
    BTreeNodeKey::Copy(z->keys[j], y->keys[j + btree_page_keys]);
    BTreeNodeVal::Copy(z->vals[j], y->vals[j + btree_page_keys]);
  }

  if (y->page_type != kLeaf) {
    for (size_t j = 0; j < btree_page_keys - 1; ++j) {
      z->children[j] = y->children[j + btree_page_keys];
    }
  }

  y->size = btree_page_keys - 1;

  for (int64_t j = x->size; j >= child_id + 1; j--) {
    x->children[j + 1] = x->children[j];
  }
  x->children[child_id + 1] = z->id;

  for (int64_t j = x->size - 1; j >= child_id; --j) {
    BTreeNodeKey::Copy(x->keys[j + 1], x->keys[j]);
    BTreeNodeVal::Copy(x->vals[j + 1], x->vals[j]);
  }
  BTreeNodeKey::Copy(x->keys[child_id], y->keys[btree_page_keys - 1]);
  BTreeNodeVal::Copy(x->vals[child_id], y->vals[btree_page_keys - 1]);
  x->size += 1;
}

#ifndef NDEBUG
  #if !defined(NIMBLEDB_OS_WINDOWS)
    #define BOLD(x) "\e[1m" x "\e[0m"
  #else
    #define BOLD(x) x
  #endif

void DB::DebugRenderBTree(std::ostream& in) {
  in << "\n\n===================\n";
  in << std::format("root id: {}\n", root_id_);
  in << std::format("nodes count: {}\n", nodes_.size());
  in << std::format("btree_page_keys: {}\n", btree_page_keys);
  in << "\n";

  std::queue<NodeId> q;
  q.push(root_id_);

  for (size_t t = 0; t < 10 && !q.empty(); ++t) {
    const auto node = GetNode(q.front());
    q.pop();

    std::string type;
    switch (node->page_type) {
      case kLeaf:
        type = "leaf";
        break;
      case kInterior:
        type = "interior";
        break;
    }

    in << std::format("=> " BOLD("node") "[{}]:\t",
                      static_cast<int64_t>(node->id));
    in << std::format(BOLD("size") "={}\t", node->size);
    in << std::format(BOLD("type") "={}\t", type);

    if (node->page_type != kLeaf) {
      in << BOLD("children") "=[";
      for (int64_t i = 0; i <= node->size; ++i) {
        q.push(node->children[i]);

        in << node->children[i];
        if (i + 1 <= node->size) {
          in << ", ";
        }
      }
      in << "]\t";
    }

    in << BOLD("data") "=[";
    for (int64_t i = 0; i < node->size; ++i) {
      in << std::format("'{}'=\'{}\'", BTreeNodeKey::ToString(node->keys[i]),
                        BTreeNodeVal::ToString(node->vals[i]))
         << (i + 1 < node->size ? ", " : "");
    }
    in << "]";

    in << "\n";
  }

  in << "\n===================\n\n";
}

#endif  // !DEBUG

}  // namespace NIMBLEDB_NAMESPACE
