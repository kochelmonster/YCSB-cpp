//
//  leaves_db.h
//  YCSB-cpp
//
//  Leaves embedded database binding
//

#ifndef YCSB_C_LEAVES_DB_H_
#define YCSB_C_LEAVES_DB_H_

#include <endian.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "core/db.h"
#include "utils/properties.h"

// Include Leaves database headers
#include <leaves/confluence.hpp>
#include <leaves/leaves.hpp>

namespace ycsbc {

class LeavesDB : public DB {
 public:
  LeavesDB()
      : fieldcount_(0),
        binary_key_(false),
        batch_size_(1),
        pending_(0),
        txn_active_(false) {}
  ~LeavesDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string& table, Slice key,
              const std::unordered_set<std::string>* fields, Fields& result) override;

  Status Scan(const std::string& table, Slice key, int len,
              const std::unordered_set<std::string>* fields,
              std::vector<Fields>& result) override;

  Status Update(const std::string& table, Slice key,
                const ReadonlyFields& values) override;

  Status Insert(const std::string& table, Slice key,
                const ReadonlyFields& values) override;

  Status Delete(const std::string& table, Slice key) override;

  Status BeginTransaction();
  Status CommitTransaction();
  Status RollbackTransaction();

  bool SupportsMultiThreadWrite() const override;

  void FlushPending() override;

 private:
  using SingleDB = leaves::TDB<leaves::MapStorage>;
  using SingleCursor = SingleDB::Cursor;

  enum LeavesFormat {
    kSingleRow,
    kConfluence,
  };
  LeavesFormat format_;

  // Database instance management
  static std::shared_ptr<leaves::MapStorage> storage_;
  static std::shared_ptr<SingleDB> single_db_;
  static std::shared_ptr<leaves::MapConfluenceDB> confluence_db_;
  static int ref_cnt_;
  static std::mutex mu_;

  int fieldcount_;
  std::string dbpath_;
  size_t mapsize_;
  SingleCursor cursor_;
  leaves::MapConfluenceCursor confluence_cursor_;
  Fields updated_fields_;
  bool sync_ = false;
  bool binary_key_ = false;
  bool wal_enabled_ = false;
  int batch_size_ = 1;
  int pending_;
  bool txn_active_;
  char key_buf_[8];

  // Encode a YCSB key ("user" + decimal) into a leaves Slice.
  // Binary mode: strip "user" prefix, parse uint64, store as 8-byte big-endian.
  // ASCII mode:  use the raw string as-is.
  leaves::Slice EncodeKey(Slice key) {
    if (!binary_key_) {
      return leaves::Slice(key.data(), key.size());
    }
    // Skip the "user" prefix (4 bytes)
    uint64_t n = std::strtoull(key.data() + 4, nullptr, 10);
    uint64_t be = htobe64(n);
    std::memcpy(key_buf_, &be, 8);
    return leaves::Slice(key_buf_, 8);
  }

  // Record one mutation; commit when batch is full.
  void CommitMutation() {
    if (txn_active_) return;
    if (++pending_ >= batch_size_) {
      if (format_ == kConfluence) {
        confluence_cursor_.commit(sync_);
      } else {
        cursor_.commit(sync_);
      }
      pending_ = 0;
    }
  }

  void EnsureMutationReady() {
    if (!txn_active_ && pending_ == 0) {
      if (format_ == kConfluence) {
        confluence_cursor_.start_transaction();
      } else {
        cursor_.start_transaction(wal_enabled_);
      }
    }
  }
};

DB* NewLeavesDB();

}  // namespace ycsbc

#endif  // YCSB_C_LEAVES_DB_H_