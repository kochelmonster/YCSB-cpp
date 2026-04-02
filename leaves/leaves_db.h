//
//  leaves_db.h
//  YCSB-cpp
//
//  Leaves embedded database binding
//

#ifndef YCSB_C_LEAVES_DB_H_
#define YCSB_C_LEAVES_DB_H_

#include <string>
#include <mutex>
#include <memory>
#include <endian.h>

#include "core/db.h"
#include "utils/properties.h"

// Include Leaves database headers
#include <leaves/leaves.hpp>

namespace ycsbc {

class LeavesDB : public DB {
 public:
  LeavesDB() : fieldcount_(0), binary_key_(false), batch_size_(1), pending_(0), txn_active_(false) {}
  ~LeavesDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::unordered_set<std::string> *fields, Fields &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result);

  Status Update(const std::string &table, const std::string &key, Fields &values);

  Status Insert(const std::string &table, const std::string &key, Fields &values);

  Status Delete(const std::string &table, const std::string &key);

  Status BeginTransaction();
  Status CommitTransaction();
  Status RollbackTransaction();

 private:
  enum LeavesFormat {
    kSingleRow,
  };
  LeavesFormat format_;

  // Database instance management
  static std::shared_ptr<leaves::MapStorage> storage_;
  static int ref_cnt_;
  static std::mutex mu_;
  
  int fieldcount_;
  std::string dbpath_;
  size_t mapsize_;
  leaves::MapStorage::DB::Cursor cursor_;
  bool sync_;
  bool binary_key_;
  int batch_size_;
  int pending_;
  bool txn_active_;
  char key_buf_[8];

  // Encode a YCSB key ("user" + decimal) into a leaves Slice.
  // Binary mode: strip "user" prefix, parse uint64, store as 8-byte big-endian.
  // ASCII mode:  use the raw string as-is.
  leaves::Slice EncodeKey(const std::string &key) {
    if (!binary_key_) {
      return leaves::Slice(key.data(), key.size());
    }
    // Skip the "user" prefix (4 bytes)
    uint64_t n = std::strtoull(key.data() + 4, nullptr, 10);
    uint64_t be = htobe64(n);
    std::memcpy(key_buf_, &be, 8);
    return leaves::Slice(key_buf_, 8);
  }

  // Commit pending mutations if any.
  void FlushPending() {
    if (txn_active_) return;
    if (pending_ > 0) {
      cursor_.commit(sync_);
      pending_ = 0;
    }
  }

  // Record one mutation; commit when batch is full.
  void CommitMutation() {
    if (txn_active_) return;
    if (++pending_ >= batch_size_) {
      cursor_.commit(sync_);
      pending_ = 0;
    }
  }
};

DB *NewLeavesDB();

} // ycsbc

#endif // YCSB_C_LEAVES_DB_H_