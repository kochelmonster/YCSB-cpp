//
//  lmdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LMDB_DB_H_
#define YCSB_C_LMDB_DB_H_

#include <string>
#include <mutex>
#include <cstdlib>
#include <cstring>
#include <endian.h>

#include "core/db.h"
#include "utils/utils.h"

#include <lmdb.h>

namespace ycsbc {

class LmdbDB : public DB {
 public:
  LmdbDB() : binary_key_(false), batch_size_(1), pending_(0), write_txn_(nullptr), txn_active_(false) {}
  ~LmdbDB() {}

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
  bool binary_key_;
  int batch_size_;
  int pending_;
  MDB_txn *write_txn_;
  bool txn_active_;
  char key_buf_[8];

  MDB_val EncodeKey(const std::string &key) {
    MDB_val k;
    if (!binary_key_) {
      k.mv_data = const_cast<void *>(static_cast<const void *>(key.data()));
      k.mv_size = key.size();
    } else {
      uint64_t n = std::strtoull(key.data() + 4, nullptr, 10);
      uint64_t be = htobe64(n);
      std::memcpy(key_buf_, &be, 8);
      k.mv_data = key_buf_;
      k.mv_size = 8;
    }
    return k;
  }
  void FlushBatch() {
    if (txn_active_) return;
    if (pending_ > 0 && write_txn_ != nullptr) {
      int ret = mdb_txn_commit(write_txn_);
      write_txn_ = nullptr;
      pending_ = 0;
      if (ret) throw utils::Exception(std::string("mdb_txn_commit: ") + mdb_strerror(ret));
    }
  }
  void CommitMutation() {
    if (txn_active_) return;
    if (++pending_ >= batch_size_) FlushBatch();
  }

  static size_t field_count_;
  static std::string field_prefix_;

  static MDB_env *env_;
  static MDB_dbi dbi_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB *NewLmdbDB();

} // ycsbc

#endif // YCSB_C_LMDB_DB_H_

