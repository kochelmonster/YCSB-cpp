//
//  lmdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LMDB_DB_H_
#define YCSB_C_LMDB_DB_H_

#include <endian.h>
#include <lmdb.h>

#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

#include "core/dataset.h"
#include "core/db.h"
#include "utils/utils.h"

namespace ycsbc {

class LmdbDB : public DB {
 public:
  LmdbDB() : txn_(nullptr), txn_active_(false) {}
  ~LmdbDB() {}

  void Init();

  void Cleanup();

  void FlushPending() override;

  Status Read(const std::string& table, Slice key,
              const std::unordered_set<std::string>* fields, Fields& result,
              bool rmw = false);
  Status Scan(const std::string& table, Slice key, int len,
              const std::unordered_set<std::string>* fields,
              std::vector<Fields>& result);
  Status Update(const std::string& table, Slice key,
                const ReadonlyFields& values);
  Status Insert(const std::string& table, Slice key,
                const ReadonlyFields& values);
  Status Delete(const std::string& table, Slice key);
  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;

  Status Load(const std::string& table, Dataset& batch) override;

  void EnsureTransaction(unsigned int flags = 0);

 private:
  Fields current_values_;
  MDB_txn* txn_;
  bool txn_active_;
  size_t batch_size_;

  static size_t field_count_;
  static std::string field_prefix_;

  static MDB_env* env_;
  static MDB_dbi dbi_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB* NewLmdbDB();

}  // namespace ycsbc

#endif  // YCSB_C_LMDB_DB_H_