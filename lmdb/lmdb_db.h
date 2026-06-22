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

#include "core/dataset.h"
#include "core/db.h"
#include "utils/utils.h"

#include <lmdb.h>

namespace ycsbc {

class LmdbDB : public DB {
 public:
  LmdbDB() : write_txn_(nullptr), txn_active_(false) {}
  ~LmdbDB() {}

  void Init();

  void Cleanup();

  void FlushPending() override;

  Status Read(const std::string &table, Slice key,
              const std::unordered_set<std::string> *fields, Fields &result);

  Status Scan(const std::string &table, Slice key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result);

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values);

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values);

  Status Delete(const std::string &table, Slice key);

  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;

  Status Load(const std::string &table, Dataset &batch) override;

 private:
  Fields current_values_;
  MDB_txn *write_txn_;
  bool txn_active_;

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