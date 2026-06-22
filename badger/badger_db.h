//
//  badger_db.h
//  YCSB-cpp
//
//  BadgerDB database binding for YCSB-cpp (via CGo shared library)
//

#ifndef YCSB_C_BADGER_DB_H_
#define YCSB_C_BADGER_DB_H_

#include <string>
#include <mutex>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <vector>

#include "core/dataset.h"
#include "core/db.h"
#include "utils/utils.h"
#include "cbadger/cbadger.h"

namespace ycsbc {

class BadgerDB : public DB {
 public:
  BadgerDB() : sync_writes_(false), txn_active_(false) {}
  ~BadgerDB() {}

  void Init();
  void Cleanup();

  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;
  void FlushPending() override;

  Status Read(const std::string &table, Slice key,
               const std::unordered_set<std::string> *fields, Fields &result) override;

  Status Scan(const std::string &table, Slice key, int len,
               const std::unordered_set<std::string> *fields, std::vector<Fields> &result) override;

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) override;

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) override;

  Status Delete(const std::string &table, Slice key) override;

  Status Load(const std::string &table, Dataset &batch) override;

 private:
  bool sync_writes_;
  bool txn_active_;
  Fields updated_fields_;
  // Batch accumulation for writes within a transaction
  std::vector<std::string> batch_keys_;
  std::vector<std::string> batch_vals_;

  static badger_db_t db_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB *NewBadgerDB();

} // namespace ycsbc

#endif // YCSB_C_BADGER_DB_H_