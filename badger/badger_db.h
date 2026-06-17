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

#include "core/db.h"
#include "utils/utils.h"
#include "cbadger/cbadger.h"

namespace ycsbc {

class BadgerDB : public DB {
 public:
  BadgerDB() {}
  ~BadgerDB() {}

  void Init();
  void Cleanup();

  Status Read(const std::string &table, Slice key,
               const std::unordered_set<std::string> *fields, Fields &result) override;

  Status Scan(const std::string &table, Slice key, int len,
               const std::unordered_set<std::string> *fields, std::vector<Fields> &result) override;

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) override;

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) override;

  Status Delete(const std::string &table, Slice key) override;

  bool SupportsMultiThreadWrite() { return true; }

 private:
  bool sync_writes_;
  Fields updated_fields_;

  static badger_db_t db_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB *NewBadgerDB();

} // namespace ycsbc

#endif // YCSB_C_BADGER_DB_H_
