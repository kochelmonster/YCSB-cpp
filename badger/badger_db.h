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
  BadgerDB() : binary_key_(false) {}
  ~BadgerDB() {}

  void Init();
  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::unordered_set<std::string> *fields, Fields &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result);

  Status Update(const std::string &table, const std::string &key, Fields &values);

  Status Insert(const std::string &table, const std::string &key, Fields &values);

  Status Delete(const std::string &table, const std::string &key);

 private:
  bool binary_key_;
  char key_buf_[8];

  std::string EncodeKey(const std::string &key) {
    if (!binary_key_) {
      return key;
    }
    uint64_t n = std::strtoull(key.data() + 4, nullptr, 10);
    uint64_t be = htobe64(n);
    return std::string(reinterpret_cast<const char *>(&be), 8);
  }

  static badger_db_t db_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB *NewBadgerDB();

} // namespace ycsbc

#endif // YCSB_C_BADGER_DB_H_
