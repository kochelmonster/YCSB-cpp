//
//  null_db.h
//  YCSB-cpp
//
//  Null database for measuring benchmark overhead
//

#ifndef YCSB_C_NULL_DB_H_
#define YCSB_C_NULL_DB_H_

#include "core/db.h"

namespace ycsbc {

class NullDB : public DB {
 public:
  NullDB() {}
  ~NullDB() {}

  void Init() override {}

  void Cleanup() override {}

  Status Read(const std::string &table, Slice key,
              const std::unordered_set<std::string> *fields, Fields &result,
              bool rmw = false) override {
    return kOK;
  }

  Status Scan(const std::string &table, Slice key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result) override {
    return kOK;
  }

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) override {
    return kOK;
  }

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) override {
    return kOK;
  }

  Status Delete(const std::string &table, Slice key) override {
    return kOK;
  }

  Status Load(const std::string &table, Dataset &batch) override {
    return kOK;
  }
};

} // namespace ycsbc

#endif // YCSB_C_NULL_DB_H_
