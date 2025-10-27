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

#include "core/db.h"
#include "utils/properties.h"

// Include Leaves database headers
#include <leaves/leaves.hpp>

namespace ycsbc {

class LeavesDB : public DB {
 public:
  LeavesDB() : fieldcount_(0) {}
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

 private:
  enum LeavesFormat {
    kSingleRow,
  };
  LeavesFormat format_;

  // Database instance management
  static std::unique_ptr<leaves::MapStorage> storage_;
  static int ref_cnt_;
  static std::mutex mu_;
  
  int fieldcount_;
  std::string dbpath_;
  size_t mapsize_;
  leaves::MapStorage::DB::Cursor cursor_;
  bool sync_;
};

DB *NewLeavesDB();

} // ycsbc

#endif // YCSB_C_LEAVES_DB_H_