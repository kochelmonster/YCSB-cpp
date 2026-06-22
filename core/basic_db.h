//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include "db.h"
#include "core/dataset.h"
#include "utils/properties.h"

#include <iostream>
#include <string>
#include <mutex>

namespace ycsbc {

class BasicDB : public DB {
 public:
  BasicDB() : out_(nullptr) {}

  void Init();

  Status Read(const std::string &table, Slice key,
              const std::unordered_set<std::string> *fields, Fields &result);

  Status Scan(const std::string &table, Slice key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result);

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values);

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values);

  Status Delete(const std::string &table, Slice key);

  Status Load(const std::string &table, Dataset &batch) override;

 private:
  static std::mutex mutex_;

  std::ostream *out_;
};

DB *NewBasicDB();

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

