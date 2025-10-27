// Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
// SPDX-License-Identifier: Apache-2.0

#ifndef _WIREDTIGER_DB_H
#define _WIREDTIGER_DB_H

#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

namespace ycsbc {

class WTDB : public DB {
 public:
  WTDB() {}
  ~WTDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::unordered_set<std::string> *fields, Fields &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, Fields &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, Fields &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:

  Status ReadSingleEntry(const std::string &table, const std::string &key,
                         const std::unordered_set<std::string> *fields, Fields &result);
  Status ScanSingleEntry(const std::string &table, const std::string &key, int len,
                         const std::unordered_set<std::string> *fields,
                         std::vector<Fields> &result);
  Status UpdateSingleEntry(const std::string &table, const std::string &key,
                           Fields &values);
  Status InsertSingleEntry(const std::string &table, const std::string &key,
                           Fields &values);
  Status DeleteSingleEntry(const std::string &table, const std::string &key);

  Status (WTDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::unordered_set<std::string> *, Fields &);
  Status (WTDB::*method_scan_)(const std::string &, const std::string &, int,
                                    const std::unordered_set<std::string> *,
                                    std::vector<Fields> &);
  Status (WTDB::*method_update_)(const std::string &, const std::string &,
                                      Fields &);
  Status (WTDB::*method_insert_)(const std::string &, const std::string &,
                                      Fields &);
  Status (WTDB::*method_delete_)(const std::string &, const std::string &);

  static WT_CONNECTION *conn_;
  WT_SESSION *session_{nullptr};
  WT_CURSOR *cursor_{nullptr};

  static int ref_cnt_;
  static std::mutex mu_;

};

DB *NewRocksdbDB();

} // namespace ycsbc

#endif
