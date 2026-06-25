//
//  leveldb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LEVELDB_DB_H_
#define YCSB_C_LEVELDB_DB_H_

#include <iostream>
#include <string>
#include <mutex>
#include <cstdlib>
#include <cstring>
#include <endian.h>

#include "core/dataset.h"
#include "core/db.h"
#include "utils/properties.h"
#include "utils/utils.h"

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>
#include <leveldb/write_batch.h>

namespace ycsbc {

class LeveldbDB : public DB {
 public:
  LeveldbDB() : sync_(false), txn_active_(false) {}
  ~LeveldbDB() {}

  void Init();

  void Cleanup();

  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;
  void FlushPending() override;

  Status Load(const std::string &table, Dataset &batch) override;

  Status Read(const std::string &table, Slice key,
               const std::unordered_set<std::string> *fields, Fields &result,
               bool rmw = false) override {
    return (this->*(method_read_))(table, key, fields, result, rmw);
  }

  Status Scan(const std::string &table, Slice key, int len,
               const std::unordered_set<std::string> *fields, std::vector<Fields> &result) override {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) override {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) override {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, Slice key) override {
    return (this->*(method_delete_))(table, key);
  }

 private:
  enum LdbFormat {
    kSingleEntry,
    kRowMajor,
    kColumnMajor
  };
  LdbFormat format_;
  Fields updated_fields_;

  void GetOptions(const utils::Properties &props, leveldb::Options *opt);
  std::string BuildCompKey(const std::string &key, const std::string &field_name);
  std::string KeyFromCompKey(const std::string &comp_key);
  std::string FieldFromCompKey(const std::string &comp_key);

  Status ReadSingleEntry(const std::string &table, Slice key,
                         const std::unordered_set<std::string> *fields, Fields &result,
                         bool rmw = false);
  Status ScanSingleEntry(const std::string &table, Slice key, int len,
                         const std::unordered_set<std::string> *fields,
                         std::vector<Fields> &result);
  Status UpdateSingleEntry(const std::string &table, Slice key,
                           const ReadonlyFields &values);
  Status InsertSingleEntry(const std::string &table, Slice key,
                           const ReadonlyFields &values);
  Status DeleteSingleEntry(const std::string &table, Slice key);

  Status ReadCompKeyRM(const std::string &table, Slice key,
                       const std::unordered_set<std::string> *fields, Fields &result,
                       bool rmw = false);
  Status ScanCompKeyRM(const std::string &table, Slice key, int len,
                       const std::unordered_set<std::string> *fields,
                       std::vector<Fields> &result);
  Status ReadCompKeyCM(const std::string &table, Slice key,
                       const std::unordered_set<std::string> *fields, Fields &result,
                       bool rmw = false);
  Status ScanCompKeyCM(const std::string &table, Slice key, int len,
                       const std::unordered_set<std::string> *fields,
                       std::vector<Fields> &result);
  Status InsertCompKey(const std::string &table, Slice key,
                       const ReadonlyFields &values);
  Status DeleteCompKey(const std::string &table, Slice key);

  Status (LeveldbDB::*method_read_)(const std::string &, Slice,
                                    const std::unordered_set<std::string> *, Fields &,
                                    bool);
  Status (LeveldbDB::*method_scan_)(const std::string &, Slice, int,
                                    const std::unordered_set<std::string> *,
                                    std::vector<Fields> &);
  Status (LeveldbDB::*method_update_)(const std::string &, Slice,
                                      const ReadonlyFields &);
  Status (LeveldbDB::*method_insert_)(const std::string &, Slice,
                                      const ReadonlyFields &);
  Status (LeveldbDB::*method_delete_)(const std::string &, Slice);

  int fieldcount_;
  std::string field_prefix_;

  bool sync_;
  bool txn_active_;
  leveldb::WriteBatch write_batch_;

  void FlushWriteBatch() {
    leveldb::WriteOptions wopt;
    wopt.sync = sync_;
    leveldb::Status s = db_->Write(wopt, &write_batch_);
    if (!s.ok()) throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
    write_batch_.Clear();
  }

  static leveldb::DB *db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewLeveldbDB();

} // ycsbc

#endif // YCSB_C_LEVELDB_DB_H_