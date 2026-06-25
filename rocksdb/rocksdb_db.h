//
//  rocksdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_ROCKSDB_DB_H_
#define YCSB_C_ROCKSDB_DB_H_

#include <string>
#include <mutex>
#include <cstdlib>
#include <cstring>
#include <endian.h>

#include "core/dataset.h"
#include "core/db.h"
#include "utils/properties.h"
#include "utils/utils.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

namespace ycsbc {

class RocksdbDB : public DB {
 public:
  RocksdbDB() : txn_active_(false) {}
  ~RocksdbDB() {}

  void Init();

  void Cleanup();

  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;
  void FlushPending() override;

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

  Status Load(const std::string &table, Dataset &batch) override;

 private:
  enum RocksFormat {
    kSingleRow,
  };
  RocksFormat format_;
  Fields updated_fields_;

  void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                  std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);

  Status ReadSingle(const std::string &table, Slice key,
                    const std::unordered_set<std::string> *fields, Fields &result,
                    bool rmw = false);
  Status ScanSingle(const std::string &table, Slice key, int len,
                    const std::unordered_set<std::string> *fields,
                    std::vector<Fields> &result);
  Status UpdateSingle(const std::string &table, Slice key,
                      const ReadonlyFields &values);
  Status MergeSingle(const std::string &table, Slice key,
                     const ReadonlyFields &values);
  Status InsertSingle(const std::string &table, Slice key,
                      const ReadonlyFields &values);
  Status DeleteSingle(const std::string &table, Slice key);

  Status (RocksdbDB::*method_read_)(const std::string &, Slice,
                                    const std::unordered_set<std::string> *, Fields &,
                                    bool);
  Status (RocksdbDB::*method_scan_)(const std::string &, Slice,
                                    int, const std::unordered_set<std::string> *,
                                    std::vector<Fields> &);
  Status (RocksdbDB::*method_update_)(const std::string &, Slice,
                                      const ReadonlyFields &);
  Status (RocksdbDB::*method_insert_)(const std::string &, Slice,
                                      const ReadonlyFields &);
  Status (RocksdbDB::*method_delete_)(const std::string &, Slice);

  int fieldcount_;

  bool txn_active_;
  rocksdb::WriteBatch write_batch_;

  void FlushWriteBatch() {
    if (write_batch_.Count() > 0) {
      rocksdb::Status s = db_->Write(wopt_, &write_batch_);
      if (!s.ok()) throw utils::Exception(std::string("RocksDB Write: ") + s.ToString());
      write_batch_.Clear();
    }
  }

  static std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  static rocksdb::DB *db_;
  static int ref_cnt_;
  static std::mutex mu_;
  static rocksdb::WriteOptions wopt_;
};

DB *NewRocksdbDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_