//
//  leaves_db.h
//  YCSB-cpp
//
//  Leaves embedded database binding
//

#ifndef YCSB_C_LEAVES_DB_H_
#define YCSB_C_LEAVES_DB_H_

#include <endian.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "core/dataset.h"
#include "core/db.h"
#include "utils/properties.h"

// Include Leaves database headers
#include <leaves/confluence.hpp>
#include <leaves/mmap.hpp>

namespace ycsbc {

class LeavesDB : public DB {
 public:
  LeavesDB()
      : fieldcount_(0),
        txn_active_(false) {}
  ~LeavesDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string& table, Slice key,
              const std::unordered_set<std::string>* fields, Fields& result,
              bool rmw = false) override;

  Status Scan(const std::string& table, Slice key, int len,
              const std::unordered_set<std::string>* fields,
              std::vector<Fields>& result) override;

  Status Update(const std::string& table, Slice key,
                const ReadonlyFields& values) override;

  Status Insert(const std::string& table, Slice key,
                const ReadonlyFields& values) override;

  Status Delete(const std::string& table, Slice key) override;

  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;

  void FlushPending() override;

  Status Load(const std::string &table, Dataset &batch) override;

 private:
  using SingleDB = leaves::MapStorage::DB;
  using SingleCursor = SingleDB::Cursor;
  using ConfluenceDB = leaves::MapStorage::ConfluenceDB;
  using ConfluenceCursor = ConfluenceDB::Cursor;

  enum LeavesFormat {
    kSingleRow,
    kConfluence,
  };
  LeavesFormat format_;

  // Database instance management
  static std::shared_ptr<leaves::MapStorage> storage_;
  static SingleDB single_db_;
  static ConfluenceDB confluence_db_;
  static int ref_cnt_;
  static std::mutex mu_;

  int fieldcount_;
  std::string dbpath_;
  size_t batch_size_;
  size_t mapsize_;
  SingleCursor cursor_;
  ConfluenceCursor confluence_cursor_;
  Fields updated_fields_;
  bool sync_ = false;
  bool wal_enabled_ = false;
  bool txn_active_;
};

DB* NewLeavesDB();

}  // namespace ycsbc

#endif  // YCSB_C_LEAVES_DB_H_