//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "dataset.h"
#include "measurements.h"
#include "utils/timer.h"
#include "utils/utils.h"

namespace ycsbc {

class DBWrapper : public DB {
 public:
  DBWrapper(DB *db, Measurements *measurements) : db_(db), measurements_(measurements) {}
  ~DBWrapper() {
    delete db_;
  }
  void Init() {
    db_->Init();
  }
  void Cleanup() {
    db_->Cleanup();
  }
  void FlushPending() {
    db_->FlushPending();
  }
  Status BeginTransaction() {
    timer_.Start();
    Status s = db_->BeginTransaction();
    uint64_t elapsed = timer_.End();
    measurements_->Report(BEGIN_TXN, elapsed);
    return s;
  }
  Status CommitTransaction() {
    timer_.Start();
    Status s = db_->CommitTransaction();
    uint64_t elapsed = timer_.End();
    measurements_->Report(COMMIT_TXN, elapsed);
    return s;
  }
  Status RollbackTransaction() {
    timer_.Start();
    Status s = db_->RollbackTransaction();
    uint64_t elapsed = timer_.End();
    measurements_->Report(ROLLBACK_TXN, elapsed);
    return s;
  }
  Status Load(const std::string &table, Dataset &batch) {
    // No per-op measurement; the caller measures the full load phase externally.
    return db_->Load(table, batch);
  }
  Status Read(const std::string &table, Slice key,
              const std::unordered_set<std::string> *fields, Fields &result,
              bool rmw = false) {
    timer_.Start();
    Status s = db_->Read(table, key, fields, result, rmw);
    uint64_t elapsed = timer_.End();
    if (s == kSkip) {
      // Adapter indicated the read was unnecessary (e.g. RMW when
      // Update will re-read anyway). Do not measure the wasted time.
      return s;
    }
    if (rmw) {
      if (s == kOK) {
        measurements_->Report(READMODIFYWRITE, elapsed);
      } else {
        measurements_->Report(READMODIFYWRITE_FAILED, elapsed);
      }
    } else {
      if (s == kOK) {
        measurements_->Report(READ, elapsed);
      } else {
        measurements_->Report(READ_FAILED, elapsed);
      }
    }
    return s;
  }
  Status Scan(const std::string &table, Slice key, int record_count,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result) {
    timer_.Start();
    Status s = db_->Scan(table, key, record_count, fields, result);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(SCAN, elapsed);
    } else {
      measurements_->Report(SCAN_FAILED, elapsed);
    }
    return s;
  }
  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) {
    timer_.Start();
    Status s = db_->Update(table, key, values);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(UPDATE, elapsed);
    } else {
      measurements_->Report(UPDATE_FAILED, elapsed);
    }
    return s;
  }
  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) {
    timer_.Start();
    Status s = db_->Insert(table, key, values);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(INSERT, elapsed);
    } else {
      measurements_->Report(INSERT_FAILED, elapsed);
    }
    return s;
  }
  Status Delete(const std::string &table, Slice key) {
    timer_.Start();
    Status s = db_->Delete(table, key);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(DELETE, elapsed);
    } else {
      measurements_->Report(DELETE_FAILED, elapsed);
    }
    return s;
  }
 private:
  DB *db_;
  Measurements *measurements_;
  utils::Timer<uint64_t, std::nano> timer_;
};

} // ycsbc

#endif // YCSB_C_DB_WRAPPER_H_