//
//  badger_db.cc
//  YCSB-cpp
//
//  BadgerDB database binding for YCSB-cpp (via CGo shared library)
//

#include <sys/stat.h>

#include "badger_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/properties.h"
#include "utils/utils.h"

namespace {
  const std::string PROP_DBPATH = "badger.dbpath";
  const std::string PROP_DBPATH_DEFAULT = "/tmp/ycsb-badger";

  const std::string PROP_SYNC_WRITES = "badger.sync_writes";
  const std::string PROP_SYNC_WRITES_DEFAULT = "false";
} // anonymous

namespace ycsbc {

badger_db_t BadgerDB::db_ = nullptr;
int BadgerDB::ref_cnt_ = 0;
std::mutex BadgerDB::mutex_;

void BadgerDB::Init() {
  const utils::Properties &props = *props_;

  sync_writes_ = props.GetProperty(PROP_SYNC_WRITES, PROP_SYNC_WRITES_DEFAULT) == "true";
  txn_active_ = false;

  const std::lock_guard<std::mutex> lock(mutex_);

  if (ref_cnt_++) {
    return;
  }

  const std::string &db_path = props.GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  if (db_path.empty()) {
    throw utils::Exception("BadgerDB db path is missing");
  }

  int ret = badger_open(const_cast<char *>(db_path.c_str()), sync_writes_, &db_);
  if (ret != BADGER_OK) {
    throw utils::Exception("BadgerDB: failed to open database at " + db_path);
  }
}

void BadgerDB::Cleanup() {
  FlushPending();
  const std::lock_guard<std::mutex> lock(mutex_);
  if (--ref_cnt_) {
    return;
  }
  if (db_) {
    badger_close(db_);
    db_ = nullptr;
  }
}

DB::Status BadgerDB::BeginTransaction() {
  if (txn_active_) return kError;
  FlushPending();
  txn_active_ = true;
  batch_keys_.clear();
  batch_vals_.clear();
  return kOK;
}

DB::Status BadgerDB::CommitTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;

  if (batch_keys_.empty()) {
    return kOK;
  }

  // Build arrays for badger_batch_set
  int n = batch_keys_.size();
  std::vector<char*> keys(n);
  std::vector<size_t> key_lens(n);
  std::vector<char*> vals(n);
  std::vector<size_t> val_lens(n);

  for (int i = 0; i < n; i++) {
    keys[i] = const_cast<char*>(batch_keys_[i].data());
    key_lens[i] = batch_keys_[i].size();
    vals[i] = const_cast<char*>(batch_vals_[i].data());
    val_lens[i] = batch_vals_[i].size();
  }

  int ret = badger_batch_set(db_, keys.data(), key_lens.data(),
                              vals.data(), val_lens.data(), n);
  batch_keys_.clear();
  batch_vals_.clear();

  if (ret != BADGER_OK) {
    return kError;
  }
  return kOK;
}

DB::Status BadgerDB::RollbackTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  batch_keys_.clear();
  batch_vals_.clear();
  return kOK;
}

void BadgerDB::FlushPending() {
  // Called at thread exit; commit any accumulated batch.
  if (txn_active_) {
    txn_active_ = false;
    // Commit the pending batch
    if (!batch_keys_.empty()) {
      int n = batch_keys_.size();
      std::vector<char*> keys(n);
      std::vector<size_t> key_lens(n);
      std::vector<char*> vals(n);
      std::vector<size_t> val_lens(n);

      for (int i = 0; i < n; i++) {
        keys[i] = const_cast<char*>(batch_keys_[i].data());
        key_lens[i] = batch_keys_[i].size();
        vals[i] = const_cast<char*>(batch_vals_[i].data());
        val_lens[i] = batch_vals_[i].size();
      }

      badger_batch_set(db_, keys.data(), key_lens.data(),
                       vals.data(), val_lens.data(), n);
    }
    batch_keys_.clear();
    batch_vals_.clear();
  }
}

DB::Status BadgerDB::Load(const std::string &table, Dataset &batch) {
  FlushPending();

  int n = batch.OpCount();
  std::vector<std::string> keys;
  std::vector<std::string> vals;
  keys.reserve(n);
  vals.reserve(n);

  for (int i = 0; i < n; ++i) {
    const auto &item = batch.Next();
    const auto &data = item.values.data();
    keys.push_back(item.key.ToString());
    vals.push_back(std::string(data.data(), data.size()));
  }

  std::vector<char*> key_ptrs(n);
  std::vector<size_t> key_lens(n);
  std::vector<char*> val_ptrs(n);
  std::vector<size_t> val_lens(n);

  for (int i = 0; i < n; i++) {
    key_ptrs[i] = const_cast<char*>(keys[i].data());
    key_lens[i] = keys[i].size();
    val_ptrs[i] = const_cast<char*>(vals[i].data());
    val_lens[i] = vals[i].size();
  }

  int ret = badger_batch_set(db_, key_ptrs.data(), key_lens.data(),
                              val_ptrs.data(), val_lens.data(), n);
  if (ret != BADGER_OK) {
    return kError;
  }
  return kOK;
}

DB::Status BadgerDB::Read(const std::string &table, Slice key,
                          const std::unordered_set<std::string> *fields, Fields &result) {
  // Reads always go directly to the DB (badger_batch_set doesn't support reading
  // uncommitted data, and writes within a batch are still visible via normal reads
  // since Badger's internal transactions are per-operation).
  char *val = nullptr;
  size_t val_len = 0;

  int ret = badger_get(db_, const_cast<char *>(key.data()), key.size(),
                       &val, &val_len);
  if (ret == BADGER_NOT_FOUND) {
    return kNotFound;
  }
  if (ret != BADGER_OK) {
    return kError;
  }

  if (fields != nullptr) {
    ReadonlyFields readonly(val, val_len);
    readonly.filter(result, *fields);
  } else {
    ReadonlyFields readonly(val, val_len);
    result = readonly;
  }
  badger_free(val);
  return kOK;
}

DB::Status BadgerDB::Scan(const std::string &table, Slice key, int len,
                          const std::unordered_set<std::string> *fields,
                          std::vector<Fields> &result) {
  char **keys_out = nullptr;
  size_t *key_lens = nullptr;
  char **vals_out = nullptr;
  size_t *val_lens = nullptr;
  int count = 0;

  int ret = badger_scan(db_, const_cast<char *>(key.data()), key.size(),
                         len, &keys_out, &key_lens, &vals_out, &val_lens, &count);
  if (ret != BADGER_OK) {
    return kError;
  }
  if (count == 0) {
    return kNotFound;
  }

  result.reserve(count);
  for (int i = 0; i < count; i++) {
    result.emplace_back();
    Fields &values = result.back();
    if (fields != nullptr) {
      ReadonlyFields readonly(vals_out[i], val_lens[i]);
      readonly.filter(values, *fields);
    } else {
      ReadonlyFields readonly(vals_out[i], val_lens[i]);
      values = readonly;
    }
  }

  badger_free_scan_results(keys_out, key_lens, vals_out, val_lens, count);
  return kOK;
}

DB::Status BadgerDB::Update(const std::string &table, Slice key, const ReadonlyFields &values) {
  // Read current value from DB
  char *val = nullptr;
  size_t val_len = 0;
  int ret = badger_get(db_, const_cast<char *>(key.data()), key.size(),
                       &val, &val_len);
  if (ret == BADGER_NOT_FOUND) {
    return kNotFound;
  }
  if (ret != BADGER_OK) {
    return kError;
  }

  // Merge fields
  ReadonlyFields readonly(val, val_len);
  updated_fields_ = readonly;
  updated_fields_.update(values);
  badger_free(val);

  const auto& buffer = updated_fields_.buffer();

  if (txn_active_) {
    // Accumulate in batch buffer
    batch_keys_.push_back(key.ToString());
    batch_vals_.push_back(std::string(buffer.data(), buffer.size()));
  } else {
    // Write directly to DB
    ret = badger_set(db_, const_cast<char *>(key.data()), key.size(),
                      const_cast<char *>(buffer.data()), buffer.size());
    if (ret != BADGER_OK) {
      return kError;
    }
  }
  return kOK;
}

DB::Status BadgerDB::Insert(const std::string &table, Slice key, const ReadonlyFields &values) {
  const auto &data = values.data();

  if (txn_active_) {
    // Accumulate in batch buffer
    batch_keys_.push_back(key.ToString());
    batch_vals_.push_back(std::string(data.data(), data.size()));
  } else {
    int ret = badger_set(db_, const_cast<char *>(key.data()), key.size(),
                         const_cast<char *>(data.data()), data.size());
    if (ret != BADGER_OK) {
      return kError;
    }
  }
  return kOK;
}

DB::Status BadgerDB::Delete(const std::string &table, Slice key) {
  // Badger's cbadger API doesn't support batch delete.
  // Issue delete directly to DB even during a transaction.
  int ret = badger_delete(db_, const_cast<char *>(key.data()), key.size());
  if (ret == BADGER_NOT_FOUND) {
    return kNotFound;
  }
  if (ret != BADGER_OK) {
    return kError;
  }
  return kOK;
}

DB *NewBadgerDB() {
  return new BadgerDB;
}

const bool registered = DBFactory::RegisterDB("badger", NewBadgerDB);

} // namespace ycsbc