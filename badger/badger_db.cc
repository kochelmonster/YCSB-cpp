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

  const std::string PROP_BINARY_KEY = "badger.binary_key";
  const std::string PROP_BINARY_KEY_DEFAULT = "false";

  const std::string PROP_SYNC_WRITES = "badger.sync_writes";
  const std::string PROP_SYNC_WRITES_DEFAULT = "false";
} // anonymous

namespace ycsbc {

badger_db_t BadgerDB::db_ = nullptr;
int BadgerDB::ref_cnt_ = 0;
std::mutex BadgerDB::mutex_;

void BadgerDB::Init() {
  const utils::Properties &props = *props_;

  binary_key_ = props.GetProperty(PROP_BINARY_KEY, PROP_BINARY_KEY_DEFAULT) == "true";
  sync_writes_ = props.GetProperty(PROP_SYNC_WRITES, PROP_SYNC_WRITES_DEFAULT) == "true";

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
  const std::lock_guard<std::mutex> lock(mutex_);
  if (--ref_cnt_) {
    return;
  }
  if (db_) {
    badger_close(db_);
    db_ = nullptr;
  }
}

DB::Status BadgerDB::Read(const std::string &table, Slice key,
                          const std::unordered_set<std::string> *fields, Fields &result) {
  std::string encoded = EncodeKey(key);
  char *val = nullptr;
  size_t val_len = 0;

  int ret = badger_get(db_, const_cast<char *>(encoded.data()), encoded.size(),
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
  std::string encoded = EncodeKey(key);
  char **keys_out = nullptr;
  size_t *key_lens = nullptr;
  char **vals_out = nullptr;
  size_t *val_lens = nullptr;
  int count = 0;

  int ret = badger_scan(db_, const_cast<char *>(encoded.data()), encoded.size(),
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
  std::string encoded = EncodeKey(key);

  // Read current value
  char *val = nullptr;
  size_t val_len = 0;
  int ret = badger_get(db_, const_cast<char *>(encoded.data()), encoded.size(),
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
  ret = badger_set(db_, const_cast<char *>(encoded.data()), encoded.size(),
                   const_cast<char *>(buffer.data()), buffer.size());
  if (ret != BADGER_OK) {
    return kError;
  }
  return kOK;
}

DB::Status BadgerDB::Insert(const std::string &table, Slice key, const ReadonlyFields &values) {
  std::string encoded = EncodeKey(key);
  const auto &data = values.data();

  int ret = badger_set(db_, const_cast<char *>(encoded.data()), encoded.size(),
                       const_cast<char *>(data.data()), data.size());
  if (ret != BADGER_OK) {
    return kError;
  }
  return kOK;
}

DB::Status BadgerDB::Delete(const std::string &table, Slice key) {
  std::string encoded = EncodeKey(key);

  int ret = badger_delete(db_, const_cast<char *>(encoded.data()), encoded.size());
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
