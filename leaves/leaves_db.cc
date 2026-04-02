//
//  leaves_db.cc
//  YCSB-cpp
//
//  Leaves embedded database binding implementation
//

#include "leaves_db.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <cstdlib>

#include "core/core_workload.h"
#include "core/db_factory.h"

namespace {
const std::string PROP_DBPATH = "leaves.dbpath";
const std::string PROP_DBPATH_DEFAULT = "/tmp/ycsb-leaves";

const std::string PROP_MAPSIZE = "leaves.mapsize";
const std::string PROP_MAPSIZE_DEFAULT = "1073741824";  // 1GB

const std::string PROP_FORMAT = "leaves.format";
const std::string PROP_FORMAT_DEFAULT = "single";

const std::string PROP_DESTROY = "leaves.destroy";
const std::string PROP_DESTROY_DEFAULT = "false";

const std::string PROP_SYNC = "leaves.sync";
const std::string PROP_SYNC_DEFAULT = "false";

const std::string PROP_BINARY_KEY = "leaves.binary_key";
const std::string PROP_BINARY_KEY_DEFAULT = "false";

const std::string PROP_BATCH_SIZE = "leaves.batch_size";
const std::string PROP_BATCH_SIZE_DEFAULT = "1";
}  // namespace

namespace ycsbc {

std::shared_ptr<leaves::MapStorage> LeavesDB::storage_(nullptr);
int LeavesDB::ref_cnt_(0);
std::mutex LeavesDB::mu_;

void LeavesDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties& props = *props_;
  dbpath_ = props.GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  mapsize_ = std::stoull(props.GetProperty(PROP_MAPSIZE, PROP_MAPSIZE_DEFAULT));

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  sync_ = props.GetProperty(PROP_SYNC, PROP_SYNC_DEFAULT) == "true";
  binary_key_ = props.GetProperty(PROP_BINARY_KEY, PROP_BINARY_KEY_DEFAULT) == "true";
  batch_size_ = std::stoi(props.GetProperty(PROP_BATCH_SIZE, PROP_BATCH_SIZE_DEFAULT));
  if (batch_size_ < 1) batch_size_ = 1;
  pending_ = 0;

  format_ = kSingleRow;
  const std::string& format =
      props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
  } else {
    throw utils::Exception("Unknown format");
  }

  bool destroy =
      props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true";
  if (destroy) {
    std::remove(dbpath_.c_str());
  }

  ref_cnt_++;
  if (ref_cnt_ == 1) {
    // First instance initializes the storage
    try {
      storage_ =
          std::make_shared<leaves::MapStorage>(dbpath_.c_str(), mapsize_);
      std::cout << "Leaves database initialized: " << dbpath_ << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Failed to initialize Leaves database: " << e.what()
                << std::endl;
      throw;
    }
  }

  cursor_ = (*storage_)["benchmark"].cursor();
}

void LeavesDB::Cleanup() {
  FlushPending();
  const std::lock_guard<std::mutex> lock(mu_);
  ref_cnt_--;
  if (ref_cnt_ == 0) {
    storage_.reset();
    std::cout << "Leaves database closed" << std::endl;
  }
}

DB::Status LeavesDB::BeginTransaction() {
  if (txn_active_) return kError;
  FlushPending();
  cursor_.start_transaction();
  pending_ = 0;
  txn_active_ = true;
  return kOK;
}

DB::Status LeavesDB::CommitTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  cursor_.commit(sync_);
  pending_ = 0;
  return kOK;
}

DB::Status LeavesDB::RollbackTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  cursor_.rollback();
  pending_ = 0;
  return kOK;
}

DB::Status LeavesDB::Read(const std::string& table, const std::string& key,
                          const std::unordered_set<std::string>* fields,
                          Fields& result) {
  try {
    FlushPending();
    leaves::Slice key_slice = EncodeKey(key);

    cursor_.find(key_slice);

    if (!cursor_.is_valid()) {
      return kNotFound;
    }
    
    leaves::Slice value_slice = cursor_.value();

    ReadonlyFields readonly(value_slice.data(), value_slice.size());
    if (fields) {
      readonly.filter(result, *fields);
    } else {
      result = readonly;
    }

    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Read error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Scan(const std::string& table, const std::string& key,
                          int len, const std::unordered_set<std::string>* fields,
                          std::vector<Fields>& result) {
  try {
    FlushPending();
    leaves::Slice key_slice = EncodeKey(key);

    cursor_.find(key_slice);

    result.clear();
    int count = 0;

    while (cursor_.is_valid() && count < len) {
      leaves::Slice value_slice = cursor_.value();
      ReadonlyFields readonly(value_slice.data(), value_slice.size());

      result.emplace_back();
      Fields &values = result.back();

      if (fields) {
        ReadonlyFields readonly(value_slice.data(), value_slice.size());
        readonly.filter(values, *fields);
      } else {
        values = readonly;
      }

      cursor_.next();
      count++;
    }

    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Scan error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Update(const std::string& table, const std::string& key,
                            Fields& values) {
  try {
    leaves::Slice key_slice = EncodeKey(key);
    cursor_.find(key_slice);
    if (!cursor_.is_valid()) {
      return kNotFound;
    }

    // Read existing value
    leaves::Slice existing_value = cursor_.value();
    
    ReadonlyFields readonly(existing_value.data(), existing_value.size());
    Slice updated_data = values.update(readonly);
    leaves::Slice value_slice(updated_data.data(), updated_data.size());
    cursor_.value(value_slice);

    CommitMutation();
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Update error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Insert(const std::string& table, const std::string& key,
                            Fields& values) {
  try {
    leaves::Slice key_slice = EncodeKey(key);
    cursor_.find(key_slice);

    const std::string& data = values.buffer();
    leaves::Slice value_slice(data.data(), data.size());
    cursor_.value(value_slice);

    CommitMutation();
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Insert error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Delete(const std::string& table, const std::string& key) {
  try {
    leaves::Slice key_slice = EncodeKey(key);

    cursor_.find(key_slice);

    if (!cursor_.is_valid()) {
      return kNotFound;
    }

    cursor_.remove();
    CommitMutation();
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Delete error: " << e.what() << std::endl;
    return kError;
  }
}

DB* NewLeavesDB() { return new LeavesDB; }

const bool registered = DBFactory::RegisterDB("leaves", NewLeavesDB);

}  // namespace ycsbc