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
}  // namespace

namespace ycsbc {

std::unique_ptr<leaves::MapStorage> LeavesDB::storage_(nullptr);
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
          std::make_unique<leaves::MapStorage>(dbpath_.c_str(), mapsize_);
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
  const std::lock_guard<std::mutex> lock(mu_);
  ref_cnt_--;
  if (ref_cnt_ == 0) {
    storage_.reset();
    std::cout << "Leaves database closed" << std::endl;
  }
}

DB::Status LeavesDB::Read(const std::string& table, const std::string& key,
                          const std::vector<std::string>* fields,
                          std::vector<Field>& result) {
  try {
    // Create key with table prefix
    leaves::Slice key_slice(key.data(), key.size());

    cursor_.find(key_slice);

    if (!cursor_.is_valid()) {
      return kNotFound;
    }

    // Check if the key matches exactly
    leaves::Slice found_key = cursor_.key();
    if (found_key.size() != key.size() ||
        std::memcmp(found_key.data(), key.data(), key.size()) != 0) {
      return kNotFound;
    }

    leaves::Slice value_slice = cursor_.value();
    std::string value_str(value_slice.data(), value_slice.size());

    result.clear();
    if (fields) {
      DeserializeRowFilter(result, value_str, *fields);
    } else {
      DeserializeRow(result, value_str);
    }

    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Read error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Scan(const std::string& table, const std::string& key,
                          int len, const std::vector<std::string>* fields,
                          std::vector<std::vector<Field>>& result) {
  try {
    // Create key with table prefix
    std::string full_key = table + ":" + key;
    leaves::Slice key_slice(full_key.data(), full_key.size());

    cursor_.find(key_slice);

    result.clear();
    int count = 0;

    while (cursor_.is_valid() && count < len) {
      leaves::Slice found_key = cursor_.key();
      std::string found_key_str(found_key.data(), found_key.size());

      // Check if key still has the table prefix
      if (found_key_str.find(table + ":") != 0) {
        break;
      }

      leaves::Slice value_slice = cursor_.value();
      std::string value_str(value_slice.data(), value_slice.size());

      std::vector<Field> values;
      if (fields) {
        DeserializeRowFilter(values, value_str, *fields);
      } else {
        DeserializeRow(values, value_str);
      }
      result.push_back(values);

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
                            std::vector<Field>& values) {
  try {
    // Create key with table prefix
    std::string full_key = table + ":" + key;
    leaves::Slice key_slice(full_key.data(), full_key.size());

    cursor_.find(key_slice);

    if (!cursor_.is_valid()) {
      cursor_.rollback();
      return kNotFound;
    }

    // Check if the key matches exactly
    leaves::Slice found_key = cursor_.key();
    if (found_key.size() != full_key.size() ||
        std::memcmp(found_key.data(), full_key.data(), full_key.size()) != 0) {
      cursor_.rollback();
      return kNotFound;
    }

    std::string data;
    SerializeRow(values, data);
    leaves::Slice value_slice(data.data(), data.size());
    cursor_.value(value_slice);

    cursor_.commit(sync_);
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Update error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Insert(const std::string& table, const std::string& key,
                            std::vector<Field>& values) {
  try {
    if (!cursor_.start_transaction()) {
      return kError;
    }

    // Create key with table prefix
    std::string full_key = table + ":" + key;
    leaves::Slice key_slice(full_key.data(), full_key.size());

    cursor_.find(key_slice);

    std::string data;
    SerializeRow(values, data);
    leaves::Slice value_slice(data.data(), data.size());
    cursor_.value(value_slice);

    cursor_.commit(sync_);
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Insert error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Delete(const std::string& table, const std::string& key) {
  try {
    if (!cursor_.start_transaction()) {
      return kError;
    }

    // Create key with table prefix
    std::string full_key = table + ":" + key;
    leaves::Slice key_slice(full_key.data(), full_key.size());

    cursor_.find(key_slice);

    if (!cursor_.is_valid()) {
      cursor_.rollback();
      return kNotFound;
    }

    // Check if the key matches exactly
    leaves::Slice found_key = cursor_.key();
    if (found_key.size() != full_key.size() ||
        std::memcmp(found_key.data(), full_key.data(), full_key.size()) != 0) {
      cursor_.rollback();
      return kNotFound;
    }

    cursor_.remove();
    cursor_.commit(sync_);
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Delete error: " << e.what() << std::endl;
    return kError;
  }
}

void LeavesDB::SerializeRow(const std::vector<Field>& values,
                            std::string& data) {
  data.clear();
  for (const Field& field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void LeavesDB::DeserializeRowFilter(std::vector<Field>& values,
                                    const std::string& data,
                                    const std::vector<std::string>& fields) {
  const char* p = data.data();
  const char* lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void LeavesDB::DeserializeRowFilter(std::vector<Field>& values, const char* p,
                                    const char* lim,
                                    const std::vector<std::string>& fields) {
  std::vector<Field> row;
  DeserializeRow(row, p, lim);
  values.clear();
  for (const Field& field : row) {
    if (std::find(fields.begin(), fields.end(), field.name) != fields.end()) {
      values.push_back(field);
    }
  }
}

void LeavesDB::DeserializeRow(std::vector<Field>& values,
                              const std::string& data) {
  const char* p = data.data();
  const char* lim = p + data.size();
  DeserializeRow(values, p, lim);
}

void LeavesDB::DeserializeRow(std::vector<Field>& values, const char* p,
                              const char* lim) {
  values.clear();
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    std::string field_name(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    std::string field_value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field_name, field_value});
  }
  assert(p == lim);
}

DB* NewLeavesDB() { return new LeavesDB; }

const bool registered = DBFactory::RegisterDB("leaves", NewLeavesDB);

}  // namespace ycsbc