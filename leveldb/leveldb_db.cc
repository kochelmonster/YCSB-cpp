//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "leveldb_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

#include <leveldb/options.h>
#include <leveldb/write_batch.h>

namespace {
  const std::string PROP_NAME = "leveldb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "leveldb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_DESTROY = "leveldb.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_SYNC = "leveldb.sync";
  const std::string PROP_SYNC_DEFAULT = "false";

  const std::string PROP_COMPRESSION = "leveldb.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";

  const std::string PROP_WRITE_BUFFER_SIZE = "leveldb.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_FILE_SIZE = "leveldb.max_file_size";
  const std::string PROP_MAX_FILE_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_OPEN_FILES = "leveldb.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "0";

  const std::string PROP_CACHE_SIZE = "leveldb.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_FILTER_BITS = "leveldb.filter_bits";
  const std::string PROP_FILTER_BITS_DEFAULT = "0";

  const std::string PROP_BLOCK_SIZE = "leveldb.block_size";
  const std::string PROP_BLOCK_SIZE_DEFAULT = "0";

  const std::string PROP_BLOCK_RESTART_INTERVAL = "leveldb.block_restart_interval";
  const std::string PROP_BLOCK_RESTART_INTERVAL_DEFAULT = "0";
} // anonymous

namespace ycsbc {

leveldb::DB *LeveldbDB::db_ = nullptr;
int LeveldbDB::ref_cnt_ = 0;
std::mutex LeveldbDB::mu_;

void LeveldbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string &format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleEntry;
    method_read_ = &LeveldbDB::ReadSingleEntry;
    method_scan_ = &LeveldbDB::ScanSingleEntry;
    method_update_ = &LeveldbDB::UpdateSingleEntry;
    method_insert_ = &LeveldbDB::InsertSingleEntry;
    method_delete_ = &LeveldbDB::DeleteSingleEntry;
  } else if (format == "row") {
    format_ = kRowMajor;
    method_read_ = &LeveldbDB::ReadCompKeyRM;
    method_scan_ = &LeveldbDB::ScanCompKeyRM;
    method_update_ = &LeveldbDB::InsertCompKey;
    method_insert_ = &LeveldbDB::InsertCompKey;
    method_delete_ = &LeveldbDB::DeleteCompKey;
  } else if (format == "column") {
    format_ = kColumnMajor;
    method_read_ = &LeveldbDB::ReadCompKeyCM;
    method_scan_ = &LeveldbDB::ScanCompKeyCM;
    method_update_ = &LeveldbDB::InsertCompKey;
    method_insert_ = &LeveldbDB::InsertCompKey;
    method_delete_ = &LeveldbDB::DeleteCompKey;
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                             CoreWorkload::FIELD_COUNT_DEFAULT));
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                      CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

  sync_ = props.GetProperty(PROP_SYNC, PROP_SYNC_DEFAULT) == "true";
  txn_active_ = false;
  write_batch_.Clear();

  ref_cnt_++;
  if (db_) {
    return;
  }

  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("LevelDB db path is missing");
  }

  leveldb::Options opt;
  opt.create_if_missing = true;
  GetOptions(props, &opt);

  leveldb::Status s;

  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = leveldb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("LevelDB DestroyDB: ") + s.ToString());
    }
  }
  s = leveldb::DB::Open(opt, db_path, &db_);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Open: ") + s.ToString());
  }
}

void LeveldbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  FlushWriteBatch();
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}

DB::Status LeveldbDB::BeginTransaction() {
  write_batch_.Clear();
  txn_active_ = true;
  return kOK;
}

DB::Status LeveldbDB::CommitTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  FlushWriteBatch();
  return kOK;
}

DB::Status LeveldbDB::RollbackTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  write_batch_.Clear();
  return kOK;
}

void LeveldbDB::FlushPending() {
  // Called at thread exit; commit any writes accumulated in the batch.
  FlushWriteBatch();
}

void LeveldbDB::GetOptions(const utils::Properties &props, leveldb::Options *opt) {
  size_t writer_buffer_size = std::stol(props.GetProperty(PROP_WRITE_BUFFER_SIZE,
                                                          PROP_WRITE_BUFFER_SIZE_DEFAULT));
  if (writer_buffer_size > 0) {
    opt->write_buffer_size = writer_buffer_size;
  }
  size_t max_file_size = std::stol(props.GetProperty(PROP_MAX_FILE_SIZE,
                                                     PROP_MAX_FILE_SIZE_DEFAULT));
  if (max_file_size > 0) {
    opt->max_file_size = max_file_size;
  }
  size_t cache_size = std::stol(props.GetProperty(PROP_CACHE_SIZE,
                                                  PROP_CACHE_SIZE_DEFAULT));
  if (cache_size > 0) {
    opt->block_cache = leveldb::NewLRUCache(cache_size);
  }
  int max_open_files = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES,
                                                   PROP_MAX_OPEN_FILES_DEFAULT));
  if (max_open_files > 0) {
    opt->max_open_files = max_open_files;
  }
  std::string compression = props.GetProperty(PROP_COMPRESSION,
                                              PROP_COMPRESSION_DEFAULT);
  if (compression == "snappy") {
    opt->compression = leveldb::kSnappyCompression;
  } else {
    opt->compression = leveldb::kNoCompression;
  }
  int filter_bits = std::stoi(props.GetProperty(PROP_FILTER_BITS,
                                                PROP_FILTER_BITS_DEFAULT));
  if (filter_bits > 0) {
    opt->filter_policy = leveldb::NewBloomFilterPolicy(filter_bits);
  }
  int block_size = std::stoi(props.GetProperty(PROP_BLOCK_SIZE,
                                               PROP_BLOCK_SIZE_DEFAULT)); 
  if (block_size > 0) {
    opt->block_size = block_size;
  }
  int block_restart_interval = std::stoi(props.GetProperty(PROP_BLOCK_RESTART_INTERVAL,
                                                 PROP_BLOCK_RESTART_INTERVAL_DEFAULT));
  if (block_restart_interval > 0) {
    opt->block_restart_interval = block_restart_interval;
  }
}

std::string LeveldbDB::BuildCompKey(const std::string &key, const std::string &field_name) {
  switch (format_) {
    case kRowMajor:
      return key + ":" + field_name;
      break;
    case kColumnMajor:
      return field_name + ":" + key;
      break;
    default:
      throw utils::Exception("wrong format");
  }
}

std::string LeveldbDB::KeyFromCompKey(const std::string &comp_key) {
  size_t idx = comp_key.find(":");
  assert(idx != std::string::npos);
  return comp_key.substr(0, idx);
}

std::string LeveldbDB::FieldFromCompKey(const std::string &comp_key) {
  size_t idx = comp_key.find(":");
  assert(idx != std::string::npos);
  return comp_key.substr(idx + 1);
}

DB::Status LeveldbDB::ReadSingleEntry(const std::string &table, Slice key,
                                      const std::unordered_set<std::string> *fields,
                                      Fields &result) {
  // Read directly from the DB (LevelDB WriteBatch does not support reads).
  std::string data;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), leveldb::Slice(key.data(), key.size()), &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
  }
  ReadonlyFields readonly(data.data(), data.size());
  if (fields != nullptr) {
    readonly.filter(result, *fields);
  } else {
    result = readonly;
  }
  return kOK;
}

DB::Status LeveldbDB::ScanSingleEntry(const std::string &table, Slice key, int len,
                                      const std::unordered_set<std::string> *fields,
                                      std::vector<Fields> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(leveldb::Slice(key.data(), key.size()));
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.emplace_back();
    Fields &values = result.back();
    ReadonlyFields readonly(data.data(), data.size());
    if (fields != nullptr) {
      readonly.filter(values, *fields);
    } else {
      values = readonly;
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

DB::Status LeveldbDB::UpdateSingleEntry(const std::string &table, Slice key,
                                        const ReadonlyFields &values) {
  // Read current value from DB
  std::string data;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), leveldb::Slice(key.data(), key.size()), &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
  }
  ReadonlyFields readonly(data.data(), data.size());
  updated_fields_ = readonly;
  updated_fields_.update(values);
  const auto& buffer = updated_fields_.buffer();
  write_batch_.Put(leveldb::Slice(key.data(), key.size()), leveldb::Slice(buffer.data(), buffer.size()));
  return kOK;
}

DB::Status LeveldbDB::InsertSingleEntry(const std::string &table, Slice key,
                                        const ReadonlyFields &values) {
  const auto& data = values.data();
  write_batch_.Put(leveldb::Slice(key.data(), key.size()), leveldb::Slice(data.data(), data.size()));
  return kOK;
}

DB::Status LeveldbDB::DeleteSingleEntry(const std::string &table, Slice key) {
  write_batch_.Delete(leveldb::Slice(key.data(), key.size()));
  return kOK;
}

DB::Status LeveldbDB::ReadCompKeyRM(const std::string &table, Slice key,
                                    const std::unordered_set<std::string> *fields,
                                    Fields &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(leveldb::Slice(key.data(), key.size()));
  if (!db_iter->Valid() || KeyFromCompKey(db_iter->key().ToString()) != key.ToString()) {
    return kNotFound;
  }
  if (fields != nullptr) {
    for (int i = 0; i < fieldcount_ && db_iter->Valid(); i++) {
      std::string comp_key = db_iter->key().ToString();
      std::string cur_val = db_iter->value().ToString();
      std::string cur_key = KeyFromCompKey(comp_key);
      std::string cur_field = FieldFromCompKey(comp_key);
      assert(cur_key == key.ToString());
      assert(cur_field == field_prefix_ + std::to_string(i));

      if (fields->find(cur_field) != fields->end()) {
        result.add(cur_field, cur_val);
      }
      db_iter->Next();
    }
    assert(result.size() == fields->size());
  } else {
    for (int i = 0; i < fieldcount_ && db_iter->Valid(); i++) {
      std::string comp_key = db_iter->key().ToString();
      std::string cur_val = db_iter->value().ToString();
      std::string cur_key = KeyFromCompKey(comp_key);
      std::string cur_field = FieldFromCompKey(comp_key);
      assert(cur_key == key.ToString());
      assert(cur_field == field_prefix_ + std::to_string(i));

      result.add(cur_field, cur_val);
      db_iter->Next();
    }
    assert(result.size() == fieldcount_);
  }
  delete db_iter;
  return kOK;
}

DB::Status LeveldbDB::ScanCompKeyRM(const std::string &table, Slice key, int len,
                                    const std::unordered_set<std::string> *fields,
                                    std::vector<Fields> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(leveldb::Slice(key.data(), key.size()));
  assert(db_iter->Valid() && KeyFromCompKey(db_iter->key().ToString()) == key.ToString());
  for (int i = 0; i < len && db_iter->Valid(); i++) {
    result.emplace_back();
    Fields &values = result.back();
    if (fields != nullptr) {
      for (int j = 0; j < fieldcount_ && db_iter->Valid(); j++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_field == field_prefix_ + std::to_string(j));

        if (fields->find(cur_field) != fields->end()) {
          values.add(cur_field, cur_val);
        }
        db_iter->Next();
      }
      assert(values.size() == fields->size());
    } else {
      for (int j = 0; j < fieldcount_ && db_iter->Valid(); j++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_field == field_prefix_ + std::to_string(j));

        values.add(cur_field, cur_val);
        db_iter->Next();
      }
      assert(values.size() == fieldcount_);
    }
  }
  delete db_iter;
  return kOK;
}

DB::Status LeveldbDB::ReadCompKeyCM(const std::string &table, Slice key,
                                      const std::unordered_set<std::string> *fields,
                                      Fields &result) {
  return kNotImplemented;
}

DB::Status LeveldbDB::ScanCompKeyCM(const std::string &table, Slice key, int len,
                                      const std::unordered_set<std::string> *fields,
                                      std::vector<Fields> &result) {
  return kNotImplemented;
}

DB::Status LeveldbDB::InsertCompKey(const std::string &table, Slice key,
                                    const ReadonlyFields &values) {
  std::string comp_key;
  for (auto it = values.begin(); it != values.end(); ++it) {
    auto [name, value] = *it;
    comp_key = BuildCompKey(key.ToString(), std::string(name.data(), name.size()));
    write_batch_.Put(comp_key, leveldb::Slice(value.data(), value.size()));
  }
  return kOK;
}

DB::Status LeveldbDB::DeleteCompKey(const std::string &table, Slice key) {
  std::string comp_key;
  for (int i = 0; i < fieldcount_; i++) {
    comp_key = BuildCompKey(key.ToString(), field_prefix_ + std::to_string(i));
    write_batch_.Delete(comp_key);
  }
  return kOK;
}

DB::Status LeveldbDB::Load(const std::string &table, Dataset &batch) {
  // Use a single WriteBatch for efficient bulk loading.
  leveldb::WriteOptions wopt;
  wopt.sync = sync_;
  leveldb::WriteBatch wb;
  int n = batch.OpCount();
  for (int i = 0; i < n; ++i) {
    const auto &item = batch.Next();
    const auto &data = item.values.data();
    wb.Put(leveldb::Slice(item.key.data(), item.key.size()),
           leveldb::Slice(data.data(), data.size()));
  }
  leveldb::Status s = db_->Write(wopt, &wb);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Load: ") + s.ToString());
  }
  return kOK;
}

DB *NewLeveldbDB() {
  return new LeveldbDB;
}

const bool registered = DBFactory::RegisterDB("leveldb", NewLeveldbDB);

} // ycsbc