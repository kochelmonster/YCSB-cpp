//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"
#include "random_byte_generator.h"
#include "utils/utils.h"
#include "utils/sha256.h"

#include <algorithm>
#include <random>
#include <string>
#include <unordered_set>

using ycsbc::CoreWorkload;
using std::string;

// Thread-local reusable buffers (moved from CoreWorkload members for thread-safety)
static thread_local std::string tl_key_buffer;
static thread_local ycsbc::Fields tl_result_buffer;
static thread_local ycsbc::Fields tl_values_buffer;
static thread_local std::unordered_set<std::string> tl_fields_buffer;
static thread_local std::vector<ycsbc::Fields> tl_scan_result_buffer;
static thread_local ycsbc::RandomByteGenerator tl_byte_generator;

const char *ycsbc::kOperationString[ycsbc::MAXOPTYPE] = {
  "INSERT",
  "READ",
  "UPDATE",
  "SCAN",
  "READMODIFYWRITE",
  "DELETE",
  "INSERT-FAILED",
  "READ-FAILED",
  "UPDATE-FAILED",
  "SCAN-FAILED",
  "READMODIFYWRITE-FAILED",
  "DELETE-FAILED"
};

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY = "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT = "1";

const string CoreWorkload::MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
const string CoreWorkload::MIN_SCAN_LENGTH_DEFAULT = "1";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::HASH_ALGO_PROPERTY = "hashalgo";
const string CoreWorkload::HASH_ALGO_DEFAULT = "fnv";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const std::string CoreWorkload::FIELD_NAME_PREFIX = "fieldnameprefix";
const std::string CoreWorkload::FIELD_NAME_PREFIX_DEFAULT = "field";

const std::string CoreWorkload::ZIPFIAN_CONST_PROPERTY = "zipfian_const";
const std::string CoreWorkload::TRANSACTION_MODE_PROPERTY = "transactionmode";
const std::string CoreWorkload::TRANSACTION_MODE_DEFAULT = "none";

namespace ycsbc {

void CoreWorkload::Init(const utils::Properties &p) {
  table_name_ = p.GetProperty(TABLENAME_PROPERTY,TABLENAME_DEFAULT);

  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
  field_prefix_ = p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
  field_len_generator_ = GetFieldLenGenerator(p);

  // Pre-build field names to avoid string construction in hot path
  field_names_.reserve(field_count_);
  for (int i = 0; i < field_count_; ++i) {
    field_names_.push_back(field_prefix_ + std::to_string(i));
  }

  double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                   READ_PROPORTION_DEFAULT));
  double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                     UPDATE_PROPORTION_DEFAULT));
  double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                     INSERT_PROPORTION_DEFAULT));
  double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                   SCAN_PROPORTION_DEFAULT));
  double readmodifywrite_proportion = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));

  record_count_ = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY));
  std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                           REQUEST_DISTRIBUTION_DEFAULT);
  int min_scan_len = std::stoi(p.GetProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_DEFAULT));
  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  int insert_start = std::stoi(p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT));

  zero_padding_ = std::stoi(p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT));
  explicit_transaction_mode_ = p.GetProperty(TRANSACTION_MODE_PROPERTY, TRANSACTION_MODE_DEFAULT) == "multikey_acid";

  read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                    READ_ALL_FIELDS_DEFAULT));
  write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                     WRITE_ALL_FIELDS_DEFAULT));

  if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    ordered_inserts_ = false;
  } else {
    ordered_inserts_ = true;
  }

  hash_algo_ = p.GetProperty(HASH_ALGO_PROPERTY, HASH_ALGO_DEFAULT);


  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }

  insert_key_sequence_ = new CounterGenerator(insert_start);
  transaction_insert_key_sequence_ = new AcknowledgedCounterGenerator(record_count_);

  if (request_dist == "uniform") {
    key_chooser_ = new UniformGenerator(0, record_count_ - 1);

  } else if (request_dist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    int new_keys = (int)(op_count * insert_proportion * 2); // a fudge factor
    if (p.ContainsKey(ZIPFIAN_CONST_PROPERTY)) {
      double zipfian_const = std::stod(p.GetProperty(ZIPFIAN_CONST_PROPERTY));
      key_chooser_ = new ScrambledZipfianGenerator(0, record_count_ + new_keys - 1, zipfian_const);
    } else {
      key_chooser_ = new ScrambledZipfianGenerator(record_count_ + new_keys);
    }
  } else if (request_dist == "latest") {
    key_chooser_ = new SkewedLatestGenerator(*transaction_insert_key_sequence_);
  } else {
    throw utils::Exception("Unknown request distribution: " + request_dist);
  }

  field_chooser_ = new UniformGenerator(0, field_count_ - 1);

  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(min_scan_len, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(min_scan_len, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " + scan_len_dist);
  }
}

ycsbc::Generator<uint64_t> *CoreWorkload::GetFieldLenGenerator(
    const utils::Properties &p) {
  string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                        FIELD_LENGTH_DISTRIBUTION_DEFAULT);
  int field_len = std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
  if(field_len_dist == "constant") {
    return new ConstGenerator(field_len);
  } else if(field_len_dist == "uniform") {
    return new UniformGenerator(1, field_len);
  } else if(field_len_dist == "zipfian") {
    return new ZipfianGenerator(1, field_len);
  } else {
    throw utils::Exception("Unknown field length distribution: " + field_len_dist);
  }
}

std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    if (hash_algo_ == "sha256") {
      char num_buf[32];
      snprintf(num_buf, sizeof(num_buf), "%lu", key_num);
      tl_key_buffer = sha256(std::string(num_buf));
      return tl_key_buffer;
    } else { // fnv
      key_num = utils::Hash(key_num);
    }
  }
  
  // Build key directly in thread-local buffer to avoid allocations
  tl_key_buffer.clear();
  tl_key_buffer.append("user");
  
  // Convert key_num to string and calculate padding
  char num_buf[32];
  int num_len = snprintf(num_buf, sizeof(num_buf), "%lu", key_num);
  
  int fill = std::max(0, zero_padding_ - num_len);
  tl_key_buffer.append(fill, '0');
  tl_key_buffer.append(num_buf, num_len);
  
  return tl_key_buffer;
}

void CoreWorkload::BuildValues(Fields &values) {
  values.clear();
  for (int i = 0; i < field_count_; ++i) {
    // Use pre-built field name
    const std::string& field_name = field_names_[i];
    uint64_t len = field_len_generator_->Next();
    
    // Build value string
    std::string field_value;
    field_value.reserve(len);
    std::generate_n(std::back_inserter(field_value), len, []() { return tl_byte_generator.Next(); });
    
    values.push(field_name, field_value);
  }
}

void CoreWorkload::BuildSingleValue(Fields &values) {
  values.clear();
  const std::string& field_name = NextFieldName();
  uint64_t len = field_len_generator_->Next();
  
  // Build value string
  std::string field_value;
  field_value.reserve(len);
  std::generate_n(std::back_inserter(field_value), len, []() { return tl_byte_generator.Next(); });
  
  values.push(field_name, field_value);
}

uint64_t CoreWorkload::NextTransactionKeyNum() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > transaction_insert_key_sequence_->Last());
  return key_num;
}

const std::string& CoreWorkload::NextFieldName() {
  // Return pre-built field name by const reference to avoid copy
  return field_names_[field_chooser_->Next()];
}

bool CoreWorkload::DoInsert(DB &db) {
  BuildKeyName(insert_key_sequence_->Next());
  tl_values_buffer.clear();
  BuildValues(tl_values_buffer);
  return db.Insert(table_name_, tl_key_buffer, tl_values_buffer) == DB::kOK;
}

bool CoreWorkload::DoTransaction(DB &db) {
  if (explicit_transaction_mode_) {
    return TransactionMultiKeyAcid(db) == DB::kOK;
  }

  DB::Status status;
  switch (op_chooser_.Next()) {
    case READ:
      status = TransactionRead(db);
      break;
    case UPDATE:
      status = TransactionUpdate(db);
      break;
    case INSERT:
      status = TransactionInsert(db);
      break;
    case SCAN:
      status = TransactionScan(db);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite(db);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  return (status == DB::kOK);
}

DB::Status CoreWorkload::TransactionRead(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  BuildKeyName(key_num);
  tl_result_buffer.clear();
  if (!read_all_fields()) {
    tl_fields_buffer.clear();
    tl_fields_buffer.insert(NextFieldName());
    return db.Read(table_name_, tl_key_buffer, &tl_fields_buffer, tl_result_buffer);
  } else {
    return db.Read(table_name_, tl_key_buffer, NULL, tl_result_buffer);
  }
}

DB::Status CoreWorkload::TransactionReadModifyWrite(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  BuildKeyName(key_num);
  tl_result_buffer.clear();

  if (!read_all_fields()) {
    tl_fields_buffer.clear();
    tl_fields_buffer.insert(NextFieldName());
    db.Read(table_name_, tl_key_buffer, &tl_fields_buffer, tl_result_buffer);
  } else {
    db.Read(table_name_, tl_key_buffer, NULL, tl_result_buffer);
  }

  tl_values_buffer.clear();
  if (write_all_fields()) {
    BuildValues(tl_values_buffer);
  } else {
    BuildSingleValue(tl_values_buffer);
  }
  return db.Update(table_name_, tl_key_buffer, tl_values_buffer);
}

DB::Status CoreWorkload::TransactionScan(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  BuildKeyName(key_num);
  int len = scan_len_chooser_->Next();
  tl_scan_result_buffer.clear();
  if (!read_all_fields()) {
    tl_fields_buffer.clear();
    tl_fields_buffer.insert(NextFieldName());
    return db.Scan(table_name_, tl_key_buffer, len, &tl_fields_buffer, tl_scan_result_buffer);
  } else {
    return db.Scan(table_name_, tl_key_buffer, len, NULL, tl_scan_result_buffer);
  }
}

DB::Status CoreWorkload::TransactionUpdate(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  BuildKeyName(key_num);
  tl_values_buffer.clear();
  if (write_all_fields()) {
    BuildValues(tl_values_buffer);
  } else {
    BuildSingleValue(tl_values_buffer);
  }
  return db.Update(table_name_, tl_key_buffer, tl_values_buffer);
}

DB::Status CoreWorkload::TransactionInsert(DB &db) {
  uint64_t key_num = transaction_insert_key_sequence_->Next();
  BuildKeyName(key_num);
  tl_values_buffer.clear();
  BuildValues(tl_values_buffer);
  DB::Status s = db.Insert(table_name_, tl_key_buffer, tl_values_buffer);
  transaction_insert_key_sequence_->Acknowledge(key_num);
  return s;
}

void CoreWorkload::PrepareOps(int n, bool is_loading, std::vector<WorkItem> &out) {
  out.reserve(n);
  if (is_loading) {
    for (int i = 0; i < n; ++i) {
      WorkItem item;
      item.type = WorkItem::OpType::INSERT;
      item.key = BuildKeyName(insert_key_sequence_->Next());
      BuildValues(item.values);
      out.push_back(std::move(item));
    }
    return;
  }

  for (int i = 0; i < n; ++i) {
    WorkItem item;
    switch (op_chooser_.Next()) {
      case READ:
        item.type = WorkItem::OpType::READ;
        item.key = BuildKeyName(NextTransactionKeyNum());
        break;
      case UPDATE:
        item.type = WorkItem::OpType::UPDATE;
        item.key = BuildKeyName(NextTransactionKeyNum());
        if (write_all_fields_) BuildValues(item.values);
        else BuildSingleValue(item.values);
        break;
      case INSERT: {
        item.type = WorkItem::OpType::INSERT;
        uint64_t key_num = transaction_insert_key_sequence_->Next();
        item.key = BuildKeyName(key_num);
        BuildValues(item.values);
        transaction_insert_key_sequence_->Acknowledge(key_num);
        break;
      }
      case SCAN:
        item.type = WorkItem::OpType::SCAN;
        item.key = BuildKeyName(NextTransactionKeyNum());
        item.scan_len = scan_len_chooser_->Next();
        break;
      case READMODIFYWRITE:
        item.type = WorkItem::OpType::READMODIFYWRITE;
        item.key = BuildKeyName(NextTransactionKeyNum());
        if (write_all_fields_) BuildValues(item.values);
        else BuildSingleValue(item.values);
        break;
      default:
        throw utils::Exception("Operation request is not recognized!");
    }
    out.push_back(std::move(item));
  }
}

DB::Status CoreWorkload::TransactionMultiKeyAcid(DB &db) {
  uint64_t first_key_num = NextTransactionKeyNum();
  uint64_t second_key_num;
  do {
    second_key_num = NextTransactionKeyNum();
  } while (record_count_ > 1 && second_key_num == first_key_num);

  std::string first_key = BuildKeyName(first_key_num);
  std::string second_key = BuildKeyName(second_key_num);

  DB::Status status = db.BeginTransaction();
  if (status != DB::kOK) {
    return status;
  }

  tl_result_buffer.clear();
  status = db.Read(table_name_, first_key, NULL, tl_result_buffer);
  if (status != DB::kOK) {
    db.RollbackTransaction();
    return status;
  }

  Fields second_result;
  status = db.Read(table_name_, second_key, NULL, second_result);
  if (status != DB::kOK) {
    db.RollbackTransaction();
    return status;
  }

  tl_values_buffer.clear();
  BuildSingleValue(tl_values_buffer);
  status = db.Update(table_name_, first_key, tl_values_buffer);
  if (status != DB::kOK) {
    db.RollbackTransaction();
    return status;
  }

  Fields second_update;
  BuildSingleValue(second_update);
  status = db.Update(table_name_, second_key, second_update);
  if (status != DB::kOK) {
    db.RollbackTransaction();
    return status;
  }

  status = db.CommitTransaction();
  if (status != DB::kOK) {
    db.RollbackTransaction();
  }
  return status;
}

} // ycsbc
