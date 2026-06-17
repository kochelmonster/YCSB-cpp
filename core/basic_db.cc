//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "basic_db.h"
#include "core/db_factory.h"

namespace {
  const std::string PROP_SILENT = "basic.silent";
  const std::string PROP_SILENT_DEFAULT = "false";
}

namespace ycsbc {

std::mutex BasicDB:: mutex_;

void BasicDB::Init() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (props_->GetProperty(PROP_SILENT, PROP_SILENT_DEFAULT) == "true") {
    out_ = new std::ofstream;
    out_->setstate(std::ios_base::badbit);
  } else {
    out_ = &std::cout;
  }
}

DB::Status BasicDB::Read(const std::string &table, Slice key,
                         const std::unordered_set<std::string> *fields, Fields &result) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "READ " << table << ' ' << key.ToString();
  if (fields) {
    *out_ << " [ ";
    for (auto f : *fields) {
      *out_ << f << ' ';
    }
    *out_ << ']' << std::endl;
  } else {
    *out_  << " < all fields >" << std::endl;
  }
  return kOK;
}

DB::Status BasicDB::Scan(const std::string &table, Slice key, int len,
                         const std::unordered_set<std::string> *fields,
                         std::vector<Fields> &result) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "SCAN " << table << ' ' << key.ToString() << " " << len;
  if (fields) {
    *out_ << " [ ";
    for (auto f : *fields) {
      *out_ << f << ' ';
    }
    *out_ << ']' << std::endl;
  } else {
    *out_  << " < all fields >" << std::endl;
  }
  return kOK;
}

DB::Status BasicDB::Update(const std::string &table, Slice key,
                           const ReadonlyFields &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "UPDATE " << table << ' ' << key.ToString() << " [ ";
  for (auto it = values.begin(); it != values.end(); ++it) {
    auto [name, value] = *it;
    *out_ << name.ToString() << '=' << value.ToString() << ' ';
  }
  *out_ << ']' << std::endl;
  return kOK;
}

DB::Status BasicDB::Insert(const std::string &table, Slice key,
                           const ReadonlyFields &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "INSERT " << table << ' ' << key.ToString() << " [ ";
  for (auto it = values.begin(); it != values.end(); ++it) {
    auto [name, value] = *it;
    *out_ << name.ToString() << '=' << value.ToString() << ' ';
  }
  *out_ << ']' << std::endl;
  return kOK;
}

DB::Status BasicDB::Delete(const std::string &table, Slice key) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "DELETE " << table << ' ' << key.ToString() << std::endl;
  return kOK;
}

DB *NewBasicDB() {
  return new BasicDB;
}

const bool registered = DBFactory::RegisterDB("basic", NewBasicDB);

} // ycsbc
