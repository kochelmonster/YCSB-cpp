//
//  db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_H_
#define YCSB_C_DB_H_

#include "utils/properties.h"
#include "utils/fields.h"

#include <vector>
#include <string>
#include <unordered_set>

namespace ycsbc {

///
/// Database interface layer.
/// per-thread DB instance.
///
class DB {
 public:
  struct Field {
    std::string name;
    std::string value;
  };
  enum Status {
    kOK = 0,
    kError,
    kNotFound,
    kNotImplemented
  };
  ///
  /// Initializes any state for accessing this DB.
  ///
  virtual void Init() { }
  ///
  /// Clears any state for accessing this DB.
  ///
  virtual void Cleanup() { }
  ///
  /// Reads a record from the database.
  /// Field/value pairs from the result are stored in a Fields object.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A Fields object for the result.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  virtual Status Read(const std::string &table, const std::string &key,
                   const std::unordered_set<std::string> *fields,
                   Fields &result) = 0;
  ///
  /// Performs a range scan for a set of records in the database.
  /// Field/value pairs from the result are stored in a vector of Fields objects.
  ///
  /// @param table The name of the table.
  /// @param key The key of the first record to read.
  /// @param record_count The number of records to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of Fields objects, one per record
  /// @return Zero on success, or a non-zero error code on error.
  ///
  virtual Status Scan(const std::string &table, const std::string &key,
                   int record_count, const std::unordered_set<std::string> *fields,
                   std::vector<Fields> &result) = 0;
  ///
  /// Updates a record in the database.
  /// Field/value pairs in the specified Fields object are written to the record,
  /// overwriting any existing values with the same field names.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to write.
  /// @param values A Fields object with field/value pairs to update in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual Status Update(const std::string &table, const std::string &key,
                     Fields &values) = 0;
  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified Fields object are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A Fields object with field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual Status Insert(const std::string &table, const std::string &key,
                     Fields &values) = 0;
  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual Status Delete(const std::string &table, const std::string &key) = 0;

  virtual ~DB() { }

  void SetProps(utils::Properties *props) {
    props_ = props;
  }
 protected:
  utils::Properties *props_;
};

} // ycsbc

#endif // YCSB_C_DB_H_
