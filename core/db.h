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

class Dataset;

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
    kNotImplemented,
    kSkip       // Operation intentionally skipped (e.g., Read in RMW when Update reads anyway)
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
  /// Flushes any pending writes without closing the DB.
  /// Called at the end of each client thread's work to ensure partial batches
  /// are committed before the thread exits (prevents lock leaks on shared DBs).
  ///
  virtual void FlushPending() { }
  ///
  /// Begins an explicit transaction if supported.
  ///
  virtual Status BeginTransaction() { return kNotImplemented; }
  ///
  /// Commits an explicit transaction if supported.
  ///
  virtual Status CommitTransaction() { return kNotImplemented; }
  ///
  /// Rolls back an explicit transaction if supported.
  ///
  virtual Status RollbackTransaction() { return kNotImplemented; }
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
  virtual Status Read(const std::string &table, Slice key,
                   const std::unordered_set<std::string> *fields,
                   Fields &result, bool rmw = false) = 0;
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
  virtual Status Scan(const std::string &table, Slice key,
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
  virtual Status Update(const std::string &table, Slice key,
                     const ReadonlyFields &values) = 0;
  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified Fields object are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A Fields object with field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual Status Insert(const std::string &table, Slice key,
                     const ReadonlyFields &values) = 0;
  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual Status Delete(const std::string &table, Slice key) = 0;

  ///
  /// Bulk-load a set of pre-generated operations into the database.
  /// The default implementation returns kNotImplemented.
  /// Adapters should override this to use their most efficient bulk-load
  /// path (e.g. WriteBatch for LevelDB, single cursor transaction for Leaves).
  ///
  /// @param table The name of the table.
  /// @param batch A Dataset containing only INSERT WorkItems.
  /// @return kOK on success, or a non-zero error code on error.
  ///
  virtual Status Load(const std::string &table, Dataset &batch) = 0;

  virtual ~DB() { }

  void SetProps(utils::Properties *props) {
    props_ = props;
  }
 protected:
  utils::Properties *props_;
};

} // ycsbc

#endif // YCSB_C_DB_H_
