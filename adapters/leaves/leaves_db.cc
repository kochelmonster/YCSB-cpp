//
//  leaves_db.cc
//  YCSB-cpp
//
//  Leaves embedded database binding implementation
//

#include "leaves_db.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
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

const std::string PROP_MERGE_THRESHOLD = "leaves.merge_threshold";
const std::string PROP_MERGE_THRESHOLD_DEFAULT = "0";  // 0 = leave default

const std::string PROP_MAX_ATTACHED_AGE_MS = "leaves.max_attached_age_ms";
const std::string PROP_MAX_ATTACHED_AGE_MS_DEFAULT =
  "0";  // 0 = leave default

const std::string PROP_WAL = "leaves.wal";
const std::string PROP_WAL_DEFAULT = "false";
}  // namespace

namespace ycsbc {

std::shared_ptr<leaves::MapStorage> LeavesDB::storage_(nullptr);
LeavesDB::SingleDB LeavesDB::single_db_{};
LeavesDB::ConfluenceDB LeavesDB::confluence_db_{};

int LeavesDB::ref_cnt_(0);
std::mutex LeavesDB::mu_;

void LeavesDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties& props = *props_;
  dbpath_ = props.GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  mapsize_ = std::stoull(props.GetProperty(PROP_MAPSIZE, PROP_MAPSIZE_DEFAULT));

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
  batch_size_ = std::stoull(props.GetProperty(
      CoreWorkload::BATCH_SIZE_PROPERTY, CoreWorkload::BATCH_SIZE_DEFAULT));

  sync_ = props.GetProperty(PROP_SYNC, PROP_SYNC_DEFAULT) == "true";
  wal_enabled_ = props.GetProperty(PROP_WAL, PROP_WAL_DEFAULT) == "true";
  txn_active_ = false;

  format_ = kSingleRow;
  const std::string& format =
      props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
  } else if (format == "confluence") {
    format_ = kConfluence;
  } else {
    throw utils::Exception("Unknown format");
  }

  ref_cnt_++;
  if (ref_cnt_ == 1) {
    bool destroy =
        props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true";

    if (destroy) {
      std::remove(dbpath_.c_str());
    }
    // First instance initializes the storage
    try {
      storage_ = leaves::MapStorage::create(dbpath_.c_str(), mapsize_);
      std::cout << "Leaves storage initialized: " << dbpath_ << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Failed to initialize Leaves storage: " << e.what()
                << std::endl;
      throw;
    }
  }

  if (format_ == kConfluence) {
    if (!confluence_db_) {
      confluence_db_ = storage_->open<leaves::MapStorage::ConfluenceDB>(
          "benchmark");

      uint32_t merge_threshold =
          static_cast<uint32_t>(std::stoul(props.GetProperty(
              PROP_MERGE_THRESHOLD, PROP_MERGE_THRESHOLD_DEFAULT)));
      uint64_t max_attached_age_ms =
          static_cast<uint64_t>(std::stoull(props.GetProperty(
              PROP_MAX_ATTACHED_AGE_MS, PROP_MAX_ATTACHED_AGE_MS_DEFAULT)));

      if (merge_threshold > 0) {
        confluence_db_.set_merge_write_threshold(merge_threshold);
        std::cout << "Leaves merge_write_threshold set to " << merge_threshold
                  << std::endl;
      }
      if (max_attached_age_ms > 0) {
        confluence_db_.set_max_attached_age_ms(max_attached_age_ms);
        std::cout << "Leaves max_attached_age_ms set to "
                  << max_attached_age_ms << std::endl;
      }
    }
    confluence_cursor_ = confluence_db_.cursor();
  } else {
    if (!single_db_) {
      single_db_ = storage_->open("benchmark");
    }
    cursor_ = single_db_.cursor();
  }
}

void LeavesDB::FlushPending() {
  // Called at thread exit. Commit any active transaction or accumulated writes.
  if (txn_active_) {
    if (format_ == kConfluence) {
      confluence_cursor_.commit(sync_);
    } else {
      cursor_.commit(sync_);
    }
    txn_active_ = false;
    return;
  }

  // Even if no explicit transaction, commit any open cursor state.
  bool has_open_txn = (format_ == kConfluence)
                          ? confluence_cursor_.is_transaction_active()
                          : cursor_.is_transaction_active();
  if (has_open_txn) {
    if (format_ == kConfluence) {
      confluence_cursor_.commit(sync_);
    } else {
      cursor_.commit(sync_);
    }
  }
}

void LeavesDB::Cleanup() {
  FlushPending();
  const std::lock_guard<std::mutex> lock(mu_);
  // Reset the per-thread cursor while the shared DB is still alive.
  // This ensures the cursor's destructor (which calls back into _cdb)
  // runs before confluence_db_ / single_db_ is torn down below.
  if (format_ == kConfluence) {
    confluence_cursor_ = ConfluenceCursor{};
  } else {
    cursor_ = SingleCursor{};
  }
  ref_cnt_--;
  if (ref_cnt_ == 0) {
    single_db_ = SingleDB{};
    confluence_db_ = ConfluenceDB{};
    storage_.reset();
    std::cout << "Leaves database closed" << std::endl;
  }
}

DB::Status LeavesDB::BeginTransaction() {
  if (txn_active_) return kError;
  // Flush any pending cursor state before starting a new transaction.
  FlushPending();
  if (format_ == kConfluence) {
    confluence_cursor_.start_transaction();
  } else if (batch_size_ > 1 || wal_enabled_) {
    cursor_.start_transaction(false, wal_enabled_);
  }

  txn_active_ = true;
  return kOK;
}

DB::Status LeavesDB::CommitTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  if (format_ == kConfluence) {
    confluence_cursor_.commit(sync_);
  } else {
    cursor_.commit(sync_);
  }
  return kOK;
}

DB::Status LeavesDB::RollbackTransaction() {
  if (!txn_active_) return kNotImplemented;
  txn_active_ = false;
  if (format_ == kConfluence) {
    confluence_cursor_.rollback();
  } else {
    cursor_.rollback();
  }
  return kOK;
}

DB::Status LeavesDB::Read(const std::string& /*table*/, Slice key,
                          const std::unordered_set<std::string>* fields,
                          Fields& result, bool rmw) {
  // When rmw is true, Update() will re-read and merge internally, so skip.
  if (rmw) return kSkip;

  try {
    leaves::Slice key_slice(key.data(), key.size());

    leaves::Slice value_slice;
    if (format_ == kConfluence) {
      confluence_cursor_.find(key_slice);
      if (!confluence_cursor_.is_valid()) {
        return kNotFound;
      }
      value_slice = confluence_cursor_.value();
    } else {
      cursor_.update();
      cursor_.find(key_slice);

      if (!cursor_.is_valid()) {
        // Refresh cursor to see latest committed data and retry
        cursor_.find(key_slice);
        if (!cursor_.is_valid()) {
          return kNotFound;
        }
      }

      value_slice = cursor_.value();
    }

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

DB::Status LeavesDB::Scan(const std::string& /*table*/, Slice key, int len,
                          const std::unordered_set<std::string>* fields,
                          std::vector<Fields>& result) {
  try {
    leaves::Slice key_slice(key.data(), key.size());

    if (format_ == kConfluence) {
      confluence_cursor_.find(key_slice);
    } else {
      cursor_.find(key_slice);
    }

    result.clear();
    int count = 0;

    while (((format_ == kConfluence) ? confluence_cursor_.is_valid()
                                     : cursor_.is_valid()) &&
           count < len) {
      leaves::Slice value_slice = (format_ == kConfluence)
                                      ? confluence_cursor_.value()
                                      : cursor_.value();
      ReadonlyFields readonly(value_slice.data(), value_slice.size());

      result.emplace_back();
      Fields& values = result.back();

      if (fields) {
        ReadonlyFields readonly(value_slice.data(), value_slice.size());
        readonly.filter(values, *fields);
      } else {
        values = readonly;
      }

      if (format_ == kConfluence) {
        confluence_cursor_.next();
      } else {
        cursor_.next();
      }
      count++;
    }

    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Scan error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Update(const std::string& /*table*/, Slice key,
                            const ReadonlyFields& values) {
  try {
    leaves::Slice key_slice(key.data(), key.size());
    if (format_ == kConfluence) {
      confluence_cursor_.find(key_slice);
      if (!confluence_cursor_.is_valid()) {
        return kNotFound;
      }

      leaves::Slice existing_value = confluence_cursor_.value();
      ReadonlyFields readonly(existing_value.data(), existing_value.size());
      updated_fields_ = readonly;
      updated_fields_.update(values);
      const auto& buffer = updated_fields_.buffer();
      leaves::Slice value_slice(buffer.data(), buffer.size());
      confluence_cursor_.value(value_slice);
    } else {
      cursor_.find(key_slice);
      if (!cursor_.is_valid()) {
        return kNotFound;
      }
      leaves::Slice existing_value = cursor_.value();
      ReadonlyFields readonly(existing_value.data(), existing_value.size());
      updated_fields_ = readonly;
      updated_fields_.update(values);
      const auto& buffer = updated_fields_.buffer();
      leaves::Slice value_slice(buffer.data(), buffer.size());
      cursor_.value(value_slice);
    }

    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Update error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Insert(const std::string& /*table*/, Slice key,
                            const ReadonlyFields& values) {
  try {
    leaves::Slice key_slice(key.data(), key.size());
    if (format_ == kConfluence) {
      confluence_cursor_.find(key_slice);
    } else {
      cursor_.find(key_slice);
    }

    const auto& data = values.data();
    leaves::Slice value_slice(data.data(), data.size());
    if (format_ == kConfluence) {
      confluence_cursor_.value(value_slice);
    } else {
      cursor_.value(value_slice);
    }

    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Insert error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Delete(const std::string& /*table*/, Slice key) {
  try {
    leaves::Slice key_slice(key.data(), key.size());

    if (format_ == kConfluence) {
      // Same ordering rule as Update(): call value() before is_valid() so
      // _materialize_full() runs and finds the key in the main DB.
      confluence_cursor_.find(key_slice);
      (void)confluence_cursor_.value();  // trigger _materialize_full()
      if (!confluence_cursor_.is_valid()) {
        return kNotFound;
      }
      // Re-position for the actual remove write.
      confluence_cursor_.find(key_slice);
      confluence_cursor_.remove();
    } else {
      cursor_.find(key_slice);

      if (!cursor_.is_valid()) {
        return kNotFound;
      }

      cursor_.remove();
    }
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Delete error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Load(const std::string& /*table*/, Dataset& batch) {
  try {
    // Use single cursor transaction for efficient bulk loading.
    if (format_ == kConfluence) {
      cursor_ = confluence_db_._internal_main().cursor();
    }

    cursor_.start_transaction();
    int n = batch.OpCount();
    for (int i = 0; i < n; ++i) {
      const auto& item = batch.Next();
      leaves::Slice key_slice(item.key.data(), item.key.size());
      const auto& data = item.values.data();
      leaves::Slice value_slice(data.data(), data.size());
      cursor_.find(key_slice);
      cursor_.value(value_slice);
    }
    cursor_.commit();
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Load error: " << e.what() << std::endl;
    return kError;
  }
}

DB* NewLeavesDB() { return new LeavesDB; }

const bool registered = DBFactory::RegisterDB("leaves", NewLeavesDB);

}  // namespace ycsbc