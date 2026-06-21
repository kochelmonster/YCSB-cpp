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

const std::string PROP_BATCH_SIZE = "leaves.batch_size";
const std::string PROP_BATCH_SIZE_DEFAULT = "1";

const std::string PROP_MERGE_THRESHOLD = "leaves.merge_threshold";
const std::string PROP_MERGE_THRESHOLD_DEFAULT = "0";  // 0 = leave default

const std::string PROP_WAL = "leaves.wal";
const std::string PROP_WAL_DEFAULT = "false";
}  // namespace

namespace ycsbc {

std::shared_ptr<leaves::MapStorage> LeavesDB::storage_(nullptr);
std::shared_ptr<LeavesDB::SingleDB> LeavesDB::single_db_(nullptr);
std::shared_ptr<leaves::MapConfluenceDB> LeavesDB::confluence_db_(nullptr);
int LeavesDB::ref_cnt_(0);
std::mutex LeavesDB::mu_;

bool LeavesDB::SupportsMultiThreadWrite() const {
  if (props_ == nullptr) return false;
  return props_->GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT) == "confluence";
}

void LeavesDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties& props = *props_;
  dbpath_ = props.GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  mapsize_ = std::stoull(props.GetProperty(PROP_MAPSIZE, PROP_MAPSIZE_DEFAULT));

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  sync_ = props.GetProperty(PROP_SYNC, PROP_SYNC_DEFAULT) == "true";
  wal_enabled_ = props.GetProperty(PROP_WAL, PROP_WAL_DEFAULT) == "true";
  batch_size_ =
      std::stoi(props.GetProperty(PROP_BATCH_SIZE, PROP_BATCH_SIZE_DEFAULT));
  if (batch_size_ < 1) batch_size_ = 1;
  pending_ = 0;

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
      storage_ =
          std::make_shared<leaves::MapStorage>(dbpath_.c_str(), mapsize_);
      std::cout << "Leaves storage initialized: " << dbpath_ << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Failed to initialize Leaves storage: " << e.what()
                << std::endl;
      throw;
    }
  }

  if (format_ == kConfluence) {
    if (!confluence_db_) {
      confluence_db_ =
          std::make_shared<leaves::MapConfluenceDB>(storage_, "benchmark");

      uint32_t merge_threshold =
          static_cast<uint32_t>(std::stoul(props.GetProperty(
              PROP_MERGE_THRESHOLD, PROP_MERGE_THRESHOLD_DEFAULT)));

      if (merge_threshold > 0) {
        confluence_db_->set_merge_write_threshold(merge_threshold);
        std::cout << "Leaves merge_write_threshold set to " << merge_threshold
                  << std::endl;
      }
    }
    confluence_cursor_ = confluence_db_->cursor();
  } else {
    if (!single_db_) {
      single_db_ = std::make_shared<SingleDB>(storage_, "benchmark");
    }
    cursor_ = single_db_->cursor();
  }
}

void LeavesDB::FlushPending() {
  if (txn_active_) return;
  // Commit if there are pending writes OR if a transaction is open.
  bool has_open_txn = (format_ == kConfluence)
                          ? confluence_cursor_.is_transaction_active()
                          : cursor_.is_transaction_active();
  if (pending_ > 0 || has_open_txn) {
    if (format_ == kConfluence) {
      confluence_cursor_.commit(sync_);
    } else {
      cursor_.commit(sync_);
    }
    pending_ = 0;
  }
}

void LeavesDB::Cleanup() {
  FlushPending();
  const std::lock_guard<std::mutex> lock(mu_);
  // Reset the per-thread cursor while the shared DB is still alive.
  // This ensures the cursor's destructor (which calls back into _cdb)
  // runs before confluence_db_ / single_db_ is torn down below.
  if (format_ == kConfluence) {
    confluence_cursor_ = leaves::MapConfluenceCursor{};
  } else {
    cursor_ = SingleCursor{};
  }
  ref_cnt_--;
  if (ref_cnt_ == 0) {
    single_db_.reset();

    if (confluence_db_) {
      std::cout << "Tributary highwater at cleanup: "
                << confluence_db_->_internal()->_tributaries_count.load(
                       std::memory_order_relaxed)
                << std::endl;
    }
    confluence_db_.reset();
    storage_.reset();
    std::cout << "Leaves database closed" << std::endl;
  }
}

DB::Status LeavesDB::BeginTransaction() {
  if (txn_active_) return kError;
  FlushPending();
  if (format_ == kConfluence) {
    confluence_cursor_.start_transaction();
  } else {
    cursor_.start_transaction(wal_enabled_);
  }
  pending_ = 0;
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
  pending_ = 0;
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
  pending_ = 0;
  return kOK;
}

DB::Status LeavesDB::Read(const std::string& /*table*/, Slice key,
                          const std::unordered_set<std::string>* fields,
                          Fields& result) {
  try {
    // kSingleRow: the write-transaction cursor already sees all committed data;
    // cursor_.update() below handles any stale snapshot after a batch commit.
    // kConfluence: flush to ensure consistent reads in the multi-threaded
    // model.
    if (format_ == kConfluence) {
      FlushPending();
    }
    leaves::Slice key_slice(key.data(), key.size());

    leaves::Slice value_slice;
    if (format_ == kConfluence) {
      confluence_cursor_.find(key_slice);
      if (!confluence_cursor_.is_valid()) {
        return kNotFound;
      }
      value_slice = confluence_cursor_.value();
    } else {
      cursor_.find(key_slice);

      if (!cursor_.is_valid()) {
        // Refresh cursor to see latest committed data and retry
        cursor_.update();
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
    if (format_ == kConfluence) {
      FlushPending();
    }
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
    EnsureMutationReady();
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

    CommitMutation();
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Update error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Insert(const std::string& /*table*/, Slice key,
                            const ReadonlyFields& values) {
  try {
    EnsureMutationReady();
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

    CommitMutation();
    return kOK;
  } catch (const std::exception& e) {
    std::cerr << "Leaves Insert error: " << e.what() << std::endl;
    return kError;
  }
}

DB::Status LeavesDB::Delete(const std::string& /*table*/, Slice key) {
  try {
    EnsureMutationReady();
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