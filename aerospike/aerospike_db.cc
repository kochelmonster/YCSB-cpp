#include "aerospike_db.h"
#include "core/db_factory.h"
#include <cstring>
#include <iostream>
#include <thread>
#include <chrono>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_string.h>

namespace ycsbc {

DB *NewAerospikeDB();

AerospikeDB::~AerospikeDB() {
  Cleanup();
}

void AerospikeDB::Init() {
  const utils::Properties &props = *props_;
  
  // Get configuration
  std::string host = props.GetProperty("aerospike.host", "127.0.0.1");
  int port = std::stoi(props.GetProperty("aerospike.port", "3000"));
  ns_ = props.GetProperty("aerospike.namespace", "test");
  async_mode_ = props.GetProperty("aerospike.async", "false") == "true";
  max_concurrent_ = std::stoi(props.GetProperty("aerospike.max_concurrent", "100"));
  
  // Initialize config
  as_config_init(&config_);
  as_config_add_host(&config_, host.c_str(), port);
  
  // Set aggressive connection pool settings for performance
  config_.conn_pools_per_node = 2;  // More connections per node
  config_.max_conns_per_node = 300; // Allow many concurrent connections
  config_.async_max_conns_per_node = 300;
  
  // Optimize policies for low latency
  as_policy_read_init(&read_policy_);
  read_policy_.base.total_timeout = 1000;  // 1 second timeout
  read_policy_.base.socket_timeout = 1000;
  read_policy_.deserialize = false;  // Don't deserialize on read (we handle it)
  
  as_policy_write_init(&write_policy_);
  write_policy_.base.total_timeout = 1000;
  write_policy_.base.socket_timeout = 1000;
  write_policy_.compression_threshold = 0;  // Disable compression for speed
  
  as_policy_remove_init(&remove_policy_);
  remove_policy_.base.total_timeout = 1000;
  remove_policy_.base.socket_timeout = 1000;
  
  // Create event loops if async mode
  if (async_mode_) {
    as_policy_event policy;
    as_policy_event_init(&policy);
    
    if (!as_event_create_loops(1)) {
      throw std::runtime_error("Event loop creation failed");
    }
    
    event_loop_ = as_event_loop_get_by_index(0);
    if (!event_loop_) {
      throw std::runtime_error("Failed to get event loop");
    }
  }
  
  // Initialize client
  aerospike_init(&as_, &config_);
  
  // Connect
  as_error_init(&err_);
  if (aerospike_connect(&as_, &err_) != AEROSPIKE_OK) {
    std::cerr << "Aerospike connection failed: " << err_.message << std::endl;
    throw std::runtime_error("Aerospike connection failed");
  }
  
  initialized_ = true;
  
  // Truncate namespace to start clean
  std::string set_name = props.GetProperty("aerospike.set", "usertable");
  char *response = nullptr;
  as_error_init(&err_);
  
  // Use info command to truncate the set
  std::string truncate_cmd = "truncate:namespace=" + ns_ + ";set=" + set_name;
  if (aerospike_info_any(&as_, &err_, nullptr, truncate_cmd.c_str(), &response) != AEROSPIKE_OK) {
    // Truncate may fail if set doesn't exist yet - this is okay
    std::cerr << "Aerospike truncate warning: " << err_.message << " (this is normal on first run)" << std::endl;
  }
  if (response) {
    free(response);
  }
}

void AerospikeDB::Cleanup() {
  if (initialized_) {
    // Wait for all async operations to complete
    if (async_mode_) {
      WaitForAsyncOps();
    }
    
    as_error_init(&err_);
    aerospike_close(&as_, &err_);
    aerospike_destroy(&as_);
    
    // Cleanup event loops
    if (async_mode_ && event_loop_) {
      as_event_close_loops();
      event_loop_ = nullptr;
    }
    
    initialized_ = false;
  }
}

void AerospikeDB::WaitForAsyncOps() {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return pending_ops_.load() == 0; });
}

void AerospikeDB::SetRecord(as_record *rec, Fields &values) {
  for (auto it = values.begin(); it != values.end(); ++it) {
    auto [name, value] = *it;
    // Convert Slice to string
    std::string name_str = name.ToString();
    std::string value_str = value.ToString();
    as_record_set_str(rec, name_str.c_str(), value_str.c_str());
  }
}

void AerospikeDB::GetRecord(const as_record *rec, Fields &result,
                            const std::unordered_set<std::string> *fields) {
  if (fields != nullptr && !fields->empty()) {
    // Read specific fields
    for (const auto &field_name : *fields) {
      as_bin_value *bin_value = as_record_get(rec, field_name.c_str());
      if (bin_value != nullptr) {
        // Extract string value from as_bin_value
        as_val *val = (as_val *)bin_value;
        if (as_val_type(val) == AS_STRING) {
          as_string *str = as_string_fromval(val);
          const char *value = as_string_get(str);
          if (value != nullptr) {
            result.add(field_name.c_str(), value);
          }
        }
      }
    }
  } else {
    // Read all bins
    as_record_iterator it;
    as_record_iterator_init(&it, rec);
    
    while (as_record_iterator_has_next(&it)) {
      as_bin *bin = as_record_iterator_next(&it);
      const char *name = as_bin_get_name(bin);
      
      // Get value
      as_val *val = (as_val *)as_bin_get_value(bin);
      if (name != nullptr && val != nullptr && as_val_type(val) == AS_STRING) {
        as_string *str = as_string_fromval(val);
        const char *value = as_string_get(str);
        if (value != nullptr) {
          result.add(name, value);
        }
      }
    }
    
    as_record_iterator_destroy(&it);
  }
}

// Async callback structures
struct AsyncReadContext {
  AerospikeDB* db;
  Fields* result;
  const std::unordered_set<std::string>* fields;
  DB::Status status;
  bool completed;
  std::mutex mutex;
  std::condition_variable cv;
};

void AerospikeDB::ReadCallback(as_error* err, as_record* record, void* udata, as_event_loop* event_loop) {
  (void)event_loop;  // Unused parameter
  AsyncReadContext* ctx = static_cast<AsyncReadContext*>(udata);
  
  // NULL err means success in async callbacks
  if ((!err || err->code == AEROSPIKE_OK) && record) {
    ctx->db->GetRecord(record, *ctx->result, ctx->fields);
    ctx->status = DB::kOK;
  } else if (err && err->code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
    ctx->status = DB::kNotFound;
  } else {
    ctx->status = DB::kError;
  }
  
  {
    std::lock_guard<std::mutex> lock(ctx->mutex);
    ctx->completed = true;
  }
  ctx->cv.notify_one();
  
  ctx->db->pending_ops_--;
  ctx->db->cv_.notify_one();
}

DB::Status AerospikeDB::Read(const std::string &table, const std::string &key,
                             const std::unordered_set<std::string> *fields,
                             Fields &result) {
  if (async_mode_) {
    // Async mode
    while (pending_ops_.load() >= max_concurrent_) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    pending_ops_++;
    
    as_key as_key;
    as_key_init_str(&as_key, ns_.c_str(), table.c_str(), key.c_str());
    
    AsyncReadContext ctx;
    ctx.db = this;
    ctx.result = &result;
    ctx.fields = fields;
    ctx.completed = false;
    
    as_error err;
    as_error_init(&err);
    
    if (aerospike_key_get_async(&as_, &err, &read_policy_, &as_key, ReadCallback, &ctx, event_loop_, nullptr) != AEROSPIKE_OK) {
      as_key_destroy(&as_key);
      pending_ops_--;
      return kError;
    }
    
    // Wait for completion
    {
      std::unique_lock<std::mutex> lock(ctx.mutex);
      ctx.cv.wait(lock, [&ctx] { return ctx.completed; });
    }
    
    as_key_destroy(&as_key);
    return ctx.status;
  } else {
    // Sync mode
    as_key as_key;
    as_key_init_str(&as_key, ns_.c_str(), table.c_str(), key.c_str());
    
    as_record *rec = nullptr;
    as_error_init(&err_);
    
    if (aerospike_key_get(&as_, &err_, &read_policy_, &as_key, &rec) != AEROSPIKE_OK) {
      as_key_destroy(&as_key);
      return kNotFound;
    }
    
    GetRecord(rec, result, fields);
    
    as_record_destroy(rec);
    as_key_destroy(&as_key);
    
    return kOK;
  }
}

DB::Status AerospikeDB::Scan(const std::string &table, const std::string &key, int record_count,
                             const std::unordered_set<std::string> *fields,
                             std::vector<Fields> &result) {
  // Aerospike doesn't support efficient range scans by key
  // We'll implement a simple approach: try to read sequential keys
  for (int i = 0; i < record_count; i++) {
    std::string scan_key = key + std::to_string(i);
    Fields record;
    
    Status s = Read(table, scan_key, fields, record);
    if (s == kOK) {
      result.push_back(std::move(record));
    }
  }
  
  return kOK;
}

struct AsyncWriteContext {
  AerospikeDB* db;
  DB::Status status;
  bool completed;
  std::mutex mutex;
  std::condition_variable cv;
};

void AerospikeDB::WriteCallback(as_error* err, void* udata, as_event_loop* event_loop) {
  (void)event_loop;  // Unused parameter
  
  AsyncWriteContext* ctx = static_cast<AsyncWriteContext*>(udata);
  
  // NULL err means success in async callbacks
  if (!err || err->code == AEROSPIKE_OK) {
    ctx->status = DB::kOK;
  } else {
    ctx->status = DB::kError;
  }
  
  {
    std::lock_guard<std::mutex> lock(ctx->mutex);
    ctx->completed = true;
  }
  ctx->cv.notify_one();
  
  ctx->db->pending_ops_--;
  ctx->db->cv_.notify_one();
}

DB::Status AerospikeDB::Update(const std::string &table, const std::string &key,
                               Fields &values) {
  if (async_mode_) {
    // Async mode
    while (pending_ops_.load() >= max_concurrent_) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    pending_ops_++;
    
    as_key as_key;
    as_key_init_str(&as_key, ns_.c_str(), table.c_str(), key.c_str());
    
    as_record rec;
    as_record_inita(&rec, values.size());
    SetRecord(&rec, values);
    
    AsyncWriteContext ctx;
    ctx.db = this;
    ctx.completed = false;
    
    as_error err;
    as_error_init(&err);
    
    if (aerospike_key_put_async(&as_, &err, &write_policy_, &as_key, &rec, WriteCallback, &ctx, event_loop_, nullptr) != AEROSPIKE_OK) {
      as_record_destroy(&rec);
      as_key_destroy(&as_key);
      pending_ops_--;
      return kError;
    }
    
    // Wait for completion
    {
      std::unique_lock<std::mutex> lock(ctx.mutex);
      ctx.cv.wait(lock, [&ctx] { return ctx.completed; });
    }
    
    as_record_destroy(&rec);
    as_key_destroy(&as_key);
    return ctx.status;
  } else {
    // Sync mode
    as_key as_key;
    as_key_init_str(&as_key, ns_.c_str(), table.c_str(), key.c_str());
    
    as_record rec;
    as_record_inita(&rec, values.size());
    SetRecord(&rec, values);
    
    as_error_init(&err_);
    if (aerospike_key_put(&as_, &err_, &write_policy_, &as_key, &rec) != AEROSPIKE_OK) {
      as_record_destroy(&rec);
      as_key_destroy(&as_key);
      return kError;
    }
    
    as_record_destroy(&rec);
    as_key_destroy(&as_key);
    
    return kOK;
  }
}

DB::Status AerospikeDB::Insert(const std::string &table, const std::string &key,
                               Fields &values) {
  return Update(table, key, values);
}

DB::Status AerospikeDB::Delete(const std::string &table, const std::string &key) {
  if (async_mode_) {
    // Async mode
    while (pending_ops_.load() >= max_concurrent_) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    pending_ops_++;
    
    as_key as_key;
    as_key_init_str(&as_key, ns_.c_str(), table.c_str(), key.c_str());
    
    AsyncWriteContext ctx;
    ctx.db = this;
    ctx.completed = false;
    
    as_error err;
    as_error_init(&err);
    
    if (aerospike_key_remove_async(&as_, &err, &remove_policy_, &as_key, WriteCallback, &ctx, event_loop_, nullptr) != AEROSPIKE_OK) {
      as_key_destroy(&as_key);
      pending_ops_--;
      return kError;
    }
    
    // Wait for completion
    {
      std::unique_lock<std::mutex> lock(ctx.mutex);
      ctx.cv.wait(lock, [&ctx] { return ctx.completed; });
    }
    
    as_key_destroy(&as_key);
    return ctx.status;
  } else {
    // Sync mode
    as_key as_key;
    as_key_init_str(&as_key, ns_.c_str(), table.c_str(), key.c_str());
    
    as_error_init(&err_);
    if (aerospike_key_remove(&as_, &err_, &remove_policy_, &as_key) != AEROSPIKE_OK) {
      as_key_destroy(&as_key);
      return kError;
    }
    
    as_key_destroy(&as_key);
    return kOK;
  }
}

DB *NewAerospikeDB() {
  return new AerospikeDB();
}

const bool registered = DBFactory::RegisterDB("aerospike", NewAerospikeDB);

} // namespace ycsbc
