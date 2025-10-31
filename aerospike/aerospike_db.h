#ifndef YCSB_C_AEROSPIKE_DB_H_
#define YCSB_C_AEROSPIKE_DB_H_

#include "core/db.h"
#include "utils/properties.h"
#include "utils/fields.h"
#include <string>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_config.h>
#include <aerospike/as_event.h>

namespace ycsbc {

class AerospikeDB : public DB {
 public:
  AerospikeDB() = default;
  ~AerospikeDB();

  void Init() override;
  void Cleanup() override;
  
  Status Read(const std::string &table, const std::string &key,
             const std::unordered_set<std::string> *fields,
             Fields &result) override;
             
  Status Scan(const std::string &table, const std::string &key, int record_count,
             const std::unordered_set<std::string> *fields,
             std::vector<Fields> &result) override;
             
  Status Update(const std::string &table, const std::string &key,
               Fields &values) override;
               
  Status Insert(const std::string &table, const std::string &key,
               Fields &values) override;
               
  Status Delete(const std::string &table, const std::string &key) override;

 private:
  aerospike as_;
  as_config config_;
  as_error err_;
  std::string ns_;  // namespace
  bool initialized_ = false;
  bool async_mode_ = false;
  as_event_loop* event_loop_ = nullptr;
  int max_concurrent_ = 100;
  
  // Async synchronization
  std::atomic<int> pending_ops_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  
  // Performance-optimized policies
  as_policy_read read_policy_;
  as_policy_write write_policy_;
  as_policy_remove remove_policy_;
  
  void SetRecord(as_record *rec, Fields &values);
  void GetRecord(const as_record *rec, Fields &result,
                 const std::unordered_set<std::string> *fields);
                 
  // Async callbacks
  static void ReadCallback(as_error* err, as_record* record, void* udata, as_event_loop* event_loop);
  static void WriteCallback(as_error* err, void* udata, as_event_loop* event_loop);
  
  void WaitForAsyncOps();
};

} // namespace ycsbc

#endif // YCSB_C_AEROSPIKE_DB_H_
