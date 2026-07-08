//
//  redis_db.h
//  YCSB-cpp
//
//  Redis database binding for YCSB-cpp
//

#ifndef YCSB_C_REDIS_DB_H_
#define YCSB_C_REDIS_DB_H_

#include "core/dataset.h"
#include "core/db.h"
#include <string>
#include <hiredis/hiredis.h>

namespace ycsbc {

class RedisDB : public DB {
 public:
  RedisDB() : context_(nullptr), timeout_ms_(1000), destroy_(false), txn_active_(false) {}
  ~RedisDB() {}

  void Init();
  void Cleanup();

  Status BeginTransaction() override;
  Status CommitTransaction() override;
  Status RollbackTransaction() override;
  void FlushPending() override;

  Status Read(const std::string &table, Slice key,
               const std::unordered_set<std::string> *fields, Fields &result,
               bool rmw = false) override;

  Status Scan(const std::string &table, Slice key, int len,
               const std::unordered_set<std::string> *fields, std::vector<Fields> &result) override;

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) override;

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) override;

  Status Delete(const std::string &table, Slice key) override;

  Status Load(const std::string &table, Dataset &batch) override;

 private:
  redisContext *context_;
  std::string host_;
  int port_;
  int timeout_ms_;
  bool destroy_;
  bool txn_active_;
  std::vector<const char*> argv_;
  std::vector<size_t> argvlen_;

  std::string BuildRedisKey(const std::string &table, Slice key);
  std::string BuildIndexKey(const std::string &table);
  void CheckReply(redisReply *reply, const std::string &source);
  Status ReadHashFields(const std::string &redis_key,
                        const std::unordered_set<std::string> *fields,
                        Fields &result);
  Status IndexKey(const std::string &table, Slice key);
  Status DeindexKey(const std::string &table, Slice key);
};

DB *NewRedisDB();

} // namespace ycsbc

#endif // YCSB_C_REDIS_DB_H_