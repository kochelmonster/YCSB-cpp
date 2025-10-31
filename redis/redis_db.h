//
//  redis_db.h
//  YCSB-cpp
//
//  Redis database binding for YCSB-cpp
//

#ifndef YCSB_C_REDIS_DB_H_
#define YCSB_C_REDIS_DB_H_

#include "core/db.h"
#include <string>
#include <hiredis/hiredis.h>

namespace ycsbc {

class RedisDB : public DB {
 public:
  RedisDB() : context_(nullptr) {}
  ~RedisDB() {}

  void Init();
  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::unordered_set<std::string> *fields, Fields &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::unordered_set<std::string> *fields, std::vector<Fields> &result);

  Status Update(const std::string &table, const std::string &key, Fields &values);

  Status Insert(const std::string &table, const std::string &key, Fields &values);

  Status Delete(const std::string &table, const std::string &key);

 private:
  redisContext *context_;
  std::string host_;
  int port_;
  int timeout_ms_;
  
  std::string BuildRedisKey(const std::string &table, const std::string &key);
  void CheckReply(redisReply *reply);
};

DB *NewRedisDB();

} // namespace ycsbc

#endif // YCSB_C_REDIS_DB_H_
