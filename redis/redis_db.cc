//
//  redis_db.cc
//  YCSB-cpp
//
//  Redis database binding for YCSB-cpp
//

#include "redis_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/properties.h"
#include "utils/utils.h"

#include <cstring>

namespace {
const std::string PROP_HOST = "redis.host";
const std::string PROP_HOST_DEFAULT = "127.0.0.1";

const std::string PROP_PORT = "redis.port";
const std::string PROP_PORT_DEFAULT = "6379";

const std::string PROP_TIMEOUT = "redis.timeout";
const std::string PROP_TIMEOUT_DEFAULT = "1000";
} // anonymous

namespace ycsbc {

void RedisDB::Init() {
  host_ = props_->GetProperty(PROP_HOST, PROP_HOST_DEFAULT);
  port_ = std::stoi(props_->GetProperty(PROP_PORT, PROP_PORT_DEFAULT));
  timeout_ms_ = std::stoi(props_->GetProperty(PROP_TIMEOUT, PROP_TIMEOUT_DEFAULT));

  struct timeval timeout = { timeout_ms_ / 1000, (timeout_ms_ % 1000) * 1000 };
  context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
  
  if (context_ == nullptr || context_->err) {
    if (context_) {
      throw utils::Exception("Redis connection error: " + std::string(context_->errstr));
    } else {
      throw utils::Exception("Redis connection error: can't allocate redis context");
    }
  }
  
  // Clear the database
  redisReply *reply = (redisReply *)redisCommand(context_, "FLUSHDB");
  if (reply) {
    freeReplyObject(reply);
  }
}

void RedisDB::Cleanup() {
  if (context_) {
    redisFree(context_);
    context_ = nullptr;
  }
}

std::string RedisDB::BuildRedisKey(const std::string &table, const std::string &key) {
  return table + ":" + key;
}

void RedisDB::CheckReply(redisReply *reply) {
  if (reply == nullptr) {
    throw utils::Exception("Redis error: NULL reply");
  }
}

DB::Status RedisDB::Read(const std::string &table, const std::string &key,
                         const std::unordered_set<std::string> *fields, Fields &result) {
  std::string redis_key = BuildRedisKey(table, key);
  
  redisReply *reply;
  
  if (fields == nullptr) {
    // Get all fields using HGETALL
    reply = (redisReply *)redisCommand(context_, "HGETALL %s", redis_key.c_str());
  } else {
    // Get specific fields using HMGET
    std::string cmd = "HMGET " + redis_key;
    for (const auto &field : *fields) {
      cmd += " " + field;
    }
    reply = (redisReply *)redisCommand(context_, cmd.c_str());
  }
  
  CheckReply(reply);
  
  Status status = kOK;
  
  if (fields == nullptr) {
    // HGETALL returns array of field-value pairs
    if (reply->type == REDIS_REPLY_ARRAY) {
      if (reply->elements == 0) {
        status = kNotFound;
      } else {
        for (size_t i = 0; i < reply->elements; i += 2) {
          std::string field_name(reply->element[i]->str, reply->element[i]->len);
          std::string field_value(reply->element[i+1]->str, reply->element[i+1]->len);
          result.add(field_name, field_value);
        }
      }
    } else {
      status = kError;
    }
  } else {
    // HMGET returns array of values in same order as fields
    if (reply->type == REDIS_REPLY_ARRAY) {
      size_t i = 0;
      for (const auto &field : *fields) {
        if (i >= reply->elements || reply->element[i]->type == REDIS_REPLY_NIL) {
          status = kNotFound;
          break;
        }
        std::string field_value(reply->element[i]->str, reply->element[i]->len);
        result.add(field, field_value);
        i++;
      }
    } else {
      status = kError;
    }
  }
  
  freeReplyObject(reply);
  return status;
}

DB::Status RedisDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::unordered_set<std::string> *fields, std::vector<Fields> &result) {
  // Redis doesn't have built-in range scan like traditional databases
  // We implement this using SCAN with a pattern match
  std::string pattern = BuildRedisKey(table, key) + "*";
  
  unsigned long long cursor = 0;
  int count = 0;
  
  do {
    redisReply *reply = (redisReply *)redisCommand(context_, 
        "SCAN %llu MATCH %s COUNT 100", cursor, pattern.c_str());
    
    CheckReply(reply);
    
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
      freeReplyObject(reply);
      return kError;
    }
    
    // Get cursor for next iteration
    cursor = strtoull(reply->element[0]->str, nullptr, 10);
    
    // Process keys in this batch
    redisReply *keys = reply->element[1];
    for (size_t i = 0; i < keys->elements && count < len; i++) {
      std::string found_key(keys->element[i]->str, keys->element[i]->len);
      
      result.emplace_back();
      Fields &values = result.back();
      
      // Extract the original key from redis_key (remove table prefix)
      std::string key_only = found_key.substr(table.length() + 1);
      
      // Read the fields for this key
      Read(table, key_only, fields, values);
      count++;
    }
    
    freeReplyObject(reply);
    
    if (count >= len) break;
    
  } while (cursor != 0);
  
  return count > 0 ? kOK : kNotFound;
}

DB::Status RedisDB::Update(const std::string &table, const std::string &key, Fields &values) {
  std::string redis_key = BuildRedisKey(table, key);
  
  // Build HMSET command
  std::vector<const char*> argv;
  std::vector<size_t> argvlen;
  
  argv.push_back("HMSET");
  argvlen.push_back(5);
  
  argv.push_back(redis_key.c_str());
  argvlen.push_back(redis_key.length());
  
  for (auto it = values.begin(); it != values.end(); ++it) {
    auto [name, value] = *it;
    argv.push_back(name.data());
    argvlen.push_back(name.size());
    argv.push_back(value.data());
    argvlen.push_back(value.size());
  }
  
  redisReply *reply = (redisReply *)redisCommandArgv(context_, argv.size(), 
      argv.data(), argvlen.data());
  
  CheckReply(reply);
  
  Status status = (reply->type == REDIS_REPLY_STATUS && 
                   strcmp(reply->str, "OK") == 0) ? kOK : kError;
  
  freeReplyObject(reply);
  return status;
}

DB::Status RedisDB::Insert(const std::string &table, const std::string &key, Fields &values) {
  // In Redis, INSERT is the same as UPDATE (HMSET creates if not exists)
  return Update(table, key, values);
}

DB::Status RedisDB::Delete(const std::string &table, const std::string &key) {
  std::string redis_key = BuildRedisKey(table, key);
  
  redisReply *reply = (redisReply *)redisCommand(context_, "DEL %s", redis_key.c_str());
  CheckReply(reply);
  
  Status status = (reply->type == REDIS_REPLY_INTEGER && reply->integer > 0) ? kOK : kNotFound;
  
  freeReplyObject(reply);
  return status;
}

DB *NewRedisDB() {
  return new RedisDB;
}

const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

} // namespace ycsbc
