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

const std::string PROP_DESTROY = "redis.destroy";
const std::string PROP_DESTROY_DEFAULT = "false";
} // anonymous

namespace ycsbc {

void RedisDB::Init() {
  host_ = props_->GetProperty(PROP_HOST, PROP_HOST_DEFAULT);
  port_ = std::stoi(props_->GetProperty(PROP_PORT, PROP_PORT_DEFAULT));
  timeout_ms_ = std::stoi(props_->GetProperty(PROP_TIMEOUT, PROP_TIMEOUT_DEFAULT));
  destroy_ = props_->GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true";

  struct timeval timeout = { timeout_ms_ / 1000, (timeout_ms_ % 1000) * 1000 };
  context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
  
  if (context_ == nullptr || context_->err) {
    if (context_) {
      throw utils::Exception("Redis connection error: " + std::string(context_->errstr));
    } else {
      throw utils::Exception("Redis connection error: can't allocate redis context");
    }
  }
  
  if (destroy_) {
    redisReply *reply = (redisReply *)redisCommand(context_, "FLUSHDB");
    CheckReply(reply);
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

std::string RedisDB::BuildIndexKey(const std::string &table) {
  return table + ":__index__";
}

void RedisDB::CheckReply(redisReply *reply) {
  if (reply == nullptr) {
    throw utils::Exception("Redis error: NULL reply");
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    std::string message = reply->str ? reply->str : "unknown Redis error";
    freeReplyObject(reply);
    throw utils::Exception("Redis error: " + message);
  }
}

DB::Status RedisDB::ReadHashFields(const std::string &redis_key,
                                   const std::unordered_set<std::string> *fields,
                                   Fields &result) {
  redisReply *reply;

  if (fields == nullptr) {
    reply = (redisReply *)redisCommand(context_, "HGETALL %s", redis_key.c_str());
  } else {
    std::vector<const char *> argv;
    std::vector<size_t> argvlen;
    argv.reserve(fields->size() + 2);
    argvlen.reserve(fields->size() + 2);

    argv.push_back("HMGET");
    argvlen.push_back(5);
    argv.push_back(redis_key.c_str());
    argvlen.push_back(redis_key.size());
    for (const auto &field : *fields) {
      argv.push_back(field.data());
      argvlen.push_back(field.size());
    }
    reply = (redisReply *)redisCommandArgv(context_, argv.size(), argv.data(), argvlen.data());
  }

  CheckReply(reply);

  Status status = kOK;
  result.clear();

  if (fields == nullptr) {
    if (reply->type == REDIS_REPLY_ARRAY) {
      if (reply->elements == 0) {
        status = kNotFound;
      } else {
        for (size_t i = 0; i + 1 < reply->elements; i += 2) {
          std::string field_name(reply->element[i]->str, reply->element[i]->len);
          std::string field_value(reply->element[i + 1]->str, reply->element[i + 1]->len);
          result.add(field_name, field_value);
        }
      }
    } else {
      status = kError;
    }
  } else {
    if (reply->type == REDIS_REPLY_ARRAY) {
      size_t index = 0;
      for (const auto &field : *fields) {
        if (index >= reply->elements || reply->element[index]->type == REDIS_REPLY_NIL) {
          status = kNotFound;
          break;
        }
        std::string field_value(reply->element[index]->str, reply->element[index]->len);
        result.add(field, field_value);
        ++index;
      }
    } else {
      status = kError;
    }
  }

  freeReplyObject(reply);
  return status;
}

DB::Status RedisDB::IndexKey(const std::string &table, const std::string &key) {
  const std::string index_key = BuildIndexKey(table);
  redisReply *reply = (redisReply *)redisCommand(context_, "ZADD %s 0 %s", index_key.c_str(), key.c_str());
  CheckReply(reply);
  freeReplyObject(reply);
  return kOK;
}

DB::Status RedisDB::DeindexKey(const std::string &table, const std::string &key) {
  const std::string index_key = BuildIndexKey(table);
  redisReply *reply = (redisReply *)redisCommand(context_, "ZREM %s %s", index_key.c_str(), key.c_str());
  CheckReply(reply);
  freeReplyObject(reply);
  return kOK;
}

DB::Status RedisDB::Read(const std::string &table, const std::string &key,
                         const std::unordered_set<std::string> *fields, Fields &result) {
  return ReadHashFields(BuildRedisKey(table, key), fields, result);
}

DB::Status RedisDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::unordered_set<std::string> *fields, std::vector<Fields> &result) {
  const std::string index_key = BuildIndexKey(table);
  const std::string start = "[" + key;

  redisReply *reply = (redisReply *)redisCommand(
      context_, "ZRANGEBYLEX %s %s + LIMIT 0 %d", index_key.c_str(), start.c_str(), len);
  CheckReply(reply);

  if (reply->type != REDIS_REPLY_ARRAY) {
    freeReplyObject(reply);
    return kError;
  }

  result.clear();
  for (size_t index = 0; index < reply->elements; ++index) {
    const redisReply *member = reply->element[index];
    std::string scanned_key(member->str, member->len);
    result.emplace_back();
    Status status = ReadHashFields(BuildRedisKey(table, scanned_key), fields, result.back());
    if (status != kOK) {
      result.pop_back();
    }
  }

  freeReplyObject(reply);
  return result.empty() ? kNotFound : kOK;
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
  if (status == kOK) {
    IndexKey(table, key);
  }
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
  if (status == kOK) {
    DeindexKey(table, key);
  }
  return status;
}

DB *NewRedisDB() {
  return new RedisDB;
}

const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

} // namespace ycsbc
