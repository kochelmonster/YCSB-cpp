//
//  lmdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include <string.h>
#include <sys/stat.h>
#if defined(_MSC_VER)
#include "direct.h"
#define mkdir(x, y) _mkdir(x)
#endif

#include "lmdb_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/properties.h"
#include "utils/utils.h"

#include <lmdb.h>

namespace {
  const std::string PROP_DBPATH = "lmdb.dbpath";
  const std::string PROP_DBPATH_DEFAULT = "";

  const std::string PROP_MAPSIZE = "lmdb.mapsize";
  const std::string PROP_MAPSIZE_DEFAULT = "-1";

  const std::string PROP_NOSYNC = "lmdb.nosync";
  const std::string PROP_NOSYNC_DEFAULT = "false";

  const std::string PROP_NOMETASYNC = "lmdb.nometasync";
  const std::string PROP_NOMETASYNC_DEFAULT = "false";

  const std::string PROP_NORDAHEAD = "lmdb.noreadahead";
  const std::string PROP_NORDAHEAD_DEFAULT = "false";

  const std::string PROP_WRITEMAP = "lmdb.writemap";
  const std::string PROP_WRITEMAP_DEFAULT = "false";

  const std::string PROP_MAPASYNC = "lmdb.mapasync";
  const std::string PROP_MAPASYNC_DEFAULT = "false";
} // anonymous

namespace ycsbc {

size_t LmdbDB::field_count_;
std::string LmdbDB::field_prefix_;

MDB_env *LmdbDB::env_;
MDB_dbi LmdbDB::dbi_;
int LmdbDB::ref_cnt_ = 0;
std::mutex LmdbDB::mutex_;

void LmdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mutex_);

  const utils::Properties &props = *props_;

  if (ref_cnt_++) {
    return;
  }

  field_count_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                    CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

  int ret;
  int env_opt = MDB_NOTLS;  // Enable lock-free reads across threads
  if (props.GetProperty(PROP_NOSYNC, PROP_NOSYNC_DEFAULT) == "true") {
    env_opt |= MDB_NOSYNC;
  }
  if (props.GetProperty(PROP_NOMETASYNC, PROP_NOMETASYNC_DEFAULT) == "true") {
    env_opt |= MDB_NOMETASYNC;
  }
  if (props.GetProperty(PROP_NORDAHEAD, PROP_NORDAHEAD_DEFAULT) == "true") {
    env_opt |= MDB_NORDAHEAD;
  }
  if (props.GetProperty(PROP_WRITEMAP, PROP_WRITEMAP_DEFAULT) == "true") {
    env_opt |= MDB_WRITEMAP;
  }
  if (props.GetProperty(PROP_MAPASYNC, PROP_MAPASYNC_DEFAULT) == "true") {
    env_opt |= MDB_MAPASYNC;
  }
  ret = mdb_env_create(&env_);
  if  (ret) {
    throw utils::Exception(std::string("Init mdb_env_create: ") + mdb_strerror(ret));
  }
  size_t map_size = std::stoul(props.GetProperty(PROP_MAPSIZE, PROP_MAPSIZE_DEFAULT));
  if (map_size >= 0) {
    ret = mdb_env_set_mapsize(env_, map_size);
    if (ret) {
      throw utils::Exception(std::string("Init mdb_env_set_mapsize: ") + mdb_strerror(ret));
    }
  }
  const std::string &db_path = props.GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("LMDB db path is missing");
  }
  ret = mkdir(db_path.c_str(), 0775);
  if (ret && errno != EEXIST) {
    throw utils::Exception(std::string("Init mkdir: ") + strerror(errno));
  }
  ret = mdb_env_open(env_, db_path.c_str(), env_opt, 0664);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_env_open: ") + mdb_strerror(ret));
  }

  MDB_txn *txn;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_open(txn, nullptr, 0, &dbi_);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_open: ") + mdb_strerror(ret));
  }
  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_txn_commit: ") + mdb_strerror(ret));
  }
}

void LmdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mutex_);
  if (--ref_cnt_) {
    return;
  }
  mdb_close(env_, dbi_);
  mdb_env_close(env_);
}

DB::Status LmdbDB::Read(const std::string &table, const std::string &key, const std::unordered_set<std::string> *fields,
                        Fields &result) {
  DB::Status s = kOK;
  MDB_txn *txn;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  // Use MDB_RDONLY | MDB_NOTLS for fast lock-free reads
  ret = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
  if (ret) {
    throw utils::Exception(std::string("Read mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
  if (ret == MDB_NOTFOUND) {
    s = kNotFound;
    mdb_txn_abort(txn);
    return s;
  } else if (ret) {
    mdb_txn_abort(txn);
    throw utils::Exception(std::string("Read mdb_get: ") + mdb_strerror(ret));
  }
  if (fields != nullptr) {
    ReadonlyFields readonly(static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
    readonly.filter(result, *fields);
  } else {
    ReadonlyFields readonly(static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
    result = readonly;
  }
  // Abort instead of commit for read-only transactions (faster)
  mdb_txn_abort(txn);
  return s;
}

DB::Status LmdbDB::Scan(const std::string &table, const std::string &key, int len,
                        const std::unordered_set<std::string> *fields,
                        std::vector<Fields> &result) {
  DB::Status s = kOK;
  MDB_txn *txn;
  MDB_cursor *cursor;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Scan mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_cursor_open(txn, dbi_, &cursor);
  if (ret) {
    throw utils::Exception(std::string("Scan mdb_cursor_open: ") + mdb_strerror(ret));
  }
  ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_SET);
  if (ret == MDB_NOTFOUND) {
    s = kNotFound;
    goto cleanup;
  } else if (ret) {
    throw utils::Exception(std::string("Scan mdb_cursor_get: ") + mdb_strerror(ret));
  }
  for (int i = 0; !ret && i < len; i++) {
    result.emplace_back();
    Fields &values = result.back();
    if (fields != nullptr) {
      ReadonlyFields readonly(static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
      readonly.filter(values, *fields);
    } else {
      ReadonlyFields readonly(static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
      values = readonly;
    }
    ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
  }
cleanup:
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  return s;
}

DB::Status LmdbDB::Update(const std::string &table, const std::string &key, Fields &values) {
  MDB_txn *txn;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_get: ") + mdb_strerror(ret));
  }
  Fields current_values;
  ReadonlyFields readonly(static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
  current_values = readonly;
  
  Slice updated_data = current_values.update(values);

  val_slice.mv_data = const_cast<char *>(updated_data.data());
  val_slice.mv_size = updated_data.size();
  ret = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_put: ") + mdb_strerror(ret));
  }

  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_txn_commit: ") + mdb_strerror(ret));
  }
  return kOK;
}

DB::Status LmdbDB::Insert(const std::string &table, const std::string &key, Fields &values) {
  MDB_txn *txn;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  const std::string& data = values.buffer();
  val_slice.mv_data = static_cast<void *>(const_cast<char *>(data.data()));
  val_slice.mv_size = data.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Insert mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
  if (ret) {
    throw utils::Exception(std::string("Insert mdb_put: ") + mdb_strerror(ret));
  }
  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Insert mdb_txn_commit: ") + mdb_strerror(ret));
  }
  return kOK;
}

DB::Status LmdbDB::Delete(const std::string &table, const std::string &key) {
  MDB_txn *txn;
  MDB_val key_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Delete mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_del(txn, dbi_, &key_slice, nullptr);
  if (ret) {
    throw utils::Exception(std::string("Delete mdb_del: ") + mdb_strerror(ret));
  }
  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Delete mdb_txn_commit: ") + mdb_strerror(ret));
  }
  return kOK;
}

DB *NewLmdbDB() {
  return new LmdbDB;
}

const bool registered = DBFactory::RegisterDB("lmdb", NewLmdbDB);

} // ycsbc
