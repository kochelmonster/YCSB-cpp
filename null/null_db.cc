//
//  null_db.cc
//  YCSB-cpp
//
//  Null database for measuring benchmark overhead
//

#include "null_db.h"
#include "core/db_factory.h"

namespace ycsbc {

DB *NewNullDB() {
  return new NullDB;
}

const bool registered = DBFactory::RegisterDB("null", NewNullDB);

} // namespace ycsbc
