//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <string>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim,
                        std::vector<ycsbc::CoreWorkload::WorkItem> *pregenerated) {

  try {
    if (init_db) {
      db->Init();
    }

    int ops = 0;

    // Pre-generated path: hot loop contains only DB calls, no key/value generation.
    // Pre-generation is mandatory so the measurement reflects DB cost, not framework cost.
    Fields result_buf;
    std::vector<Fields> scan_result_buf;
    const std::string &table = wl->table_name();
    for (int i = 0; i < num_ops; ++i) {
      if (rlim) {
        rlim->Consume(1);
      }
      auto &item = (*pregenerated)[i];
      switch (item.type) {
        case CoreWorkload::WorkItem::OpType::INSERT:
          db->Insert(table, item.key, item.values);
          break;
        case CoreWorkload::WorkItem::OpType::UPDATE:
          db->Update(table, item.key, item.values);
          break;
        case CoreWorkload::WorkItem::OpType::READ:
          result_buf.clear();
          db->Read(table, item.key, nullptr, result_buf);
          break;
        case CoreWorkload::WorkItem::OpType::SCAN:
          scan_result_buf.clear();
          db->Scan(table, item.key, item.scan_len, nullptr, scan_result_buf);
          break;
        case CoreWorkload::WorkItem::OpType::READMODIFYWRITE:
          result_buf.clear();
          db->Read(table, item.key, nullptr, result_buf);
          db->Update(table, item.key, item.values);
          break;
      }
      ops++;
    }

    // Flush any pending writes (e.g. partial batch) so locks are released
    // before this thread exits, even if cleanup_db is false.
    db->FlushPending();

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    return ops;
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
