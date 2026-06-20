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

#include "core/dataset.h"
#include "core_workload.h"
#include "db.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB* db, CoreWorkload* wl, const int num_ops,
                        utils::CountDownLatch* latch, utils::RateLimiter* rlim,
                        ycsbc::Dataset* dataset) {
  try {
    int ops = 0;

    // Pre-generated path: hot loop contains only DB calls, no key/value
    // generation. Pre-generation is mandatory so the measurement reflects DB
    // cost, not framework cost.
    Fields result_buf;
    std::vector<Fields> scan_result_buf;
    const std::string& table = wl->table_name();
    for (int i = 0; i < num_ops; ++i) {
      if (rlim) {
        rlim->Consume(1);
      }
      const auto& item = dataset->Next();
      const auto& values = item.values;

      switch (item.type) {
        case CoreWorkload::WorkItem::OpType::INSERT:
          db->Insert(table, item.key, values);
          break;
        case CoreWorkload::WorkItem::OpType::UPDATE:
          db->Update(table, item.key, values);
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
          db->Update(table, item.key, values);
          break;
      }
      ops++;
    }

    // Flush any pending writes (e.g. partial batch) so locks are released
    // before this thread exits.
    db->FlushPending();

    latch->CountDown();
    return ops;
  } catch (const utils::Exception& e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

}  // namespace ycsbc

#endif  // YCSB_C_CLIENT_H_