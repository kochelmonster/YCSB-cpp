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
    const int batch_size = wl->batch_size();

    for (int i = 0; i < num_ops; i += batch_size) {
      if (rlim) {
        rlim->Consume(batch_size);
      }

      int batch_end = std::min(i + batch_size, num_ops);
      int batch_len = batch_end - i;

      // Begin transaction for this batch.  The adapter may decide to
      // optimise single-operation batches (batch_size==1) differently
      // from multi-operation batches.
      db->BeginTransaction();

      for (int j = 0; j < batch_len; ++j) {
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
            db->Read(table, item.key, nullptr, result_buf, true);
            db->Update(table, item.key, item.values);
            break;
        }
        ops++;
      }

      // Commit transaction for this batch.
      db->CommitTransaction();
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