//
//  db_writer_proxy.h
//  YCSB-cpp
//
//  Dedicated writer thread proxy for single-writer databases.
//  Reads pass through to per-thread DB instances.
//  Writes are queued and executed by a single background thread.
//

#ifndef YCSB_C_DB_WRITER_PROXY_H_
#define YCSB_C_DB_WRITER_PROXY_H_

#include <string>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <iostream>

#include "db.h"

namespace ycsbc {

struct WriteOp {
  enum Type : uint8_t { UPDATE, INSERT, DELETE_OP };
  Type type;
  std::string table;
  std::string key;
  std::string values_buffer; // serialized Fields data
};

class SharedWriteQueue {
 public:
  explicit SharedWriteQueue(size_t max_size = 4096) : max_size_(max_size) {}

  void Enqueue(WriteOp op) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_cv_.wait(lock, [this] { return queue_.size() < max_size_ || stop_; });
    if (stop_) return;
    in_flight_++;
    queue_.push_back(std::move(op));
    not_empty_cv_.notify_one();
  }

  bool Dequeue(WriteOp &op) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_cv_.wait(lock, [this] { return !queue_.empty() || stop_; });
    if (queue_.empty()) return false;
    op = std::move(queue_.front());
    queue_.pop_front();
    not_full_cv_.notify_one();
    return true;
  }

  // Signal that a dequeued op has been fully processed
  void MarkProcessed() {
    std::lock_guard<std::mutex> lock(mutex_);
    in_flight_--;
    if (in_flight_ == 0) {
      drained_cv_.notify_all();
    }
  }

  // Block until all enqueued writes have been fully processed
  void Drain() {
    std::unique_lock<std::mutex> lock(mutex_);
    drained_cv_.wait(lock, [this] { return in_flight_ == 0; });
  }

  void RequestStop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
    not_empty_cv_.notify_all();
    not_full_cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable not_empty_cv_;
  std::condition_variable not_full_cv_;
  std::condition_variable drained_cv_;
  std::deque<WriteOp> queue_;
  size_t max_size_;
  int64_t in_flight_ = 0;
  bool stop_ = false;
};

// Runs on the dedicated writer thread
inline void WriterThreadFunc(DB *writer_db, SharedWriteQueue *queue) {
  try {
    writer_db->Init();
    WriteOp op;
    while (queue->Dequeue(op)) {
      switch (op.type) {
        case WriteOp::UPDATE: {
          ReadonlyFields values(op.values_buffer.data(), op.values_buffer.size());
          writer_db->Update(op.table, op.key, values);
          break;
        }
        case WriteOp::INSERT: {
          ReadonlyFields values(op.values_buffer.data(), op.values_buffer.size());
          writer_db->Insert(op.table, op.key, values);
          break;
        }
        case WriteOp::DELETE_OP:
          writer_db->Delete(op.table, op.key);
          break;
      }
      queue->MarkProcessed();
    }
    writer_db->Cleanup();
  } catch (const std::exception &e) {
    std::cerr << "Writer thread exception: " << e.what() << std::endl;
  }
}

///
/// DB proxy that passes reads to a per-thread DB instance and
/// queues writes to a shared dedicated writer thread.
///
class DBWriterProxy : public DB {
 public:
  DBWriterProxy(DB *read_db, SharedWriteQueue *queue)
      : read_db_(read_db), queue_(queue) {}

  ~DBWriterProxy() {
    delete read_db_;
  }

  void Init() override {
    read_db_->Init();
  }

  void Cleanup() override {
    read_db_->Cleanup();
  }

  Status Read(const std::string &table, Slice key,
              const std::unordered_set<std::string> *fields, Fields &result) override {
    return read_db_->Read(table, key, fields, result);
  }

  Status Scan(const std::string &table, Slice key, int record_count,
              const std::unordered_set<std::string> *fields,
              std::vector<Fields> &result) override {
    return read_db_->Scan(table, key, record_count, fields, result);
  }

  Status Update(const std::string &table, Slice key, const ReadonlyFields &values) override {
    WriteOp op;
    op.type = WriteOp::UPDATE;
    op.table = table;
    op.key = key.ToString();
    auto data = values.data();
    op.values_buffer.assign(data.data(), data.size());
    queue_->Enqueue(std::move(op));
    return kOK;
  }

  Status Insert(const std::string &table, Slice key, const ReadonlyFields &values) override {
    WriteOp op;
    op.type = WriteOp::INSERT;
    op.table = table;
    op.key = key.ToString();
    auto data = values.data();
    op.values_buffer.assign(data.data(), data.size());
    queue_->Enqueue(std::move(op));
    return kOK;
  }

  Status Delete(const std::string &table, Slice key) override {
    WriteOp op;
    op.type = WriteOp::DELETE_OP;
    op.table = table;
    op.key = key.ToString();
    queue_->Enqueue(std::move(op));
    return kOK;
  }

  Status BeginTransaction() override {
    return read_db_->BeginTransaction();
  }

  Status CommitTransaction() override {
    return read_db_->CommitTransaction();
  }

  Status RollbackTransaction() override {
    return read_db_->RollbackTransaction();
  }

 private:
  DB *read_db_;            // per-thread, owned
  SharedWriteQueue *queue_; // shared, not owned
};

} // ycsbc

#endif // YCSB_C_DB_WRITER_PROXY_H_
