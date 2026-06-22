#ifndef YCSB_C_DATASET_H_
#define YCSB_C_DATASET_H_

#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>

#include "core/core_workload.h"
#include "utils/properties.h"

namespace ycsbc {

struct Meta {
  uint64_t next_record_offset;
  uint64_t key_offset;
  uint64_t op_specific_data_offset;
};

class Dataset {
 public:
  Dataset(const utils::Properties &props, CoreWorkload &workload, int thread_id, bool is_loading);
  ~Dataset();

  void Open(int op_count);
  void Generate(int op_count);
  std::string GetFullPath(int op_count) const;
  void DEBUG(int op_count);

  int OpCount() const { return op_count_; }
  const CoreWorkload::WorkItem &Next();

 private:

  const utils::Properties &props_;
  CoreWorkload &workload_;
  int thread_id_;
  bool is_loading_;
  std::string path_;
  std::string workload_props_hash_;
  bool force_generate_;
  int op_count_;

  int fd_;
  void *map_;
  size_t size_;
  char *current_;
  CoreWorkload::WorkItem current_work_item_;
};

} // ycsbc

#endif // YCSB_C_DATASET_H_