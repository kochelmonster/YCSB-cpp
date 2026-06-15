
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
  uint32_t next_record_offset;
  uint32_t key_offset;
  uint32_t op_specific_data_offset;
};

class Dataset {
 public:
  Dataset(const utils::Properties &props, CoreWorkload &workload, int thread_id, bool is_loading);
  ~Dataset();

  void Open();
  void Generate(int op_count);
  std::string GetFullPath() const;

  const CoreWorkload::WorkItem &Next();

 private:
  void GenerateFileName();

  const utils::Properties &props_;
  CoreWorkload &workload_;
  int thread_id_;
  bool is_loading_;
  std::string path_;
  std::string filename_;
  bool force_generate_;

  int fd_;
  void *map_;
  size_t size_;
  char *current_;
  CoreWorkload::WorkItem current_work_item_;
};

} // ycsbc

#endif // YCSB_C_DATASET_H_
