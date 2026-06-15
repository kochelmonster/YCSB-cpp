
#include "dataset.h"

#include <sys/stat.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "core_workload.h"
#include "utils/utils.h"

namespace ycsbc {

Dataset::Dataset(const utils::Properties &props, CoreWorkload &workload,
                 int thread_id, bool is_loading)
    : props_(props),
      workload_(workload),
      thread_id_(thread_id),
      is_loading_(is_loading),
      fd_(-1),
      map_(nullptr),
      size_(0),
      current_(nullptr) {
  path_ = "../" + props.GetProperty("dataset_path", ".dataset");
  force_generate_ =
      utils::StrToBool(props.GetProperty("force_generate", "false"));
  GenerateFileName();
}

Dataset::~Dataset() {
  if (map_) {
    munmap(map_, size_);
  }
  if (fd_ != -1) {
    close(fd_);
  }
}

void Dataset::Open() {
  fd_ = open((path_ + "/" + filename_).c_str(), O_RDONLY);
  if (fd_ == -1) {
    throw utils::Exception("Failed to open dataset file");
  }

  struct stat sb;
  if (fstat(fd_, &sb) == -1) {
    throw utils::Exception("Failed to get file size");
  }
  size_ = sb.st_size;

  map_ = mmap(NULL, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
  if (map_ == MAP_FAILED) {
    throw utils::Exception("Failed to map file to memory");
  }
  current_ = static_cast<char *>(map_);
}

void Dataset::Generate(int op_count) {
  std::filesystem::create_directories(path_);
  std::ofstream ofs(path_ + "/" + filename_, std::ios::binary);
  if (!ofs.is_open()) {
    throw utils::Exception("Failed to open dataset file for writing");
  }

  workload_.PrepareOpsForFile(ofs, op_count, is_loading_);
  ofs.close();
}

std::string Dataset::GetFullPath() const {
  return path_ + "/" + filename_;
}

const CoreWorkload::WorkItem &Dataset::Next() {
  if (current_ >= static_cast<char *>(map_) + size_) {
    throw utils::Exception("End of dataset");
  }

  current_work_item_.type = static_cast<CoreWorkload::WorkItem::OpType>(*current_);
  current_++;

  uint32_t meta_len = *reinterpret_cast<uint32_t *>(current_);
  current_ += sizeof(uint32_t);

  const Meta *meta = reinterpret_cast<const Meta *>(current_);
  current_ += meta_len;

  const char *key_ptr = reinterpret_cast<const char *>(map_) + meta->key_offset;
  uint32_t key_len = *reinterpret_cast<const uint32_t *>(key_ptr);
  key_ptr += sizeof(uint32_t);
  current_work_item_.key = Slice(key_ptr, key_len);

  const char *op_specific_data_ptr =
      reinterpret_cast<const char *>(map_) + meta->op_specific_data_offset;

  switch (current_work_item_.type) {
    case CoreWorkload::WorkItem::OpType::INSERT:
    case CoreWorkload::WorkItem::OpType::UPDATE: {
      uint32_t fields_size = *reinterpret_cast<const uint32_t *>(op_specific_data_ptr);
      current_work_item_.values = ReadonlyFields(op_specific_data_ptr, fields_size);
      break;
    }
    case CoreWorkload::WorkItem::OpType::SCAN: {
      current_work_item_.scan_len = *reinterpret_cast<const uint32_t *>(op_specific_data_ptr);
      break;
    }
    default:
      break;
  }

  current_ = reinterpret_cast<char *>(map_) + meta->next_record_offset;

  return current_work_item_;
}

void Dataset::GenerateFileName() {
  std::stringstream ss;
  ss << props_.GetProperty("workload", "workloada") << "-";
  if (is_loading_) {
    ss << "load-";
  } else {
    ss << "run-";
  }
  ss << "recordcount" << props_.GetProperty(CoreWorkload::RECORD_COUNT_PROPERTY)
     << "-";
  ss << "operationcount"
     << props_.GetProperty(CoreWorkload::OPERATION_COUNT_PROPERTY) << "-";
  ss << "thread" << thread_id_;
  filename_ = ss.str() + ".dat";
}

}  // namespace ycsbc
