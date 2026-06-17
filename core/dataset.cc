
#include "dataset.h"

#include <sys/stat.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include "core_workload.h"
#include "utils/utils.h"

namespace ycsbc {

Dataset::Dataset(const utils::Properties& props, CoreWorkload& workload,
                 int thread_id, bool is_loading)
    : props_(props),
      workload_(workload),
      thread_id_(thread_id),
      is_loading_(is_loading),
      workload_props_hash_(workload.GetPropertiesHash()),
      fd_(-1),
      map_(nullptr),
      size_(0),
      current_(nullptr) {
  path_ = props.GetProperty("dataset_path", ".dataset");
  force_generate_ =
      utils::StrToBool(props.GetProperty("force_generate", "false"));
}

Dataset::~Dataset() {
  if (map_) {
    munmap(map_, size_);
  }
  if (fd_ != -1) {
    close(fd_);
  }
}

void Dataset::Open(int op_count) {
  fd_ = open(GetFullPath(op_count).c_str(), O_RDONLY);
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
  current_ = static_cast<char*>(map_);
}

void Dataset::Generate(int op_count) {
  std::filesystem::create_directories(path_);
  std::ofstream ofs(GetFullPath(op_count), std::ios::binary);
  if (!ofs.is_open()) {
    throw utils::Exception("Failed to open dataset file for writing");
  }

  workload_.PrepareOpsForFile(ofs, op_count, is_loading_);
  ofs.close();
}

void Dataset::DEBUG(int op_count) {
  /* Test the dataset*/
  Open(op_count);
  for (int i = 0; i < op_count; ++i) {
    const auto& item = Next();
    const auto& values = item.values;

    std::string type_str;
    switch (item.type) {
      case CoreWorkload::WorkItem::OpType::INSERT:
        type_str = "INSERT";
        break;
      case CoreWorkload::WorkItem::OpType::UPDATE:
        type_str = "UPDATE";
        break;
      case CoreWorkload::WorkItem::OpType::READ:
        type_str = "READ";
        break;
      case CoreWorkload::WorkItem::OpType::SCAN:
        type_str = "SCAN";
        break;
      case CoreWorkload::WorkItem::OpType::READMODIFYWRITE:
        type_str = "READMODIFYWRITE";
        break;
      default:
        type_str = "UNKNOWN";
        break;
    }

#if 0
    std::cout << "Testitem: " << i << ": " << type_str << " " << item.key.size() << " with "
              << values.size() << " fields" << "  size: " << values.data().size() << std::endl;
#endif

    if (values.data().size() > 2048) {
      std::cout << "Error Testitem: " << i << ": " << type_str << " " << item.key.size() << " with "
              << values.size() << " fields" << "  size: " << values.data().size() << std::endl;
      throw std::runtime_error("Field data size exceeds 2048 bytes");
    }
      
#if 0
    ReadonlyFields readonly(values);
    // check correctnes of the values
    for (auto current_it = readonly.begin(); current_it != readonly.end();
         ++current_it) {
      std::cout << "  field name: " << current_it.name().ToString()
                << ", value size: " << current_it.value().size() << std::endl;
    }
#endif
  }

  if (map_) {
    munmap(map_, size_);
    map_ = nullptr;
  }
  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
}

std::string Dataset::GetFullPath(int op_count) const {
  std::stringstream ss;
  ss << workload_props_hash_ << "-";
  if (is_loading_) {
    ss << "load-";
  } else {
    ss << "run-";
  }
  ss << "recordcount" << props_.GetProperty(CoreWorkload::RECORD_COUNT_PROPERTY)
     << "-";
  ss << "operationcount" << op_count << "-";
  ss << "thread" << thread_id_;
  return path_ + "/" + ss.str() + ".dat";
}

const CoreWorkload::WorkItem& Dataset::Next() {
  if (current_ >= static_cast<char*>(map_) + size_) {
    throw utils::Exception("End of dataset");
  }

  current_work_item_.type =
      static_cast<CoreWorkload::WorkItem::OpType>(*current_);
  current_++;

  uint64_t meta_len = *reinterpret_cast<uint64_t*>(current_);
  current_ += sizeof(uint64_t);

  const Meta* meta = reinterpret_cast<const Meta*>(current_);
  current_ += meta_len;

  const char* key_ptr = reinterpret_cast<const char*>(map_) + meta->key_offset;
  uint32_t key_len = *reinterpret_cast<const uint32_t*>(key_ptr);
  key_ptr += sizeof(uint32_t);
  current_work_item_.key = Slice(key_ptr, key_len);

  const char* op_specific_data_ptr =
      reinterpret_cast<const char*>(map_) + meta->op_specific_data_offset;

  switch (current_work_item_.type) {
    case CoreWorkload::WorkItem::OpType::INSERT:
    case CoreWorkload::WorkItem::OpType::UPDATE: {
      uint32_t fields_size =
          *reinterpret_cast<const uint32_t*>(op_specific_data_ptr);
      current_work_item_.values =
          ReadonlyFields(op_specific_data_ptr + sizeof(uint32_t), fields_size);
      break;
    }
    case CoreWorkload::WorkItem::OpType::SCAN: {
      current_work_item_.scan_len =
          *reinterpret_cast<const uint32_t*>(op_specific_data_ptr);
      break;
    }
    default:
      current_work_item_.values = ReadonlyFields();
      break;
  }

  current_ = reinterpret_cast<char*>(map_) + meta->next_record_offset;

  return current_work_item_;
}

}  // namespace ycsbc
