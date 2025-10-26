//
//  serialization.h
//  YCSB-cpp
//
//  Common serialization/deserialization for database bindings
//

#ifndef YCSB_C_SERIALIZATION_H_
#define YCSB_C_SERIALIZATION_H_

#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

#include "core/db.h"

namespace ycsbc {
namespace utils {

class Serialization {
 public:
  Serialization() {
    field_buffer_.reserve(1024);
    value_buffer_.reserve(1024);
    data_buffer_.reserve(1024);
  }

  // Serialize a row of fields into a binary format
  // Format: [field_count:uint32][len:uint32][name:bytes][len:uint32][value:bytes]...
  // Returns reference to internal buffer containing serialized data
  const std::string& SerializeRow(const std::vector<ycsbc::DB::Field>& values) {
    SerializeRowImpl(values);
    return data_buffer_;
  }

  // Deserialize all fields from binary data
  void DeserializeRow(std::vector<ycsbc::DB::Field>& values,
                      const char* data_ptr, size_t data_len,
                      size_t expected_field_count = 0) {
    DeserializeRowImpl(values, data_ptr, data_len, expected_field_count);
  }

  // Deserialize from std::string
  void DeserializeRow(std::vector<ycsbc::DB::Field>& values,
                      const std::string& data,
                      size_t expected_field_count = 0) {
    DeserializeRow(values, data.data(), data.size(), expected_field_count);
  }

  // Deserialize only specific fields (filtered)
  void DeserializeRowFilter(std::vector<ycsbc::DB::Field>& values,
                            const char* data_ptr, size_t data_len,
                            const std::vector<std::string>& fields) {
    DeserializeRowFilterImpl(values, data_ptr, data_len, fields);
  }

  // Deserialize filtered from std::string
  void DeserializeRowFilter(std::vector<ycsbc::DB::Field>& values,
                            const std::string& data,
                            const std::vector<std::string>& fields) {
    DeserializeRowFilter(values, data.data(), data.size(), fields);
  }

  // Merge update fields into current values
  // Updates fields in current_values with matching fields from update_values
  void MergeUpdate(std::vector<ycsbc::DB::Field>& current_values,
                   const std::vector<ycsbc::DB::Field>& update_values) {
    for (const ycsbc::DB::Field& new_field : update_values) {
      bool found MAYBE_UNUSED = false;
      for (ycsbc::DB::Field& cur_field : current_values) {
        if (cur_field.name == new_field.name) {
          found = true;
          cur_field.value = new_field.value;
          break;
        }
      }
      assert(found);
    }
  }

 private:
  // Reusable buffers to avoid allocations during deserialization
  mutable std::string field_buffer_;
  mutable std::string value_buffer_;
  // Reusable buffer for serialization
  mutable std::string data_buffer_;

  void SerializeRowImpl(const std::vector<ycsbc::DB::Field>& values) {
    data_buffer_.clear();
    
    // Write field count first
    uint32_t field_count = values.size();
    data_buffer_.append(reinterpret_cast<const char*>(&field_count), sizeof(uint32_t));
    
    // Write each field
    for (const ycsbc::DB::Field& field : values) {
      uint32_t len = field.name.size();
      data_buffer_.append(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
      data_buffer_.append(field.name.data(), field.name.size());
      len = field.value.size();
      data_buffer_.append(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
      data_buffer_.append(field.value.data(), field.value.size());
    }
  }

  void DeserializeRowImpl(std::vector<ycsbc::DB::Field>& values,
                          const char* data_ptr, size_t data_len,
                          size_t expected_field_count = 0) {
    const char* p = data_ptr;
    const char* lim = p + data_len;
    values.clear();

    // Read field count first
    assert(p + sizeof(uint32_t) <= lim);
    uint32_t field_count = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    
    // Reserve space for efficiency
    values.reserve(field_count);

    // Read each field - reuse buffers to avoid allocations
    for (uint32_t i = 0; i < field_count; ++i) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      field_buffer_.assign(p, len);
      p += len;

      len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      value_buffer_.assign(p, len);
      p += len;

      values.push_back({field_buffer_, value_buffer_});
    }

    if (expected_field_count > 0) {
      assert(values.size() == expected_field_count);
    }
  }

  void DeserializeRowFilterImpl(std::vector<ycsbc::DB::Field>& values,
                                const char* data_ptr, size_t data_len,
                                const std::vector<std::string>& fields) {
    const char* p = data_ptr;
    const char* lim = p + data_len;
    values.clear();

    // Read field count first
    assert(p + sizeof(uint32_t) <= lim);
    uint32_t field_count = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    
    values.reserve(fields.size());

    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    
    // Read each field - reuse buffers to avoid allocations
    for (uint32_t i = 0; i < field_count && filter_iter != fields.end(); ++i) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      field_buffer_.assign(p, len);
      p += len;

      len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      value_buffer_.assign(p, len);
      p += len;

      if (*filter_iter == field_buffer_) {
        values.push_back({field_buffer_, value_buffer_});
        filter_iter++;
      }
    }

    assert(values.size() == fields.size());
  }
};

}  // namespace utils
}  // namespace ycsbc

#endif  // YCSB_C_SERIALIZATION_H_
