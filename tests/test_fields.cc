#include <iostream>
#include <cassert>
#include "utils/fields.h"

using namespace ycsbc;

void test_basic_operations() {
  std::cout << "Testing basic operations..." << std::endl;
  
  Fields fields;
  
  // Test push
  fields.push("field0", "value0");
  fields.push("field1", "value1");
  fields.push("field2", "value2");
  
  assert(fields.size() == 3);
  
  // Test iteration
  int i = 0;
  for (auto it = fields.begin(); it != fields.end(); ++it, ++i) {
    auto [name, value] = *it;
    std::string expected_name = "field" + std::to_string(i);
    std::string expected_value = "value" + std::to_string(i);
    
    assert(name.ToString() == expected_name);
    assert(value.ToString() == expected_value);
  }
  
  std::cout << "✓ Basic operations passed" << std::endl;
}

void test_update() {
  std::cout << "Testing update operations..." << std::endl;
  
  Fields original;
  original.push("field0", "value0");
  original.push("field1", "value1");
  original.push("field2", "value2");
  
  Fields updates;
  updates.push("field1", "updated1");
  updates.push("field3", "value3");  // New field
  
  Slice result = original.update(updates);
  
  // Create a ReadonlyFields from the result to check
  ReadonlyFields updated(result);
  
  // Check results
  assert(updated.size() == 4);
  
  for (auto it = updated.begin(); it != updated.end(); ++it) {
    auto [name, value] = *it;
    std::string name_str = name.ToString();
    std::string value_str = value.ToString();
    
    if (name_str == "field0") {
      assert(value_str == "value0");  // Unchanged
    } else if (name_str == "field1") {
      assert(value_str == "updated1");  // Updated
    } else if (name_str == "field2") {
      assert(value_str == "value2");  // Unchanged
    } else if (name_str == "field3") {
      assert(value_str == "value3");  // New
    } else {
      assert(false && "Unexpected field");
    }
  }
  
  std::cout << "✓ Update operations passed" << std::endl;
}

void test_vector_conversion() {
  std::cout << "Testing vector conversion..." << std::endl;
  
  std::vector<DB::Field> vec = {
    {"name1", "value1"},
    {"name2", "value2"}
  };
  
  Fields fields;
  fields.from_vector(vec);
  
  assert(fields.size() == 2);
  
  std::vector<DB::Field> result;
  fields.to_vector(result);
  
  assert(result.size() == 2);
  assert(result[0].name == "name1");
  assert(result[0].value == "value1");
  assert(result[1].name == "name2");
  assert(result[1].value == "value2");
  
  std::cout << "✓ Vector conversion passed" << std::endl;
}

void test_clear_and_reuse() {
  std::cout << "Testing clear and reuse..." << std::endl;
  
  Fields fields;
  fields.push("test1", "value1");
  fields.push("test2", "value2");
  
  assert(fields.size() == 2);
  
  fields.clear();
  assert(fields.size() == 0);
  assert(fields.empty());
  
  // Reuse after clear
  fields.push("new1", "newval1");
  assert(fields.size() == 1);
  
  auto it = fields.begin();
  auto [name, value] = *it;
  assert(name.ToString() == "new1");
  assert(value.ToString() == "newval1");
  
  std::cout << "✓ Clear and reuse passed" << std::endl;
}

int main() {
  std::cout << "Running Fields class tests..." << std::endl;
  
  test_basic_operations();
  test_update();
  test_vector_conversion();
  test_clear_and_reuse();
  
  std::cout << "\n✓ All tests passed!" << std::endl;
  return 0;
}
