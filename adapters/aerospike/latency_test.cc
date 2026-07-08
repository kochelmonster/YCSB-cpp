#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_key.h>
#include <aerospike/as_config.h>
#include <iostream>
#include <chrono>

int main() {
    // Connect to Aerospike
    as_config config;
    as_config_init(&config);
    as_config_add_host(&config, "127.0.0.1", 3000);
    
    config.conn_pools_per_node = 2;
    config.max_conns_per_node = 300;
    
    aerospike as;
    aerospike_init(&as, &config);
    
    as_error err;
    if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
        std::cerr << "Connection failed: " << err.message << std::endl;
        return 1;
    }
    
    std::cout << "Connected to Aerospike" << std::endl;
    
    // Write a record
    as_key key;
    as_key_init_str(&key, "test", "testset", "testkey");
    
    as_record rec;
    as_record_inita(&rec, 1);
    as_record_set_str(&rec, "field1", "value1");
    
    // Time write
    auto start = std::chrono::high_resolution_clock::now();
    if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
        std::cerr << "Write failed: " << err.message << std::endl;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto write_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    as_record_destroy(&rec);
    
    // Time read
    as_record *rec_read = NULL;
    start = std::chrono::high_resolution_clock::now();
    if (aerospike_key_get(&as, &err, NULL, &key, &rec_read) != AEROSPIKE_OK) {
        std::cerr << "Read failed: " << err.message << std::endl;
    }
    end = std::chrono::high_resolution_clock::now();
    auto read_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    if (rec_read) {
        as_record_destroy(rec_read);
    }
    
    std::cout << "Write latency: " << write_us << " μs (" << (1000000.0 / write_us) << " ops/sec)" << std::endl;
    std::cout << "Read latency: " << read_us << " μs (" << (1000000.0 / read_us) << " ops/sec)" << std::endl;
    
    // Test with 1000 operations
    int iterations = 1000;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
        as_key temp_key;
        std::string key_str = "key" + std::to_string(i);
        as_key_init_str(&temp_key, "test", "testset", key_str.c_str());
        
        as_record temp_rec;
        as_record_inita(&temp_rec, 1);
        as_record_set_str(&temp_rec, "field1", "value1");
        
        aerospike_key_put(&as, &err, NULL, &temp_key, &temp_rec);
        as_record_destroy(&temp_rec);
        as_key_destroy(&temp_key);
    }
    end = std::chrono::high_resolution_clock::now();
    auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    std::cout << "\n" << iterations << " writes:" << std::endl;
    std::cout << "Total time: " << total_us << " μs" << std::endl;
    std::cout << "Average latency: " << (total_us / iterations) << " μs/op" << std::endl;
    std::cout << "Throughput: " << (iterations * 1000000.0 / total_us) << " ops/sec" << std::endl;
    
    as_key_destroy(&key);
    aerospike_close(&as, &err);
    aerospike_destroy(&as);
    
    return 0;
}
