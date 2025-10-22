#!/bin/bash

# Deterministic YCSB Benchmark Script
# This script ensures reproducible results by controlling randomness

set -e

# Set deterministic environment
export YCSB_SEED=12345
export RANDOM_SEED=12345

# Configuration
YCSB_BIN="./build/ycsb"
WORKLOADS_DIR="./workloads"
RESULTS_DIR="./benchmark_results_deterministic"
TIMESTAMP="deterministic_$(date +"%Y%m%d_%H%M%S")"

# Databases to test (you can modify this list)
DATABASES=("rocksdb" "leveldb" "lmdb" "leaves")
WORKLOADS=("workloadc_deterministic")

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "========================================"
echo "YCSB-cpp Deterministic Benchmark"
echo "Timestamp: $TIMESTAMP"
echo "Seed: $YCSB_SEED"
echo "========================================"

# Function to run a single benchmark
run_benchmark() {
    local db=$1
    local workload=$2
    local phase=$3  # load or run
    
    echo "Running $phase phase for $db with $workload..."
    
    local output_file="$RESULTS_DIR/${db}_${workload}_${phase}_${TIMESTAMP}.log"
    local properties_file="${db}/${db}.properties"
    
    # Clean up previous database files
    case $db in
        "rocksdb")
            rm -rf /tmp/ycsb-rocksdb
            ;;
        "leveldb") 
            rm -rf /tmp/ycsb-leveldb
            ;;
        "lmdb")
            rm -rf /tmp/ycsb-lmdb
            ;;
        "wiredtiger")
            rm -rf /tmp/ycsb-wiredtiger
            ;;
        "leaves")
            rm -rf /tmp/ycsb-leaves
            ;;
    esac
    
    # Set the seed for this run (consistent across runs)
    export RANDOM=$YCSB_SEED
    
    # Run the benchmark with single thread for deterministic results
    if [ "$phase" = "load" ]; then
        $YCSB_BIN -load -db $db -P $properties_file -P $WORKLOADS_DIR/$workload -threads 1 -s | tee "$output_file"
    else
        $YCSB_BIN -run -db $db -P $properties_file -P $WORKLOADS_DIR/$workload -threads 1 -s | tee "$output_file"
    fi
    
    echo "Completed $phase phase for $db with $workload"
    echo "Results saved to: $output_file"
    echo "----------------------------------------"
}

# Record start time
start_time=$(date +%s)

# Run benchmarks for each database and workload combination
for db in "${DATABASES[@]}"; do
    echo "========================================"
    echo "Testing Database: $db"
    echo "========================================"
    
    for workload in "${WORKLOADS[@]}"; do
        echo "Testing workload: $workload"
        
        # Load phase
        run_benchmark "$db" "$workload" "load"
        
        # Run phase  
        run_benchmark "$db" "$workload" "run"
        
        echo ""
    done
    
    echo "Completed all workloads for $db"
    echo ""
done

# Calculate total time
end_time=$(date +%s)
total_time=$((end_time - start_time))

echo "========================================"
echo "Deterministic Benchmark Complete!"
echo "Total execution time: ${total_time} seconds"
echo "Results directory: $RESULTS_DIR"
echo "========================================"

echo ""
echo "All results should be identical across multiple runs"
echo "To verify determinism, run this script multiple times and compare results"