#!/bin/bash

# YCSB-cpp Benchmark Script for All Databases
# This script runs all workloads (A-F) across all enabled databases

set -e

# Configuration
YCSB_BIN="./build/ycsb"
WORKLOADS_DIR="./workloads"
RESULTS_DIR="./benchmark_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Databases to test
DATABASES=("rocksdb" "leveldb" "lmdb" "wiredtiger" "leaves")
WORKLOADS=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "========================================"
echo "YCSB-cpp Multi-Database Benchmark"
echo "Timestamp: $TIMESTAMP"
echo "========================================"

# Function to run a single benchmark
run_benchmark() {
    local db=$1
    local workload=$2
    local phase=$3  # load or run
    
    echo "Running $phase phase for $db with $workload..."
    
    local output_file="$RESULTS_DIR/${db}_${workload}_${phase}_${TIMESTAMP}.log"
    local properties_file="${db}/${db}.properties"
    
    # Clean up previous database files ONLY before load phase
    if [ "$phase" = "load" ]; then
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
    fi
    
    # Run the benchmark
    if [ "$phase" = "load" ]; then
        $YCSB_BIN -load -db $db -P $properties_file -P $WORKLOADS_DIR/$workload -s | tee "$output_file"
    else
        $YCSB_BIN -run -db $db -P $properties_file -P $WORKLOADS_DIR/$workload -s | tee "$output_file"
    fi
    
    echo "Completed $phase phase for $db with $workload"
    echo "Results saved to: $output_file"
    echo "----------------------------------------"
}

# Function to extract and summarize results
summarize_results() {
    local summary_file="$RESULTS_DIR/benchmark_summary_${TIMESTAMP}.txt"
    
    echo "=======================================" > "$summary_file"
    echo "YCSB-cpp Benchmark Summary" >> "$summary_file"
    echo "Timestamp: $TIMESTAMP" >> "$summary_file"
    echo "=======================================" >> "$summary_file"
    echo "" >> "$summary_file"
    
    for db in "${DATABASES[@]}"; do
        echo "=== $db ===" >> "$summary_file"
        for workload in "${WORKLOADS[@]}"; do
            local run_file="$RESULTS_DIR/${db}_${workload}_run_${TIMESTAMP}.log"
            if [ -f "$run_file" ]; then
                echo "--- $workload ---" >> "$summary_file"
                grep -E "(READ|UPDATE|INSERT|SCAN).*Operations" "$run_file" >> "$summary_file" 2>/dev/null || echo "No operation stats found" >> "$summary_file"
                grep -E "(READ|UPDATE|INSERT|SCAN).*AverageLatency" "$run_file" >> "$summary_file" 2>/dev/null || echo "No latency stats found" >> "$summary_file"
                echo "" >> "$summary_file"
            fi
        done
        echo "" >> "$summary_file"
    done
    
    echo "Summary saved to: $summary_file"
}

# Main benchmark execution
echo "Starting comprehensive benchmark across ${#DATABASES[@]} databases and ${#WORKLOADS[@]} workloads..."
echo ""

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
echo "Benchmark Complete!"
echo "Total execution time: ${total_time} seconds"
echo "Results directory: $RESULTS_DIR"
echo "========================================"

# Generate summary
summarize_results

echo ""
echo "To view results:"
echo "ls -la $RESULTS_DIR"
echo ""
echo "To view summary:"
echo "cat $RESULTS_DIR/benchmark_summary_${TIMESTAMP}.txt"