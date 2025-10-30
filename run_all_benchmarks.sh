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
DATABASES=("rocksdb" "leveldb" "lmdb" "wiredtiger" "leaves" "sqlite")
WORKLOADS=("workloada" "workloadb" "workloadc" "workloadd" "workloadf" "workload_scan" "workload_scan100" "workload_scan10")

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
            "null")
                # Null database has no files to clean
                ;;
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
            "sqlite")
                rm -f /tmp/ycsb-sqlite.db /tmp/ycsb-sqlite.db-wal /tmp/ycsb-sqlite.db-shm
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
    
    # Log database size after each phase
    if [ "$phase" = "load" ] || [ "$phase" = "run" ]; then
        local db_path=""
        case $db in
            "null")
                echo "Database size: N/A (null database)" | tee -a "$output_file"
                ;;
            "rocksdb")
                db_path="/tmp/ycsb-rocksdb"
                ;;
            "leveldb")
                db_path="/tmp/ycsb-leveldb"
                ;;
            "lmdb")
                db_path="/tmp/ycsb-lmdb"
                ;;
            "wiredtiger")
                db_path="/tmp/ycsb-wiredtiger"
                ;;
            "leaves")
                db_path="/tmp/ycsb-leaves"
                ;;
            "sqlite")
                db_path="/tmp/ycsb-sqlite.db"
                ;;
        esac
        
        if [ -n "$db_path" ]; then
            if [ -e "$db_path" ]; then
                local db_size=$(du -sh "$db_path" 2>/dev/null | cut -f1)
                echo "Database size: $db_size" | tee -a "$output_file"
            fi
        fi
    fi
    
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
                grep "Database size:" "$run_file" >> "$summary_file" 2>/dev/null || true
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