#!/bin/bash

# YCSB-cpp Benchmark Script for Application Workloads
#
# Modes:
#   MATRIX_MODE=throughput  -> default engine settings
#   MATRIX_MODE=durability  -> stricter durability-oriented settings
#
# Scenario coverage (Option 2 delivery):
#   - baseline
#   - binary_key
#   - batch_insert_{1,8,32,64}
#   - batch_update_{1,8,32,64}
#   - acid_aci
#   - acid_txn

set -e

YCSB_BIN="./build/ycsb"
WORKLOADS_DIR="./workloads"
RESULTS_DIR="./benchmark_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MATRIX_MODE="${MATRIX_MODE:-throughput}"
LOAD_BATCH_SIZE="${LOAD_BATCH_SIZE:-64}"

DATABASES=("rocksdb" "leveldb" "lmdb" "wiredtiger" "leaves" "sqlite" "redis")
if [ -n "${BENCHMARK_DATABASES:-}" ]; then
    read -r -a DATABASES <<< "$BENCHMARK_DATABASES"
fi

BASE_WORKLOADS=(
    "workload_kv_session"
    "workload_kv_cache"
    "workload_kv_analytics_read"
    "workload_kv_ingest"
    "workload_kv_latest"
    "workload_kv_range10"
    "workload_kv_range100"
    "workload_kv_rmw"
)

SCENARIOS=(
    "baseline"
    "binary_key"
    "batch_insert_1"
    "batch_insert_8"
    "batch_insert_32"
    "batch_insert_64"
    "batch_update_1"
    "batch_update_8"
    "batch_update_32"
    "batch_update_64"
    "acid_aci"
    "acid_txn"
)

if [ -n "${BENCHMARK_SCENARIOS:-}" ]; then
    read -r -a SCENARIOS <<< "$BENCHMARK_SCENARIOS"
elif [ "$MATRIX_MODE" = "durability" ]; then
    SCENARIOS=("baseline")
fi

COMMON_ARGS=()
if [ -n "${EXTRA_PROPERTIES_FILE:-}" ]; then
    COMMON_ARGS+=("-P" "$EXTRA_PROPERTIES_FILE")
fi
if [ -n "${THREADCOUNT:-}" ]; then
    COMMON_ARGS+=("-p" "threadcount=$THREADCOUNT")
fi

mkdir -p "$RESULTS_DIR"

echo "========================================"
echo "YCSB-cpp Multi-Database Benchmark"
echo "Timestamp: $TIMESTAMP"
echo "Mode: $MATRIX_MODE"
echo "Scenarios: ${SCENARIOS[*]}"
echo "========================================"

ENTRY_SCENARIO=()
ENTRY_BATCH=()
ENTRY_BINARY=()
ENTRY_WORKLOAD=()
ENTRY_DB=()
ENTRY_LOAD_FILE=()
ENTRY_RUN_FILE=()

supports_binary_key() {
    local db=$1
    case "$db" in
        leaves|lmdb|leveldb|rocksdb)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

supports_batch_size() {
    supports_binary_key "$1"
}

scenario_binary_key() {
    local scenario=$1
    if [ "$scenario" = "binary_key" ]; then
        echo "true"
    else
        echo "false"
    fi
}

scenario_batch_size() {
    local scenario=$1
    case "$scenario" in
        batch_insert_*|batch_update_*)
            echo "${scenario##*_}"
            ;;
        *)
            echo "1"
            ;;
    esac
}

scenario_workloads() {
    local scenario=$1

    if [ -n "${BENCHMARK_WORKLOADS:-}" ]; then
        echo "$BENCHMARK_WORKLOADS"
        return
    fi

    case "$scenario" in
        baseline|binary_key)
            echo "${BASE_WORKLOADS[*]}"
            ;;
        batch_insert_*)
            echo "workload_kv_batch_insert"
            ;;
        batch_update_*)
            echo "workload_kv_batch_update"
            ;;
        acid_aci)
            echo "workload_kv_acid_aci"
            ;;
        acid_txn)
            echo "workload_kv_acid_txn"
            ;;
        *)
            echo "${BASE_WORKLOADS[*]}"
            ;;
    esac
}

supports_scenario() {
    local db=$1
    local scenario=$2

    case "$scenario" in
        acid_txn)
            [ "$db" = "wiredtiger" ] || [ "$db" = "lmdb" ] || [ "$db" = "leaves" ]
            return
            ;;
        *)
            return 0
            ;;
    esac
}

db_mode_args() {
    local db=$1
    local scenario=$2

    if [ "$MATRIX_MODE" = "durability" ] || [ "$scenario" = "acid_aci" ] || [ "$scenario" = "acid_txn" ]; then
        case "$db" in
            leaves)
                echo "-p leaves.sync=true"
                ;;
            leveldb)
                echo "-p leveldb.sync=true"
                ;;
            rocksdb)
                echo "-p rocksdb.sync=true"
                ;;
            lmdb)
                echo "-p lmdb.nosync=false -p lmdb.nometasync=false -p lmdb.mapasync=false"
                ;;
            wiredtiger)
                echo "-p wiredtiger.log.enabled=true -p wiredtiger.transaction_sync.enabled=true -p wiredtiger.transaction_sync.method=fsync"
                ;;
            *)
                echo ""
                ;;
        esac
    else
        echo ""
    fi
}

scenario_db_args() {
    local db=$1
    local scenario=$2
    local phase=${3:-run}
    local binary_key
    local batch_size
    binary_key=$(scenario_binary_key "$scenario")
    batch_size=$(scenario_batch_size "$scenario")

    if [ "$phase" = "load" ] && supports_batch_size "$db"; then
        batch_size="$LOAD_BATCH_SIZE"
    fi

    if ! supports_scenario "$db" "$scenario"; then
        echo ""
        return 2
    fi

    if [ "$binary_key" = "true" ] && ! supports_binary_key "$db"; then
        echo ""
        return 2
    fi

    if [[ "$scenario" == batch_* ]] && ! supports_batch_size "$db"; then
        echo ""
        return 2
    fi

    case "$db" in
        leaves|leveldb|rocksdb|lmdb)
            echo "-p ${db}.binary_key=${binary_key} -p ${db}.batch_size=${batch_size}"
            ;;
        redis)
            if [ "$phase" = "load" ]; then
                echo "-p redis.destroy=true"
            else
                echo "-p redis.destroy=false"
            fi
            ;;
        *)
            echo ""
            ;;
    esac

    return 0
}

clean_db_path() {
    local db=$1
    case "$db" in
        rocksdb)
            rm -rf /tmp/ycsb-rocksdb
            ;;
        leveldb)
            rm -rf /tmp/ycsb-leveldb
            ;;
        lmdb)
            rm -rf /tmp/ycsb-lmdb
            ;;
        wiredtiger)
            rm -rf /tmp/ycsb-wiredtiger
            ;;
        leaves)
            rm -rf /tmp/ycsb-leaves
            ;;
        sqlite)
            rm -f /tmp/ycsb-sqlite.db /tmp/ycsb-sqlite.db-wal /tmp/ycsb-sqlite.db-shm
            ;;
        *)
            ;;
    esac
}

append_db_size() {
    local db=$1
    local output_file=$2
    local db_path=""

    case "$db" in
        null)
            echo "Database size: N/A (null database)" | tee -a "$output_file"
            return
            ;;
        rocksdb)
            db_path="/tmp/ycsb-rocksdb"
            ;;
        leveldb)
            db_path="/tmp/ycsb-leveldb"
            ;;
        lmdb)
            db_path="/tmp/ycsb-lmdb"
            ;;
        wiredtiger)
            db_path="/tmp/ycsb-wiredtiger"
            ;;
        leaves)
            db_path="/tmp/ycsb-leaves"
            ;;
        sqlite)
            db_path="/tmp/ycsb-sqlite.db"
            ;;
    esac

    if [ -n "$db_path" ] && [ -e "$db_path" ]; then
        local db_size
        db_size=$(du -sh "$db_path" 2>/dev/null | cut -f1)
        echo "Database size: $db_size" | tee -a "$output_file"
    fi
}

LAST_OUTPUT_FILE=""

run_benchmark() {
    local db=$1
    local scenario=$2
    local workload=$3
    local phase=$4

    local output_file="$RESULTS_DIR/${db}_${scenario}_${workload}_${phase}_${TIMESTAMP}.log"
    local properties_file="${db}/${db}.properties"
    local cmd=()
    local mode_args=()
    local scenario_args=()
    local scenario_arg_text
    local scenario_status

    if [ "$phase" = "load" ]; then
        clean_db_path "$db"
    fi

    read -r -a mode_args <<< "$(db_mode_args "$db" "$scenario")"

    scenario_arg_text=$(scenario_db_args "$db" "$scenario" "$phase")
    scenario_status=$?
    if [ "$scenario_status" -ne 0 ]; then
        echo "Skipping unsupported scenario '${scenario}' for db '${db}'"
        LAST_OUTPUT_FILE=""
        return 2
    fi
    read -r -a scenario_args <<< "$scenario_arg_text"

    echo "Running $phase phase for $db with $workload (scenario=$scenario)..."

    cmd=("$YCSB_BIN" "-$phase" "-db" "$db" "-P" "$properties_file" "-P" "$WORKLOADS_DIR/$workload")
    if [ ${#COMMON_ARGS[@]} -gt 0 ]; then
        cmd+=("${COMMON_ARGS[@]}")
    fi
    if [ ${#mode_args[@]} -gt 0 ]; then
        cmd+=("${mode_args[@]}")
    fi
    if [ ${#scenario_args[@]} -gt 0 ]; then
        cmd+=("${scenario_args[@]}")
    fi
    cmd+=("-s")

    "${cmd[@]}" | tee "$output_file"
    append_db_size "$db" "$output_file"

    echo "Completed $phase phase for $db with $workload (scenario=$scenario)"
    echo "Results saved to: $output_file"
    echo "----------------------------------------"

    LAST_OUTPUT_FILE="$output_file"
    return 0
}

extract_phase_throughput() {
    local file=$1
    local phase_label=$2

    if [ ! -f "$file" ]; then
        echo ""
        return
    fi

    sed -n "s/^${phase_label} throughput(ops\\/sec):[[:space:]]*//p" "$file" | tail -n1
}

summarize_results() {
    local summary_file="$RESULTS_DIR/benchmark_summary_${TIMESTAMP}.txt"
    local i

    echo "=======================================" > "$summary_file"
    echo "YCSB-cpp Benchmark Summary" >> "$summary_file"
    echo "Timestamp: $TIMESTAMP" >> "$summary_file"
    echo "Mode: $MATRIX_MODE" >> "$summary_file"
    echo "=======================================" >> "$summary_file"
    echo "" >> "$summary_file"

    for ((i=0; i<${#ENTRY_DB[@]}; i++)); do
        echo "=== ${ENTRY_DB[$i]} | ${ENTRY_SCENARIO[$i]} | ${ENTRY_WORKLOAD[$i]} ===" >> "$summary_file"
        grep -E "Load runtime|Load throughput|0 sec: .*\[INSERT:" "${ENTRY_LOAD_FILE[$i]}" >> "$summary_file" 2>/dev/null || true
        grep -E "Run runtime|Run throughput|0 sec: .*operations;" "${ENTRY_RUN_FILE[$i]}" >> "$summary_file" 2>/dev/null || true
        grep "Database size:" "${ENTRY_RUN_FILE[$i]}" >> "$summary_file" 2>/dev/null || true
        echo "" >> "$summary_file"
    done

    echo "Summary saved to: $summary_file"
}

generate_matrix_csv() {
    local throughput_csv="$RESULTS_DIR/throughput_matrix_${TIMESTAMP}.csv"
    local durability_csv="$RESULTS_DIR/durability_session_matrix_${TIMESTAMP}.csv"
    local i

    echo "scenario,batch_size,binary_key,workload,database,load_throughput_ops_sec,run_throughput_ops_sec" > "$throughput_csv"
    if [ "$MATRIX_MODE" = "durability" ]; then
        echo "scenario,batch_size,binary_key,workload,database,load_throughput_ops_sec,run_throughput_ops_sec" > "$durability_csv"
    fi

    for ((i=0; i<${#ENTRY_DB[@]}; i++)); do
        local load_tp
        local run_tp
        load_tp=$(extract_phase_throughput "${ENTRY_LOAD_FILE[$i]}" "Load")
        run_tp=$(extract_phase_throughput "${ENTRY_RUN_FILE[$i]}" "Run")

        if [ -z "$load_tp" ] || [ -z "$run_tp" ]; then
            continue
        fi

        echo "${ENTRY_SCENARIO[$i]},${ENTRY_BATCH[$i]},${ENTRY_BINARY[$i]},${ENTRY_WORKLOAD[$i]},${ENTRY_DB[$i]},${load_tp},${run_tp}" >> "$throughput_csv"

        if [ "$MATRIX_MODE" = "durability" ] && [ "${ENTRY_SCENARIO[$i]}" = "baseline" ] && [ "${ENTRY_WORKLOAD[$i]}" = "workload_kv_session" ]; then
            echo "${ENTRY_SCENARIO[$i]},${ENTRY_BATCH[$i]},${ENTRY_BINARY[$i]},${ENTRY_WORKLOAD[$i]},${ENTRY_DB[$i]},${load_tp},${run_tp}" >> "$durability_csv"
        fi
    done

    echo "Matrix CSV saved to: $throughput_csv"
    if [ "$MATRIX_MODE" = "durability" ]; then
        echo "Durability session CSV saved to: $durability_csv"
    fi
}

echo "Starting comprehensive benchmark across ${#DATABASES[@]} databases and ${#SCENARIOS[@]} scenarios..."
echo ""

start_time=$(date +%s)

for db in "${DATABASES[@]}"; do
    echo "========================================"
    echo "Testing Database: $db"
    echo "========================================"

    for scenario in "${SCENARIOS[@]}"; do
        local_workloads=()
        read -r -a local_workloads <<< "$(scenario_workloads "$scenario")"

        for workload in "${local_workloads[@]}"; do
            if [ ! -f "$WORKLOADS_DIR/$workload" ]; then
                echo "Skipping missing workload file: $WORKLOADS_DIR/$workload"
                continue
            fi

            if ! run_benchmark "$db" "$scenario" "$workload" "load"; then
                continue
            fi
            load_file="$LAST_OUTPUT_FILE"

            if ! run_benchmark "$db" "$scenario" "$workload" "run"; then
                continue
            fi
            run_file="$LAST_OUTPUT_FILE"

            ENTRY_SCENARIO+=("$scenario")
            ENTRY_BATCH+=("$(scenario_batch_size "$scenario")")
            ENTRY_BINARY+=("$(scenario_binary_key "$scenario")")
            ENTRY_WORKLOAD+=("$workload")
            ENTRY_DB+=("$db")
            ENTRY_LOAD_FILE+=("$load_file")
            ENTRY_RUN_FILE+=("$run_file")

            echo ""
        done
    done

    echo "Completed all scenarios for $db"
    echo ""
done

end_time=$(date +%s)
total_time=$((end_time - start_time))

echo "========================================"
echo "Benchmark Complete!"
echo "Total execution time: ${total_time} seconds"
echo "Results directory: $RESULTS_DIR"
echo "========================================"

summarize_results
generate_matrix_csv

echo ""
echo "To view results:"
echo "ls -la $RESULTS_DIR"
echo ""
echo "To view summary:"
echo "cat $RESULTS_DIR/benchmark_summary_${TIMESTAMP}.txt"