# YCSB-cpp

Yahoo! Cloud Serving Benchmark ([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with the following additions:

 * Tail latency reporting using [HdrHistogram_c](https://github.com/HdrHistogram/HdrHistogram_c)
 * Modified workloads to be more similar to the original YCSB
 * Supported databases: LevelDB, RocksDB, LMDB, WiredTiger, SQLite, Redis, Leaves

## Differences from original Upstream master

while executing benchmarks I noticed that the Benchmark Frame work has significant flaws.

1. A big part of the benchmark time was spent in the benchmark code itself.
2. Transaction support was hardcoded and only provided on scenario.
3. Different benchmark runs used different datasets, which made it hard to compare results across runs.

Major refactors were applied to the benchmark framework to address these issues, including:

### Core runtime and execution model

* Program entry moved from top-level `ycsbc.cc` to `core/ycsbc.cc`
* The workload/client path was significantly expanded in `core/core_workload.{h,cc}` and `core/client.h`, including transaction-oriented operations (`BEGIN_TXN`, `COMMIT_TXN`, `ROLLBACK_TXN`) and explicit failure operation counters
* Pre-generated and replayed operation datasets were added via `core/dataset.{h,cc}` and `CoreWorkload::PrepareOpsForFile(...)` so benchmark hot loops can focus on DB calls
* Core infrastructure additions include `core/db_wrapper.h`, `core/acknowledged_counter_generator.{h,cc}`, `core/random_byte_generator.h`, and updated measurement/status reporting in `core/measurements.{h,cc}`

### Database bindings and adapter layout

* Legacy shared adapter code under `db/` was removed (for example `db/db_factory.*`, `db/redis_db.*`)
* Binding implementations are now organized under `adapters/` with per-engine directories (`adapters/rocksdb/`, `adapters/leveldb/`, `adapters/lmdb/`, `adapters/wiredtiger/`, `adapters/sqlite/`, `adapters/redis/`, `adapters/leaves/`, plus additional integration directories such as `adapters/aerospike/`, `adapters/badger/`, `adapters/dragonfly/`, and `adapters/null/`)
* Redis binding was reworked (`adapters/redis/redis_db.{h,cc}`), and the vendored `redis/hiredis/` subtree was removed (38 files)
* RocksDB support was expanded with a full adapter/options path (`adapters/rocksdb/rocksdb_db.{h,cc}`, `adapters/rocksdb/options.ini`)
* SQLite and WiredTiger bindings include dedicated adapter and configuration files (`adapters/sqlite/sqlite_db.{h,cc}`, `adapters/wiredtiger/wiredtiger_db.{h,cc}`)

### Workloads and benchmark automation

* A larger application-oriented workload suite was added under `workloads/workload_kv_*` (session, cache, ingest, analytics read, range, concurrent, ACID, batch variants)
* Legacy workload names were normalized by renaming `workloads/workload[a-f].spec` to `workloads/workload[a-f]`
* End-to-end benchmark orchestration and analysis scripts were added:
    * `run_all_benchmarks.sh`
    * `run_deterministic_benchmark.sh`
    * `run_leaves_benchmark.sh`
    * `create_throughput_matrix.py`
    * `create_benchmark_graphs.py`
    * `merge_benchmark_csvs.py`

### Docs, utilities, and tests

* `PROPERTIES.md` was added as a consolidated property reference
* Utility headers were refactored from `core/` into `utils/` (`properties.h`, `timer.h`, `utils.h`) and new helpers were added (`utils/fields.h`, `utils/rate_limit.h`, `utils/countdown_latch.h`, `utils/sha256.{h,cpp}`)
* Test coverage was extended with `tests/test_fields.cc`

For an exhaustive machine-generated file list for this exact baseline, run:

```bash
git diff --name-status --find-renames origin/master...HEAD
```

# Build YCSB-cpp

## Build with Makefile on POSIX

Initialize the submodule and use `make` to build.

```
git clone https://github.com/ls4154/YCSB-cpp.git
cd YCSB-cpp
git submodule update --init
make
```

Databases to bind must be specified as build options. You may also need to add additional link flags (e.g., `-lsnappy`).

To bind LevelDB:
```
make BIND_LEVELDB=1
```

To build with additional libraries and include directories:
```
make BIND_LEVELDB=1 EXTRA_CXXFLAGS=-I/example/leveldb/include \
                    EXTRA_LDFLAGS="-L/example/leveldb/build -lsnappy"
```

Or modify config section in `Makefile`.

RocksDB build example:
```
EXTRA_CXXFLAGS ?= -I/example/rocksdb/include
EXTRA_LDFLAGS ?= -L/example/rocksdb -ldl -lz -lsnappy -lzstd -lbz2 -llz4

BIND_ROCKSDB ?= 1
```

## Build with CMake on POSIX

```shell
git submodule update --init
mkdir build
cd build
cmake -DBIND_ROCKSDB=1 -DBIND_WIREDTIGER=1 -DBIND_LMDB=1 -DBIND_LEVELDB=1 -DBIND_SQLITE=1 -DBIND_REDIS=1 -DBIND_LEAVES=1 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZSTD=1 ..
make
```

## Build with CMake+vcpkg on Windows

See [BUILD_ON_WINDOWS](BUILD_ON_WINDOWS.md).

## Running

Load data with leveldb:
```
./ycsb -load -db leveldb -P workloads/workloada -P adapters/leveldb/leveldb.properties -s
```

Run workload A with leveldb:
```
./ycsb -run -db leveldb -P workloads/workloada -P adapters/leveldb/leveldb.properties -s
```

Load and run workload B with rocksdb:
```
./ycsb -load -run -db rocksdb -P workloads/workloadb -P adapters/rocksdb/rocksdb.properties -s
```

Pass additional properties:
```
./ycsb -load -db leveldb -P workloads/workloadb -P adapters/rocksdb/rocksdb.properties \
    -p threadcount=4 -p recordcount=10000000 -p leveldb.cache_size=134217728 -s
```

## Application Workload Suite

The repository includes application-oriented workload files in `workloads/` so the comparison is framed around real use cases instead of only the original A/B/C labels.

| Workload | Application area | Request mix |
|----------|------------------|-------------|
| `workload_kv_session` | Session store / mutable user state | 50% read, 50% update |
| `workload_kv_cache` | Cache / metadata / lookup service | 95% read, 5% update |
| `workload_kv_analytics_read` | Read-only lookup / feature serving | 100% read |
| `workload_kv_ingest` | Event or log ingestion | 10% read, 20% update, 70% insert |
| `workload_kv_latest` | Recency-biased feed / timeline | 95% read, 5% insert |
| `workload_kv_range10` | Ordered range scan, short page | 50% read, 50% scan, fixed length 10 |
| `workload_kv_range100` | Ordered range scan, larger window | 50% read, 50% scan, fixed length 100 |
| `workload_kv_rmw` | Read-modify-write records | 50% read, 50% read-modify-write |

## Benchmark Runner

Run the multi-database matrix from the repository root:

```shell
bash ./run_all_benchmarks.sh
```

Use a smaller scale for a first comparison pass:

```shell
EXTRA_PROPERTIES_FILE=./workloads/medium_workload.properties bash ./run_all_benchmarks.sh
```

Run a durability-oriented matrix with stricter sync settings where supported:

```shell
MATRIX_MODE=durability EXTRA_PROPERTIES_FILE=./workloads/medium_workload.properties bash ./run_all_benchmarks.sh
```

Restrict the run to a subset of databases or workloads:

```shell
BENCHMARK_DATABASES="leaves rocksdb lmdb" \
BENCHMARK_WORKLOADS="workload_kv_session workload_kv_ingest" \
EXTRA_PROPERTIES_FILE=./workloads/medium_workload.properties \
bash ./run_all_benchmarks.sh
```

Include Redis as a practical comparator if a local Redis server is available:

```shell
INCLUDE_REDIS=1 EXTRA_PROPERTIES_FILE=./workloads/medium_workload.properties bash ./run_all_benchmarks.sh
```

## Configuration

For detailed information about all available configuration properties, see [PROPERTIES.md](PROPERTIES.md).
