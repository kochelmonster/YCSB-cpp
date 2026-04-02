# YCSB-cpp

Yahoo! Cloud Serving Benchmark ([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with the following additions:

 * Tail latency reporting using [HdrHistogram_c](https://github.com/HdrHistogram/HdrHistogram_c)
 * Modified workloads to be more similar to the original YCSB
 * Supported databases: LevelDB, RocksDB, LMDB, WiredTiger, SQLite, Redis, Leaves

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
./ycsb -load -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

Run workload A with leveldb:
```
./ycsb -run -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

Load and run workload B with rocksdb:
```
./ycsb -load -run -db rocksdb -P workloads/workloadb -P rocksdb/rocksdb.properties -s
```

Pass additional properties:
```
./ycsb -load -db leveldb -P workloads/workloadb -P rocksdb/rocksdb.properties \
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
