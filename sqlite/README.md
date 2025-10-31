# SQLite Binding for YCSB-cpp

This directory contains the SQLite binding for YCSB-cpp.

## Building

To build with SQLite support using CMake:

```bash
mkdir build
cd build
cmake .. -DBIND_SQLITE=ON
make
```

To build with SQLite support using Make:

```bash
make BIND_SQLITE=1
```

## Configuration

Configuration options are set in `sqlite.properties`:

### Database Path
- `sqlite.dbpath`: Path to SQLite database file (default: `/tmp/ycsb-sqlite.db`)

### Performance Tuning

- `sqlite.cache_size`: Cache size in KB (negative) or pages (positive)
  - Default: `-524288` (512 MB)
  - Larger cache improves performance for in-memory operations

- `sqlite.page_size`: Database page size in bytes
  - Default: `8192` (8 KB)
  - Larger pages can improve sequential access performance

- `sqlite.journal_mode`: Journal mode for transactions
  - Default: `WAL` (Write-Ahead Logging)
  - Options: `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, `WAL`, `OFF`
  - WAL mode provides better concurrency

- `sqlite.synchronous`: Synchronization level
  - Default: `OFF` (for maximum benchmark speed)
  - Options: `OFF` < `NORMAL` < `FULL`
  - **Warning**: `OFF` sacrifices durability for performance

- `sqlite.create_table`: Whether to create table automatically
  - Default: `true`

## Running Benchmarks

```bash
# Load data
./build/ycsb -load -db sqlite -P sqlite/sqlite.properties -P workloads/workloada

# Run workload
./build/ycsb -run -db sqlite -P sqlite/sqlite.properties -P workloads/workloada
```

## Performance Characteristics

SQLite is optimized for:
- Embedded applications
- Single-writer scenarios
- Serverless architecture

Expected performance (optimized settings):
- **Load throughput**: ~50-100K ops/sec
- **Run throughput**: ~100-200K ops/sec (read-heavy)
- **Run throughput**: ~50-100K ops/sec (write-heavy)

## Notes

- SQLite uses a single file for the entire database plus WAL files
- WAL mode creates additional `-wal` and `-shm` files
- Performance is excellent for reads but limited for concurrent writes
- Settings are optimized for benchmarking (trades durability for speed)
- For production use, consider using `synchronous=NORMAL` or `FULL`

## Cleanup

To remove the database between runs:

```bash
rm -f /tmp/ycsb-sqlite.db /tmp/ycsb-sqlite.db-wal /tmp/ycsb-sqlite.db-shm
```
