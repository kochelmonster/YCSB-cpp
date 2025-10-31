# Redis Binding for YCSB-cpp

This directory contains the Redis binding for YCSB-cpp.

## Prerequisites

- Redis server installed and running
- hiredis library (Redis C client)

### Installing Redis and hiredis

**Ubuntu/Debian:**
```bash
sudo apt-get install redis-server libhiredis-dev
```

**macOS:**
```bash
brew install redis hiredis
```

## Building

To build with Redis support using CMake:

```bash
mkdir build
cd build
cmake .. -DBIND_REDIS=ON
make
```

To build with Redis support using Make:

```bash
make BIND_REDIS=1
```

## Configuration

Configuration options are set in `redis.properties`:

- `redis.host`: Redis server hostname (default: `127.0.0.1`)
- `redis.port`: Redis server port (default: `6379`)
- `redis.timeout`: Connection timeout in milliseconds (default: `1000`)

## Running Benchmarks

1. Start Redis server:
```bash
redis-server
```

2. Load data:
```bash
./build/ycsb -load -db redis -P redis/redis.properties -P workloads/workloada
```

3. Run workload:
```bash
./build/ycsb -run -db redis -P redis/redis.properties -P workloads/workloada
```

## Data Model

Redis is a key-value store. This binding stores YCSB records as Redis hashes:

- **Key format**: `{table}:{key}` (e.g., `usertable:user1234`)
- **Fields**: Stored as hash fields using `HMSET`/`HGETALL`/`HMGET`

Example Redis commands:
```
HMSET usertable:user1234 field0 value0 field1 value1 ...
HGETALL usertable:user1234
HMGET usertable:user1234 field0 field1
DEL usertable:user1234
```

## Performance Characteristics

Redis is optimized for:
- In-memory storage with optional persistence
- Very low latency operations
- High throughput for simple operations
- Single-threaded architecture (use pipelining for max performance)

Expected performance (local Redis):
- **Load throughput**: ~100-200K ops/sec
- **Run throughput**: ~150-300K ops/sec (simple operations)
- **Latency**: Sub-millisecond for most operations

## Notes

- Redis stores all data in memory - ensure sufficient RAM
- **Scan operations use Redis SCAN command with pattern matching**
  - Redis SCAN is **not optimized for range queries** 
  - Expected scan performance: ~50-100 ops/sec (vs ~100K ops/sec for point reads)
  - This is a Redis limitation, not a binding issue
  - For workloads requiring efficient range scans, consider databases like Leaves or LMDB
- For production, consider Redis persistence options (RDB/AOF)
- For clustering, consider Redis Cluster (requires additional setup)

## Cleanup

To clear the database between runs:
```bash
redis-cli FLUSHDB
```

Or to clear all databases:
```bash
redis-cli FLUSHALL
```
