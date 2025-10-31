# Aerospike Database Binding for YCSB-cpp

## Overview

Aerospike is a high-performance NoSQL database known for its hybrid memory architecture and sub-millisecond latency. It combines in-memory performance with persistent storage by directly accessing SSDs, bypassing the filesystem for low-latency operations.

## Key Features

- **Hybrid Storage**: Combines RAM for indexes and SSDs for data storage
- **Sub-millisecond Latency**: Claims to be one of the fastest NoSQL databases
- **Direct SSD Access**: Bypasses filesystem overhead for better performance
- **ACID Transactions**: Supports strong consistency guarantees
- **Horizontal Scalability**: Designed for distributed deployments

## Architecture

Aerospike uses a unique data model:
- **Namespace**: Similar to a database (e.g., "test")
- **Set**: Similar to a table (e.g., "usertable")
- **Key**: Unique identifier for a record
- **Bins**: Individual fields within a record (like columns)

## Installation

### 1. Install Aerospike Server

#### Using Docker (Recommended)
```bash
docker run -d --name aerospike -p 3000:3000 aerospike/aerospike-server
```

#### Using Native Package
Download and install from https://aerospike.com/download/

For Ubuntu/Debian:
```bash
wget -O aerospike.tgz https://download.aerospike.com/artifacts/aerospike-server-community/7.0.0.7/aerospike-server-community_7.0.0.7_tools-10.2.0_ubuntu22.04_x86_64.tgz
tar -xvf aerospike.tgz
cd aerospike-server-community_*
sudo ./asinstall
sudo systemctl start aerospike
```

### 2. Install Aerospike C Client

The client library should already be installed if you built YCSB-cpp with BIND_AEROSPIKE=ON. If you need to install it manually:

```bash
# Install dependencies
sudo apt-get install -y libc6-dev libssl-dev autoconf automake libtool g++ zlib1g-dev libyaml-dev

# Clone and build
git clone --depth 1 https://github.com/aerospike/aerospike-client-c.git
cd aerospike-client-c
git submodule update --init
make
sudo make install
```

### 3. Build YCSB-cpp with Aerospike

```bash
cd build
cmake .. -DBIND_AEROSPIKE=ON [other database options]
make
```

## Configuration

Edit `aerospike/aerospike.properties`:

```properties
# Server connection
aerospike.host=127.0.0.1
aerospike.port=3000

# Database structure
aerospike.namespace=test
aerospike.set=usertable

# Async mode (requires libuv)
aerospike.async=false
aerospike.max_concurrent=100
```

### Async Mode

The binding supports both synchronous and asynchronous operations:

**Synchronous Mode** (default):
- Simple blocking operations
- Lower overhead per operation
- Best for single-threaded benchmarks

**Asynchronous Mode** (`aerospike.async=true`):
- Uses libuv event loops
- Allows concurrent operations
- Lower CPU usage per operation
- Requires libuv1-dev to be installed
- Requires Aerospike C client built with `EVENT_LIB=libuv`

To enable async mode:

1. Install libuv:
```bash
sudo apt-get install libuv1-dev
```

2. Rebuild Aerospike C client with libuv support:
```bash
cd /path/to/aerospike-client-c
make clean
make EVENT_LIB=libuv -j$(nproc)
sudo make install
```

3. Rebuild YCSB-cpp (CMakeLists.txt already includes libuv linkage)

4. Set `aerospike.async=true` in aerospike.properties

**Note**: Due to YCSB-cpp's synchronous API, the async mode waits for each operation to complete before proceeding to the next. This means async mode has similar throughput to sync mode for typical workloads but may have slightly different CPU usage characteristics.

## Usage

### Load Data
```bash
./ycsb -load -db aerospike \
  -P aerospike/aerospike.properties \
  -P workloads/workloada
```

### Run Benchmark
```bash
./ycsb -run -db aerospike \
  -P aerospike/aerospike.properties \
  -P workloads/workloada
```

### Run All Benchmarks
```bash
./run_all_benchmarks.sh
```

## Performance Characteristics

### Strengths
- **Very Fast Reads**: Sub-millisecond latency for in-memory operations
- **Fast Writes**: Direct SSD writes with minimal overhead
- **Hybrid Workloads**: Excellent for mixed read/write patterns
- **Scalability**: Designed for distributed, high-throughput workloads

### Considerations
- **Memory Requirements**: Indexes stored in RAM require significant memory
- **Scan Operations**: Sequential scans are not as efficient as indexed lookups
- **Complexity**: More complex setup compared to embedded databases

## Comparison with Other YCSB-cpp Databases

| Database | Storage Type | Latency | Best Use Case |
|----------|-------------|---------|---------------|
| Aerospike | Hybrid (RAM+SSD) | Sub-ms | High-throughput, low-latency |
| Redis | In-memory | Sub-ms | Pure caching, fast access |
| RocksDB | File-based | Low-ms | Embedded, write-heavy |
| LMDB | Memory-mapped | Low-ms | Embedded, read-heavy |
| LevelDB | File-based | Low-ms | Embedded, general purpose |

## Data Management

### Truncation
The binding automatically truncates the set on initialization to ensure clean benchmark runs. This is equivalent to:
```bash
asinfo -v "truncate:namespace=test;set=usertable"
```

### Monitoring
Check server status:
```bash
asinfo -v status
asinfo -v "namespace/test"
```

### Cleanup
To completely remove data:
```bash
# Using Docker
docker rm -f aerospike

# Using native install
sudo systemctl stop aerospike
sudo rm -rf /opt/aerospike/data/*
```

## Troubleshooting

### Connection Failed
- Ensure Aerospike server is running: `docker ps` or `sudo systemctl status aerospike`
- Check port 3000 is accessible: `telnet 127.0.0.1 3000`
- Verify namespace exists: `asinfo -v namespaces`

### Truncate Warning
The warning "truncate warning: ... (this is normal on first run)" is expected when the set doesn't exist yet and can be ignored.

### Performance Issues
- Check memory usage: Aerospike needs sufficient RAM for indexes
- Verify SSD configuration: Check `storage-engine` in aerospike.conf
- Monitor metrics: `asinfo -v statistics`

## References

- [Aerospike Official Website](https://aerospike.com/)
- [Aerospike C Client Documentation](https://www.aerospike.com/docs/client/c/)
- [Aerospike Architecture](https://aerospike.com/docs/architecture/)
- [Performance Tuning Guide](https://aerospike.com/docs/operations/configure/)
