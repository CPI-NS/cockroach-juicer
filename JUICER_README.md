# Juicer Benchmark Guide

This guide explains how to build CockroachDB, start a local cluster, and use the benchmark tool for testing.

## Table of Contents

- [Juicer Benchmark Guide](#juicer-benchmark-guide)
  - [Table of Contents](#table-of-contents)
  - [Prerequisites](#prerequisites)
  - [Building CockroachDB](#building-cockroachdb)
  - [Starting a Local CockroachDB Cluster](#starting-a-local-cockroachdb-cluster)
    - [Single-Node Cluster (Insecure Mode)](#single-node-cluster-insecure-mode)
    - [Multi-Node Cluster (Insecure Mode)](#multi-node-cluster-insecure-mode)
    - [Verify Cluster Status](#verify-cluster-status)
  - [Using the Benchmark Tool](#using-the-benchmark-tool)
    - [Building the Benchmark Tool](#building-the-benchmark-tool)
    - [Initializing Test Data](#initializing-test-data)
    - [Running RMW Operations](#running-rmw-operations)
    - [Benchmark Tool Options](#benchmark-tool-options)
  - [Troubleshooting](#troubleshooting)
    - [Cluster Name Mismatch](#cluster-name-mismatch)
    - [Connection Timeout](#connection-timeout)
  - [Example Workflow](#example-workflow)
  - [Additional Resources](#additional-resources)

## Prerequisites

- [Bazel](https://bazel.build/) installed and configured
- Go toolchain (managed by Bazel)
- A terminal with bash support

Run `./dev doctor` firstly.

## Building CockroachDB

Build the CockroachDB binary using the `dev` tool:

```bash
# Build cockroach
./dev build cockroach

# The binary will be located at the project root:
# ./cockroach
```

## Starting a Local CockroachDB Cluster

### Single-Node Cluster (Insecure Mode)

Start a single-node CockroachDB cluster in insecure mode:

```bash
# Create a data directory
mkdir -p cockroach-data

# Start the cluster using the built binary
./cockroach start \
  --insecure \
  --store=cockroach-data \
  --listen-addr=localhost:26257 \
  --http-addr=localhost:8080 \
  --join=localhost:26257
```

**Note**: For the first node, you can omit `--join` or use `--join=localhost:26257`.

### Multi-Node Cluster (Insecure Mode)

To start a multi-node cluster, start each node with different ports:

**Node 1:**
```bash
./cockroach start \
  --insecure \
  --store=cockroach-data/node1 \
  --listen-addr=localhost:26257 \
  --http-addr=localhost:8080 \
  --join=localhost:26257,localhost:26258,localhost:26259
```

**Node 2:**
```bash
./cockroach start \
  --insecure \
  --store=cockroach-data/node2 \
  --listen-addr=localhost:26258 \
  --http-addr=localhost:8081 \
  --join=localhost:26257,localhost:26258,localhost:26259
```

**Node 3:**
```bash
./cockroach start \
  --insecure \
  --store=cockroach-data/node3 \
  --listen-addr=localhost:26259 \
  --http-addr=localhost:8082 \
  --join=localhost:26257,localhost:26258,localhost:26259
```

### Verify Cluster Status

Check the cluster status:

```bash
# Using SQL shell
./cockroach sql --insecure --host=localhost:26257 -e "SHOW CLUSTER SETTING cluster.organization"

# Or access the Admin UI
open http://localhost:8080
```

## Using the Benchmark Tool

The benchmark tool is a standalone KV client that can connect to a CockroachDB cluster and perform Read-Modify-Write (RMW) operations for testing.

### Building the Benchmark Tool

Build the benchmark binary using the `dev` tool:

```bash
# Build benchmark
./dev build benchmark

# The binary will be located at:
# bin/benchmark
```

### Initializing Test Data

Before running benchmarks, you may want to initialize test data:

```bash
# Initialize 10,000 keys with prefix "key" in range 1-10000
./bin/benchmark \
  --addrs=localhost:26257 \
  --insecure=true \
  --init=true \
  --init-keys=10000 \
  --init-prefix=key \
  --init-range=10000 \
  --init-batch=100 \
  --init-concurrent=4

# Options:
#   --init-keys: Number of keys to initialize (default: 10000)
#   --init-prefix: Key prefix (default: "key")
#   --init-range: Key range for generation (default: 10000)
#   --init-batch: Batch size for writes (default: 100)
#   --init-concurrent: Number of concurrent writers (default: 1)
#   --init-bulk: Use BulkAdder for high-performance initialization
```

**High-Performance Initialization (BulkAdder):**

For faster initialization of large datasets:

```bash
./bin/benchmark \
  --addrs=localhost:26257 \
  --insecure=true \
  --init=true \
  --init-keys=100000 \
  --init-prefix=key \
  --init-range=10000 \
  --init-bulk=true
```

### Running RMW Operations

Run Read-Modify-Write operations:

```bash
# Basic RMW operation on a single key
./bin/benchmark \
  --addrs=localhost:26257 \
  --insecure=true \
  --key=test-key

# Options:
#   --addrs: Comma-separated list of bootstrap addresses (default: "localhost:26257")
#   --insecure: Use insecure connection (default: true)
#   --cluster: Cluster name (default: "default")
#   --key: Key to use for RMW operation (default: "test-key")
```

### Benchmark Tool Options

Full list of available options:

```bash
./bin/benchmark --help
```

**Connection Options:**
- `--addrs`: Comma-separated list of bootstrap addresses (e.g., `localhost:26257,localhost:26258`)
- `--insecure`: Use insecure connection (default: `true`)
- `--certs`: Directory containing SSL certificates (if not insecure)
- `--cluster`: Cluster name (default: `"default"`)

**Data Initialization Options:**
- `--init`: Initialize test data
- `--init-keys`: Number of keys to initialize (default: `10000`)
- `--init-prefix`: Prefix for initialized keys (default: `"key"`)
- `--init-range`: Key range for initialization (default: `10000`)
- `--init-batch`: Batch size for initialization (default: `100`)
- `--init-concurrent`: Number of concurrent writers (default: `1`)
- `--init-bulk`: Use BulkAdder for high-performance initialization

**RMW Operation Options:**
- `--key`: Key to use for RMW operation (default: `"test-key"`)

## Troubleshooting

### Cluster Name Mismatch

If you see an error like:
```
peer node does not have a cluster name configured, cannot use --cluster-name
```

**Solution**: Either start the CockroachDB server with a cluster name:
```bash
./cockroach start --insecure --cluster-name=default ...
```

Or run the benchmark without specifying a cluster name:
```bash
./bin/benchmark --addrs=localhost:26257 --cluster=""
```

### Connection Timeout

If the benchmark tool cannot connect to the cluster:

1. Verify the cluster is running:
   ```bash
   ./cockroach sql --insecure --host=localhost:26257 -e "SELECT 1"
   ```

2. Check the address and port:
   ```bash
   # Make sure the address matches what you started the cluster with
   ./bin/benchmark --addrs=localhost:26257
   ```

## Example Workflow

Complete example workflow:

```bash
# 1. Build CockroachDB
./dev build cockroach

# 2. Start a single-node cluster
mkdir -p cockroach-data
./cockroach start \
  --insecure \
  --store=cockroach-data \
  --listen-addr=localhost:26257 \
  --http-addr=localhost:8080

# 3. Build the benchmark tool
./dev build benchmark

# 4. Initialize test data
./bin/benchmark \
  --addrs=localhost:26257 \
  --insecure=true \
  --init=true \
  --init-keys=10000 \
  --init-prefix=key \
  --init-range=10000 \
  --init-concurrent=4

# 5. Run RMW operations
./bin/benchmark \
  --addrs=localhost:26257 \
  --insecure=true \
  --key=test-key

# 6. Access Admin UI (optional)
open http://localhost:8080
```

## Additional Resources

- [CockroachDB Documentation](https://www.cockroachlabs.com/docs/stable/)
- [CockroachDB Architecture](https://www.cockroachlabs.com/docs/stable/architecture/overview.html)

