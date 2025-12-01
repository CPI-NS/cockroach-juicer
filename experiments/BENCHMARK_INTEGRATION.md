# New KV Benchmark Integration

The experimental framework has been updated to work with the new KV-based benchmark binary (`pkg/cmd/benchmark`).

## What Changed

### Previous Benchmark (SQL-based)
- Used `--url` flag with PostgreSQL connection strings
- Required SQL-based setup
- Limited control over KV operations

### New Benchmark (KV-based)
- Uses `--addrs` flag with comma-separated bootstrap addresses
- Direct KV client connection using `kv.DB`
- Uses `db.NewJuicerTxn()` for Juicer transactions
- Built-in data initialization with 3 methods
- Better performance and control

## Key Changes in Framework

### 1. Configuration (`config_parser.py`)

Added `DataInitConfig` class:

```python
@dataclass
class DataInitConfig:
    enabled: bool = True
    num_keys: int = 10000
    key_prefix: str = "key"
    key_range: int = 10000
    batch_size: int = 100
    concurrent: int = 1
    use_bulk: bool = False
```

### 2. Benchmark Runner (`benchmark_runner.py`)

**Command building changed from:**
```python
cmd = [benchmark_bin, f"--url={db_url}", ...]
```

**To:**
```python
addrs = [f"localhost:{base_port + i}" for i in range(num_nodes)]
cmd = [benchmark_bin, f"--addrs={','.join(addrs)}", "--insecure", ...]
```

**Added data initialization:**
```python
def _initialize_data(self) -> bool:
    """Initialize benchmark data using --init flag"""
    cmd = [
        benchmark_bin,
        f"--addrs={addrs}",
        "--insecure",
        "--init",
        f"--init-keys={num_keys}",
        ...
    ]
```

### 3. YAML Configuration

All example configs now include `data_init` section:

```yaml
data_init:
  enabled: true         # Initialize data before benchmarks
  num_keys: 10000       # Number of keys to initialize
  key_prefix: "key"     # Prefix for keys
  key_range: 10000      # Key range (1 to key_range)
  batch_size: 100       # Batch size for initialization
  concurrent: 1         # Concurrent writers
  use_bulk: false       # Use BulkAdder for fast init
```

## Data Initialization Methods

The new benchmark supports 3 initialization methods:

### Method 1: Simple Batch Writes (Default)
```yaml
data_init:
  concurrent: 1
  use_bulk: false
```
- Uses regular KV batches
- Good for small datasets (<100k keys)
- ~1,000-5,000 keys/sec

### Method 2: BulkAdder (SST Import)
```yaml
data_init:
  use_bulk: true
```
- Uses SST batch import
- Best for large datasets (>100k keys)
- ~50,000-100,000 keys/sec
- Bypasses transaction layer

### Method 3: Concurrent Batch Writes
```yaml
data_init:
  concurrent: 4
  use_bulk: false
```
- Multiple goroutines writing batches
- Good for medium datasets (10k-100k keys)
- ~5,000-20,000 keys/sec
- Works well with multi-node clusters

## Benchmark Execution Flow

```
1. Start CockroachDB cluster (single or multi-node)
   ↓
2. Initialize data (if data_init.enabled == true)
   - Runs: benchmark --addrs=... --init --init-keys=...
   - Waits for completion
   ↓
3. For each experiment configuration:
   a. Reset database (optional)
   b. Run benchmark workload
      - Runs: benchmark --addrs=... --tx-count=... --ops-per-tx=...
   c. Collect metrics
   ↓
4. Stop cluster
5. Export results (CSV/JSON)
```

## Command Line Flags

### Cluster Connection
- `--addrs`: Comma-separated bootstrap addresses (e.g., `localhost:26257,localhost:26258`)
- `--insecure`: Use insecure connection (no SSL)
- `--certs`: SSL certificates directory (if secure)
- `--cluster`: Cluster name (default: "default")

### Data Initialization
- `--init`: Enable data initialization mode
- `--init-keys`: Number of keys to initialize
- `--init-prefix`: Key prefix (default: "key")
- `--init-range`: Key range (1 to init-range)
- `--init-batch`: Batch size for writes
- `--init-concurrent`: Number of concurrent writers
- `--init-bulk`: Use BulkAdder for high-performance init

### Workload Execution
- `--tx-count`: Number of transactions to execute
- `--ops-per-tx`: Operations per transaction
- `--key-range`: Key range for workload
- `--distribution`: Distribution type (uniform/zipfian)
- `--zipfian-s`: Zipfian skew parameter
- `--zipfian-v`: Zipfian velocity parameter
- `--workers`: Total number of worker threads
- `--protocol`: Concurrency control protocol (2PL/2PL-WW)

## Multi-Node Cluster Support

The framework automatically builds the correct `--addrs` string for multi-node clusters:

**Single-node (num_nodes=1):**
```
--addrs=localhost:26257
```

**3-node cluster (num_nodes=3):**
```
--addrs=localhost:26257,localhost:26258,localhost:26259
```

The benchmark client connects to all nodes and uses gossip to discover the cluster topology.

## Example Configuration

```yaml
experiment:
  name: "kv-benchmark-test"
  output_dir: "./results"
  repeat_count: 3

cluster:
  num_nodes: 3
  base_port: 26257

data_init:
  enabled: true
  num_keys: 100000
  key_prefix: "bench"
  key_range: 100000
  batch_size: 500
  concurrent: 4
  use_bulk: false

workload:
  tx_count: [10000]
  ops_per_tx: [10]
  key_range: [100000]
  distribution: ["uniform"]
  read_write_ratio: [0.5]

concurrency:
  num_clients: [2]
  workers_per_client: [20]

juicer:
  enabled: [true, false]
  flush_time_us: [100]

protocol:
  type: ["2PL-WW"]
```

## Migration Guide

If you have old experiment configs, update them:

### 1. Add `data_init` section
```yaml
data_init:
  enabled: true
  num_keys: 10000
  key_prefix: "key"
  key_range: 10000
  batch_size: 100
  concurrent: 1
  use_bulk: false
```

### 2. Remove any manual SQL initialization scripts
The new benchmark handles initialization automatically.

### 3. Update cluster configuration (if needed)
```yaml
cluster:
  num_nodes: 1
  base_port: 26257
  base_http_port: 8080
  data_dir: "./cockroach-data"
  log_dir: "./logs"
  store_size: "10GB"
```

## Implementation Details

### Files Modified

1. **`framework/config_parser.py`**
   - Added `DataInitConfig` dataclass (lines 47-55)
   - Added parsing for `data_init` section (lines 127-136)

2. **`framework/benchmark_runner.py`**
   - Added `_initialize_data()` method (lines 166-214)
   - Updated `_build_benchmark_command()` to use `--addrs` (lines 216-245)
   - Added data initialization before experiments (lines 62-69)

3. **All YAML configs in `configs/`**
   - Added `data_init` section with sensible defaults

### Key Functions

**Data Initialization:**
```python
# cockroach-juicer/experiments/framework/benchmark_runner.py:166
def _initialize_data(self) -> bool
```

**Command Building:**
```python
# cockroach-juicer/experiments/framework/benchmark_runner.py:216
def _build_benchmark_command(self, bench_params: Dict[str, Any]) -> List[str]
```

## Troubleshooting

### Issue: "error creating client: gossip connection timeout"

**Cause:** Benchmark can't connect to cluster

**Fix:**
- Verify cluster is running: `./cockroach node status --insecure --host=localhost:26257`
- Check `--addrs` matches cluster ports
- Ensure firewall allows connections

### Issue: "Data initialization failed"

**Cause:** Various initialization errors

**Fix:**
- Check logs in stderr output
- Reduce `num_keys` or `batch_size`
- Try `use_bulk: true` for large datasets
- Ensure cluster has enough disk space (`store_size`)

### Issue: "Benchmark timeout"

**Cause:** Workload takes longer than `timeout_seconds`

**Fix:**
- Increase `timeout_seconds` in experiment config
- Reduce `tx_count` or `key_range`
- Use more workers for parallelism

## Performance Tips

1. **Large datasets (>100k keys):**
   ```yaml
   data_init:
     use_bulk: true
     batch_size: 1000
   ```

2. **Multi-node clusters:**
   ```yaml
   data_init:
     concurrent: 4  # Match or exceed num_nodes
   ```

3. **Fast iteration during development:**
   ```yaml
   data_init:
     enabled: false  # Skip init if data already loaded
   ```

4. **High-contention workloads:**
   ```yaml
   workload:
     key_range: [100]  # Small key space
     distribution: ["zipfian"]
     zipfian_s: [1.5]  # High skew
   ```

## Next Steps

After updating your configs:

1. Build the new benchmark binary:
   ```bash
   cd cockroach-juicer
   ./dev build benchmark
   ```

2. Run an experiment:
   ```bash
   cd experiments
   python scripts/run_experiment.py configs/baseline_2pl.yaml
   ```

3. Check results:
   ```bash
   ls -l results/baseline/
   ```
