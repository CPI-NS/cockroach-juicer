# CockroachDB Juicer Evaluation Plan

## Overview

This document outlines the evaluation experiments for CockroachDB with Juicer integration, including required graphs, workloads, and implementation tasks.

## Experimental Setup

### Testbed
- **Platform**: AWS EC2 (us-east-1 region, single-region experiments)
- **Servers**: 9 CockroachDB nodes
- **Clients**: 1-9 client machines (as needed to saturate system)
- **Replication**: 3 replicas per shard (CockroachDB Raft default)

### Dataset
- **Key Space**: 10 million keys
- **Distribution**: Hash-based sharding across 9 servers
- **Hot Key Distribution**: Evenly distributed across all 9 servers (no single hot server)

### Workload
- **Transaction Type**: Read-Modify-Write (RMW)
- **Operations per Transaction**: 1 RMW operation
- **Access Pattern**: Zipfian distribution

### Default Parameters
- **Zipf**: 0.99
- **Replicas**: 3 (CockroachDB Raft)
- **Client Count**: "Knee" value from Graph 7 (75-80% of peak throughput)

---

## Required Graphs

### Graph 7: Median Latency vs. Throughput (Line Graph)
**Purpose**: Find system saturation point (the "knee")

**Config**: `configs/eval_graph7_knee.yaml`

**Experiment**:
- **Fixed**: Zipf = 0.99, workload = RMW
- **Varying**: Number of clients [1, 2, 4, 8, 16, 32, 64, 128, 256]
- **X-axis**: Throughput (ops/sec)
- **Y-axis**: Median latency (ms)
- **Lines**: Baseline CockroachDB vs. Juicer

**Knee Definition**: Point where system reaches 75-80% of peak throughput

**Output**: Identify optimal client count for Graphs 8 & 9

---

### Graph 8: Throughput vs. Zipf (Bar Graph)
**Purpose**: Show impact of contention on throughput

**Config**: `configs/eval_graph8_throughput_zipf.yaml`

**Experiment**:
- **Fixed**: Client count = knee value from Graph 7
- **Varying**: Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
- **X-axis**: Zipf value (6 discrete values)
- **Y-axis**: Throughput (ops/sec)
- **Bars**: Baseline CockroachDB vs. Juicer (grouped bars)

**Expected**: Higher Zipf = more contention = lower throughput (Juicer should help)

---

### Graph 9: Abort Rate vs. Zipf (Bar Graph)
**Purpose**: Show impact of contention on transaction abort rate

**Config**: `configs/eval_graph9_abort_zipf.yaml`

**Experiment**:
- **Fixed**: Same as Graph 8
- **Varying**: Same Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
- **X-axis**: Zipf value (6 discrete values)
- **Y-axis**: Abort rate (%)
- **Bars**: Baseline CockroachDB vs. Juicer

**Abort Rate Calculation**:
```
abort_rate = (total_aborts / total_attempts) * 100
```
Example: 1 transaction aborted 7 times, succeeded on 8th attempt → 7/8 = 87.5%

---

## Implementation Checklist

### ✅ Already Supported by Framework
- [x] Multi-node cluster (9 servers)
- [x] Remote deployment (AWS EC2)
- [x] Zipfian distribution
- [x] Configurable client count
- [x] Latency metrics (P50, P99)
- [x] Throughput measurement
- [x] Abort rate tracking

### ⚠️ Needs Implementation

#### 1. **Read-Modify-Write (RMW) Workload**
**Location**: `pkg/cmd/benchmark/workload.go`

**Current**: Transaction has N separate read/write operations
**Required**: Transaction has 1 atomic RMW operation

**Implementation**:
```go
// Read-Modify-Write operation
func executeRMW(ctx context.Context, txn *kv.Txn, key string) error {
    // Read current value
    result, err := txn.Get(ctx, key)
    if err != nil {
        return err
    }

    // Modify value
    var newValue []byte
    if result.Value != nil {
        oldValue, _ := result.Value.GetInt()
        newValue = strconv.AppendInt(nil, oldValue+1, 10)
    } else {
        newValue = []byte("1")
    }

    // Write back
    return txn.Put(ctx, key, newValue)
}
```

**Config Flag**: Add `--workload-type=rmw` flag to benchmark binary

---

#### 2. **Hash-Based Key Distribution**
**Location**: `pkg/cmd/benchmark/workload.go`

**Problem**: Need to ensure hot keys are evenly distributed across 9 servers

**Current**: Sequential keys `key-1`, `key-2`, ..., `key-10000000`
**Required**: Hash-based keys to ensure even distribution

**Implementation**:
```go
import "hash/fnv"

func hashKey(id int) string {
    h := fnv.New64a()
    h.Write([]byte(fmt.Sprintf("%d", id)))
    hashValue := h.Sum64()
    return fmt.Sprintf("key-%d", hashValue % 10000000)
}
```

**Why**: Even with Zipfian access (favoring low IDs), the hash spreads hot keys across all servers

---

#### 3. **Raft Replication Configuration**
**Location**: `experiments/framework/benchmark_runner.py`

**Required**: Set CockroachDB to use 3 replicas per shard

**Implementation**: Add SQL command after cluster initialization:
```python
def _configure_replication(self) -> bool:
    """Configure CockroachDB replication factor."""
    try:
        result = subprocess.run([
            str(self.cockroach_bin),
            "sql",
            "--insecure",
            f"--host=localhost:{self.base_port}",
            "--execute=ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;"
        ], capture_output=True, timeout=10)

        return result.returncode == 0
    except Exception as e:
        self.logger.error(f"Failed to configure replication: {e}")
        return False
```

Call in `run_all()` after cluster start, before data initialization.

---

#### 4. **Specialized Graph Generation**
**Location**: `experiments/framework/visualization.py`

**Add Three New Plot Functions**:

##### Graph 7: Latency vs. Throughput (Line)
```python
def plot_latency_throughput_knee(self, results: ExperimentResults):
    """
    Line graph: X = throughput, Y = median latency
    Multiple lines for different client counts
    Identify and mark the "knee" point
    """
    # Group results by client count
    # Plot throughput (x) vs P50 latency (y)
    # Annotate knee point (75-80% of peak throughput)
    # Save to: output_dir/graph7_knee.png
```

##### Graph 8: Throughput vs. Zipf (Bar)
```python
def plot_throughput_zipf_bar(self, results: ExperimentResults):
    """
    Bar graph: X = Zipf values, Y = throughput
    Grouped bars: Baseline vs Juicer
    """
    # Group results by Zipf value
    # Create grouped bar chart
    # Save to: output_dir/graph8_throughput_zipf.png
```

##### Graph 9: Abort Rate vs. Zipf (Bar)
```python
def plot_abort_zipf_bar(self, results: ExperimentResults):
    """
    Bar graph: X = Zipf values, Y = abort rate (%)
    Grouped bars: Baseline vs Juicer
    """
    # Group results by Zipf value
    # Create grouped bar chart for abort rates
    # Save to: output_dir/graph9_abort_zipf.png
```

---

#### 5. **Abort Rate Calculation Enhancement**
**Location**: `pkg/cmd/benchmark/workload.go`

**Current**: Tracks aborted transactions
**Required**: Track total attempts (including retries)

**Modification**:
```go
type WorkloadResults struct {
    TotalTxs      int
    CommittedTxs  int
    AbortedTxs    int
    TotalAttempts int  // NEW: includes retries
    // ... existing fields
}

// In worker loop:
for retries := 0; retries < maxRetries; retries++ {
    results.TotalAttempts++  // Count every attempt
    err := executeTransaction(...)
    if err == nil {
        results.CommittedTxs++
        break
    }
    results.AbortedTxs++  // Count every abort
}
```

**Metrics Output**:
```
abort_rate_percent = (TotalAttempts - CommittedTxs) / TotalAttempts * 100
```

---

## Execution Workflow

### Step 1: Find the Knee (Graph 7)
```bash
# Update EC2 hostnames in eval_graph7_knee.yaml
python3 ./scripts/run_experiment.py config configs/eval_graph7_knee.yaml

# Analyze results to find knee point
# Example: if knee is at 64 workers with throughput = 50K ops/sec
```

### Step 2: Update Configs with Knee Value
```bash
# Edit eval_graph8_throughput_zipf.yaml
# Set: workers_per_client: [64]

# Edit eval_graph9_abort_zipf.yaml
# Set: workers_per_client: [64]
```

### Step 3: Run Zipf Sweep (Graphs 8 & 9)
```bash
# Throughput vs Zipf
python3 ./scripts/run_experiment.py config configs/eval_graph8_throughput_zipf.yaml

# Abort Rate vs Zipf (can reuse same run as Graph 8)
python3 ./scripts/run_experiment.py config configs/eval_graph9_abort_zipf.yaml
```

### Step 4: Generate Publication-Quality Graphs
```bash
# Add specialized plotting functions to visualization.py
# Re-run plot generation with custom graph types
```

---

## Expected Results

### Graph 7 (Knee)
- **Low client count**: Low latency, low throughput (underutilized)
- **Knee point**: Latency starts increasing, throughput near peak (optimal)
- **High client count**: High latency, throughput plateaus (saturated)
- **Juicer benefit**: Higher throughput at knee, lower latency

### Graph 8 (Throughput vs Zipf)
- **Low Zipf (0.1)**: Uniform access, high throughput (low contention)
- **High Zipf (1.2)**: Hot keys, low throughput (high contention)
- **Juicer benefit**: Higher throughput at high Zipf values

### Graph 9 (Abort Rate vs Zipf)
- **Low Zipf**: Low abort rate (little contention)
- **High Zipf**: High abort rate (many transactions conflict on hot keys)
- **Juicer benefit**: Lower abort rate at high Zipf values

---

## AWS EC2 Instance Recommendations

### Server Nodes (9 instances)
- **Instance Type**: `m5.2xlarge` (8 vCPUs, 32 GB RAM, up to 10 Gbps network)
- **Storage**: 200 GB gp3 SSD per instance
- **Purpose**: CockroachDB servers with Raft replication

### Client Nodes (1-9 instances)
- **Instance Type**: `c5.2xlarge` (8 vCPUs, 16 GB RAM, optimized for CPU)
- **Purpose**: Benchmark client load generators

### Network
- **VPC**: Single VPC in us-east-1
- **Subnet**: Single availability zone (low latency)
- **Security Groups**:
  - Intra-cluster: Allow 26257, 8080 within VPC
  - SSH: Allow 22 from your IP

### Estimated Cost
- **Servers**: 9 × $0.384/hr = $3.46/hr
- **Clients**: 9 × $0.34/hr = $3.06/hr
- **Total**: ~$6.50/hr × 10 hours = $65 for complete evaluation

---

## Next Steps

1. **Implement RMW workload** in `workload.go`
2. **Add hash-based key distribution** for even hot key spread
3. **Implement replication configuration** in benchmark runner
4. **Add specialized plotting functions** for the 3 graphs
5. **Test locally** with smaller scale (3 nodes, 1K keys)
6. **Deploy to AWS EC2** and run full evaluation
7. **Generate publication-quality graphs**

---

## Notes

- **Graph 8 & 9 use same data**: Run experiment once, generate both graphs
- **Knee finding is iterative**: May need to refine client count range
- **Disk image recommended**: Pre-build binaries on image to save deployment time
- **Monitor AWS costs**: Use spot instances to reduce costs by ~70%
