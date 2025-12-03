# Juicer Evaluation on AWS EC2 - Summary

## Overview

This document summarizes the evaluation setup for testing **Juicer-enabled CockroachDB** on AWS EC2, as specified by your advisor.

---

## Evaluation Goals

Compare **baseline CockroachDB** vs **Juicer-enabled CockroachDB** performance under high contention workloads.

**Key Metrics**:
1. **Throughput** (ops/sec)
2. **Latency** (P50, P95, P99 in milliseconds)
3. **Abort Rate** (% of transaction attempts that abort)

---

## Architecture

### Deployment
- **Platform**: AWS EC2
- **Region**: us-east-1 (single region for low latency)
- **Availability Zones**: 3 (us-east-1a, us-east-1b, us-east-1c)
- **Servers**: 9 CockroachDB nodes (3 per AZ)
- **Clients**: 9-18 benchmark clients (variable to find saturation)
- **Replication**: Factor of 3 (Raft fault tolerance)

### Why Single Region?
Your advisor specified "single-region only for crdb" to:
- Minimize network latency between nodes
- Focus evaluation on Juicer's contention handling (not WAN latency)
- Provide optimal baseline for comparison

### Hot Key Distribution Strategy
**Requirement**: Hot keys must be evenly distributed across all 9 servers.

**Solution**: Hash-based key generation
- Benchmark uses `hashKey()` function (workload.go:174-182)
- Maps Zipfian-selected key IDs to hashed physical keys
- Ensures even sharding even with skewed access patterns
- **Config flag**: `use_hash_keys: true`

**Why This Matters**:
Without hashing, Zipfian distribution would concentrate hot keys on a single server, creating an unfair bottleneck that doesn't test Juicer's distributed capabilities.

---

## Workload Specification

### Transaction Type: Read-Modify-Write (RMW)
- **1 operation per transaction**: Single RMW operation
- **Operation**: GetForUpdate() → modify value → Put() → Commit()
- **No pure reads**: `read_write_ratio = 0.0` (100% RMW)

### Access Pattern: Zipfian Distribution
- **Default**: zipf_s = 0.99 (high skew, high contention)
- **Variation**: 0.1, 0.4, 0.6, 0.8, 0.99, 1.2 (for Graphs 8 & 9)
- **Keyspace**: 1,000,000 keys

### Abort Rate Calculation
**Formula**: `abort_rate = (total_attempts - committed_txs) / total_attempts * 100%`

**Example**:
- 1 transaction aborts 7 times, succeeds on 8th attempt
- Abort rate = 7/8 = 87.5%

This counts **all retry attempts**, as specified by your advisor.

---

## Evaluation Graphs

### Graph 7: Finding the Knee (Latency vs Throughput)

**Purpose**: Identify the optimal operating point (75-80% of peak throughput)

**Methodology**:
- **Varying parameter**: Number of clients (concurrency)
- **Workload**: Default (zipf=0.99, 1M keys, RMW)
- **Concurrency levels**: 9, 18, 36, 72, 108, 144, 216, 288 total workers
  - Achieved by varying `workers_per_client`: 1, 2, 4, 8, 12, 16, 24, 32
  - With fixed `num_clients = 9`

**X-axis**: Throughput (ops/sec)
**Y-axis**: Median latency (ms)
**Plot type**: Line graph (2 lines: Baseline, Juicer)

**Finding the Knee**:
1. Plot throughput vs latency for both baseline and Juicer
2. Identify where latency starts increasing sharply
3. Select concurrency level at 75-80% of peak throughput
4. This becomes the "default" for Graphs 8 & 9

**Expected Result**: Juicer should achieve higher throughput with lower latency.

**Config**: `experiments/configs/aws_eval_graph7_knee.yaml`

---

### Graph 8: Throughput vs Zipf

**Purpose**: Show how contention level (Zipf skew) affects throughput

**Methodology**:
- **Fixed parameter**: Clients at "knee" (e.g., 9 clients × 8 workers = 72 total)
- **Varying parameter**: Zipfian skew
- **Zipf values**: 0.1, 0.4, 0.6, 0.8, 0.99, 1.2

**X-axis**: 6 discrete Zipf values
**Y-axis**: System throughput (ops/sec)
**Plot type**: Bar graph (2 bars per Zipf value: Baseline, Juicer)

**Expected Result**:
- Higher Zipf = lower throughput (more contention)
- Juicer should outperform baseline, especially at high Zipf

**Config**: `experiments/configs/aws_eval_graph8_throughput_zipf.yaml`

**NOTE**: After running Graph 7, update `workers_per_client` in this config to match the knee value.

---

### Graph 9: Abort Rate vs Zipf

**Purpose**: Show how Juicer reduces aborts under varying contention

**Methodology**:
- **Same as Graph 8** (fixed clients at knee, varying Zipf)
- Focus on abort rate metric instead of throughput

**X-axis**: 6 discrete Zipf values
**Y-axis**: Abort rate (%)
**Plot type**: Bar graph (2 bars per Zipf value: Baseline, Juicer)

**Expected Result**:
- Higher Zipf = higher abort rate (more contention)
- Juicer should significantly reduce abort rate

**Config**: `experiments/configs/aws_eval_graph9_abort_zipf.yaml`

**NOTE**: Update `workers_per_client` to match knee value from Graph 7.

---

## Configuration Files

### 1. `aws_eval_graph7_knee.yaml`
- **Purpose**: Find the knee of latency-throughput curve
- **Experiments**: 8 concurrency × 2 juicer × 5 repeats = 80 runs
- **Runtime**: ~80 minutes
- **Output**: `../results/aws/graph7_knee/`

### 2. `aws_eval_graph8_throughput_zipf.yaml`
- **Purpose**: Throughput vs Zipf comparison
- **Experiments**: 6 zipf × 2 juicer × 5 repeats = 60 runs
- **Runtime**: ~60 minutes
- **Output**: `../results/aws/graph8_throughput_zipf/`

### 3. `aws_eval_graph9_abort_zipf.yaml`
- **Purpose**: Abort rate vs Zipf comparison
- **Experiments**: 6 zipf × 2 juicer × 5 repeats = 60 runs
- **Runtime**: ~60 minutes
- **Output**: `../results/aws/graph9_abort_zipf/`

**Total Experiment Time**: ~3.5 hours (including cluster setup/teardown)

---

## Implementation Details

### CockroachDB Configuration

**Replication Factor**: 3 (Raft fault tolerance)
- Configured via: `ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;`
- Implementation: `benchmark_runner.py:247-283`
- Automatically applied when `num_nodes >= 3`

**Concurrency Control**: MVCC (native CockroachDB)
- **NOT using 2PL-WW** (that's the custom server implementation)
- Juicer works on top of CockroachDB's MVCC
- `protocol: ["MVCC"]` is metadata only

**Data Initialization**:
- 1M keys pre-loaded via bulk insert
- Hash-based key distribution enabled
- Concurrent writers for fast initialization

### Benchmark Client

**Binary**: `pkg/cmd/benchmark/`

**Key Features**:
- Zipfian distribution (configurable skew)
- Hash-based key generation (`use_hash_keys: true`)
- Accurate abort rate tracking (counts all retry attempts)
- Per-transaction latency measurement
- Automatic retry on abort (up to max attempts)

**Metrics Output**:
- Total transactions
- Total attempts (including retries)
- Committed transactions
- Aborted transactions
- Abort rate (%)
- Latency percentiles (P50, P95, P99)
- Throughput (ops/sec)

### Experimental Framework

**Architecture**:
- `run_experiment.py`: Main orchestrator
- `config_parser.py`: YAML config parser
- `benchmark_runner.py`: Experiment execution
- `cluster_manager.py`: CockroachDB cluster lifecycle
- `workload_generator.py`: Data initialization
- `visualization.py`: Graph generation

**Features**:
- Remote deployment via SSH/SFTP
- Automatic binary deployment
- Parallel experiment execution
- Result aggregation across trials
- Automatic plot generation

---

## Critical Verification Points

### ✅ 1. Hot Key Distribution
**Check**: `data_init.use_hash_keys: true` in all config files

Without this, hot keys concentrate on one server → unfair evaluation.

### ✅ 2. Replication Factor
**Check**: Cluster starts with 9 nodes, replication automatically set to 3

Verify in CockroachDB admin UI:
- Navigate to http://<SERVER_IP>:8080
- Check "Replication" metrics show 3 replicas per range

### ✅ 3. Abort Rate Calculation
**Check**: `workload.go:311` uses formula:
```go
abortRate = (TotalAttempts - CommittedTxs) / TotalAttempts * 100.0
```

This correctly counts retries, not just final failures.

### ✅ 4. Single-Region Deployment
**Check**: All servers in us-east-1 (different AZs, same region)

Network latency between nodes should be <1ms.

### ✅ 5. Workload Type
**Check**:
- `ops_per_tx: [1]` (single RMW operation)
- `read_write_ratio: [0.0]` (100% RMW, no pure reads)

### ✅ 6. Knee Definition
**Check**: After Graph 7, knee should be at 75-80% of peak throughput

If latency is still flat at highest concurrency, increase `workers_per_client` values.

---

## Expected Results

### Baseline CockroachDB
- **Throughput**: Degrades significantly with high Zipf
- **Latency**: Increases under contention
- **Abort Rate**: High with zipf=0.99 (likely 40-80%)

### Juicer-enabled CockroachDB
- **Throughput**: 1.5-3× higher than baseline under high contention
- **Latency**: Lower than baseline at same throughput
- **Abort Rate**: Significantly reduced (likely 10-30% vs baseline 40-80%)

### Key Insight
Juicer's transaction reordering should prevent unnecessary aborts by:
1. Detecting conflicting transactions early
2. Reordering operations to minimize wait time
3. Reducing cascading aborts

---

## Next Steps

1. **Provision AWS EC2 instances** (see `AWS_DEPLOYMENT_GUIDE.md`)
2. **Update config files** with actual instance IPs
3. **Build binaries** for Linux x86_64
4. **Run Graph 7** to find the knee
5. **Update Graphs 8 & 9** with knee value
6. **Run Graphs 8 & 9**
7. **Analyze results** and prepare publication graphs

---

## Troubleshooting

### Issue: High abort rate even with Juicer

**Possible causes**:
1. `use_hash_keys: false` → hot keys on one server
2. Replication not configured → check admin UI
3. Clients not distributed → network bottleneck
4. Insufficient keyspace → increase to 10M keys

### Issue: Can't find the knee (flat latency)

**Solution**: Increase maximum concurrency:
```yaml
workers_per_client: [1, 2, 4, 8, 12, 16, 24, 32, 48, 64]
```

Keep increasing until latency curve shows clear inflection point.

### Issue: Low throughput (<1000 ops/sec)

**Possible causes**:
1. Network latency between nodes → verify all in us-east-1
2. Slow disk I/O → use gp3 with 3000 IOPS
3. CPU bottleneck → upgrade to c5.4xlarge
4. Only 1 server running → check cluster status

---

## Cost Estimate

**Instance Costs** (us-east-1, on-demand):
- 9 servers (m5.2xlarge): $3.46/hour
- 9 clients (c5.2xlarge): $3.06/hour
- **Total**: $6.52/hour ≈ $23/day

**For 4-hour experiment**: ~$26

**With Spot Instances**: ~$8 (70% savings)

**Recommendation**: Use Spot Instances for experiments (low risk of interruption for <4 hour runs).

---

## Files Created

1. **Config Files**:
   - `experiments/configs/aws_eval_graph7_knee.yaml`
   - `experiments/configs/aws_eval_graph8_throughput_zipf.yaml`
   - `experiments/configs/aws_eval_graph9_abort_zipf.yaml`

2. **Documentation**:
   - `experiments/AWS_DEPLOYMENT_GUIDE.md` (step-by-step setup)
   - `experiments/EVALUATION_SUMMARY.md` (this file)

3. **Experimental Framework** (existing):
   - `experiments/scripts/run_experiment.py`
   - `experiments/framework/*.py`

---

## Key Differences from CloudLab

| Aspect | CloudLab (old plan) | AWS EC2 (current plan) |
|--------|-------------------|----------------------|
| Nodes | 2 servers, 2 clients | 9 servers, 9-18 clients |
| Region | Single site | Single region (us-east-1), 3 AZs |
| Keyspace | 10K keys | 1M keys |
| Replication | N/A (2 nodes) | Factor of 3 (Raft) |
| Hot keys | Not addressed | Hash-based distribution |
| Graphs | 3 basic graphs | 3 publication-quality graphs |
| Purpose | Initial testing | Full evaluation for paper |

---

## Questions for Your Advisor

1. **Knee definition**: Confirm 75-80% of peak throughput is correct?
2. **Repeat count**: 5 trials sufficient for statistical significance?
3. **Zipf values**: Are 0.1, 0.4, 0.6, 0.8, 0.99, 1.2 the right range?
4. **Baseline comparison**: Should we also compare against other systems (e.g., MySQL, PostgreSQL)?
5. **Multi-region**: Will we eventually test multi-region, or stay single-region only?

---

**End of Summary**

Good luck with your evaluation! 🚀
