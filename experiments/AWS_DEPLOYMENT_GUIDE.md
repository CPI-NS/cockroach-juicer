# AWS Deployment Guide for CockroachDB Juicer Experiments

Complete guide for running multi-region open-loop experiments on AWS.

---

## Prerequisites

### AWS Infrastructure
- 18 EC2 t3.2xlarge instances across 3 regions (us-east-1, us-east-2, us-west-1)
  - 9 server instances (3 per region) with 100GB EBS volumes
  - 9 client instances (3 per region)
- Security groups configured for cluster communication and SSH access
- SSH key at: `~/.ssh/aws-juicer-key.pem`

### Local Setup
- Python virtual environment: `experiments/venv/`
- Binaries built: `cockroach` and `bin/benchmark`
- Configuration file: `experiments/configs/aws_multiregion_graph7_knee.yaml`

---

## First-Time AWS Setup

Only run this section once when creating AWS infrastructure for the first time. If instances already exist, skip to "Running Experiments".

### 1. Create AWS Instances

Script: `scripts/aws/aws_setup_multiregion_fixed.sh`

Creates SSH key pair, security groups, and launches 18 EC2 instances across 3 regions.

```bash
cd cockroach-juicer/experiments/scripts/aws
bash aws_setup_multiregion_fixed.sh
```

Time: 5-10 minutes

### 2. Mount EBS Volumes

Script: `scripts/aws/mount_ebs_smart.py`

Detects and mounts 100GB EBS volumes on server instances at `/mnt/data`. Only needs to be run once.

```bash
cd cockroach-juicer/experiments/scripts/aws
python3 mount_ebs_smart.py
```

Time: 2-3 minutes

### 3. Install Dependencies

Script: `scripts/aws/install_dependencies.sh`

Installs required system packages on all instances. Only needs to be run once.

```bash
bash scripts/aws/install_dependencies.sh
```

Time: 3-5 minutes

---

## Testing Locally First

Recommended to validate the framework and open-loop client before deploying to AWS.

### Multi-Node Local Test

```bash
cd cockroach-juicer/experiments
source venv/bin/activate
python3 scripts/run_experiment.py config configs/test_openloop_multinode.yaml
```

This runs a 3-node local cluster with multiple target rates to verify:
- Open-loop client works correctly
- Framework executes benchmarks properly
- Graph 7 plotting generates knee curves
- No errors in data collection or metrics parsing

Time: 15-20 minutes

Results: `results/test_openloop_multinode/plots/eval_graph7_knee.png`

If local test succeeds, AWS deployment will work.

---

## Running Experiments

### Step 1: Start Instances

AWS assigns new public IPs each time instances start.

```bash
cd cockroach-juicer/experiments
source venv/bin/activate
bash scripts/aws/aws_start_instances.sh
```

Time: 2-3 minutes

### Step 2: Update Configuration with New IPs

Updates all config files with current instance IP addresses.

```bash
python3 scripts/update_configs.py
```

Time: <5 seconds

### Step 3: Build Binaries

If not already built or if code has changed:

```bash
cd cockroach-juicer
./dev build cockroach
./dev build benchmark
cd experiments
```

Time: 5-10 minutes (first build), 1-2 minutes (incremental)

Binary locations:
- `cockroach-juicer/cockroach`
- `cockroach-juicer/bin/benchmark`

### Step 4: Run Experiment

Main experiment configuration: `configs/aws_multiregion_graph7_knee.yaml`

```bash
python3 scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml
```

What happens:
1. Deploys binaries to all 18 instances (if `deploy_binaries: true`)
2. Starts 9-node CockroachDB cluster
3. Initializes 1M keys with zipfian distribution
4. Runs 54 benchmarks (9 target rates × 2 configs × 3 repeats)
5. Collects results and generates plots

Time: 60-90 minutes with deployment, 50-60 minutes without

### Step 5: Stop Instances

IMPORTANT: Always stop instances after experiments to avoid AWS charges.

```bash
bash scripts/aws/aws_stop_instances.sh
```

Time: 1 minute

---

## Experiment Configuration

File: `configs/aws_multiregion_graph7_knee.yaml`

### Key Parameters

**Open-Loop Settings:**
```yaml
workload:
  open_loop: true
  target_rate: [500, 1000, 2000, 3000, 4000, 5000, 6000, 8000, 10000]
  duration_seconds: 60
  warmup_percent: 0.25      # First 15s discarded
  cooldown_percent: 0.25    # Last 15s discarded
```

**Workload Characteristics:**
```yaml
  ops_per_tx: [1]
  key_range: [1000000]
  distribution: ["zipfian"]
  zipfian_s: [0.99]         # Highly skewed access
  read_write_ratio: [0.0]   # 100% writes
```

**Concurrency:**
```yaml
concurrency:
  num_clients: [9]          # 9 clients across 3 regions
```

**Juicer Testing:**
```yaml
juicer:
  enabled: [false, true]    # Test baseline and Juicer
  flush_time_us: [100]
```

**Experiment Settings:**
```yaml
experiment:
  repeat_count: 3           # 3 trials per configuration

remote:
  deploy_binaries: true     # Set to false to skip binary upload on re-runs
```

Total benchmark runs: 9 rates × 2 configs (baseline/juicer) × 3 repeats = 54 runs

### Warm-up and Cool-down

Each 60-second run is divided:
- 0-15s: Warm-up (discarded) - handles cold caches, connection setup
- 15-45s: Measurement (used for metrics) - steady-state performance
- 45-60s: Cool-down (discarded) - handles shutdown artifacts

Only the middle 30 seconds are used for calculating latency and throughput.

### Synchronized Start

Framework automatically synchronizes all clients to start at the same Unix timestamp (10 seconds in the future). This ensures fair load distribution across distributed clients.

---

## Results

### Output Location

```
results/aws_multiregion/graph7_knee_openloop/
├── results_YYYYMMDD_HHMMSS.csv       # Raw data
├── results_YYYYMMDD_HHMMSS.json      # Structured results
├── summary_YYYYMMDD_HHMMSS.txt       # Summary statistics
└── plots/
    └── eval_graph7_knee.png          # Latency vs Throughput knee curve
```

Results are automatically downloaded to local machine before instances stop.

### Graph 7: The Knee Curve

Plot shows median latency (Y-axis) vs achieved throughput (X-axis).

Expected pattern:
- Low load (500-2000 ops/sec): Flat latency region, system easily handles load
- The knee (3000-5000 ops/sec): Latency begins increasing as system approaches saturation
- Saturation (>6000 ops/sec): Steep latency increase, throughput plateaus

The throughput at which the curve bends upward indicates system capacity.

---

## Troubleshooting

### SSH Connection Failed

```bash
chmod 400 ~/.ssh/aws-juicer-key.pem
bash scripts/aws/fix_security_groups.sh
```

### Cluster Won't Start

Check logs and kill old processes:
```bash
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<server-ip>
tail -f /mnt/data/logs/cockroach.log
```

Kill cluster if needed:
```bash
bash scripts/aws/kill_cockroach_cluster.sh
```

### Low Throughput / High Latency

Verify all nodes joined cluster:
```bash
# On any server instance
./cockroach node status --insecure --host=localhost:26257
```

Should show all 9 nodes as 'live'.

### Re-running Without Deploying Binaries

Edit `configs/aws_multiregion_graph7_knee.yaml`:
```yaml
remote:
  deploy_binaries: false
```

Saves 5 minutes on subsequent runs.

---

## Important Notes

### Open-Loop vs Closed-Loop

- Closed-loop: Send request, wait for response, send next (throughput limited by latency)
- Open-loop: Send requests at fixed arrival rate regardless of response time

Open-loop mode is necessary for finding system saturation point because it maintains target arrival rate even when system slows down.

### Abort Rate Typically 0%

CockroachDB's MVCC with read refresh rarely restarts transactions:
- Write-write conflicts: one transaction waits
- Read-write conflicts: reads are refreshed instead of aborting

High latency is the actual indicator of contention, not abort rate.

### Cost Estimates

18 × t3.2xlarge instances at ~$0.33/hour each:
- Full experiment (2 hours): ~$12
- Always stop instances when done to avoid charges

---

## Scripts Reference

**Instance Management:**
- `aws_setup_multiregion_fixed.sh` - Initial instance creation (one-time)
- `aws_start_instances.sh` - Start stopped instances
- `aws_stop_instances.sh` - Stop running instances
- `mount_ebs_smart.py` - Mount EBS volumes (one-time)
- `install_dependencies.sh` - Install system packages (one-time)

**Configuration:**
- `update_configs.py` - Update config files with current IPs (run after each start)

**Cluster Management:**
- `kill_cockroach_cluster.sh` - Stop all CockroachDB processes
- `fix_security_groups.sh` - Fix security group rules

**Experiment Execution:**
- `run_experiment.py` - Main experiment runner

---

## Quick Reference

Complete workflow:

```bash
# Navigate to experiments directory
cd cockroach-juicer/experiments
source venv/bin/activate

# Start instances
bash scripts/aws/aws_start_instances.sh

# Update configs with new IPs
python3 scripts/update_configs.py

# Build binaries (if needed)
cd ..
./dev build cockroach
./dev build benchmark
cd experiments

# Run experiment
python3 scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml

# Stop instances
bash scripts/aws/aws_stop_instances.sh

# View results
ls results/aws_multiregion/graph7_knee_openloop/
open results/aws_multiregion/graph7_knee_openloop/plots/eval_graph7_knee.png
```