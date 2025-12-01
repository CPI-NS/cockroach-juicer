# CloudLab Deployment Guide for Juicer Experiments

This guide explains how to run Juicer experiments on CloudLab using the experiment framework.

## Your CloudLab Setup

- **Control Node**: `pcvm474-1.emulab.net` (you run experiments from here)
- **Servers**: 2 nodes running CockroachDB
  - `server-0`: SSH via `ssh -p 25477 harrisah@pc474.emulab.net`
  - `server-1`: SSH via `ssh -p 25478 harrisah@pc474.emulab.net`
- **Clients**: 2 nodes running benchmark
  - `client-0`: SSH via `ssh -p 25475 harrisah@pc474.emulab.net`
  - `client-1`: SSH via `ssh -p 25476 harrisah@pc474.emulab.net`

## Prerequisites

### 1. On Your Local Machine

Build the binaries:

```bash
cd /path/to/cockroach-juicer

# Build CockroachDB with Juicer
./dev build short

# Build benchmark binary
bazel build //pkg/cmd/benchmark:benchmark
cp _bazel/bin/pkg/cmd/benchmark/benchmark_/benchmark bin/benchmark
```

### 2. SSH Access

Make sure you can SSH to all nodes without password:

```bash
# Test connectivity
ssh -p 25477 harrisah@pc474.emulab.net "hostname"
ssh -p 25478 harrisah@pc474.emulab.net "hostname"
ssh -p 25475 harrisah@pc474.emulab.net "hostname"
ssh -p 25476 harrisah@pc474.emulab.net "hostname"
```

## Configuration

The experiment framework uses `configs/remote_cloudlab.yaml`. Key sections:

```yaml
experiment:
  deployment_mode: "remote"

remote:
  ssh_user: "harrisah"
  ssh_key: "~/.ssh/id_rsa"
  deploy_binaries: true  # Framework will deploy binaries automatically

  nodes:
    servers:
      - hostname: "pc474.emulab.net"
        ssh_port: 25477
        internal_hostname: "server-0"
        role: "cockroach"
        node_id: 1
      # ... more servers

    clients:
      - hostname: "pc474.emulab.net"
        ssh_port: 25475
        internal_hostname: "client-0"
        role: "benchmark"
      # ... more clients
```

## Running the Experiment

The framework handles everything automatically:

### Option 1: From Control Node (Recommended)

```bash
# SSH to control node
ssh harrisah@pcvm474-1.emulab.net

# Clone the repo on control node
git clone <your-repo> cockroach-juicer
cd cockroach-juicer/experiments

# Build binaries locally on control node OR
# Copy pre-built binaries from your local machine:
# scp -r ../bin harrisah@pcvm474-1.emulab.net:~/cockroach-juicer/

# Run the experiment
python3 scripts/run_experiment.py config configs/remote_cloudlab.yaml
```

### Option 2: From Your Local Machine

```bash
cd cockroach-juicer/experiments

# The framework will deploy binaries via SSH
python3 scripts/run_experiment.py config configs/remote_cloudlab.yaml
```

## What the Framework Does Automatically

1. **Deploy binaries**: Copies `cockroach` and `benchmark` to all nodes via SFTP
2. **Start cluster**: Starts CockroachDB on server nodes
3. **Initialize data**: Loads 1M keys with Zipfian distribution
4. **Run workloads**: Executes baseline and Juicer experiments
5. **Collect metrics**: Gathers latency, throughput, abort rates
6. **Generate plots**: Creates comparison graphs
7. **Cleanup**: Stops cluster and closes SSH connections

## Experiment Parameters

Current configuration (from `remote_cloudlab.yaml`):

```yaml
workload:
  tx_count: [10000]         # 10K transactions per run
  ops_per_tx: [1]           # Single RMW operation
  key_range: [1000000]      # 1M keys
  distribution: ["zipfian"]
  zipfian_s: [0.99]         # High skew = high contention
  read_write_ratio: [0.0]   # 100% read-modify-write

concurrency:
  num_clients: [2]          # Use both client nodes
  workers_per_client: [1, 2, 4, 8, 12, 16]  # Finding the knee

juicer:
  enabled: [false, true]    # Compare baseline vs Juicer
  flush_time_us: [100]      # 100μs flush window

cluster:
  num_nodes: 2              # 2 CockroachDB servers

data_init:
  num_keys: 1000000         # 1M keys preloaded
```

This will run:
- 2 configurations (baseline, Juicer)
- 6 concurrency levels
- 5 trials each
- **Total**: 60 experiment runs

## Monitoring Progress

The framework logs everything:

```
2025-11-28 15:26:39 - INFO - Deploying cockroach to server-0...
2025-11-28 15:26:40 - INFO - Starting 2-node cluster
2025-11-28 15:26:44 - INFO - Initializing 1M keys...
2025-11-28 15:26:50 - INFO - Running config 1/12: baseline, 1 worker
...
```

## Results

Results are saved to `../results/cloudlab/`:

```
results/cloudlab/
├── plots/
│   ├── eval_graph7_knee.png      # Latency vs Throughput
│   ├── throughput_comparison.png
│   └── abort_rate_comparison.png
├── results_YYYYMMDD_HHMMSS.csv   # Raw data
├── results_YYYYMMDD_HHMMSS.json  # Detailed results
└── summary_YYYYMMDD_HHMMSS.txt   # Human-readable summary
```

## Troubleshooting

### SSH Connection Issues

```bash
# Add to ~/.ssh/config
Host cloudlab-server-0
    HostName pc474.emulab.net
    Port 25477
    User harrisah

Host cloudlab-server-1
    HostName pc474.emulab.net
    Port 25478
    User harrisah
```

### Binary Not Found

The framework looks for binaries at:
- Local: `../../cockroach-juicer/bin/cockroach` and `../../cockroach-juicer/bin/benchmark`
- Remote: `/users/harrisah/cockroach` and `/users/harrisah/benchmark`

### Port Already in Use

If CockroachDB ports are in use:

```bash
# SSH to each server and kill old processes
ssh -p 25477 harrisah@pc474.emulab.net "pkill -9 cockroach"
ssh -p 25478 harrisah@pc474.emulab.net "pkill -9 cockroach"
```

### Check Framework Support

The framework automatically detects remote mode:

```python
# In cluster_manager.py
if self.remote_mode:
    self._deploy_binary(self.cockroach_bin, self.remote_cockroach_bin)
    self._start_remote_cluster(...)
```

## Next Steps

After the experiment completes:

1. **Review results**: Check `results/cloudlab/plots/`
2. **Analyze data**: Open the CSV file for detailed metrics
3. **Iterate**: Modify `remote_cloudlab.yaml` and re-run

## Quick Start Summary

```bash
# 1. Build binaries locally
cd cockroach-juicer
./dev build short
bazel build //pkg/cmd/benchmark:benchmark

# 2. SSH to control node
ssh harrisah@pcvm474-1.emulab.net

# 3. Clone repo and run
git clone <repo> && cd cockroach-juicer/experiments
python3 scripts/run_experiment.py config configs/remote_cloudlab.yaml

# 4. Wait ~30-60 minutes for completion

# 5. Download results
scp -r harrisah@pcvm474-1.emulab.net:~/cockroach-juicer/results/cloudlab ./
```

That's it! The framework handles all the complexity.
