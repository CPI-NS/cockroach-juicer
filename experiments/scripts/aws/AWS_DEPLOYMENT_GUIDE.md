# AWS EC2 Deployment Guide for Juicer Evaluation

This guide walks through deploying CockroachDB with Juicer on AWS EC2 for evaluation experiments.

## Architecture Overview

### Single-Region Deployment (us-east-1)

- **9 CockroachDB servers** distributed across 3 availability zones:
  - 3 servers in `us-east-1a`
  - 3 servers in `us-east-1b`
  - 3 servers in `us-east-1c`
- **9-18 client nodes** (start with 9, scale up to saturate the system)
- **Replication Factor**: 3 (Raft fault tolerance)
- **Keyspace**: 1 million keys
- **Hot-key distribution**: Hash-based keys for even sharding

### Why Single Region?

Your advisor specified "single-region only for crdb" to:
1. Minimize network latency between nodes
2. Focus on Juicer's performance under high contention
3. Avoid multi-region coordination overhead
4. Compare baseline vs Juicer in optimal network conditions

---

## Step 1: Provision EC2 Instances

### Recommended Instance Types

**CockroachDB Servers** (9 instances):
- Instance type: `m5.2xlarge` or `c5.4xlarge`
  - 8 vCPUs, 32 GB RAM
  - Enhanced networking
  - EBS-optimized
- Storage: 100 GB gp3 EBS volume (mounted at `/mnt/data`)
- Region: `us-east-1`
- Availability Zones: Distribute evenly across `us-east-1a`, `us-east-1b`, `us-east-1c`

**Benchmark Clients** (9-18 instances):
- Instance type: `c5.2xlarge`
  - 8 vCPUs, 16 GB RAM
  - Compute-optimized for benchmark workload
- Storage: 20 GB gp3 EBS volume (root)
- Region: `us-east-1`
- Availability Zone: Any (recommend spreading across AZs)

### Launch Instances via AWS Console

1. **Navigate to EC2 Dashboard** → Launch Instance

2. **Choose AMI**: Ubuntu Server 22.04 LTS (HVM), SSD Volume Type

3. **Configure Instance**:
   - Number of instances: 9 (servers) + 9 (clients) = 18 total
   - Network: Use default VPC or create dedicated VPC
   - Subnet: Select subnets in us-east-1a, us-east-1b, us-east-1c
   - Auto-assign Public IP: **Enable**
   - IAM role: None needed

4. **Add Storage**:
   - Servers: 100 GB gp3 (IOPS: 3000, Throughput: 125 MB/s)
   - Clients: 20 GB gp3

5. **Configure Security Group**:
   ```
   Inbound Rules:
   - SSH (22): Your IP
   - CockroachDB SQL (26257): Security group itself (peer communication)
   - CockroachDB HTTP (8080): Your IP (admin UI)
   - Custom TCP (26258-26266): Security group itself (multi-node)
   ```

6. **Key Pair**: Create or use existing key pair (e.g., `aws-juicer-key.pem`)

7. **Launch** and wait for instances to initialize

---

## Step 2: Configure Instances

### 2.1 Tag Your Instances

Tag instances for easy identification:
- Servers: `juicer-server-0` through `juicer-server-8`
- Clients: `juicer-client-0` through `juicer-client-8`

### 2.2 Collect Instance Information

Create a spreadsheet with:
- Instance ID
- Public IP (for SSH access)
- Private IP (for internal communication)
- Availability Zone
- Role (server or client)

Example:
```
Server 0: i-0abc123, 18.209.1.1, 10.0.1.10, us-east-1a, cockroach
Server 1: i-0def456, 18.209.1.2, 10.0.1.11, us-east-1a, cockroach
...
Client 0: i-0xyz789, 18.209.2.1, 10.0.2.10, us-east-1a, benchmark
```

### 2.3 Set Up SSH Access

```bash
# Set permissions on key
chmod 400 ~/.ssh/aws-juicer-key.pem

# Test SSH connection to first server
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<SERVER_0_PUBLIC_IP>
```

### 2.4 Mount EBS Volumes (Servers Only)

On each server instance:

```bash
# Check available disks
lsblk

# Format the EBS volume (usually /dev/nvme1n1 or /dev/xvdf)
sudo mkfs.ext4 /dev/nvme1n1

# Create mount point
sudo mkdir -p /mnt/data

# Mount the volume
sudo mount /dev/nvme1n1 /mnt/data

# Set ownership
sudo chown -R ubuntu:ubuntu /mnt/data

# Add to /etc/fstab for auto-mount on reboot
echo "/dev/nvme1n1 /mnt/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab

# Create data directories
mkdir -p /mnt/data/cockroach-data
mkdir -p /mnt/data/logs
```

---

## Step 3: Update Configuration Files

### 3.1 Update Config with Instance IPs

Edit the three config files:
- `experiments/configs/aws_eval_graph7_knee.yaml`
- `experiments/configs/aws_eval_graph8_throughput_zipf.yaml`
- `experiments/configs/aws_eval_graph9_abort_zipf.yaml`

Replace all `PLACEHOLDER` values with actual IPs:

```yaml
servers:
  - hostname: "18.209.1.1"  # Public IP for SSH
    ssh_port: 22
    internal_ip: "10.0.1.10"  # Private IP for CockroachDB communication
    role: "cockroach"
    node_id: 1
    availability_zone: "us-east-1a"
```

**IMPORTANT**:
- `hostname`: Use **public IP** (for SSH from your control machine)
- `internal_ip`: Use **private IP** (for CockroachDB inter-node communication)

### 3.2 Update SSH Key Path

```yaml
remote:
  ssh_key: "~/.ssh/aws-juicer-key.pem"  # Your actual key path
```

---

## Step 4: Build Binaries for Linux x86_64

On your **local machine** (Mac), cross-compile for Linux:

```bash
cd ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer

# Option 1: Use Docker for cross-compilation
./dev build cockroach --cross=linux-amd64
./dev build benchmark --cross=linux-amd64

# Option 2: Build on an AWS instance
# Launch a temporary c5.4xlarge Ubuntu instance, build there, then copy binaries
```

**If Docker is not available**, build on a server instance:

```bash
# SSH to one of your server instances
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<SERVER_0_PUBLIC_IP>

# Clone repo
git clone https://github.com/CPI-NS/juicer-grpc-go.git  # Or your fork
cd cockroach-juicer

# Build binaries
./dev build cockroach
./dev build benchmark

# Binaries will be in bin/ directory
ls -lh bin/cockroach bin/benchmark
```

Then copy binaries to your local machine for deployment:

```bash
# From local machine
scp -i ~/.ssh/aws-juicer-key.pem ubuntu@<SERVER_0_PUBLIC_IP>:~/cockroach-juicer/bin/cockroach ~/Downloads/
scp -i ~/.ssh/aws-juicer-key.pem ubuntu@<SERVER_0_PUBLIC_IP>:~/cockroach-juicer/bin/benchmark ~/Downloads/

# Move to local repo
mv ~/Downloads/cockroach ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/bin/
mv ~/Downloads/benchmark ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/bin/
```

---

## Step 5: Run Experiments

### 5.1 Graph 7: Finding the Knee

```bash
cd ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/experiments

# Run experiment
python3 scripts/run_experiment.py configs/aws_eval_graph7_knee.yaml
```

**What this does**:
1. Deploys `cockroach` binary to all 9 servers
2. Deploys `benchmark` binary to all 9 clients
3. Starts 9-node CockroachDB cluster with replication factor 3
4. Initializes 1M keys with hash-based distribution
5. Runs workload with varying concurrency (9, 18, 36, 72, 108, 144, 216, 288 workers)
6. Measures latency and throughput for baseline vs Juicer
7. Saves results to `../results/aws/graph7_knee/`

**Expected runtime**:
- 8 concurrency levels × 2 juicer configs × 5 repeats = 80 runs
- ~60 seconds per run = ~80 minutes total

**Finding the knee**:
After completion, analyze the latency-throughput plot:
```bash
open ../results/aws/graph7_knee/plots/eval_graph7_knee.png
```

The "knee" is where latency starts increasing sharply (75-80% of peak throughput).

### 5.2 Update Graphs 8 & 9 with Knee Value

After determining the knee (e.g., 72 workers = 9 clients × 8 workers), update:

```yaml
# In aws_eval_graph8_throughput_zipf.yaml and aws_eval_graph9_abort_zipf.yaml
concurrency:
  num_clients: [9]
  workers_per_client: [8]  # <-- UPDATE this value
```

### 5.3 Graph 8: Throughput vs Zipf

```bash
python3 scripts/run_experiment.py configs/aws_eval_graph8_throughput_zipf.yaml
```

**Expected runtime**:
- 6 zipf values × 2 juicer configs × 5 repeats = 60 runs
- ~60 seconds per run = ~60 minutes total

### 5.4 Graph 9: Abort Rate vs Zipf

```bash
python3 scripts/run_experiment.py configs/aws_eval_graph9_abort_zipf.yaml
```

**Expected runtime**: ~60 minutes (same as Graph 8)

---

## Step 6: Verify Results

### 6.1 Check Output Files

```bash
ls -la ../results/aws/graph7_knee/
# Should contain:
#   - results.json (raw data)
#   - results.csv (tabular data)
#   - summary.txt (human-readable summary)
#   - plots/ (generated graphs)

ls -la ../results/aws/graph7_knee/plots/
# Should contain:
#   - eval_graph7_knee.png (latency vs throughput)
```

### 6.2 Verify Key Metrics

**Graph 7 (Knee)**:
- Latency (P50, P95, P99) should increase as concurrency increases
- Throughput should plateau at high concurrency
- Juicer should show lower latency at same throughput

**Graph 8 (Throughput vs Zipf)**:
- Higher zipf (more skew) = lower throughput
- Juicer should outperform baseline, especially at high zipf

**Graph 9 (Abort Rate vs Zipf)**:
- Higher zipf = higher abort rate
- Juicer should significantly reduce abort rate

---

## Critical Configuration Checks

### ✅ Hot Key Distribution

Verify hash-based keys are enabled:
```yaml
data_init:
  use_hash_keys: true  # MUST be true
```

Without this, hot keys will concentrate on a single server, violating your requirement.

### ✅ Replication Factor

CockroachDB replication is controlled via zone configs. The framework should set:
```sql
ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;
```

Check cluster_manager.py for this logic. If missing, we need to add it.

### ✅ Abort Rate Calculation

Verify workload.go:311 calculates:
```go
abortRate = (TotalAttempts - CommittedTxs) / TotalAttempts * 100
```

This correctly counts retries as specified by your advisor.

---

## Troubleshooting

### SSH Connection Issues

```bash
# Test connectivity
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<PUBLIC_IP> echo "Connection successful"

# Check security group allows SSH from your IP
aws ec2 describe-security-groups --group-ids <SG_ID>
```

### CockroachDB Won't Start

```bash
# SSH to server and check logs
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<SERVER_IP>
tail -f /mnt/data/logs/cockroach-node-1.log

# Common issues:
# - Port 26257 already in use: pkill cockroach
# - Permission denied: sudo chown ubuntu:ubuntu /mnt/data
# - Disk full: df -h /mnt/data
```

### High Abort Rate (Even with Juicer)

- Check that `use_hash_keys: true` is set
- Verify 1M keyspace is properly sharded
- Check CockroachDB admin UI: http://<SERVER_PUBLIC_IP>:8080
  - Navigate to "Metrics" → "Replication" → Verify even range distribution

### Low Throughput

- Verify all 9 servers are running: `ps aux | grep cockroach` on each
- Check network latency between nodes
- Increase client workers if not saturating

---

## Cost Optimization

### Estimated Costs (us-east-1)

**Servers** (9 × m5.2xlarge):
- On-Demand: $0.384/hour × 9 = $3.46/hour
- Spot: ~$0.12/hour × 9 = $1.08/hour (70% savings)

**Clients** (9 × c5.2xlarge):
- On-Demand: $0.34/hour × 9 = $3.06/hour
- Spot: ~$0.10/hour × 9 = $0.90/hour

**Total**: ~$6.50/hour on-demand, ~$2/hour spot

**For 3-hour experiment run**: ~$19.50 on-demand, ~$6 spot

### Use Spot Instances

For experiments, use Spot Instances to save 70%:

1. Launch with Spot request instead of On-Demand
2. Set max price to on-demand price
3. Accept that instances may be terminated (rare for short experiments)

### Stop Instances When Not Running

```bash
# Stop all instances (preserves EBS data)
aws ec2 stop-instances --instance-ids <ID1> <ID2> ... <ID18>

# Restart when needed
aws ec2 start-instances --instance-ids <ID1> <ID2> ... <ID18>
```

---

## Next Steps

1. **Provision** 18 EC2 instances (9 servers + 9 clients)
2. **Update** config files with actual IPs
3. **Build** binaries for Linux x86_64
4. **Run** Graph 7 to find the knee
5. **Update** Graph 8 & 9 configs with knee value
6. **Run** Graph 8 & 9
7. **Analyze** results and generate publication graphs

Good luck with your evaluation!
