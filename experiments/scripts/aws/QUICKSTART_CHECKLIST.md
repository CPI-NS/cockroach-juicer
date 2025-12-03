# AWS Evaluation - Complete Step-by-Step Checklist

This is your complete guide from zero to finished evaluation graphs. Follow every step in order.

---

## Phase 1: AWS Setup (30-60 minutes)

### Step 1.1: Launch EC2 Instances

1. **Log in to AWS Console**: https://console.aws.amazon.com/ec2/

2. **Launch Server Instances** (do this first):
   - Click **"Launch Instance"**
   - Name: `juicer-server-0` (you'll create 9 total: server-0 through server-8)
   - **AMI**: Ubuntu Server 22.04 LTS (HVM), SSD Volume Type
   - **Instance type**: `m5.2xlarge` (8 vCPUs, 32 GB RAM)
   - **Key pair**: Create new or use existing (e.g., `aws-juicer-key`)
     - If creating new: Download the `.pem` file and save to `~/.ssh/aws-juicer-key.pem`
   - **Network settings**:
     - VPC: Default VPC
     - Auto-assign public IP: **Enable**
     - Create security group named `juicer-cluster-sg` with rules:
       ```
       Type            Protocol    Port Range    Source
       SSH             TCP         22            My IP
       Custom TCP      TCP         26257         juicer-cluster-sg (the security group itself)
       Custom TCP      TCP         26258-26266   juicer-cluster-sg
       Custom TCP      TCP         8080          My IP
       All ICMP        ICMP        All           juicer-cluster-sg
       ```
   - **Configure storage**:
     - Root volume: 30 GB gp3
     - **Add New Volume**:
       - Volume type: gp3
       - Size: 100 GB
       - IOPS: 3000
       - Throughput: 125 MB/s
       - Device name: /dev/sdf
   - **Advanced details** → **Placement group**: (leave blank)
   - **Number of instances**: 9
   - **Advanced details** → **Subnet**:
     - For first 3 instances: Select subnet in us-east-1a
     - For next 3 instances: Select subnet in us-east-1b
     - For last 3 instances: Select subnet in us-east-1c

   Actually, you need to launch them separately to assign different subnets:

   **Launch 3 instances**:
   - Name: `juicer-server-0`, `juicer-server-1`, `juicer-server-2`
   - Subnet: us-east-1a
   - Everything else as above

   **Launch 3 more instances**:
   - Name: `juicer-server-3`, `juicer-server-4`, `juicer-server-5`
   - Subnet: us-east-1b
   - Everything else as above

   **Launch 3 more instances**:
   - Name: `juicer-server-6`, `juicer-server-7`, `juicer-server-8`
   - Subnet: us-east-1c
   - Everything else as above

3. **Launch Client Instances** (simpler, smaller):
   - Click **"Launch Instance"**
   - Name: `juicer-client-0` through `juicer-client-8`
   - **AMI**: Ubuntu Server 22.04 LTS
   - **Instance type**: `c5.2xlarge` (8 vCPUs, 16 GB RAM)
   - **Key pair**: Same as servers (`aws-juicer-key`)
   - **Network settings**:
     - Same security group: `juicer-cluster-sg`
     - Auto-assign public IP: **Enable**
   - **Configure storage**: 20 GB gp3 (root volume only, no extra volume)
   - **Number of instances**: 9
   - Launch all in any subnet (doesn't matter for clients)

4. **Wait for instances to initialize**: All should show "Running" state with 2/2 status checks passed (~5 minutes)

### Step 1.2: Collect Instance Information

1. In AWS Console → EC2 → Instances
2. Create a spreadsheet or text file with this information:

```
# SERVERS
server-0: public_ip=18.XXX.XXX.1   private_ip=10.0.1.10   az=us-east-1a
server-1: public_ip=18.XXX.XXX.2   private_ip=10.0.1.11   az=us-east-1a
server-2: public_ip=18.XXX.XXX.3   private_ip=10.0.1.12   az=us-east-1a
server-3: public_ip=18.XXX.XXX.4   private_ip=10.0.2.10   az=us-east-1b
server-4: public_ip=18.XXX.XXX.5   private_ip=10.0.2.11   az=us-east-1b
server-5: public_ip=18.XXX.XXX.6   private_ip=10.0.2.12   az=us-east-1b
server-6: public_ip=18.XXX.XXX.7   private_ip=10.0.3.10   az=us-east-1c
server-7: public_ip=18.XXX.XXX.8   private_ip=10.0.3.11   az=us-east-1c
server-8: public_ip=18.XXX.XXX.9   private_ip=10.0.3.12   az=us-east-1c

# CLIENTS
client-0: public_ip=18.XXX.XXX.10  private_ip=10.0.4.10
client-1: public_ip=18.XXX.XXX.11  private_ip=10.0.4.11
client-2: public_ip=18.XXX.XXX.12  private_ip=10.0.4.12
client-3: public_ip=18.XXX.XXX.13  private_ip=10.0.4.13
client-4: public_ip=18.XXX.XXX.14  private_ip=10.0.4.14
client-5: public_ip=18.XXX.XXX.15  private_ip=10.0.4.15
client-6: public_ip=18.XXX.XXX.16  private_ip=10.0.4.16
client-7: public_ip=18.XXX.XXX.17  private_ip=10.0.4.17
client-8: public_ip=18.XXX.XXX.18  private_ip=10.0.4.18
```

3. Fill in the actual IPs from AWS Console

### Step 1.3: Set Up SSH Access

1. **Set permissions on your SSH key**:
```bash
chmod 400 ~/.ssh/aws-juicer-key.pem
```

2. **Test SSH to first server**:
```bash
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.1
# You should see Ubuntu welcome message
exit
```

3. **Test SSH to first client**:
```bash
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.10
exit
```

### Step 1.4: Mount EBS Volumes on Servers

Do this on **each of the 9 server instances** (not clients):

```bash
# SSH to server
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.1

# Check available disks
lsblk
# Look for a disk that's not mounted (usually /dev/nvme1n1 or /dev/xvdf)

# Format the EBS volume (ONLY DO THIS ONCE!)
sudo mkfs.ext4 /dev/nvme1n1

# Create mount point
sudo mkdir -p /mnt/data

# Mount the volume
sudo mount /dev/nvme1n1 /mnt/data

# Set ownership
sudo chown -R ubuntu:ubuntu /mnt/data

# Auto-mount on reboot
echo "/dev/nvme1n1 /mnt/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab

# Verify it worked
df -h /mnt/data
# Should show ~99GB available

# Create directories
mkdir -p /mnt/data/cockroach-data
mkdir -p /mnt/data/logs

exit
```

**Repeat this for all 9 servers**. Or use this loop from your local machine:

```bash
# Create a script
cat > mount_ebs.sh << 'EOF'
#!/bin/bash
lsblk | grep -q nvme1n1 || exit 0
sudo mkfs.ext4 -F /dev/nvme1n1
sudo mkdir -p /mnt/data
sudo mount /dev/nvme1n1 /mnt/data
sudo chown -R ubuntu:ubuntu /mnt/data
echo "/dev/nvme1n1 /mnt/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
mkdir -p /mnt/data/cockroach-data
mkdir -p /mnt/data/logs
df -h /mnt/data
EOF

# Run on all servers
for ip in 18.XXX.XXX.1 18.XXX.XXX.2 18.XXX.XXX.3 18.XXX.XXX.4 18.XXX.XXX.5 18.XXX.XXX.6 18.XXX.XXX.7 18.XXX.XXX.8 18.XXX.XXX.9; do
  echo "Setting up $ip..."
  scp -i ~/.ssh/aws-juicer-key.pem mount_ebs.sh ubuntu@$ip:~
  ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@$ip 'bash mount_ebs.sh'
done
```

**✅ Checkpoint**: You should now have 18 running EC2 instances (9 servers with 100GB data volumes, 9 clients)

---

## Phase 2: Build Binaries (30-60 minutes)

You have two options: build on your local Mac (requires Docker) or build on an EC2 instance.

### Option A: Build on EC2 Instance (Recommended)

1. **SSH to one of your server instances**:
```bash
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.1
```

2. **Install build dependencies**:
```bash
# Update system
sudo apt-get update
sudo apt-get install -y git curl

# Install Bazelisk (Bazel build tool)
sudo wget -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64
sudo chmod +x /usr/local/bin/bazel
```

3. **Clone your repository**:
```bash
cd ~
git clone https://github.com/CPI-NS/juicer-grpc-go.git  # Or your fork
cd cockroach-juicer
```

4. **Set up GitHub authentication** (for private repo access):
```bash
# Option 1: Use Personal Access Token (PAT)
git config --global credential.helper store
# Create PAT at https://github.com/settings/tokens with 'repo' scope
# Then:
echo "https://YOUR_GITHUB_USERNAME:YOUR_PAT@github.com" > ~/.git-credentials
chmod 600 ~/.git-credentials

# Option 2: Use SSH keys (if you have them)
git config --global url."git@github.com:".insteadOf "https://github.com/"
```

5. **Configure Bazel for limited resources**:
```bash
cat >> ~/.bazelrc.user << 'EOF'
build --jobs=4
build --local_ram_resources=HOST_RAM*0.5
build --local_cpu_resources=4
EOF
```

6. **Build cockroach binary** (this will take 20-40 minutes):
```bash
./dev build cockroach
```

Watch for errors. If you see memory pressure, reduce jobs:
```bash
./dev build cockroach -- --jobs=2
```

7. **Build benchmark binary** (5-10 minutes):
```bash
./dev build benchmark
```

8. **Verify binaries exist**:
```bash
ls -lh bin/cockroach bin/benchmark
# Should show two files, each 100-500 MB

# Test they work
./bin/cockroach version
./bin/benchmark --help
```

9. **Copy binaries to your local machine**:
```bash
# From your LOCAL machine (new terminal):
cd ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer

# Copy from EC2 to local
scp -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.1:~/cockroach-juicer/bin/cockroach ./bin/
scp -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.1:~/cockroach-juicer/bin/benchmark ./bin/

# Verify locally
ls -lh bin/cockroach bin/benchmark
file bin/cockroach
# Should say: "ELF 64-bit LSB executable, x86-64"
```

**✅ Checkpoint**: You should have `bin/cockroach` and `bin/benchmark` (Linux x86_64) on your local machine

---

## Phase 3: Configure Experiment (15 minutes)

### Step 3.1: Update Graph 7 Config

Edit `experiments/configs/aws_eval_graph7_knee.yaml`:

1. Find the `servers:` section
2. Replace all `PLACEHOLDER_SERVER_X_IP` with actual public IPs
3. Replace all `PLACEHOLDER_SERVER_X_PRIVATE_IP` with actual private IPs

Example:
```yaml
servers:
  - hostname: "18.209.1.1"  # <-- Your actual public IP
    ssh_port: 22
    internal_ip: "10.0.1.10"  # <-- Your actual private IP
    role: "cockroach"
    node_id: 1
    availability_zone: "us-east-1a"
```

Do this for all 9 servers.

4. Find the `clients:` section
5. Replace all `PLACEHOLDER_CLIENT_X_IP` placeholders

Example:
```yaml
clients:
  - hostname: "18.209.2.1"
    ssh_port: 22
    internal_ip: "10.0.4.10"
    role: "benchmark"
```

Do this for all 9 clients.

6. Update SSH key path:
```yaml
remote:
  ssh_user: "ubuntu"
  ssh_key: "~/.ssh/aws-juicer-key.pem"  # <-- Your actual key path
```

7. Save the file

### Step 3.2: Update Graph 8 Config

Do the exact same thing for `experiments/configs/aws_eval_graph8_throughput_zipf.yaml`:
- Replace all server IPs
- Replace all client IPs
- Update SSH key path

### Step 3.3: Update Graph 9 Config

Do the exact same thing for `experiments/configs/aws_eval_graph9_abort_zipf.yaml`:
- Replace all server IPs
- Replace all client IPs
- Update SSH key path

**✅ Checkpoint**: All 3 config files should have real IPs and SSH key path

---

## Phase 4: Set Up Python Environment (5 minutes)

On your **local machine**:

```bash
cd ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/experiments

# Run setup script
bash setup_venv.sh

# If it creates a virtual environment, activate it:
source venv/bin/activate

# If it installs to user directory, no activation needed

# Verify dependencies
python3 -c "import yaml, numpy, matplotlib, paramiko; print('All dependencies OK')"
```

**✅ Checkpoint**: Python should import all required packages without errors

---

## Phase 5: Run Experiment - Graph 7 (60-90 minutes)

### Step 5.1: Start Experiment

```bash
cd ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/experiments

# Run Graph 7 (Finding the Knee)
python3 scripts/run_experiment.py configs/aws_eval_graph7_knee.yaml
```

**What you'll see**:
```
Loading configuration from: configs/aws_eval_graph7_knee.yaml
Starting experiment: aws-eval-graph7-knee
Total configurations: 16
Repeat count: 5
Total runs: 80

Starting 9-node cluster
Deploying cockroach to server-0:22 -> /home/ubuntu/cockroach
SSH connection established to server-0:22
Successfully deployed to server-0
Deploying cockroach to server-1:22 -> /home/ubuntu/cockroach
...
Join addresses: 10.0.1.10:26257,10.0.1.11:26257,...
Starting node 1 on server-0 (10.0.1.10)
Node 1 started on server-0
...
Initializing remote cluster...
Cluster initialized successfully
Waiting for remote cluster to be ready...
Remote cluster is ready

Configuring Raft replication factor...
Setting replication factor to 3...
✓ Replication factor set to 3

Initializing data...
Deploying benchmark to client-0:22 -> /home/ubuntu/benchmark
...
Running data initialization (1000000 keys)...
✓ Data initialization completed

Running experiments (80 total)...

[1/80] Config: workers=9, juicer=false, trial=1/5
  Running benchmark...
  ✓ Completed in 62.3s
  Throughput: 803.5 ops/sec, Latency P50: 8.2ms, Abort Rate: 45.3%

[2/80] Config: workers=9, juicer=false, trial=2/5
...
```

### Step 5.2: Monitor Progress

The experiment will take **60-90 minutes**. You can monitor:

1. **In the terminal**: Watch the progress output

2. **Check cluster health**:
   - Open browser: `http://18.XXX.XXX.1:8080` (use any server IP)
   - CockroachDB Admin UI should show 9 nodes, all healthy
   - Navigate to "Metrics" → see query activity

3. **SSH to a server** (in another terminal):
```bash
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.1
tail -f /mnt/data/logs/cockroach-node-1.log
```

4. **SSH to a client** (in another terminal):
```bash
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@18.XXX.XXX.10
top
# Should see 'benchmark' process using CPU when experiments are running
```

### Step 5.3: Wait for Completion

When finished, you'll see:
```
================================================================================
Experiment: aws-eval-graph7-knee
================================================================================

Results Summary:
  Total runs: 80
  Successful: 80
  Failed: 0

Generating plots...
    [EVAL] Graph 7 (Knee): ../results/aws/graph7_knee/plots/eval_graph7_knee.png

Plots saved to: ../results/aws/graph7_knee/plots
```

**✅ Checkpoint**: Experiment completed successfully, results saved

---

## Phase 6: Analyze Graph 7 Results (10 minutes)

### Step 6.1: View the Graph

```bash
cd ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/experiments

# Open the graph
open ../results/aws/graph7_knee/plots/eval_graph7_knee.png

# Or on Linux:
# xdg-open ../results/aws/graph7_knee/plots/eval_graph7_knee.png
```

### Step 6.2: Find the Knee

Look at the plot:
- **X-axis**: Throughput (ops/sec)
- **Y-axis**: Median latency (ms)
- **Two lines**: Baseline (blue), Juicer (orange)

**Find the knee**: The point where latency starts increasing sharply

Example:
```
Workers    Throughput    Latency
9          800 ops/s     8ms    ← flat
18         1500 ops/s    10ms   ← flat
36         2800 ops/s    15ms   ← flat
72         4500 ops/s    25ms   ← starting to curve
108        5200 ops/s    45ms   ← sharp increase (KNEE is around here)
144        5400 ops/s    80ms   ← saturated
216        5500 ops/s    150ms  ← saturated
288        5600 ops/s    250ms  ← saturated
```

**The knee is at 75-80% of peak**:
- Peak throughput = 5600 ops/s
- 75% of peak = 4200 ops/s
- **Knee ≈ 72 workers** (which gives ~4500 ops/s)

### Step 6.3: Check the Raw Data

```bash
# View summary
cat ../results/aws/graph7_knee/summary.txt

# View CSV data
head -20 ../results/aws/graph7_knee/results.csv
```

### Step 6.4: Determine Knee Value

Based on the graph, determine:
- **Number of workers at the knee**: e.g., 72
- **With 9 clients**: workers_per_client = 72 / 9 = **8**

Write this down: `workers_per_client = 8`

**✅ Checkpoint**: You know the knee value (e.g., 8 workers per client)

---

## Phase 7: Update & Run Graph 8 (60 minutes)

### Step 7.1: Update Config with Knee Value

Edit `experiments/configs/aws_eval_graph8_throughput_zipf.yaml`:

Find this section:
```yaml
concurrency:
  num_clients: [9]
  workers_per_client: [8]  # <-- UPDATE this to your knee value
```

Change `[8]` to whatever you determined in Step 6.4

Example: If knee was at 72 workers with 9 clients, use `[8]`
If knee was at 108 workers with 9 clients, use `[12]`

Save the file.

### Step 7.2: Run Experiment

```bash
python3 scripts/run_experiment.py configs/aws_eval_graph8_throughput_zipf.yaml
```

This will take ~60 minutes (60 runs total).

### Step 7.3: View Results

When complete:
```bash
open ../results/aws/graph8_throughput_zipf/plots/eval_graph8_throughput_zipf.png
```

You should see a bar graph:
- X-axis: 6 Zipf values (0.1, 0.4, 0.6, 0.8, 0.99, 1.2)
- Y-axis: Throughput (ops/sec)
- Two bars per Zipf: Baseline (blue), Juicer (orange)

**Expected**: Juicer bars higher than baseline, especially at high Zipf

**✅ Checkpoint**: Graph 8 completed successfully

---

## Phase 8: Update & Run Graph 9 (60 minutes)

### Step 8.1: Update Config with Knee Value

Edit `experiments/configs/aws_eval_graph9_abort_zipf.yaml`:

Same as Graph 8:
```yaml
concurrency:
  num_clients: [9]
  workers_per_client: [8]  # <-- UPDATE to your knee value
```

Save the file.

### Step 8.2: Run Experiment

```bash
python3 scripts/run_experiment.py configs/aws_eval_graph9_abort_zipf.yaml
```

This will take ~60 minutes.

### Step 8.3: View Results

When complete:
```bash
open ../results/aws/graph9_abort_zipf/plots/eval_graph9_abort_zipf.png
```

You should see a bar graph:
- X-axis: 6 Zipf values
- Y-axis: Abort rate (%)
- Two bars per Zipf: Baseline, Juicer

**Expected**: Juicer bars much lower than baseline at high Zipf

**✅ Checkpoint**: Graph 9 completed successfully

---

## Phase 9: Final Verification (10 minutes)

### Step 9.1: Check All Results Exist

```bash
ls -la ../results/aws/graph7_knee/plots/
ls -la ../results/aws/graph8_throughput_zipf/plots/
ls -la ../results/aws/graph9_abort_zipf/plots/

# You should see:
# - eval_graph7_knee.png
# - eval_graph8_throughput_zipf.png
# - eval_graph9_abort_zipf.png
# - Plus other plots (latency_vs_flush_time.png, etc.)
```

### Step 9.2: Review All Three Graphs

Open all three:
```bash
open ../results/aws/graph7_knee/plots/eval_graph7_knee.png
open ../results/aws/graph8_throughput_zipf/plots/eval_graph8_throughput_zipf.png
open ../results/aws/graph9_abort_zipf/plots/eval_graph9_abort_zipf.png
```

Verify:
- **Graph 7**: Shows latency vs throughput curve, clear knee visible
- **Graph 8**: Shows throughput degrading with higher Zipf, Juicer better
- **Graph 9**: Shows abort rate increasing with Zipf, Juicer significantly lower

### Step 9.3: Check Key Metrics

```bash
# Graph 7 summary
cat ../results/aws/graph7_knee/summary.txt

# Graph 8 summary
cat ../results/aws/graph8_throughput_zipf/summary.txt

# Graph 9 summary
cat ../results/aws/graph9_abort_zipf/summary.txt
```

Look for:
- **Throughput improvement**: Juicer should be 1.5-3× better at high contention
- **Latency reduction**: Juicer should have lower P50/P95/P99
- **Abort rate reduction**: Juicer should cut aborts by 50-80% at high Zipf

**✅ Checkpoint**: All three graphs generated with expected results

---

## Phase 10: Cleanup (5 minutes)

### Step 10.1: Stop EC2 Instances (to save costs)

If you need to pause and come back later:

```bash
# Get all instance IDs
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=juicer-*" \
  --query "Reservations[].Instances[].[InstanceId,Tags[?Key=='Name'].Value|[0]]" \
  --output text

# Stop all instances (preserves EBS data)
aws ec2 stop-instances --instance-ids <ALL_INSTANCE_IDS>
```

Or manually in AWS Console:
1. Select all 18 instances
2. Instance state → Stop instance

### Step 10.2: Terminate When Done (if completely finished)

**WARNING**: This deletes everything!

```bash
# Terminate all instances
aws ec2 terminate-instances --instance-ids <ALL_INSTANCE_IDS>
```

Or manually:
1. Select all 18 instances
2. Instance state → Terminate instance

### Step 10.3: Download Results Locally

Before terminating, make sure you have all results:

```bash
# Results are already on your local machine at:
ls -la ~/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/results/aws/

# Optionally, back up to cloud storage or external drive
tar -czf juicer-aws-results-$(date +%Y%m%d).tar.gz ../results/aws/
```

**✅ Checkpoint**: Results safely backed up, instances terminated

---

## Troubleshooting Common Issues

### Issue: "Permission denied (publickey)"

**Fix**:
```bash
chmod 400 ~/.ssh/aws-juicer-key.pem
ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<IP>
```

### Issue: "Connection timed out"

**Fix**:
1. Check security group allows SSH from your IP
2. Verify instance is running
3. Check you're using public IP (not private IP)

### Issue: "Failed to deploy CockroachDB binary"

**Fix**:
1. Verify binaries exist locally:
   ```bash
   ls -lh bin/cockroach bin/benchmark
   file bin/cockroach  # Should say "ELF 64-bit LSB executable"
   ```
2. Check they're Linux binaries (not Mac binaries)

### Issue: "Cluster init failed"

**Fix**:
1. SSH to one server: `ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@<SERVER_IP>`
2. Check logs: `tail -f /mnt/data/logs/cockroach-node-1.log`
3. Common causes:
   - Port 26257 already in use: `sudo pkill cockroach`
   - Can't reach other nodes: Check security group allows internal communication

### Issue: High abort rate even with Juicer

**Fix**:
1. Verify `use_hash_keys: true` in config
2. Check CockroachDB admin UI shows even range distribution
3. Increase keyspace to 10M if needed

### Issue: "No such file: /mnt/data"

**Fix**: EBS volume not mounted. Go back to Step 1.4 and mount volumes.

### Issue: Experiment crashes midway

**Fix**:
1. Check cluster status: `http://<SERVER_IP>:8080`
2. SSH to server and check: `ps aux | grep cockroach`
3. Check disk space: `df -h /mnt/data`
4. Restart cluster and resume from failed point

---

## Quick Reference: Estimated Timeline

| Phase | Task | Time |
|-------|------|------|
| 1 | AWS Setup | 30-60 min |
| 2 | Build Binaries | 30-60 min |
| 3 | Configure | 15 min |
| 4 | Python Setup | 5 min |
| 5 | Run Graph 7 | 60-90 min |
| 6 | Analyze Graph 7 | 10 min |
| 7 | Run Graph 8 | 60 min |
| 8 | Run Graph 9 | 60 min |
| 9 | Verify Results | 10 min |
| 10 | Cleanup | 5 min |
| **TOTAL** | **~5-6 hours** | |

---

## Success Criteria

You're done when you have:

- [ ] All 18 EC2 instances launched and configured
- [ ] Linux x86_64 binaries built
- [ ] All 3 config files updated with real IPs
- [ ] Graph 7 completed, knee identified
- [ ] Graph 8 completed with knee value
- [ ] Graph 9 completed with knee value
- [ ] All 3 graphs saved as PNG files
- [ ] Results show Juicer outperforming baseline
- [ ] EC2 instances terminated (to avoid charges)

**Congratulations! Your evaluation is complete!** 🎉

You now have publication-quality graphs showing Juicer's performance improvements over baseline CockroachDB.

---

## Next Steps After Completion

1. **Analyze Results**: Compare metrics, calculate improvement percentages
2. **Create Publication Figures**: Import PNGs into paper, add captions
3. **Write Results Section**: Describe findings, explain Juicer's benefits
4. **Optional**: Re-run with different parameters (e.g., 10M keyspace, different Zipf values)
5. **Share with Advisor**: Show graphs and discuss findings

Good luck! 🚀
