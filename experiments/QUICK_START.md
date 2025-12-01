# Quick Start: Running CockroachDB Juicer Evaluation

## TL;DR

Run these three experiments to generate the required graphs for your paper:

```bash
# 1. Find the knee (optimal client count)
python3 ./scripts/run_experiment.py config configs/eval_graph7_knee.yaml

# 2. Throughput vs Zipf (after updating knee value in config)
python3 ./scripts/run_experiment.py config configs/eval_graph8_throughput_zipf.yaml

# 3. Abort rate vs Zipf (reuses same data as #2)
python3 ./scripts/run_experiment.py config configs/eval_graph9_abort_zipf.yaml
```

---

## Prerequisites

### 1. Build Binaries Locally
```bash
cd /path/to/cockroach-juicer
./dev build cockroach
./dev build benchmark
```

### 2. Set Up AWS EC2 Instances

#### Option A: Manual Setup (without disk image)
1. Launch 18 EC2 instances in us-east-1:
   - 9 × `m5.2xlarge` (servers)
   - 9 × `c5.2xlarge` (clients)
2. Configure security groups (ports 22, 26257, 8080)
3. Note down public hostnames and private IPs

#### Option B: Using Disk Image (recommended)
1. Create disk image with pre-built binaries (see professor's instructions)
2. Launch instances from custom image
3. Skip binary deployment step

### 3. Update Config Files

Edit the YAML configs with your EC2 instance details:

**configs/eval_graph7_knee.yaml**:
```yaml
remote:
  ssh_user: "ubuntu"
  ssh_key: "~/.ssh/your-aws-key.pem"

  nodes:
    servers:
      - hostname: "ec2-XX-XX-XX-XX.compute-1.amazonaws.com"  # Your actual hostname
        internal_ip: "10.0.1.10"  # Your VPC private IP
        role: "cockroach"
        node_id: 1
      # ... repeat for all 9 servers

    clients:
      - hostname: "ec2-YY-YY-YY-YY.compute-1.amazonaws.com"
        internal_ip: "10.0.1.20"
        role: "benchmark"
      # ... add more clients as needed
```

Repeat for `eval_graph8_throughput_zipf.yaml` and `eval_graph9_abort_zipf.yaml`.

---

## Execution Steps

### Step 1: Find the Knee (Graph 7)

This experiment sweeps client counts to find system saturation point.

```bash
cd /path/to/cockroach-juicer/experiments
python3 ./scripts/run_experiment.py config configs/eval_graph7_knee.yaml
```

**What it does**:
- Deploys binaries to all 9 servers (if `deploy_binaries: true`)
- Starts 9-node CockroachDB cluster
- Initializes 10M keys
- Runs benchmark with [1, 2, 4, 8, 16, 32, 64, 128, 256] workers
- Measures latency and throughput for each configuration
- Compares baseline vs Juicer

**Expected duration**: ~3-4 hours (9 client counts × 2 Juicer configs × 3 trials)

**Output**:
```
results/eval_graph7_knee/
├── summary_YYYYMMDD_HHMMSS.txt
├── results_YYYYMMDD_HHMMSS.json
└── plots/
    ├── latency_p50.png
    ├── throughput.png
    └── abort_rate.png
```

**Identify the knee**:
1. Open `results/eval_graph7_knee/plots/latency_p50.png`
2. Find where latency curve starts increasing sharply
3. Read corresponding throughput value (should be ~75-80% of peak)
4. Note the worker count at that point (e.g., 64 workers)

---

### Step 2: Update Configs with Knee Value

Open `configs/eval_graph8_throughput_zipf.yaml` and `configs/eval_graph9_abort_zipf.yaml`:

```yaml
concurrency:
  num_clients: [1]
  workers_per_client: [64]  # <-- UPDATE THIS with knee value from Step 1
```

---

### Step 3: Run Zipf Sweep (Graphs 8 & 9)

Now run experiments at the optimal client count (knee) while varying contention (Zipf).

```bash
# Throughput vs Zipf
python3 ./scripts/run_experiment.py config configs/eval_graph8_throughput_zipf.yaml

# Abort Rate vs Zipf (can reuse same results)
python3 ./scripts/run_experiment.py config configs/eval_graph9_abort_zipf.yaml
```

**What it does**:
- Runs benchmark with Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
- Uses optimal client count from Graph 7
- Measures throughput and abort rate for each Zipf value
- Compares baseline vs Juicer

**Expected duration**: ~2 hours (6 Zipf values × 2 Juicer configs × 3 trials)

**Output**:
```
results/eval_graph8_throughput/
├── summary_YYYYMMDD_HHMMSS.txt
├── results_YYYYMMDD_HHMMSS.json
└── plots/
    ├── throughput.png
    └── abort_rate.png
```

---

## Interpreting Results

### Graph 7: Latency vs. Throughput

**X-axis**: Throughput (ops/sec)
**Y-axis**: Median latency (ms)
**Lines**: Baseline (blue) vs Juicer (orange)

**What to look for**:
- **Knee point**: Where latency starts increasing rapidly
- **Juicer benefit**: Orange line should show higher throughput at same latency
- **Optimal client count**: Worker count at knee point (~75-80% peak throughput)

**Example**:
```
Workers    Throughput    Latency P50
   16        45K ops/s       5 ms     <- Low utilization
   32        75K ops/s       8 ms
   64        95K ops/s      12 ms     <- KNEE (80% of peak)
  128       105K ops/s      45 ms     <- Saturated
  256       110K ops/s     120 ms     <- Over-saturated
```

---

### Graph 8: Throughput vs. Zipf

**X-axis**: Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
**Y-axis**: Throughput (ops/sec)
**Bars**: Baseline (blue) vs Juicer (orange), grouped

**What to look for**:
- **Low Zipf (0.1)**: Uniform access → high throughput (little contention)
- **High Zipf (1.2)**: Hot keys → low throughput (high contention)
- **Juicer benefit**: Orange bars taller at high Zipf values

**Expected trend**:
```
Zipf    Baseline    Juicer    Improvement
0.1      100K        102K         +2%
0.4       95K         98K         +3%
0.6       85K         92K         +8%
0.8       70K         82K        +17%
0.99      50K         68K        +36%  <- Juicer shines here!
1.2       40K         58K        +45%
```

---

### Graph 9: Abort Rate vs. Zipf

**X-axis**: Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
**Y-axis**: Abort rate (%)
**Bars**: Baseline (blue) vs Juicer (orange), grouped

**What to look for**:
- **Low Zipf**: Low abort rate (~1-5%)
- **High Zipf**: High abort rate (20-60%)
- **Juicer benefit**: Orange bars shorter (lower abort rate)

**Expected trend**:
```
Zipf    Baseline    Juicer    Improvement
0.1         2%        2%           0%
0.4         5%        4%         -20%
0.6        12%        8%         -33%
0.8        25%       15%         -40%
0.99       45%       22%         -51%  <- Major benefit!
1.2        60%       30%         -50%
```

---

## Troubleshooting

### Issue: "Failed to connect to remote host"
**Solution**: Check security group allows SSH (port 22) from your IP

### Issue: "Cluster did not become ready within timeout"
**Solution**: Check security group allows 26257 within VPC internal IPs

### Issue: All throughput results are zero
**Solution**: Ensure benchmark binary was rebuilt after code changes

### Issue: Very high abort rates (>90%)
**Solution**:
- Check if data was initialized correctly (10M keys)
- Verify Zipf parameter is being used correctly
- May need to increase timeout or reduce client count

### Issue: AWS charges are high
**Solution**:
- Use spot instances (70% cheaper)
- Terminate instances after experiments
- Use smaller instance types for testing (t3.medium)

---

## Cost Optimization

### Use Spot Instances
```bash
# Request spot instances instead of on-demand
# Saves ~70% on costs
aws ec2 request-spot-instances --instance-type m5.2xlarge ...
```

### Run Smaller Test First
Before full evaluation, test with smaller scale:
```yaml
cluster:
  num_nodes: 3  # Instead of 9

data_init:
  num_keys: 100000  # Instead of 10M

workload:
  tx_count: [1000]  # Instead of 10000
```

### Terminate After Use
```bash
# Stop all instances when done
aws ec2 stop-instances --instance-ids i-xxx i-yyy ...

# Or terminate to avoid storage costs
aws ec2 terminate-instances --instance-ids i-xxx i-yyy ...
```

---

## Timeline Estimate

| Task | Duration | Cost |
|------|----------|------|
| Setup EC2 instances | 1 hour | $7 |
| Graph 7 (knee finding) | 3-4 hours | $26 |
| Graph 8 & 9 (Zipf sweep) | 2 hours | $13 |
| **Total** | **~7 hours** | **~$46** |

*(Based on 9 × m5.2xlarge servers + 9 × c5.2xlarge clients)*

---

## Next Steps After Experiments

1. **Analyze results**: Look at summary files and identify trends
2. **Generate publication graphs**: Use matplotlib to create camera-ready figures
3. **Write paper section**: Describe experimental setup, results, and insights
4. **Prepare for questions**: Understand why Juicer helps at high contention

---

## Getting Help

If you encounter issues:

1. Check logs in `results/*/logs/`
2. Look at stderr files for benchmark runs
3. Verify AWS security groups and network configuration
4. Test locally first with 1-3 nodes before scaling to 9

For framework issues, see `experiments/EVALUATION_PLAN.md` for detailed implementation notes.
