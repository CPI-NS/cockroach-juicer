#!/usr/bin/env python3
"""
Update single-region config with newly created instance IPs
"""

import subprocess
import json
import sys
from pathlib import Path

def get_instances_by_tag(tag_key, tag_value):
    """Get instances matching a tag."""
    try:
        output = subprocess.check_output([
            'aws', 'ec2', 'describe-instances',
            '--region', 'us-east-1',
            '--filters',
            f'Name=tag:{tag_key},Values={tag_value}',
            'Name=instance-state-name,Values=running',
            '--query', 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress,PrivateIpAddress]',
            '--output', 'json'
        ]).decode('utf-8')

        instances = []
        for reservation in json.loads(output):
            for instance in reservation:
                instances.append({
                    'instance_id': instance[0],
                    'public_ip': instance[1],
                    'private_ip': instance[2]
                })

        return instances

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to query AWS: {e}")
        return []

def main():
    print("=" * 60)
    print("Single-Region Config Updater")
    print("=" * 60)

    # Get server and client instances
    print("\nFetching instance information from AWS...")
    servers = get_instances_by_tag('Experiment', 'singleregion')
    servers = [s for s in servers if 'server' in subprocess.check_output([
        'aws', 'ec2', 'describe-instances',
        '--instance-ids', s['instance_id'],
        '--query', 'Reservations[0].Instances[0].Tags[?Key==`Name`].Value',
        '--output', 'text'
    ]).decode('utf-8').lower()]

    clients = get_instances_by_tag('Experiment', 'singleregion')
    clients = [c for c in clients if 'client' in subprocess.check_output([
        'aws', 'ec2', 'describe-instances',
        '--instance-ids', c['instance_id'],
        '--query', 'Reservations[0].Instances[0].Tags[?Key==`Name`].Value',
        '--output', 'text'
    ]).decode('utf-8').lower()]

    if len(servers) < 9:
        print(f"\n⚠ WARNING: Only found {len(servers)} servers (need 9)")
        sys.exit(1)

    if len(clients) < 9:
        print(f"\n⚠ WARNING: Only found {len(clients)} clients (need 9)")
        sys.exit(1)

    print(f"\n✓ Found {len(servers)} servers and {len(clients)} clients")

    # Generate server YAML
    servers_yaml = ""
    for i, server in enumerate(servers[:9]):
        servers_yaml += f"""      - hostname: "{server['public_ip']}"
        ssh_port: 22
        internal_ip: "{server['private_ip']}"
        role: "cockroach"
        node_id: {i + 1}
        availability_zone: "us-east-1a"
        region: "us-east-1"

"""

    # Generate client YAML
    clients_yaml = ""
    for i, client in enumerate(clients[:9]):
        clients_yaml += f"""      - hostname: "{client['public_ip']}"
        ssh_port: 22
        internal_ip: "{client['private_ip']}"
        role: "benchmark"
        region: "us-east-1"

"""

    config_template = """# AWS Single-Region Evaluation - Graph 7: Finding the Knee (Open-Loop)
# Auto-generated configuration file
#
# This configuration runs an open-loop benchmark to find the system's saturation point.
# Open-loop mode sends requests at a fixed arrival rate, independent of system response time.
#
# Architecture:
#   - 9 servers in us-east-1
#   - 9 clients in us-east-1
#   - Single-region CockroachDB cluster with replication factor 3

experiment:
  name: "aws-singleregion-eval-graph7-knee-openloop"
  output_dir: "../results/aws_singleregion/graph7_knee_openloop"
  repeat_count: 3            # Run each configuration 3 times for statistical smoothness
  timeout_seconds: 7200      # 2 hours timeout

  deployment_mode: "remote"

remote:
  ssh_user: "ubuntu"
  ssh_key: "/home/ubuntu/.ssh/aws-juicer-key.pem"
  ssh_host_base: ""

  nodes:
    servers:
{servers_yaml}
    clients:
{clients_yaml}
  remote_paths:
    cockroach_bin: "/home/ubuntu/cockroach"
    benchmark_bin: "/home/ubuntu/benchmark"
    data_dir: "/mnt/data/cockroach-data"
    log_dir: "/mnt/data/logs"

  deploy_binaries: true
  local_build_dir: "../../cockroach-juicer/"

workload:
  # Open-loop mode settings
  open_loop: true
  target_rate: [500, 1000, 2000, 3000, 4000, 5000, 6000, 8000, 10000]  # Sweep arrival rate to find knee
  duration_seconds: 60       # 60 second runs for stable measurements
  warmup_percent: 0.25       # 15s warmup
  cooldown_percent: 0.25     # 15s cooldown
  openloop_inflight: 200     # Max 200 concurrent in-flight transactions per client

  # Workload characteristics
  ops_per_tx: [1]
  key_range: [1000000]
  distribution: ["zipfian"]
  zipfian_s: [0.99]          # Zipfian theta parameter (skew)
  read_write_ratio: [0.0]    # 100% writes for maximum contention

concurrency:
  num_clients: [9]           # 9 clients in single region
  workers_per_client: [1]    # Not used in open-loop, but required

juicer:
  enabled: [false, true]
  flush_time_us: [100]

protocol:
  type: ["MVCC"]

cluster:
  num_nodes: 9
  base_port: 26257
  base_http_port: 8080
  store_size: "100GB"

data_init:
  enabled: true
  num_keys: 1000000          # 1M keys
  key_prefix: "key"
  key_range: 1000000
  batch_size: 10000
  concurrent: 4              # 4 concurrent workers for faster init
  use_bulk: false
  use_hash_keys: true
  init_timeout_seconds: 1800  # 30 minutes for 1M keys
"""

    # Fill in template
    config_content = config_template.format(
        servers_yaml=servers_yaml.rstrip(),
        clients_yaml=clients_yaml.rstrip()
    )

    # Write config file
    config_dir = Path(__file__).parent.parent.parent / "configs"
    config_file = config_dir / "aws_singleregion_graph7_knee.yaml"

    with open(config_file, 'w') as f:
        f.write(config_content)

    print(f"\n✓ Updated: {config_file}")

    print("\n" + "=" * 60)
    print("✅ CONFIGURATION COMPLETE!")
    print("=" * 60)
    print("\nServer IPs:")
    for i, s in enumerate(servers[:9]):
        print(f"  [{i+1}] {s['public_ip']} ({s['private_ip']})")

    print("\nClient IPs:")
    for i, c in enumerate(clients[:9]):
        print(f"  [{i+1}] {c['public_ip']} ({c['private_ip']})")

    print("\nNext steps:")
    print("1. Deploy binaries (from control node):")
    print("   python3 scripts/run_experiment.py configs/aws_singleregion_graph7_knee.yaml")
    print()

if __name__ == "__main__":
    main()
