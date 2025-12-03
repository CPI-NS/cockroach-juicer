#!/usr/bin/env python3
"""
Automatically update AWS multi-region config files with instance IPs
"""

import subprocess
import json
import sys
import os
from pathlib import Path

def get_instances_from_region(region, role):
    """Get instances of a specific role from a specific region."""
    try:
        output = subprocess.check_output([
            'aws', 'ec2', 'describe-instances',
            '--region', region,
            '--filters', f'Name=tag:Role,Values={role}', 'Name=instance-state-name,Values=running',
            '--query', 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]',
            '--output', 'json'
        ]).decode('utf-8')

        instances = []
        for reservation in json.loads(output):
            for instance in reservation:
                instances.append({
                    'name': instance[0],
                    'public_ip': instance[1],
                    'private_ip': instance[2],
                    'region': region
                })

        instances.sort(key=lambda x: x['name'])
        return instances

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to query AWS region {region}: {e}")
        return []

def get_all_instances():
    """Get all server and client instances across all regions."""
    regions = ['us-east-1', 'us-east-2', 'us-west-1']

    servers = []
    clients = []

    for region in regions:
        servers.extend(get_instances_from_region(region, 'server'))
        clients.extend(get_instances_from_region(region, 'client'))

    return servers, clients

def main():
    print("=" * 60)
    print("AWS Multi-Region Config Updater")
    print("=" * 60)

    # Check AWS CLI
    try:
        subprocess.check_output(['aws', '--version'])
    except FileNotFoundError:
        print("\nERROR: AWS CLI not found")
        sys.exit(1)

    # Get instances
    print("\nFetching instance information from AWS...")
    servers, clients = get_all_instances()

    if len(servers) < 9:
        print(f"\n⚠ WARNING: Only found {len(servers)} servers (need 9)")
        print("Please ensure all server instances are running in all regions")
        sys.exit(1)

    if len(clients) < 9:
        print(f"\n⚠ WARNING: Only found {len(clients)} clients (need 9)")
        print("Please ensure all client instances are running in all regions")
        sys.exit(1)

    print(f"\n✓ Found {len(servers)} servers and {len(clients)} clients")

    # Show what we found
    print("\nServers:")
    for i, s in enumerate(servers[:9]):
        print(f"  [{i}] {s['name']}: {s['public_ip']} ({s['private_ip']}) - {s['region']}")

    print("\nClients:")
    for i, c in enumerate(clients[:9]):
        print(f"  [{i}] {c['name']}: {c['public_ip']} ({c['private_ip']}) - {c['region']}")

    # For multi-region, we need to create new config files
    # The existing placeholder-based approach won't work well

    print("\n" + "=" * 60)
    print("Creating multi-region configuration file")
    print("=" * 60)

    config_template = """# AWS Multi-Region Evaluation - Graph 7: Finding the Knee
# Auto-generated configuration file
#
# Architecture:
#   - 9 servers across 3 regions (us-east-1, us-east-2, us-west-1)
#   - 9 clients across 3 regions
#   - Multi-region CockroachDB cluster

experiment:
  name: "aws-multiregion-eval-graph7-knee"
  output_dir: "../results/aws_multiregion/graph7_knee"
  repeat_count: 5
  timeout_seconds: 3600

  deployment_mode: "remote"

remote:
  ssh_user: "ubuntu"
  ssh_key: "{ssh_key_path}"
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
  tx_count: [50000]
  ops_per_tx: [1]
  key_range: [1000000]
  distribution: ["zipfian"]
  zipfian_s: [0.99]
  zipfian_v: [1.0]
  read_write_ratio: [0.0]

concurrency:
  num_clients: [9]
  workers_per_client: [1, 2, 4, 8, 12, 16, 24, 32]

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
  num_keys: 1000000
  key_prefix: "key"
  key_range: 1000000
  batch_size: 10000
  concurrent: 16
  use_bulk: true
  use_hash_keys: true
"""

    # Generate server YAML
    servers_yaml = ""
    for i, server in enumerate(servers[:9]):
        az_map = {
            'us-east-1': 'us-east-1a',
            'us-east-2': 'us-east-2a',
            'us-west-1': 'us-west-1a'
        }
        servers_yaml += f"""      - hostname: "{server['public_ip']}"
        ssh_port: 22
        internal_ip: "{server['private_ip']}"
        role: "cockroach"
        node_id: {i + 1}
        availability_zone: "{az_map.get(server['region'], server['region'])}"
        region: "{server['region']}"

"""

    # Generate client YAML
    clients_yaml = ""
    for i, client in enumerate(clients[:9]):
        clients_yaml += f"""      - hostname: "{client['public_ip']}"
        ssh_port: 22
        internal_ip: "{client['private_ip']}"
        role: "benchmark"
        region: "{client['region']}"

"""

    # Fill in the template
    ssh_key_path = str(Path.home() / ".ssh" / "aws-juicer-key.pem")
    config_content = config_template.format(
        ssh_key_path=ssh_key_path,
        servers_yaml=servers_yaml.rstrip(),
        clients_yaml=clients_yaml.rstrip()
    )

    # Write config file
    config_dir = Path(__file__).parent / "configs"
    config_file = config_dir / "aws_multiregion_graph7_knee.yaml"

    with open(config_file, 'w') as f:
        f.write(config_content)

    print(f"\n✓ Created: {config_file}")

    print("\n" + "=" * 60)
    print("✅ CONFIGURATION COMPLETE!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Mount EBS volumes on all servers:")
    print("     python3 mount_ebs_volumes_multiregion.py")
    print("\n2. Build binaries (on one server):")
    print(f"     ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@{servers[0]['public_ip']}")
    print("     # Then follow build instructions")
    print("\n3. Run experiments:")
    print("     python3 scripts/run_experiment.py configs/aws_multiregion_graph7_knee.yaml")
    print()

if __name__ == "__main__":
    main()
