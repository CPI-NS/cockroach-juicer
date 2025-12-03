#!/usr/bin/env python3
"""
Automatically update AWS config files with instance IPs
"""

import subprocess
import json
import sys
import os
from pathlib import Path

def get_instances():
    """Get all running Juicer instances from AWS."""
    try:
        # Get server instances
        servers_output = subprocess.check_output([
            'aws', 'ec2', 'describe-instances',
            '--region', 'us-east-1',
            '--filters', 'Name=tag:Role,Values=server', 'Name=instance-state-name,Values=running',
            '--query', 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress,Placement.AvailabilityZone]',
            '--output', 'json'
        ]).decode('utf-8')

        servers = []
        for reservation in json.loads(servers_output):
            for instance in reservation:
                servers.append({
                    'name': instance[0],
                    'public_ip': instance[1],
                    'private_ip': instance[2],
                    'az': instance[3]
                })

        servers.sort(key=lambda x: x['name'])

        # Get client instances
        clients_output = subprocess.check_output([
            'aws', 'ec2', 'describe-instances',
            '--region', 'us-east-1',
            '--filters', 'Name=tag:Role,Values=client', 'Name=instance-state-name,Values=running',
            '--query', 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]',
            '--output', 'json'
        ]).decode('utf-8')

        clients = []
        for reservation in json.loads(clients_output):
            for instance in reservation:
                clients.append({
                    'name': instance[0],
                    'public_ip': instance[1],
                    'private_ip': instance[2]
                })

        clients.sort(key=lambda x: x['name'])

        return servers[:9], clients[:9]  # Take first 9 of each

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to query AWS: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

def update_config_file(config_file, servers, clients):
    """Update a config file with actual IPs."""
    print(f"\nUpdating {config_file}...")

    if not os.path.exists(config_file):
        print(f"  ⚠ File not found: {config_file}")
        return False

    with open(config_file, 'r') as f:
        content = f.read()

    # Replace server placeholders
    for i, server in enumerate(servers):
        # Replace hostname (public IP)
        content = content.replace(
            f'hostname: "PLACEHOLDER_SERVER_{i}_IP"',
            f'hostname: "{server["public_ip"]}"'
        )
        # Replace internal_ip (private IP)
        content = content.replace(
            f'internal_ip: "PLACEHOLDER_SERVER_{i}_PRIVATE_IP"',
            f'internal_ip: "{server["private_ip"]}"'
        )
        # Replace availability zone if present
        if 'az' in server:
            content = content.replace(
                f'availability_zone: "us-east-1x"  # Placeholder',
                f'availability_zone: "{server["az"]}"'
            )

    # Replace client placeholders
    for i, client in enumerate(clients):
        # Replace hostname (public IP)
        content = content.replace(
            f'hostname: "PLACEHOLDER_CLIENT_{i}_IP"',
            f'hostname: "{client["public_ip"]}"'
        )
        # Replace internal_ip (private IP)
        content = content.replace(
            f'internal_ip: "PLACEHOLDER_CLIENT_{i}_PRIVATE_IP"',
            f'internal_ip: "{client["private_ip"]}"'
        )

    # Update SSH key path
    ssh_key_path = str(Path.home() / ".ssh" / "aws-juicer-key.pem")
    content = content.replace(
        'ssh_key: "~/.ssh/aws-juicer-key.pem"',
        f'ssh_key: "{ssh_key_path}"'
    )

    # Write updated content
    with open(config_file, 'w') as f:
        f.write(content)

    print(f"  ✓ Updated successfully")
    return True

def main():
    print("=" * 60)
    print("AWS Config Updater for Juicer Evaluation")
    print("=" * 60)

    # Check AWS CLI is available
    try:
        subprocess.check_output(['aws', '--version'])
    except FileNotFoundError:
        print("\nERROR: AWS CLI not found. Please install it:")
        print("  brew install awscli")
        sys.exit(1)

    # Get instances
    print("\nFetching instance information from AWS...")
    servers, clients = get_instances()

    if len(servers) < 9:
        print(f"\n⚠ WARNING: Only found {len(servers)} servers (need 9)")
        print("Please ensure all server instances are running")
        sys.exit(1)

    if len(clients) < 9:
        print(f"\n⚠ WARNING: Only found {len(clients)} clients (need 9)")
        print("Please ensure all client instances are running")
        sys.exit(1)

    print(f"\n✓ Found {len(servers)} servers and {len(clients)} clients")

    # Show what we found
    print("\nServers:")
    for i, s in enumerate(servers):
        print(f"  [{i}] {s['name']}: {s['public_ip']} ({s['private_ip']}) - {s['az']}")

    print("\nClients:")
    for i, c in enumerate(clients):
        print(f"  [{i}] {c['name']}: {c['public_ip']} ({c['private_ip']})")

    # Update config files
    config_dir = Path(__file__).parent / "configs"
    config_files = [
        config_dir / "aws_eval_graph7_knee.yaml",
        config_dir / "aws_eval_graph8_throughput_zipf.yaml",
        config_dir / "aws_eval_graph9_abort_zipf.yaml"
    ]

    print("\n" + "=" * 60)
    print("Updating configuration files...")
    print("=" * 60)

    for config_file in config_files:
        update_config_file(str(config_file), servers, clients)

    print("\n" + "=" * 60)
    print("✅ ALL CONFIGS UPDATED!")
    print("=" * 60)
    print("\nYour config files are ready. Next steps:")
    print("1. Test SSH connection:")
    print(f"     ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@{servers[0]['public_ip']}")
    print("\n2. Mount EBS volumes on all servers:")
    print("     python3 mount_ebs_volumes.py")
    print("\n3. Build binaries (or use the build script)")
    print("\n4. Run experiments:")
    print("     python3 scripts/run_experiment.py configs/aws_eval_graph7_knee.yaml")
    print()

if __name__ == "__main__":
    main()
