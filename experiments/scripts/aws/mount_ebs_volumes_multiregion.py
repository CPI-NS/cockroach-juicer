#!/usr/bin/env python3
"""
Mount EBS volumes on all server instances across multiple regions
"""

import subprocess
import json
import sys
import time

def get_server_ips_from_region(region):
    """Get public IPs of server instances in a specific region."""
    try:
        output = subprocess.check_output([
            'aws', 'ec2', 'describe-instances',
            '--region', region,
            '--filters', 'Name=tag:Role,Values=server', 'Name=instance-state-name,Values=running',
            '--query', 'Reservations[*].Instances[*].[PublicIpAddress,Tags[?Key==`Name`].Value|[0]]',
            '--output', 'json'
        ]).decode('utf-8')

        servers = []
        for reservation in json.loads(output):
            for instance in reservation:
                if instance[0]:
                    servers.append({
                        'ip': instance[0],
                        'name': instance[1],
                        'region': region
                    })

        return sorted(servers, key=lambda x: x['name'])

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to query region {region}: {e}")
        return []

def get_all_servers():
    """Get all server instances across all regions."""
    regions = ['us-east-1', 'us-east-2', 'us-west-1']
    all_servers = []

    for region in regions:
        servers = get_server_ips_from_region(region)
        all_servers.extend(servers)

    return all_servers

def mount_ebs_on_server(server, key_path):
    """Mount EBS volume on a single server."""
    ip = server['ip']
    name = server['name']
    region = server['region']

    print(f"\n[{name}] Mounting EBS volume on {ip} ({region})...")

    mount_script = """
#!/bin/bash
set -e

# Check if already mounted
if mountpoint -q /mnt/data; then
    echo "Already mounted"
    df -h /mnt/data
    exit 0
fi

# Find the unmounted disk (usually nvme1n1 or xvdf)
DISK=$(lsblk -ndo NAME,TYPE,MOUNTPOINT | awk '$2=="disk" && $3=="" {print "/dev/"$1}' | head -1)

if [ -z "$DISK" ]; then
    echo "No unmounted disk found"
    lsblk
    exit 1
fi

echo "Found disk: $DISK"

# Check if disk has filesystem
if ! sudo file -sL $DISK | grep -q filesystem; then
    echo "Formatting $DISK..."
    sudo mkfs.ext4 -F $DISK
fi

# Create mount point
sudo mkdir -p /mnt/data

# Mount
echo "Mounting $DISK to /mnt/data..."
sudo mount $DISK /mnt/data

# Set ownership
sudo chown -R ubuntu:ubuntu /mnt/data

# Add to fstab
if ! grep -q "/mnt/data" /etc/fstab; then
    echo "$DISK /mnt/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
fi

# Create directories
mkdir -p /mnt/data/cockroach-data
mkdir -p /mnt/data/logs

echo "Success!"
df -h /mnt/data
"""

    try:
        # Copy script to server
        with open('/tmp/mount_ebs.sh', 'w') as f:
            f.write(mount_script)

        subprocess.run([
            'scp', '-i', key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            '-o', 'ConnectTimeout=10',
            '/tmp/mount_ebs.sh',
            f'ubuntu@{ip}:~/'
        ], check=True, capture_output=True, timeout=30)

        # Execute script on server
        result = subprocess.run([
            'ssh', '-i', key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            '-o', 'ConnectTimeout=10',
            f'ubuntu@{ip}',
            'bash ~/mount_ebs.sh'
        ], capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            print(f"  ✓ Success")
            if "Filesystem" in result.stdout:
                # Show the df output line
                for line in result.stdout.split('\n'):
                    if '/mnt/data' in line:
                        print(f"    {line}")
            return True
        else:
            print(f"  ✗ Failed")
            if result.stderr:
                for line in result.stderr.strip().split('\n')[:3]:
                    print(f"    {line}")
            return False

    except subprocess.TimeoutExpired:
        print(f"  ✗ Timeout")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

def main():
    print("=" * 60)
    print("Multi-Region EBS Volume Mounting")
    print("=" * 60)

    # Check AWS CLI
    try:
        subprocess.check_output(['aws', '--version'])
    except FileNotFoundError:
        print("\nERROR: AWS CLI not found")
        sys.exit(1)

    # Check SSH key
    import os
    key_path = os.path.expanduser('~/.ssh/aws-juicer-key.pem')
    if not os.path.exists(key_path):
        print(f"\nERROR: SSH key not found at {key_path}")
        sys.exit(1)

    # Get all servers
    print("\nFetching server IPs from all regions...")
    servers = get_all_servers()

    if len(servers) < 9:
        print(f"\n⚠ WARNING: Only found {len(servers)} servers (expected 9)")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)

    print(f"\n✓ Found {len(servers)} servers")
    for server in servers:
        print(f"  - {server['name']}: {server['ip']} ({server['region']})")

    # Wait for SSH to be ready
    print("\nWaiting 30 seconds for SSH to be ready...")
    time.sleep(30)

    # Mount EBS on each server
    print("\n" + "=" * 60)
    print("Mounting EBS volumes on all servers...")
    print("=" * 60)

    success_count = 0
    failed_servers = []

    for i, server in enumerate(servers, 1):
        print(f"\n[{i}/{len(servers)}]", end=" ")
        if mount_ebs_on_server(server, key_path):
            success_count += 1
        else:
            failed_servers.append(server)

    # Summary
    print("\n" + "=" * 60)
    if success_count == len(servers):
        print("✅ ALL VOLUMES MOUNTED SUCCESSFULLY!")
    else:
        print(f"⚠ {success_count}/{len(servers)} volumes mounted")
        if failed_servers:
            print("\nFailed servers:")
            for server in failed_servers:
                print(f"  - {server['name']}: {server['ip']} ({server['region']})")
    print("=" * 60)

    print("\nNext steps:")
    print("1. Build binaries on one server:")
    print(f"     ssh -i {key_path} ubuntu@{servers[0]['ip']}")
    print("     cd ~ && git clone <your-repo>")
    print("     cd cockroach-juicer && ./dev build cockroach && ./dev build benchmark")
    print("\n2. Copy binaries to your local machine")
    print("\n3. Run experiments:")
    print("     python3 scripts/run_experiment.py configs/aws_multiregion_graph7_knee.yaml")
    print()

if __name__ == "__main__":
    main()
