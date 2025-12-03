#!/usr/bin/env python3
"""
Automatically mount EBS volumes on all server instances
"""

import subprocess
import json
import sys
import time

def get_server_ips():
    """Get public IPs of all server instances."""
    try:
        output = subprocess.check_output([
            'aws', 'ec2', 'describe-instances',
            '--region', 'us-east-1',
            '--filters', 'Name=tag:Role,Values=server', 'Name=instance-state-name,Values=running',
            '--query', 'Reservations[*].Instances[*].[PublicIpAddress]',
            '--output', 'json'
        ]).decode('utf-8')

        ips = []
        for reservation in json.loads(output):
            for instance in reservation:
                if instance[0]:
                    ips.append(instance[0])

        return sorted(ips)

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to query AWS: {e}")
        sys.exit(1)

def mount_ebs_on_server(ip, key_path):
    """Mount EBS volume on a single server."""
    print(f"\nMounting EBS volume on {ip}...")

    mount_script = """
#!/bin/bash
set -e

# Check if already mounted
if mountpoint -q /mnt/data; then
    echo "Already mounted"
    exit 0
fi

# Find the unmounted disk (usually nvme1n1)
DISK=$(lsblk -ndo NAME,TYPE,MOUNTPOINT | awk '$2=="disk" && $3=="" {print "/dev/"$1}' | head -1)

if [ -z "$DISK" ]; then
    echo "No unmounted disk found"
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
            '/tmp/mount_ebs.sh',
            f'ubuntu@{ip}:~/'
        ], check=True, capture_output=True)

        # Execute script on server
        result = subprocess.run([
            'ssh', '-i', key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            f'ubuntu@{ip}',
            'bash ~/mount_ebs.sh'
        ], capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            print(f"  ✓ Success: {ip}")
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    print(f"    {line}")
            return True
        else:
            print(f"  ✗ Failed: {ip}")
            if result.stderr:
                print(f"    Error: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print(f"  ✗ Timeout: {ip}")
        return False
    except Exception as e:
        print(f"  ✗ Error: {ip} - {e}")
        return False

def main():
    print("=" * 60)
    print("EBS Volume Mounting Script")
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

    # Get server IPs
    print("\nFetching server IPs from AWS...")
    server_ips = get_server_ips()

    if len(server_ips) < 9:
        print(f"\n⚠ WARNING: Only found {len(server_ips)} servers (expected 9)")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)

    print(f"\n✓ Found {len(server_ips)} servers")

    # Wait for SSH to be ready
    print("\nWaiting for SSH to be ready on all servers (30 seconds)...")
    time.sleep(30)

    # Mount EBS on each server
    print("\n" + "=" * 60)
    print("Mounting EBS volumes...")
    print("=" * 60)

    success_count = 0
    for i, ip in enumerate(server_ips, 1):
        print(f"\n[{i}/{len(server_ips)}] Processing {ip}...")
        if mount_ebs_on_server(ip, key_path):
            success_count += 1

    # Summary
    print("\n" + "=" * 60)
    if success_count == len(server_ips):
        print("✅ ALL VOLUMES MOUNTED SUCCESSFULLY!")
    else:
        print(f"⚠ {success_count}/{len(server_ips)} volumes mounted successfully")
    print("=" * 60)

    print("\nNext steps:")
    print("1. Verify mounts by SSH to a server:")
    print(f"     ssh -i {key_path} ubuntu@{server_ips[0]}")
    print("     df -h /mnt/data")
    print("\n2. Build binaries (on one server or locally with Docker)")
    print("\n3. Run experiments")
    print()

if __name__ == "__main__":
    main()
