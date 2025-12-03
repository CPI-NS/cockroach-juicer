#!/usr/bin/env python3
"""
Smart EBS volume mounting - finds the correct unmounted disk
"""

import subprocess
import json
import sys
import time

def get_all_servers():
    """Get all server instances across all regions."""
    regions = ['us-east-1', 'us-east-2', 'us-west-1']
    all_servers = []

    for region in regions:
        try:
            output = subprocess.check_output([
                'aws', 'ec2', 'describe-instances',
                '--region', region,
                '--filters', 'Name=tag:Role,Values=server', 'Name=instance-state-name,Values=running',
                '--query', 'Reservations[*].Instances[*].[PublicIpAddress,Tags[?Key==`Name`].Value|[0]]',
                '--output', 'json'
            ], timeout=30).decode('utf-8')

            for reservation in json.loads(output):
                for instance in reservation:
                    if instance[0]:
                        all_servers.append({
                            'ip': instance[0],
                            'name': instance[1],
                            'region': region
                        })
        except Exception as e:
            print(f"WARNING: Failed to query {region}: {e}")

    return sorted(all_servers, key=lambda x: x['name'])

def mount_ebs_on_server(server, key_path):
    """Mount EBS volume on a single server with smart disk detection."""
    ip = server['ip']
    name = server['name']
    region = server['region']

    print(f"\n[{name}] {ip} ({region})...")

    # Smarter mount script that finds the 100GB disk
    mount_script = """
#!/bin/bash
set -e

# Check if already mounted
if mountpoint -q /mnt/data 2>/dev/null; then
    echo "Already mounted:"
    df -h /mnt/data
    exit 0
fi

# Find the 100GB disk (not the root disk)
# Root disk is usually 30GB, data disk is 100GB
echo "Looking for 100GB data disk..."

# List all disks with size
DISKS=$(lsblk -ndo NAME,SIZE,TYPE | awk '$3=="disk" {print $1":"$2}')

TARGET_DISK=""
for DISK_INFO in $DISKS; do
    DISK=$(echo $DISK_INFO | cut -d: -f1)
    SIZE=$(echo $DISK_INFO | cut -d: -f2)

    # Check if it's approximately 100G (could be 99.9G, 100G, etc.)
    if echo "$SIZE" | grep -qE '^(9[5-9]|10[0-5])'; then
        # Check if not mounted
        if ! lsblk /dev/$DISK -no MOUNTPOINT | grep -q .; then
            TARGET_DISK="/dev/$DISK"
            echo "Found data disk: $TARGET_DISK ($SIZE)"
            break
        fi
    fi
done

if [ -z "$TARGET_DISK" ]; then
    echo "ERROR: Could not find 100GB unmounted disk"
    lsblk
    exit 1
fi

# Check if disk has filesystem
if ! sudo file -sL $TARGET_DISK | grep -q filesystem; then
    echo "Formatting $TARGET_DISK..."
    sudo mkfs.ext4 -F $TARGET_DISK
else
    echo "Filesystem already exists on $TARGET_DISK"
fi

# Create mount point
sudo mkdir -p /mnt/data

# Mount
echo "Mounting $TARGET_DISK to /mnt/data..."
sudo mount $TARGET_DISK /mnt/data

# Set ownership
sudo chown -R ubuntu:ubuntu /mnt/data

# Add to fstab if not already there
if ! grep -q "/mnt/data" /etc/fstab; then
    echo "$TARGET_DISK /mnt/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
fi

# Create directories
mkdir -p /mnt/data/cockroach-data
mkdir -p /mnt/data/logs

echo "Success!"
df -h /mnt/data
"""

    try:
        # Copy script
        with open('/tmp/mount_ebs_smart.sh', 'w') as f:
            f.write(mount_script)

        result = subprocess.run([
            'scp', '-i', key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            '-o', 'ConnectTimeout=15',
            '/tmp/mount_ebs_smart.sh',
            f'ubuntu@{ip}:~/'
        ], capture_output=True, timeout=30)

        if result.returncode != 0:
            print(f"  ✗ SCP failed: {result.stderr.decode()[:100]}")
            return False

        # Execute script
        result = subprocess.run([
            'ssh', '-i', key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            '-o', 'ConnectTimeout=15',
            f'ubuntu@{ip}',
            'bash ~/mount_ebs_smart.sh'
        ], capture_output=True, text=True, timeout=90)

        if result.returncode == 0:
            print(f"  ✓ Success")
            # Show the df line
            for line in result.stdout.split('\n'):
                if '/mnt/data' in line and 'Filesystem' not in line:
                    print(f"    {line.strip()}")
            return True
        else:
            print(f"  ✗ Failed")
            # Show first few lines of error
            for line in result.stderr.split('\n')[:3]:
                if line.strip():
                    print(f"    {line.strip()}")
            return False

    except subprocess.TimeoutExpired:
        print(f"  ✗ Timeout (SSH not responding)")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

def main():
    print("=" * 60)
    print("Smart EBS Volume Mounting")
    print("=" * 60)

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

    print(f"\n✓ Found {len(servers)} servers")

    # Wait for SSH
    print("\nWaiting 20 seconds for SSH to be ready...")
    time.sleep(20)

    # Mount EBS on each server
    print("\n" + "=" * 60)
    print("Mounting EBS volumes...")
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
        print("✅ ALL VOLUMES MOUNTED!")
    else:
        print(f"⚠ {success_count}/{len(servers)} volumes mounted")
        if failed_servers:
            print("\nFailed servers (you can mount these manually):")
            for server in failed_servers:
                print(f"  ssh -i {key_path} ubuntu@{server['ip']}")
    print("=" * 60)

    if success_count >= 6:
        print("\n✓ At least 6 servers mounted - you can proceed!")
        print("\nNext steps:")
        print("1. Build binaries on a working server")
        print("2. Copy binaries to your local machine")
        print("3. Run experiments")
    else:
        print("\nNeed at least 6 working servers to proceed.")

    print()

if __name__ == "__main__":
    main()
