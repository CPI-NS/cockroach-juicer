'#!/bin/bash
# Distribute binaries from build server to all other instances
# Run this ON the build server (54.208.110.229)

set -e

# Paths on remote instances
REMOTE_USER="ubuntu"
KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"  # AWS SSH key
COCKROACH_BIN="$HOME/cockroach-juicer/bin/cockroach"
BENCHMARK_BIN="$HOME/cockroach-juicer/bin/benchmark"

# All server IPs (excluding 54.208.110.229 which is this server)
SERVERS=(
    "34.204.67.217"
    "54.91.203.85"
    "18.222.126.3"
    "3.17.66.206"
    "18.118.106.162"
    "3.101.151.1"
    "54.219.238.16"
    "18.144.81.88"
)

# All client IPs
CLIENTS=(
    "44.211.216.86"
    "34.207.68.148"
    "44.201.75.196"
    "3.17.189.229"
    "18.222.218.34"
    "3.17.133.122"
    "13.57.41.216"
    "54.215.105.27"
    "54.219.155.93"
)

echo "========================================="
echo "Distributing binaries from build server"
echo "========================================="

# Check if binaries exist
if [ ! -f "$COCKROACH_BIN" ]; then
    echo "ERROR: cockroach binary not found at $COCKROACH_BIN"
    exit 1
fi

if [ ! -f "$BENCHMARK_BIN" ]; then
    echo "ERROR: benchmark binary not found at $BENCHMARK_BIN"
    exit 1
fi

echo "✓ Found cockroach binary"
echo "✓ Found benchmark binary"
echo ""

# Function to copy binaries to a remote host
copy_to_host() {
    local HOST=$1
    local ROLE=$2

    echo "========================================"
    echo "Copying binaries to $ROLE: $HOST"
    echo "========================================"

    # Copy cockroach binary (with progress)
    echo "  → Copying cockroach binary..."
    scp -v -i "$KEY_PATH" -o StrictHostKeyChecking=no "$COCKROACH_BIN" "${REMOTE_USER}@${HOST}:~/cockroach"
    echo "  ✓ cockroach binary copied"

    # Copy benchmark binary (with progress)
    echo "  → Copying benchmark binary..."
    scp -v -i "$KEY_PATH" -o StrictHostKeyChecking=no "$BENCHMARK_BIN" "${REMOTE_USER}@${HOST}:~/benchmark"
    echo "  ✓ benchmark binary copied"

    echo "  ✓✓ All binaries copied to $HOST"
    echo ""
}

# Distribute to all servers
echo "Distributing to servers..."
for SERVER in "${SERVERS[@]}"; do
    copy_to_host "$SERVER" "server"
done

echo ""
echo "Distributing to clients..."
for CLIENT in "${CLIENTS[@]}"; do
    copy_to_host "$CLIENT" "client"
done

echo ""
echo "========================================="
echo "✓ All binaries distributed successfully!"
echo "========================================="
