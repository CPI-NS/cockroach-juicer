#!/bin/bash
# Install required libraries on all AWS instances

set -e

KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"
REMOTE_USER="ubuntu"

# All instance IPs (servers + clients)
INSTANCES=(
    # Servers
    "34.204.67.217"
    "54.208.110.229"
    "54.91.203.85"
    "18.222.126.3"
    "3.17.66.206"
    "18.118.106.162"
    "3.101.151.1"
    "54.219.238.16"
    "18.144.81.88"
    # Clients
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
echo "Installing dependencies on all instances"
echo "========================================="

install_on_host() {
    local HOST=$1

    echo ""
    echo "Installing on $HOST..."

    ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${HOST}" \
        'sudo apt-get update -qq && sudo apt-get install -y -qq libc6 libgcc-s1 libstdc++6 2>/dev/null' \
        && echo "  ✓ Dependencies installed on $HOST" \
        || echo "  ✗ Failed to install on $HOST"
}

for INSTANCE in "${INSTANCES[@]}"; do
    install_on_host "$INSTANCE" &
done

# Wait for all background jobs to complete
wait

echo ""
echo "========================================="
echo "✓ All instances updated!"
echo "========================================="
