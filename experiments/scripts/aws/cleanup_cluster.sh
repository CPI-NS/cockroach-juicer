#!/bin/bash
# Clean up CockroachDB cluster on all AWS instances

set -e

KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"
REMOTE_USER="ubuntu"

# All server IPs
SERVERS=(
    "34.204.67.217"
    "54.208.110.229"
    "54.91.203.85"
    "18.222.126.3"
    "3.17.66.206"
    "18.118.106.162"
    "3.101.151.1"
    "54.219.238.16"
    "18.144.81.88"
)

echo "========================================="
echo "Cleaning up CockroachDB cluster"
echo "========================================="

cleanup_on_host() {
    local HOST=$1

    echo "Cleaning up $HOST..."

    ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${HOST}" \
        'pkill -9 cockroach 2>/dev/null || true; rm -rf cockroach-data logs 2>/dev/null || true' \
        && echo "  ✓ Cleaned $HOST" \
        || echo "  ✗ Failed to clean $HOST"
}

for SERVER in "${SERVERS[@]}"; do
    cleanup_on_host "$SERVER" &
done

# Wait for all background jobs
wait

echo ""
echo "========================================="
echo "✓ Cluster cleaned up!"
echo "========================================="
echo ""
echo "You can now run the experiment fresh:"
echo "  python scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml --skip-deploy"
echo ""
