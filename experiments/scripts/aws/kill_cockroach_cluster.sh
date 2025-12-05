#!/bin/bash
# Kill all CockroachDB processes and clean up data on all servers

set -e

echo "========================================="
echo "Stopping CockroachDB Cluster"
echo "========================================="
echo ""

# Server public IPs from all regions
SERVERS=(
    "18.212.182.48"
    "44.206.251.9"
    "44.212.39.135"
    "13.58.181.109"
    "3.138.173.119"
    "18.223.155.64"
    "54.193.130.0"
    "54.176.111.30"
    "54.193.2.46"
)

echo "Killing CockroachDB processes on all servers..."
for SERVER in "${SERVERS[@]}"; do
    echo "  Stopping on $SERVER..."
    ssh -o StrictHostKeyChecking=no -i ~/.ssh/aws-juicer-key.pem ubuntu@$SERVER "pkill -9 cockroach 2>/dev/null || true" &
done

wait
echo "  ✓ All processes killed"

echo ""
echo "Cleaning up data directories..."
for SERVER in "${SERVERS[@]}"; do
    echo "  Cleaning $SERVER..."
    ssh -o StrictHostKeyChecking=no -i ~/.ssh/aws-juicer-key.pem ubuntu@$SERVER "rm -rf /mnt/data/cockroach-data /mnt/data/logs 2>/dev/null || true" &
done

wait
echo "  ✓ All data cleaned"

echo ""
echo "========================================="
echo "✅ Cluster stopped and cleaned"
echo "========================================="
echo ""
echo "You can now restart the experiment:"
echo "  python3 scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml --skip-deploy"
echo ""
