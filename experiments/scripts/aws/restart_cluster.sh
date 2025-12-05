#!/bin/bash
# Restart CockroachDB cluster on all servers

set -e

echo "========================================="
echo "Restarting CockroachDB cluster"
echo "========================================="

# Server hostnames (us-east-1, us-east-2, us-west-1)
SERVERS=(
    "34.204.67.217"
    "44.200.65.29"
    "54.234.148.127"
    "3.135.218.75"
    "3.15.29.177"
    "18.188.24.227"
    "13.56.252.173"
    "184.169.235.174"
    "54.183.191.51"
)

echo ""
echo "Step 1: Stopping CockroachDB on all servers..."
for SERVER in "${SERVERS[@]}"; do
    echo "  Stopping on $SERVER..."
    ssh -o StrictHostKeyChecking=no -i ~/.ssh/aws-juicer-key.pem ubuntu@$SERVER "pkill -9 cockroach || true" &
done

wait
echo "  ✓ All servers stopped"

echo ""
echo "Step 2: Waiting 5 seconds..."
sleep 5

echo ""
echo "Step 3: Restart complete. Now run the experiment to start the cluster with new config:"
echo "  cd /Users/harris/Documents/cpi-ns/juicer/juicer-cc/cockroach-juicer/experiments"
echo "  python3 scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml --skip-deploy"
echo ""
