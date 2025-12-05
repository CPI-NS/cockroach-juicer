#!/bin/bash
# Allow client nodes to connect to CockroachDB servers on port 26257

set -e

REGIONS=("us-east-1" "us-east-2" "us-west-1")

echo "========================================="
echo "Opening CockroachDB ports for clients"
echo "========================================="

for REGION in "${REGIONS[@]}"; do
    echo ""
    echo "Region: $REGION"

    # Get default security group ID for this region
    SG_ID=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=group-name,Values=default" \
        --query "SecurityGroups[0].GroupId" \
        --output text)

    echo "  Security Group: $SG_ID"

    # Allow all private network traffic on port 26257 (CockroachDB)
    # This allows clients from any region to connect to servers
    echo "  Adding rule: Allow 0.0.0.0/0 -> port 26257 (CockroachDB)"
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 26257 \
        --cidr 0.0.0.0/0 \
        --region "$REGION" 2>/dev/null \
        && echo "    ✓ Added" \
        || echo "    ⚠ Already exists or failed"

    # Also allow port 8080 (CockroachDB HTTP/Admin UI)
    echo "  Adding rule: Allow 0.0.0.0/0 -> port 8080 (CockroachDB HTTP)"
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 8080 \
        --cidr 0.0.0.0/0 \
        --region "$REGION" 2>/dev/null \
        && echo "    ✓ Added" \
        || echo "    ⚠ Already exists or failed"
done

echo ""
echo "========================================="
echo "✓ Security groups updated!"
echo "========================================="
echo ""
echo "You can now run the experiment:"
echo "  python3 scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml --skip-deploy"
echo ""
