#!/bin/bash
# Allow SSH and CockroachDB ports between all instances across regions

set -e

SG_NAME="juicer-cluster-sg"
REGIONS=("us-east-1" "us-east-2" "us-west-1")

# VPC CIDR blocks for each region (AWS default VPC uses 172.31.0.0/16)
# We'll allow all private IPs to communicate
PRIVATE_CIDRS=(
    "172.31.0.0/16"   # Default VPC CIDR
    "10.0.0.0/8"      # Private network range
)

echo "========================================="
echo "Allowing cross-region communication"
echo "========================================="

for REGION in "${REGIONS[@]}"; do
    echo ""
    echo "Processing region: $REGION"

    # Get security group ID
    SG_ID=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=group-name,Values=$SG_NAME" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "")

    if [ -z "$SG_ID" ] || [ "$SG_ID" == "None" ]; then
        echo "  ⚠ Security group not found in $REGION, skipping"
        continue
    fi

    echo "  Security group ID: $SG_ID"

    for CIDR in "${PRIVATE_CIDRS[@]}"; do
        # Allow SSH from private networks
        echo "  Adding SSH rule for $CIDR..."
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 22 \
            --cidr "$CIDR" \
            --region "$REGION" 2>/dev/null && echo "    ✓ SSH rule added" || echo "    ℹ SSH rule already exists"

        # Allow CockroachDB port (26257)
        echo "  Adding CockroachDB port rule for $CIDR..."
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 26257 \
            --cidr "$CIDR" \
            --region "$REGION" 2>/dev/null && echo "    ✓ CockroachDB rule added" || echo "    ℹ CockroachDB rule already exists"

        # Allow CockroachDB admin UI (8080)
        echo "  Adding admin UI port rule for $CIDR..."
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 8080 \
            --cidr "$CIDR" \
            --region "$REGION" 2>/dev/null && echo "    ✓ Admin UI rule added" || echo "    ℹ Admin UI rule already exists"
    done
done

echo ""
echo "========================================="
echo "✓ Security groups updated!"
echo "========================================="
echo ""
echo "Instances can now communicate across all regions."
echo ""
