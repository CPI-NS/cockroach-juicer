#!/bin/bash
# Allow SSH between instances in the cluster

set -e

SG_NAME="juicer-cluster-sg"
REGIONS=("us-east-1" "us-east-2" "us-west-1")

echo "========================================="
echo "Allowing internal SSH between instances"
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

    # Add rule to allow SSH from the security group itself (instances can SSH to each other)
    echo "  Adding internal SSH rule..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 22 \
        --source-group "$SG_ID" \
        --region "$REGION" 2>/dev/null && echo "  ✓ Rule added" || echo "  ℹ Rule already exists"

    # Also allow CockroachDB port (26257) between instances
    echo "  Adding internal CockroachDB port rule..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 26257 \
        --source-group "$SG_ID" \
        --region "$REGION" 2>/dev/null && echo "  ✓ Rule added" || echo "  ℹ Rule already exists"

    # Allow CockroachDB admin UI (8080) between instances
    echo "  Adding internal admin UI port rule..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 8080 \
        --source-group "$SG_ID" \
        --region "$REGION" 2>/dev/null && echo "  ✓ Rule added" || echo "  ℹ Rule already exists"
done

echo ""
echo "========================================="
echo "✓ Security groups updated!"
echo "========================================="
echo ""
echo "Note: Instances can now SSH to each other, but you still need"
echo "to copy the AWS SSH key to the build server:"
echo ""
echo "  scp -i ~/.ssh/aws-juicer-key.pem ~/.ssh/aws-juicer-key.pem ubuntu@54.208.110.229:~/.ssh/"
echo ""
