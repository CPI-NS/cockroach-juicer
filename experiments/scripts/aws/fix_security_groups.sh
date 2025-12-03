#!/bin/bash
# Fix security groups to allow SSH from current IP

set -e

echo "=========================================="
echo "Fix Security Groups for SSH Access"
echo "=========================================="
echo ""

# Get current IP
MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "Your current IP: $MY_IP"
echo ""

SECURITY_GROUP_NAME="juicer-cluster-sg"
REGIONS=("us-east-1" "us-east-2" "us-west-1")

for REGION in "${REGIONS[@]}"; do
    echo "Fixing security group in $REGION..."

    # Get security group ID
    SG_ID=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "None")

    if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
        echo "  ⚠ Security group not found in $REGION"
        continue
    fi

    echo "  Security group: $SG_ID"

    # Remove old SSH rules (ignore errors if they don't exist)
    echo "  Removing old SSH rules..."
    aws ec2 revoke-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 22 \
        --region "$REGION" \
        --cidr 0.0.0.0/0 2>/dev/null || true

    # Get all existing SSH rules and remove them
    EXISTING_RULES=$(aws ec2 describe-security-groups \
        --group-ids "$SG_ID" \
        --region "$REGION" \
        --query 'SecurityGroups[0].IpPermissions[?FromPort==`22`].IpRanges[*].CidrIp' \
        --output text 2>/dev/null || echo "")

    for CIDR in $EXISTING_RULES; do
        if [ -n "$CIDR" ]; then
            aws ec2 revoke-security-group-ingress \
                --group-id "$SG_ID" \
                --protocol tcp \
                --port 22 \
                --cidr "$CIDR" \
                --region "$REGION" 2>/dev/null || true
        fi
    done

    # Add new SSH rule with current IP
    echo "  Adding SSH rule for $MY_IP/32..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 22 \
        --cidr "${MY_IP}/32" \
        --region "$REGION" 2>/dev/null || echo "  (Rule may already exist)"

    echo "  ✓ Done"
    echo ""
done

echo "=========================================="
echo "✅ Security groups updated!"
echo "=========================================="
echo ""
echo "Now try SSH again:"
echo "  ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@3.132.213.78"
echo ""
