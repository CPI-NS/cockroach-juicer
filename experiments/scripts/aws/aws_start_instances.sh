#!/bin/bash
# Script to start all stopped Juicer evaluation instances

set -e

echo "========================================="
echo "Start Juicer AWS Instances"
echo "========================================="
echo ""
echo "This script will start all stopped instances tagged with 'Project=juicer-eval'"
echo ""

# Check AWS CLI
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS CLI not configured"
    exit 1
fi

# Function to get stopped instances in a region
get_stopped_instances() {
    local REGION=$1
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=stopped" \
        --query 'Reservations[*].Instances[*].[InstanceId,Tags[?Key==`Name`].Value|[0]]' \
        --output text 2>/dev/null || echo ""
}

# Find and start instances in each region
REGIONS=("us-east-1" "us-east-2" "us-west-1")
TOTAL_STARTED=0

echo "Finding stopped instances..."
echo ""

for REGION in "${REGIONS[@]}"; do
    INSTANCES=$(get_stopped_instances "$REGION")

    if [ -n "$INSTANCES" ]; then
        COUNT=$(echo "$INSTANCES" | wc -l | tr -d ' ')
        echo "Region: $REGION ($COUNT stopped instances)"

        # Get instance IDs
        INSTANCE_IDS=$(aws ec2 describe-instances \
            --region "$REGION" \
            --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=stopped" \
            --query 'Reservations[*].Instances[*].InstanceId' \
            --output text 2>/dev/null)

        if [ -n "$INSTANCE_IDS" ]; then
            echo "  Starting instances: $INSTANCE_IDS"
            aws ec2 start-instances \
                --instance-ids $INSTANCE_IDS \
                --region "$REGION" \
                --output text > /dev/null
            echo "  ✓ Start command sent"
            TOTAL_STARTED=$((TOTAL_STARTED + COUNT))
        fi
        echo ""
    fi
done

if [ $TOTAL_STARTED -eq 0 ]; then
    echo "No stopped Juicer instances found."
    exit 0
fi

echo "========================================="
echo "✅ Started $TOTAL_STARTED instances"
echo "========================================="
echo ""
echo "Instances are starting up. This may take 1-2 minutes."
echo ""
echo "⚠️  IMPORTANT: Public IP addresses may have changed!"
echo "You'll need to update your config file with new public IPs."
echo ""
echo "To check status:"
echo "  aws ec2 describe-instances --region us-east-1 --filters \"Name=tag:Project,Values=juicer-eval\" --query 'Reservations[*].Instances[*].[Tags[?Key==\`Name\`].Value|[0],State.Name,PublicIpAddress,PrivateIpAddress]' --output table"
echo ""
