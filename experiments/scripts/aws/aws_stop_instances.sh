#!/bin/bash
# Script to stop or terminate all Juicer evaluation instances

set -e

echo "=========================================="
echo "Stop/Terminate Juicer AWS Instances"
echo "=========================================="
echo ""
echo "This script will find all instances tagged with 'Project=juicer-eval'"
echo ""

# Check AWS CLI
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS CLI not configured"
    exit 1
fi

# Function to get instances in a region
get_instances_in_region() {
    local REGION=$1
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=running,stopped" \
        --query 'Reservations[*].Instances[*].[InstanceId,State.Name,Tags[?Key==`Name`].Value|[0]]' \
        --output text 2>/dev/null || echo ""
}

# Collect all instances across regions
echo "Finding Juicer instances across all regions..."
echo ""

REGIONS=("us-east-1" "us-east-2" "us-west-1")
TOTAL_COUNT=0

for REGION in "${REGIONS[@]}"; do
    INSTANCES=$(get_instances_in_region "$REGION")
    if [ -n "$INSTANCES" ]; then
        COUNT=$(echo "$INSTANCES" | wc -l | tr -d ' ')
        TOTAL_COUNT=$((TOTAL_COUNT + COUNT))
        echo "Region: $REGION ($COUNT instances)"
        echo "$INSTANCES" | while read -r id state name; do
            echo "  [$state] $name ($id)"
        done
        echo ""
    fi
done

if [ $TOTAL_COUNT -eq 0 ]; then
    echo "No Juicer instances found."
    exit 0
fi

echo "Total instances found: $TOTAL_COUNT"
echo ""
echo "What would you like to do?"
echo "  1) STOP instances (saves state, can restart later, still charges for storage)"
echo "  2) TERMINATE instances (permanently deletes, stops all charges)"
echo "  3) Cancel"
echo ""
read -p "Enter choice (1/2/3): " CHOICE

case $CHOICE in
    1)
        ACTION="stop"
        AWS_ACTION="stop-instances"
        echo ""
        echo "Stopping instances (you can restart them later)..."
        ;;
    2)
        ACTION="terminate"
        AWS_ACTION="terminate-instances"
        echo ""
        echo "⚠️  WARNING: This will PERMANENTLY DELETE all instances!"
        read -p "Are you sure? Type 'DELETE' to confirm: " CONFIRM
        if [ "$CONFIRM" != "DELETE" ]; then
            echo "Cancelled."
            exit 0
        fi
        echo ""
        echo "Terminating instances..."
        ;;
    3)
        echo "Cancelled."
        exit 0
        ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

# Stop or terminate instances in each region
for REGION in "${REGIONS[@]}"; do
    INSTANCE_IDS=$(aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].InstanceId' \
        --output text 2>/dev/null)

    if [ -n "$INSTANCE_IDS" ]; then
        if [ "$ACTION" == "stop" ]; then
            echo "  Stopping instances in $REGION..."
        else
            echo "  Terminating instances in $REGION..."
        fi
        aws ec2 "$AWS_ACTION" \
            --instance-ids $INSTANCE_IDS \
            --region "$REGION" \
            --output text > /dev/null
        echo "  ✓ Done"
    fi
done

echo ""
echo "=========================================="
if [ "$ACTION" == "stop" ]; then
    echo "✅ All instances STOPPED"
    echo ""
    echo "To restart them later, use the AWS Console or:"
    echo "  aws ec2 start-instances --instance-ids <ID> --region <REGION>"
    echo ""
    echo "Note: Stopped instances still incur storage charges (~\$1/day for EBS volumes)"
else
    echo "✅ All instances TERMINATED"
    echo ""
    echo "All charges stopped. Instances are permanently deleted."
fi
echo "=========================================="
echo ""
