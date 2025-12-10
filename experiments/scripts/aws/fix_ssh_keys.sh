#!/bin/bash
# Fix SSH key pairs across all regions

set -e

echo "=========================================="
echo "Fix SSH Key Pairs Across Regions"
echo "=========================================="
echo ""

KEY_NAME="aws-juicer-key"
KEY_FILE="$HOME/.ssh/${KEY_NAME}.pem"

if [ ! -f "$KEY_FILE" ]; then
    echo "ERROR: Key file not found: $KEY_FILE"
    exit 1
fi

echo "Using key: $KEY_FILE"
echo ""

# Extract public key from private key
echo "Extracting public key..."
PUBLIC_KEY=$(ssh-keygen -y -f "$KEY_FILE")

REGIONS=("us-east-1" "us-east-2" "us-west-1")

for REGION in "${REGIONS[@]}"; do
    echo "Updating key in $REGION..."

    # Delete existing key pair if it exists
    echo "  Deleting old key pair..."
    aws ec2 delete-key-pair \
        --key-name "$KEY_NAME" \
        --region "$REGION" 2>/dev/null || echo "  (No existing key to delete)"

    # Import the correct key (base64 encode it)
    echo "  Importing new key pair..."
    aws ec2 import-key-pair \
        --key-name "$KEY_NAME" \
        --region "$REGION" \
        --public-key-material "$(echo "$PUBLIC_KEY" | base64)" \
        --output text > /dev/null

    echo "  ✓ Key imported to $REGION"
    echo ""
done

echo "=========================================="
echo "⚠️  IMPORTANT"
echo "=========================================="
echo ""
echo "The SSH keys have been updated in us-east-2 and us-west-1,"
echo "but the RUNNING INSTANCES still have the old keys."
echo ""
echo "You need to RESTART the instances in those regions for the"
echo "new keys to take effect."
echo ""
echo "Options:"
echo "  1. STOP and START the instances (preferred - keeps data)"
echo "  2. TERMINATE and re-create them (loses any data)"
echo ""
echo "To STOP instances in us-east-2 and us-west-1:"
echo ""
echo "  # Get instance IDs"
echo "  aws ec2 describe-instances --region us-east-2 \\"
echo "    --filters 'Name=tag:Project,Values=juicer-eval' 'Name=instance-state-name,Values=running' \\"
echo "    --query 'Reservations[*].Instances[*].InstanceId' --output text"
echo ""
echo "  # Stop them"
echo "  aws ec2 stop-instances --region us-east-2 --instance-ids <IDS>"
echo ""
echo "  # Wait a minute, then start them"
echo "  aws ec2 start-instances --region us-east-2 --instance-ids <IDS>"
echo ""
echo "Or use the Python script (easier):"
echo "  python3 restart_region_instances.py us-east-2"
echo "  python3 restart_region_instances.py us-west-1"
echo ""
