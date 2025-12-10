#!/bin/bash
# Launch a control node in us-east-1 for orchestrating experiments

set -e

REGION="us-east-1"
INSTANCE_TYPE="t3.2xlarge"  # 8 vCPU, 32GB RAM - same as server nodes for building
AMI_ID="ami-0c55b159cbfafe1f0"  # Ubuntu 20.04 LTS in us-east-1 (update if needed)
KEY_NAME="aws-juicer-key"
SECURITY_GROUP=""  # Will use existing juicer-eval security group

echo "========================================="
echo "Launch Juicer Control Node"
echo "========================================="
echo ""

# Check AWS CLI
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS CLI not configured"
    exit 1
fi

# Get the security group ID from existing instances
echo "Finding security group from existing instances..."
SECURITY_GROUP=$(aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].SecurityGroups[0].GroupId' \
    --output text 2>/dev/null)

if [ -z "$SECURITY_GROUP" ] || [ "$SECURITY_GROUP" == "None" ]; then
    echo "ERROR: Could not find security group from running instances"
    exit 1
fi

echo "Using security group: $SECURITY_GROUP"
echo ""

# Get latest Ubuntu 20.04 AMI
echo "Finding latest Ubuntu 20.04 AMI..."
AMI_ID=$(aws ec2 describe-images \
    --region "$REGION" \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*" \
    --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
    --output text)

echo "Using AMI: $AMI_ID"
echo ""

# Launch instance
echo "Launching control node instance..."
INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=juicer-control},{Key=Project,Value=juicer-eval},{Key=Role,Value=control}]" \
    --query 'Instances[0].InstanceId' \
    --output text)

if [ -z "$INSTANCE_ID" ]; then
    echo "ERROR: Failed to launch instance"
    exit 1
fi

echo "Launched instance: $INSTANCE_ID"
echo ""

# Wait for instance to be running
echo "Waiting for instance to be running..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

# Get instance details
INSTANCE_INFO=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' \
    --output text)

PUBLIC_IP=$(echo "$INSTANCE_INFO" | awk '{print $1}')
PRIVATE_IP=$(echo "$INSTANCE_INFO" | awk '{print $2}')

echo ""
echo "========================================="
echo "✓ Control Node Launched!"
echo "========================================="
echo ""
echo "Instance ID:  $INSTANCE_ID"
echo "Public IP:    $PUBLIC_IP"
echo "Private IP:   $PRIVATE_IP"
echo "Region:       $REGION"
echo ""
echo "Next steps:"
echo "1. Wait ~30 seconds for SSH to be ready"
echo "2. Setup control node:"
echo "   bash scripts/aws/setup_control_node.sh $PUBLIC_IP"
echo ""
