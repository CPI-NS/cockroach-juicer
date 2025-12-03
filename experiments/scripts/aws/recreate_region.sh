#!/bin/bash
# Recreate instances in a specific region with correct SSH key

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <region>"
    echo "Example: $0 us-east-2"
    exit 1
fi

REGION=$1
INSTANCE_TYPE_SERVER="t3.2xlarge"
INSTANCE_TYPE_CLIENT="t3.2xlarge"
KEY_NAME="aws-juicer-key"
SECURITY_GROUP_NAME="juicer-cluster-sg"

# AMI IDs for each region
case "$REGION" in
    us-east-1)
        AMI="ami-0866a3c8686eaeeba"
        ;;
    us-east-2)
        AMI="ami-0ea3c35c5c3284d82"
        ;;
    us-west-1)
        AMI="ami-0da424eb883458071"
        ;;
    *)
        echo "ERROR: Unknown region: $REGION"
        echo "Supported: us-east-1, us-east-2, us-west-1"
        exit 1
        ;;
esac

echo "=========================================="
echo "Recreate Instances in $REGION"
echo "=========================================="
echo ""

# Get existing instances
echo "Finding existing instances in $REGION..."
INSTANCE_IDS=$(aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=running,stopped" \
    --query 'Reservations[*].Instances[*].InstanceId' \
    --output text)

if [ -z "$INSTANCE_IDS" ]; then
    echo "No instances found in $REGION"
else
    echo "Found instances: $INSTANCE_IDS"
    echo ""
    read -p "Terminate these instances? (yes/no): " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted"
        exit 0
    fi

    echo "Terminating instances..."
    aws ec2 terminate-instances \
        --instance-ids $INSTANCE_IDS \
        --region "$REGION" \
        --output text > /dev/null

    echo "Waiting for termination..."
    aws ec2 wait instance-terminated \
        --instance-ids $INSTANCE_IDS \
        --region "$REGION"

    echo "✓ Instances terminated"
    echo ""
fi

# Get VPC and subnet
echo "Getting VPC information..."
VPC_ID=$(aws ec2 describe-vpcs \
    --region "$REGION" \
    --filters "Name=is-default,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text)

SUBNET_ID=$(aws ec2 describe-subnets \
    --region "$REGION" \
    --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'Subnets[0].SubnetId' \
    --output text)

# Get security group
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

echo "  VPC: $VPC_ID"
echo "  Subnet: $SUBNET_ID"
echo "  Security Group: $SG_ID"
echo ""

# Launch 3 servers
echo "Launching 3 servers..."
SERVER_IDS=$(aws ec2 run-instances \
    --image-id "$AMI" \
    --count 3 \
    --instance-type "$INSTANCE_TYPE_SERVER" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}},{"DeviceName":"/dev/sdf","Ebs":{"VolumeSize":100,"VolumeType":"gp3","Iops":3000,"Throughput":125}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=juicer-server-${REGION}},{Key=Role,Value=server},{Key=Project,Value=juicer-eval},{Key=Region,Value=${REGION}}]" \
    --region "$REGION" \
    --query 'Instances[*].InstanceId' \
    --output text)

echo "  Server IDs: $SERVER_IDS"

# Launch 3 clients
echo "Launching 3 clients..."
CLIENT_IDS=$(aws ec2 run-instances \
    --image-id "$AMI" \
    --count 3 \
    --instance-type "$INSTANCE_TYPE_CLIENT" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=juicer-client-${REGION}},{Key=Role,Value=client},{Key=Project,Value=juicer-eval},{Key=Region,Value=${REGION}}]" \
    --region "$REGION" \
    --query 'Instances[*].InstanceId' \
    --output text)

echo "  Client IDs: $CLIENT_IDS"
echo ""

# Wait for instances
echo "Waiting for instances to start..."
ALL_IDS="$SERVER_IDS $CLIENT_IDS"
aws ec2 wait instance-running \
    --instance-ids $ALL_IDS \
    --region "$REGION"

echo ""
echo "=========================================="
echo "✅ Instances recreated in $REGION!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Update configs: python3 update_configs_multiregion.py"
echo "2. Mount EBS volumes: python3 mount_ebs_volumes_multiregion.py"
echo ""
