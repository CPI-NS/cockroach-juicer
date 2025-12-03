#!/bin/bash
# AWS EC2 Multi-Region Setup Script - FIXED VERSION
# Properly handles SSH keys across regions

set -e

echo "=========================================="
echo "AWS Multi-Region Setup (Fixed)"
echo "=========================================="
echo ""

# Configuration
INSTANCE_TYPE_SERVER="t3.2xlarge"
INSTANCE_TYPE_CLIENT="t3.2xlarge"
KEY_NAME="aws-juicer-key"
SECURITY_GROUP_NAME="juicer-cluster-sg"

# AMI IDs for Ubuntu 22.04 LTS
AMI_US_EAST_1="ami-0866a3c8686eaeeba"
AMI_US_EAST_2="ami-0ea3c35c5c3284d82"
AMI_US_WEST_1="ami-0da424eb883458071"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI not installed"
    exit 1
fi

if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS CLI not configured"
    exit 1
fi

echo "✓ AWS CLI configured"
echo ""

# Step 1: Create SSH key in us-east-1 FIRST
echo "=========================================="
echo "Step 1: Setting up SSH key"
echo "=========================================="
echo ""

KEY_FILE="$HOME/.ssh/${KEY_NAME}.pem"

# Check if key exists locally
if [ -f "$KEY_FILE" ]; then
    echo "✓ Key already exists: $KEY_FILE"
    PUBLIC_KEY=$(ssh-keygen -y -f "$KEY_FILE")
else
    echo "Creating new key pair in us-east-1..."

    # Delete if exists in AWS
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region us-east-1 2>/dev/null || true

    # Create new key
    aws ec2 create-key-pair \
        --key-name "$KEY_NAME" \
        --region us-east-1 \
        --query 'KeyMaterial' \
        --output text > "$KEY_FILE"
    chmod 400 "$KEY_FILE"

    echo "✓ Key created: $KEY_FILE"
    PUBLIC_KEY=$(ssh-keygen -y -f "$KEY_FILE")
fi

# Step 2: Import key to other regions
echo ""
echo "Importing key to us-east-2 and us-west-1..."

# Save public key to temp file
TEMP_PUB_KEY="/tmp/aws-juicer-pubkey.tmp"
echo "$PUBLIC_KEY" > "$TEMP_PUB_KEY"

for REGION in us-east-2 us-west-1; do
    echo "  $REGION..."

    # Delete existing key
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION" 2>/dev/null || true

    # Import the public key
    aws ec2 import-key-pair \
        --key-name "$KEY_NAME" \
        --region "$REGION" \
        --public-key-material "fileb://$TEMP_PUB_KEY" \
        --output text > /dev/null

    echo "  ✓ Imported"
done

rm -f "$TEMP_PUB_KEY"

echo ""
echo "✓ SSH keys configured in all regions"
echo ""

# Function to setup region
setup_region() {
    local REGION=$1
    local AMI=$2

    echo ""
    echo "=========================================="
    echo "Setting up $REGION"
    echo "=========================================="

    # Get VPC
    VPC_ID=$(aws ec2 describe-vpcs \
        --region "$REGION" \
        --filters "Name=is-default,Values=true" \
        --query 'Vpcs[0].VpcId' \
        --output text)

    if [ "$VPC_ID" == "None" ] || [ -z "$VPC_ID" ]; then
        echo "ERROR: No default VPC in $REGION"
        return 1
    fi

    echo "  VPC: $VPC_ID"

    # Get subnet
    SUBNET_ID=$(aws ec2 describe-subnets \
        --region "$REGION" \
        --filters "Name=vpc-id,Values=$VPC_ID" \
        --query 'Subnets[0].SubnetId' \
        --output text)

    echo "  Subnet: $SUBNET_ID"

    # Create security group
    SG_ID=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "None")

    if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
        echo "  Creating security group..."
        SG_ID=$(aws ec2 create-security-group \
            --group-name "$SECURITY_GROUP_NAME" \
            --description "Juicer multi-region cluster" \
            --vpc-id "$VPC_ID" \
            --region "$REGION" \
            --query 'GroupId' \
            --output text)

        # Get current IP
        MY_IP=$(curl -s https://checkip.amazonaws.com)

        # Add rules
        aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 --cidr "${MY_IP}/32" --region "$REGION" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 8080 --cidr "${MY_IP}/32" --region "$REGION" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 26257 --cidr "0.0.0.0/0" --region "$REGION" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 26258-26266 --cidr "0.0.0.0/0" --region "$REGION" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol icmp --port -1 --cidr "0.0.0.0/0" --region "$REGION" 2>/dev/null || true

        echo "  ✓ Security group: $SG_ID"
    else
        echo "  ✓ Security group exists: $SG_ID"
    fi

    # Store values
    eval "VPC_${REGION//-/_}=\"$VPC_ID\""
    eval "SUBNET_${REGION//-/_}=\"$SUBNET_ID\""
    eval "SG_${REGION//-/_}=\"$SG_ID\""
    eval "AMI_${REGION//-/_}=\"$AMI\""
}

# Function to launch instances
launch_instances() {
    local NAME=$1
    local COUNT=$2
    local TYPE=$3
    local REGION=$4
    local AMI=$5
    local SUBNET=$6
    local SG=$7
    local ROLE=$8

    echo ""
    echo "Launching $COUNT x $TYPE in $REGION ($ROLE)..."

    if [ "$ROLE" == "server" ]; then
        BLOCK_DEVICES='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}},{"DeviceName":"/dev/sdf","Ebs":{"VolumeSize":100,"VolumeType":"gp3","Iops":3000,"Throughput":125}}]'
    else
        BLOCK_DEVICES='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]'
    fi

    INSTANCE_IDS=$(aws ec2 run-instances \
        --image-id "$AMI" \
        --count "$COUNT" \
        --instance-type "$TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG" \
        --subnet-id "$SUBNET" \
        --block-device-mappings "$BLOCK_DEVICES" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME}},{Key=Role,Value=${ROLE}},{Key=Project,Value=juicer-eval},{Key=Region,Value=${REGION}}]" \
        --region "$REGION" \
        --query 'Instances[*].InstanceId' \
        --output text)

    echo "  Instance IDs: $INSTANCE_IDS"

    aws ec2 wait instance-running --instance-ids $INSTANCE_IDS --region "$REGION"
    echo "  ✓ Running"
}

# Setup all regions
setup_region "us-east-1" "$AMI_US_EAST_1"
setup_region "us-east-2" "$AMI_US_EAST_2"
setup_region "us-west-1" "$AMI_US_WEST_1"

# Launch instances
echo ""
echo "=========================================="
echo "Launching Instances"
echo "=========================================="

launch_instances "juicer-server-east1" 3 "$INSTANCE_TYPE_SERVER" "us-east-1" "$AMI_US_EAST_1" "$SUBNET_us_east_1" "$SG_us_east_1" "server"
launch_instances "juicer-client-east1" 3 "$INSTANCE_TYPE_CLIENT" "us-east-1" "$AMI_US_EAST_1" "$SUBNET_us_east_1" "$SG_us_east_1" "client"

launch_instances "juicer-server-east2" 3 "$INSTANCE_TYPE_SERVER" "us-east-2" "$AMI_US_EAST_2" "$SUBNET_us_east_2" "$SG_us_east_2" "server"
launch_instances "juicer-client-east2" 3 "$INSTANCE_TYPE_CLIENT" "us-east-2" "$AMI_US_EAST_2" "$SUBNET_us_east_2" "$SG_us_east_2" "client"

launch_instances "juicer-server-west1" 3 "$INSTANCE_TYPE_SERVER" "us-west-1" "$AMI_US_WEST_1" "$SUBNET_us_west_1" "$SG_us_west_1" "server"
launch_instances "juicer-client-west1" 3 "$INSTANCE_TYPE_CLIENT" "us-west-1" "$AMI_US_WEST_1" "$SUBNET_us_west_1" "$SG_us_west_1" "client"

# Test SSH connections
echo ""
echo "=========================================="
echo "Testing SSH Connections"
echo "=========================================="
echo ""
echo "Waiting 30 seconds for SSH to be ready..."
sleep 30

for REGION in us-east-1 us-east-2 us-west-1; do
    echo ""
    echo "Testing $REGION..."

    TEST_IP=$(aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=running" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text)

    if [ -n "$TEST_IP" ] && [ "$TEST_IP" != "None" ]; then
        echo "  Testing SSH to $TEST_IP..."
        if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes ubuntu@$TEST_IP "echo 'SSH OK'" 2>/dev/null; then
            echo "  ✓ SSH working"
        else
            echo "  ✗ SSH failed"
        fi
    fi
done

# Collect instance info
echo ""
echo "=========================================="
echo "Instance Information"
echo "=========================================="

OUTPUT_FILE="aws_instances_multiregion.txt"
rm -f "$OUTPUT_FILE"

echo "# Juicer Multi-Region Instances" > "$OUTPUT_FILE"
echo "# Generated: $(date)" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

for REGION in us-east-1 us-east-2 us-west-1; do
    echo "" >> "$OUTPUT_FILE"
    echo "# SERVERS - $REGION" >> "$OUTPUT_FILE"
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Role,Values=server" "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]' \
        --output text | sort | while read -r name public private; do
            echo "${name}: public=${public}  private=${private}  region=${REGION}" >> "$OUTPUT_FILE"
            echo "  ${name}: ${public}"
        done

    echo "" >> "$OUTPUT_FILE"
    echo "# CLIENTS - $REGION" >> "$OUTPUT_FILE"
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Role,Values=client" "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]' \
        --output text | sort | while read -r name public private; do
            echo "${name}: public=${public}  private=${private}  region=${REGION}" >> "$OUTPUT_FILE"
            echo "  ${name}: ${public}"
        done
done

echo ""
echo "✓ Saved to: $OUTPUT_FILE"

echo ""
echo "=========================================="
echo "✅ SETUP COMPLETE!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Update configs: python3 update_configs_multiregion.py"
echo "2. Mount EBS: python3 mount_ebs_volumes_multiregion.py"
echo "3. Build binaries"
echo "4. Run experiments"
echo ""
