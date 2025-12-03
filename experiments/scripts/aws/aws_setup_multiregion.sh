#!/bin/bash
# AWS EC2 Multi-Region Setup Script for Juicer Evaluation
# This script creates instances across 3 regions: us-east-1, us-east-2, us-west-1

set -e  # Exit on any error

echo "=========================================="
echo "AWS EC2 Multi-Region Setup for Juicer"
echo "=========================================="
echo ""
echo "Deploying to 3 regions:"
echo "  - us-east-1 (Virginia): 3 servers"
echo "  - us-east-2 (Ohio): 3 servers"
echo "  - us-west-1 (California): 3 servers"
echo "  - Plus 9 client instances"
echo ""

# Configuration
INSTANCE_TYPE_SERVER="t3.2xlarge"
INSTANCE_TYPE_CLIENT="t3.2xlarge"
KEY_NAME="aws-juicer-key"
SECURITY_GROUP_NAME="juicer-cluster-sg"

# AMI IDs for Ubuntu 22.04 LTS in each region
AMI_US_EAST_1="ami-0866a3c8686eaeeba"  # Ubuntu 22.04 LTS us-east-1
AMI_US_EAST_2="ami-0ea3c35c5c3284d82"  # Ubuntu 22.04 LTS us-east-2
AMI_US_WEST_1="ami-0da424eb883458071"  # Ubuntu 22.04 LTS us-west-1

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI is not installed."
    echo "Please install it first:"
    echo "  brew install awscli"
    exit 1
fi

# Check if AWS is configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS CLI is not configured."
    echo "Please run: aws configure"
    exit 1
fi

echo "✓ AWS CLI configured"
echo ""

# Function to setup region (key pair, security group, VPC/subnet)
setup_region() {
    local REGION=$1
    local AMI=$2

    echo ""
    echo "=========================================="
    echo "Setting up region: $REGION"
    echo "=========================================="

    # Create key pair if not exists
    echo "  Checking SSH key pair..."
    if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &> /dev/null 2>&1; then
        echo "  ✓ Key pair already exists in $REGION"
    else
        echo "  Creating key pair in $REGION..."
        if [ "$REGION" == "us-east-1" ]; then
            # Only save the key file once (from first region)
            aws ec2 create-key-pair \
                --key-name "$KEY_NAME" \
                --region "$REGION" \
                --query 'KeyMaterial' \
                --output text > ~/.ssh/${KEY_NAME}.pem
            chmod 400 ~/.ssh/${KEY_NAME}.pem
            echo "  ✓ Key saved to ~/.ssh/${KEY_NAME}.pem"
        else
            # Import the same public key to other regions
            aws ec2 import-key-pair \
                --key-name "$KEY_NAME" \
                --region "$REGION" \
                --public-key-material fileb://<(ssh-keygen -y -f ~/.ssh/${KEY_NAME}.pem) &> /dev/null || {
                # If import fails, create a new key but don't save it
                aws ec2 create-key-pair \
                    --key-name "$KEY_NAME" \
                    --region "$REGION" \
                    --query 'KeyMaterial' \
                    --output text > /dev/null
            }
            echo "  ✓ Key pair configured in $REGION"
        fi
    fi

    # Get default VPC
    echo "  Getting VPC information..."
    VPC_ID=$(aws ec2 describe-vpcs \
        --region "$REGION" \
        --filters "Name=is-default,Values=true" \
        --query 'Vpcs[0].VpcId' \
        --output text)

    if [ "$VPC_ID" == "None" ] || [ -z "$VPC_ID" ]; then
        echo "  ERROR: No default VPC in $REGION"
        return 1
    fi
    echo "  ✓ VPC: $VPC_ID"

    # Get a subnet (any will do)
    SUBNET_ID=$(aws ec2 describe-subnets \
        --region "$REGION" \
        --filters "Name=vpc-id,Values=$VPC_ID" \
        --query 'Subnets[0].SubnetId' \
        --output text)
    echo "  ✓ Subnet: $SUBNET_ID"

    # Create security group
    echo "  Setting up security group..."
    SG_ID=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "None")

    if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
        echo "  Creating security group..."
        SG_ID=$(aws ec2 create-security-group \
            --group-name "$SECURITY_GROUP_NAME" \
            --description "Security group for Juicer multi-region cluster" \
            --vpc-id "$VPC_ID" \
            --region "$REGION" \
            --query 'GroupId' \
            --output text)

        # Get your current IP
        MY_IP=$(curl -s https://checkip.amazonaws.com)

        # Add SSH access
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 22 \
            --cidr "${MY_IP}/32" \
            --region "$REGION" 2>/dev/null || true

        # Add HTTP access for CockroachDB UI
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 8080 \
            --cidr "${MY_IP}/32" \
            --region "$REGION" 2>/dev/null || true

        # Add CockroachDB ports (allow from anywhere for multi-region)
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 26257 \
            --cidr "0.0.0.0/0" \
            --region "$REGION" 2>/dev/null || true

        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 26258-26266 \
            --cidr "0.0.0.0/0" \
            --region "$REGION" 2>/dev/null || true

        # Allow all ICMP
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol icmp \
            --port -1 \
            --cidr "0.0.0.0/0" \
            --region "$REGION" 2>/dev/null || true

        echo "  ✓ Security group created: $SG_ID"
    else
        echo "  ✓ Security group exists: $SG_ID"
    fi

    # Return values (store in global variables)
    eval "VPC_${REGION//-/_}=\"$VPC_ID\""
    eval "SUBNET_${REGION//-/_}=\"$SUBNET_ID\""
    eval "SG_${REGION//-/_}=\"$SG_ID\""
    eval "AMI_${REGION//-/_}=\"$AMI\""
}

# Function to launch instances
launch_instances() {
    local NAME_PREFIX=$1
    local COUNT=$2
    local INSTANCE_TYPE=$3
    local REGION=$4
    local AMI=$5
    local SUBNET=$6
    local SG=$7
    local ROLE=$8

    echo ""
    echo "Launching $COUNT x $INSTANCE_TYPE in $REGION..."
    echo "  Name: $NAME_PREFIX"
    echo "  Role: $ROLE"

    # Block device mappings
    if [ "$ROLE" == "server" ]; then
        # Servers get 100GB data volume
        BLOCK_DEVICES='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}},{"DeviceName":"/dev/sdf","Ebs":{"VolumeSize":100,"VolumeType":"gp3","Iops":3000,"Throughput":125}}]'
    else
        # Clients just get root volume
        BLOCK_DEVICES='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]'
    fi

    # Launch instances
    INSTANCE_IDS=$(aws ec2 run-instances \
        --image-id "$AMI" \
        --count "$COUNT" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG" \
        --subnet-id "$SUBNET" \
        --block-device-mappings "$BLOCK_DEVICES" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME_PREFIX}},{Key=Role,Value=${ROLE}},{Key=Project,Value=juicer-eval},{Key=Region,Value=${REGION}}]" \
        --region "$REGION" \
        --query 'Instances[*].InstanceId' \
        --output text)

    echo "  Instance IDs: $INSTANCE_IDS"
    echo "  Waiting for instances to start..."

    # Wait for instances
    aws ec2 wait instance-running \
        --instance-ids $INSTANCE_IDS \
        --region "$REGION"

    echo "  ✓ Instances running"
}

# Setup all regions
setup_region "us-east-1" "$AMI_US_EAST_1"
setup_region "us-east-2" "$AMI_US_EAST_2"
setup_region "us-west-1" "$AMI_US_WEST_1"

echo ""
echo "=========================================="
echo "Launching Instances"
echo "=========================================="

# Launch servers in us-east-1 (3 instances)
launch_instances "juicer-server-east1" 3 "$INSTANCE_TYPE_SERVER" "us-east-1" "$AMI_US_EAST_1" "$SUBNET_us_east_1" "$SG_us_east_1" "server"

# Launch servers in us-east-2 (3 instances)
launch_instances "juicer-server-east2" 3 "$INSTANCE_TYPE_SERVER" "us-east-2" "$AMI_US_EAST_2" "$SUBNET_us_east_2" "$SG_us_east_2" "server"

# Launch servers in us-west-1 (3 instances)
launch_instances "juicer-server-west1" 3 "$INSTANCE_TYPE_SERVER" "us-west-1" "$AMI_US_WEST_1" "$SUBNET_us_west_1" "$SG_us_west_1" "server"

# Launch clients in each region (3 per region = 9 total)
echo ""
echo "Launching client instances across all regions..."
launch_instances "juicer-client-east1" 3 "$INSTANCE_TYPE_CLIENT" "us-east-1" "$AMI_US_EAST_1" "$SUBNET_us_east_1" "$SG_us_east_1" "client"

launch_instances "juicer-client-east2" 3 "$INSTANCE_TYPE_CLIENT" "us-east-2" "$AMI_US_EAST_2" "$SUBNET_us_east_2" "$SG_us_east_2" "client"

launch_instances "juicer-client-west1" 3 "$INSTANCE_TYPE_CLIENT" "us-west-1" "$AMI_US_WEST_1" "$SUBNET_us_west_1" "$SG_us_west_1" "client"

echo ""
echo "Waiting for networking to be ready..."
sleep 15

# Collect instance information
echo ""
echo "=========================================="
echo "Collecting Instance Information"
echo "=========================================="

OUTPUT_FILE="aws_instances_multiregion.txt"
rm -f "$OUTPUT_FILE"

echo "# Juicer Evaluation - AWS Multi-Region Instances" > "$OUTPUT_FILE"
echo "# Generated on $(date)" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

for REGION in "us-east-1" "us-east-2" "us-west-1"; do
    echo "" >> "$OUTPUT_FILE"
    echo "# SERVERS - $REGION" >> "$OUTPUT_FILE"
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Role,Values=server" "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]' \
        --output text | sort | while read -r name public_ip private_ip; do
            echo "${name}: public=${public_ip}  private=${private_ip}  region=${REGION}" >> "$OUTPUT_FILE"
            echo "  ${name}: ${public_ip} (${REGION})"
        done
done

for REGION in "us-east-1" "us-east-2" "us-west-1"; do
    echo "" >> "$OUTPUT_FILE"
    echo "# CLIENTS - $REGION" >> "$OUTPUT_FILE"
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Role,Values=client" "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]' \
        --output text | sort | while read -r name public_ip private_ip; do
            echo "${name}: public=${public_ip}  private=${private_ip}  region=${REGION}" >> "$OUTPUT_FILE"
            echo "  ${name}: ${public_ip} (${REGION})"
        done
done

echo ""
echo "✓ Instance information saved to: $OUTPUT_FILE"
echo ""