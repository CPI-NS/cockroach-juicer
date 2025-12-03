#!/bin/bash
# AWS EC2 Setup Script for Juicer Evaluation
# This script automates the creation of EC2 instances

set -e  # Exit on any error

echo "=========================================="
echo "AWS EC2 Setup for Juicer Evaluation"
echo "=========================================="
echo ""

# Configuration
INSTANCE_TYPE_SERVER="t3.2xlarge"
INSTANCE_TYPE_CLIENT="t3.2xlarge"
AMI_ID="ami-0866a3c8686eaeeba"  # Ubuntu 22.04 LTS in us-east-1 (update if needed)
REGION="us-east-1"
KEY_NAME="aws-juicer-key"
SECURITY_GROUP_NAME="juicer-cluster-sg"

# Check if AWS is configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS CLI is not configured."
    echo "Please run: aws configure"
    echo "You'll need:"
    echo "  - AWS Access Key ID"
    echo "  - AWS Secret Access Key"
    echo "  - Default region: us-east-1"
    exit 1
fi

echo "✓ AWS CLI configured"
echo ""

# Check if key pair exists, create if not
echo "Step 1: Setting up SSH key pair..."
if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &> /dev/null; then
    echo "✓ Key pair '$KEY_NAME' already exists"
else
    echo "Creating new key pair: $KEY_NAME"
    aws ec2 create-key-pair \
        --key-name "$KEY_NAME" \
        --region "$REGION" \
        --query 'KeyMaterial' \
        --output text > ~/.ssh/${KEY_NAME}.pem
    chmod 400 ~/.ssh/${KEY_NAME}.pem
    echo "✓ Key pair created and saved to ~/.ssh/${KEY_NAME}.pem"
fi
echo ""

# Get default VPC
echo "Step 2: Getting VPC information..."
VPC_ID=$(aws ec2 describe-vpcs \
    --region "$REGION" \
    --filters "Name=is-default,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text)

if [ "$VPC_ID" == "None" ] || [ -z "$VPC_ID" ]; then
    echo "ERROR: No default VPC found. Please create a VPC first."
    exit 1
fi
echo "✓ Using VPC: $VPC_ID"
echo ""

# Get subnets for each AZ
echo "Step 3: Getting subnet information..."
SUBNET_1A=$(aws ec2 describe-subnets \
    --region "$REGION" \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=availability-zone,Values=us-east-1a" \
    --query 'Subnets[0].SubnetId' \
    --output text)

SUBNET_1B=$(aws ec2 describe-subnets \
    --region "$REGION" \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=availability-zone,Values=us-east-1b" \
    --query 'Subnets[0].SubnetId' \
    --output text)

SUBNET_1C=$(aws ec2 describe-subnets \
    --region "$REGION" \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=availability-zone,Values=us-east-1c" \
    --query 'Subnets[0].SubnetId' \
    --output text)

echo "✓ Subnet us-east-1a: $SUBNET_1A"
echo "✓ Subnet us-east-1b: $SUBNET_1B"
echo "✓ Subnet us-east-1c: $SUBNET_1C"
echo ""

# Create security group
echo "Step 4: Setting up security group..."
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "None")

if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
    echo "Creating security group: $SECURITY_GROUP_NAME"
    SG_ID=$(aws ec2 create-security-group \
        --group-name "$SECURITY_GROUP_NAME" \
        --description "Security group for Juicer evaluation cluster" \
        --vpc-id "$VPC_ID" \
        --region "$REGION" \
        --query 'GroupId' \
        --output text)

    # Get your current IP
    MY_IP=$(curl -s https://checkip.amazonaws.com)

    # Add SSH access from your IP
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 22 \
        --cidr "${MY_IP}/32" \
        --region "$REGION"

    # Add HTTP access for CockroachDB UI from your IP
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 8080 \
        --cidr "${MY_IP}/32" \
        --region "$REGION"

    # Add internal cluster communication (within security group)
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 26257 \
        --source-group "$SG_ID" \
        --region "$REGION"

    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 26258-26266 \
        --source-group "$SG_ID" \
        --region "$REGION"

    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol icmp \
        --port -1 \
        --source-group "$SG_ID" \
        --region "$REGION"

    echo "✓ Security group created: $SG_ID"
else
    echo "✓ Security group already exists: $SG_ID"
fi
echo ""

# Function to launch instances
launch_instances() {
    local NAME_PREFIX=$1
    local COUNT=$2
    local INSTANCE_TYPE=$3
    local SUBNET=$4
    local AZ=$5
    local ROLE=$6

    echo "Launching $COUNT instances: $NAME_PREFIX (type: $INSTANCE_TYPE, AZ: $AZ)"

    # Launch instances
    INSTANCE_IDS=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --count "$COUNT" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --subnet-id "$SUBNET" \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"VolumeType\":\"gp3\"}},{\"DeviceName\":\"/dev/sdf\",\"Ebs\":{\"VolumeSize\":100,\"VolumeType\":\"gp3\",\"Iops\":3000,\"Throughput\":125}}]" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME_PREFIX}},{Key=Role,Value=${ROLE}},{Key=Project,Value=juicer-eval}]" \
        --region "$REGION" \
        --query 'Instances[*].InstanceId' \
        --output text)

    echo "  Instance IDs: $INSTANCE_IDS"

    # Wait for instances to start
    echo "  Waiting for instances to start..."
    aws ec2 wait instance-running \
        --instance-ids $INSTANCE_IDS \
        --region "$REGION"

    # Tag each instance with sequential numbers
    local i=0
    for INSTANCE_ID in $INSTANCE_IDS; do
        aws ec2 create-tags \
            --resources "$INSTANCE_ID" \
            --tags "Key=Name,Value=${NAME_PREFIX}-${i}" \
            --region "$REGION"
        i=$((i + 1))
    done

    echo "✓ Instances launched and running"
    echo ""
}

# Launch server instances
echo "=========================================="
echo "Step 5: Launching SERVER instances"
echo "=========================================="
echo ""

echo "Launching servers in us-east-1a (3 instances)..."
launch_instances "juicer-server" 3 "$INSTANCE_TYPE_SERVER" "$SUBNET_1A" "us-east-1a" "server"

echo "Launching servers in us-east-1b (3 instances)..."
launch_instances "juicer-server" 3 "$INSTANCE_TYPE_SERVER" "$SUBNET_1B" "us-east-1b" "server"

echo "Launching servers in us-east-1c (3 instances)..."
launch_instances "juicer-server" 3 "$INSTANCE_TYPE_SERVER" "$SUBNET_1C" "us-east-1c" "server"

# Launch client instances
echo "=========================================="
echo "Step 6: Launching CLIENT instances"
echo "=========================================="
echo ""

echo "Launching clients (9 instances)..."
launch_instances "juicer-client" 9 "$INSTANCE_TYPE_CLIENT" "$SUBNET_1A" "us-east-1a" "client"

# Wait a bit for networking to be fully ready
echo "Waiting for networking to be ready..."
sleep 10

# Get all instance information
echo ""
echo "=========================================="
echo "Step 7: Collecting instance information"
echo "=========================================="
echo ""

# Create output file
OUTPUT_FILE="aws_instances.txt"
rm -f "$OUTPUT_FILE"

echo "# Juicer Evaluation - AWS EC2 Instances" > "$OUTPUT_FILE"
echo "# Generated on $(date)" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

echo "Getting server instance details..."
echo "# SERVERS" >> "$OUTPUT_FILE"
aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Role,Values=server" "Name=instance-state-name,Values=running" \
    --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress,Placement.AvailabilityZone]' \
    --output text | sort | while read -r name public_ip private_ip az; do
        echo "${name}: public_ip=${public_ip}  private_ip=${private_ip}  az=${az}" >> "$OUTPUT_FILE"
        echo "  ${name}: ${public_ip}"
    done

echo "" >> "$OUTPUT_FILE"
echo "Getting client instance details..."
echo "# CLIENTS" >> "$OUTPUT_FILE"
aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Role,Values=client" "Name=instance-state-name,Values=running" \
    --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]' \
    --output text | sort | while read -r name public_ip private_ip; do
        echo "${name}: public_ip=${public_ip}  private_ip=${private_ip}" >> "$OUTPUT_FILE"
        echo "  ${name}: ${public_ip}"
    done

echo ""
echo "✓ Instance information saved to: $OUTPUT_FILE"
echo ""

# Create config template
echo "=========================================="
echo "Step 8: Creating config template"
echo "=========================================="
echo ""

python3 - <<'PYTHON_SCRIPT'
import subprocess
import json

# Get server instances
servers_output = subprocess.check_output([
    'aws', 'ec2', 'describe-instances',
    '--region', 'us-east-1',
    '--filters', 'Name=tag:Role,Values=server', 'Name=instance-state-name,Values=running',
    '--query', 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress,Placement.AvailabilityZone]',
    '--output', 'json'
]).decode('utf-8')

servers = []
for reservation in json.loads(servers_output):
    for instance in reservation:
        servers.append({
            'name': instance[0],
            'public_ip': instance[1],
            'private_ip': instance[2],
            'az': instance[3]
        })

servers.sort(key=lambda x: x['name'])

# Get client instances
clients_output = subprocess.check_output([
    'aws', 'ec2', 'describe-instances',
    '--region', 'us-east-1',
    '--filters', 'Name=tag:Role,Values=client', 'Name=instance-state-name,Values=running',
    '--query', 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]',
    '--output', 'json'
]).decode('utf-8')

clients = []
for reservation in json.loads(clients_output):
    for instance in reservation:
        clients.append({
            'name': instance[0],
            'public_ip': instance[1],
            'private_ip': instance[2]
        })

clients.sort(key=lambda x: x['name'])

print("\nYour instances are ready! Here's the information:\n")
print("SERVERS:")
for i, s in enumerate(servers[:9]):  # Only take first 9
    print(f"  server-{i}: {s['public_ip']} (private: {s['private_ip']}, AZ: {s['az']})")

print("\nCLIENTS:")
for i, c in enumerate(clients[:9]):  # Only take first 9
    print(f"  client-{i}: {c['public_ip']} (private: {c['private_ip']})")

PYTHON_SCRIPT

echo ""
echo "=========================================="
echo "✅ AWS SETUP COMPLETE!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Instance details saved to: $OUTPUT_FILE"
echo "2. SSH key saved to: ~/.ssh/${KEY_NAME}.pem"
echo "3. Test SSH connection:"
echo "     ssh -i ~/.ssh/${KEY_NAME}.pem ubuntu@<SERVER_PUBLIC_IP>"
echo ""
echo "Run the next script to update your config files:"
echo "     python3 update_configs.py"
echo ""
