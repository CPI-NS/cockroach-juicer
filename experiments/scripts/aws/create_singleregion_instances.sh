#!/bin/bash
# Create 9 server and 9 client instances in us-east-1 for single-region experiments

set -e

AMI="ami-0fb0b230890ccd1e6"
KEY_NAME="aws-juicer-key"
SECURITY_GROUP="sg-0f11f26998fc9c165"
SUBNET="subnet-089037d495e4a5c55"
INSTANCE_TYPE="c5d.2xlarge"
REGION="us-east-1"

echo "========================================="
echo "Creating Single-Region Instances"
echo "========================================="
echo "Instance Type: $INSTANCE_TYPE"
echo "Region: $REGION"
echo "AMI: $AMI"
echo ""

# Create 9 server instances
echo "Creating 9 server instances..."
SERVER_IDS=$(aws ec2 run-instances \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP" \
    --subnet-id "$SUBNET" \
    --count 9 \
    --region "$REGION" \
    --block-device-mappings '[
        {
            "DeviceName": "/dev/sda1",
            "Ebs": {
                "VolumeSize": 100,
                "VolumeType": "gp3",
                "DeleteOnTermination": true
            }
        }
    ]' \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=juicer-server-singleregion},{Key=Role,Value=server},{Key=Experiment,Value=singleregion}]' \
    --query 'Instances[*].InstanceId' \
    --output text)

echo "✓ Created server instances: $SERVER_IDS"

# Create 9 client instances
echo "Creating 9 client instances..."
CLIENT_IDS=$(aws ec2 run-instances \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP" \
    --subnet-id "$SUBNET" \
    --count 9 \
    --region "$REGION" \
    --block-device-mappings '[
        {
            "DeviceName": "/dev/sda1",
            "Ebs": {
                "VolumeSize": 100,
                "VolumeType": "gp3",
                "DeleteOnTermination": true
            }
        }
    ]' \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=juicer-client-singleregion},{Key=Role,Value=client},{Key=Experiment,Value=singleregion}]' \
    --query 'Instances[*].InstanceId' \
    --output text)

echo "✓ Created client instances: $CLIENT_IDS"

echo ""
echo "Waiting for instances to be running..."
ALL_IDS="$SERVER_IDS $CLIENT_IDS"
aws ec2 wait instance-running --instance-ids $ALL_IDS --region "$REGION"
echo "✓ All instances are running"

echo ""
echo "Waiting for status checks to pass (this may take 2-3 minutes)..."
aws ec2 wait instance-status-ok --instance-ids $ALL_IDS --region "$REGION"
echo "✓ All instances passed status checks"

echo ""
echo "========================================="
echo "Instance Summary"
echo "========================================="

echo ""
echo "Server Instances:"
aws ec2 describe-instances \
    --instance-ids $SERVER_IDS \
    --region "$REGION" \
    --query 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress,PrivateIpAddress,State.Name]' \
    --output table

echo ""
echo "Client Instances:"
aws ec2 describe-instances \
    --instance-ids $CLIENT_IDS \
    --region "$REGION" \
    --query 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress,PrivateIpAddress,State.Name]' \
    --output table

echo ""
echo "========================================="
echo "✅ Setup Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Update config file with new IPs:"
echo "   python3 scripts/aws/update_singleregion_config.py"
echo ""
echo "2. Deploy binaries to new instances"
echo "3. Run experiments"
