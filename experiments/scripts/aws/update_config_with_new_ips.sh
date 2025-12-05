#!/bin/bash
# Script to fetch new public IPs after restart and update config file

set -e

echo "========================================="
echo "Update Config with New Public IPs"
echo "========================================="
echo ""

CONFIG_FILE="configs/aws_multiregion_graph7_knee.yaml"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Config file not found: $CONFIG_FILE"
    exit 1
fi

# Backup original config
BACKUP_FILE="${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
cp "$CONFIG_FILE" "$BACKUP_FILE"
echo "✓ Backed up config to: $BACKUP_FILE"
echo ""

# Function to get instances with their IPs in a region
get_instances_by_internal_ip() {
    local REGION=$1
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Project,Values=juicer-eval" "Name=instance-state-name,Values=running" \
        --query 'Reservations[*].Instances[*].[PrivateIpAddress,PublicIpAddress,Tags[?Key==`Name`].Value|[0]]' \
        --output text 2>/dev/null
}

echo "Fetching new public IPs from AWS..."
echo ""

# Create a mapping file
MAPPING_FILE="/tmp/ip_mapping.txt"
rm -f "$MAPPING_FILE"

REGIONS=("us-east-1" "us-east-2" "us-west-1")

for REGION in "${REGIONS[@]}"; do
    echo "Region: $REGION"
    INSTANCES=$(get_instances_by_internal_ip "$REGION")

    if [ -n "$INSTANCES" ]; then
        echo "$INSTANCES" | while read -r private_ip public_ip name; do
            echo "  $name: $private_ip -> $public_ip"
            echo "$private_ip|$public_ip" >> "$MAPPING_FILE"
        done
    fi
    echo ""
done

if [ ! -f "$MAPPING_FILE" ]; then
    echo "ERROR: No running instances found!"
    exit 1
fi

echo "Updating config file..."
echo ""

# Create a temporary Python script to update the YAML
cat > /tmp/update_yaml.py << 'PYTHON_SCRIPT'
import sys
import re

config_file = sys.argv[1]
mapping_file = sys.argv[2]

# Read IP mappings
ip_map = {}
with open(mapping_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line:
            private_ip, public_ip = line.split('|')
            ip_map[private_ip] = public_ip

# Read config file
with open(config_file, 'r') as f:
    config_content = f.read()

# Update hostnames based on internal_ip
updated_count = 0
lines = config_content.split('\n')
new_lines = []

i = 0
while i < len(lines):
    line = lines[i]

    # Check if this line defines internal_ip
    internal_ip_match = re.match(r'(\s+)internal_ip:\s*"([0-9.]+)"', line)

    if internal_ip_match:
        indent = internal_ip_match.group(1)
        internal_ip = internal_ip_match.group(2)

        # Look for hostname in nearby lines (could be before or after)
        # Search backwards first (up to 5 lines)
        hostname_line_idx = None
        for j in range(max(0, i-5), i):
            if re.match(r'\s+hostname:', lines[j]):
                hostname_line_idx = j
                break

        # If not found, search forwards (up to 5 lines)
        if hostname_line_idx is None:
            for j in range(i+1, min(len(lines), i+6)):
                if re.match(r'\s+hostname:', lines[j]):
                    hostname_line_idx = j
                    break

        # Update hostname if found and we have a mapping
        if hostname_line_idx is not None and internal_ip in ip_map:
            new_public_ip = ip_map[internal_ip]
            old_line = lines[hostname_line_idx]
            lines[hostname_line_idx] = re.sub(
                r'(hostname:\s*)"[^"]*"',
                f'\\1"{new_public_ip}"',
                old_line
            )
            if lines[hostname_line_idx] != old_line:
                updated_count += 1
                print(f"  Updated {internal_ip} -> {new_public_ip}")

    i += 1

# Write updated config
with open(config_file, 'w') as f:
    f.write('\n'.join(lines))

print(f"\n✓ Updated {updated_count} hostnames")
PYTHON_SCRIPT

# Run the Python script
python3 /tmp/update_yaml.py "$CONFIG_FILE" "$MAPPING_FILE"

# Cleanup
rm -f "$MAPPING_FILE"
rm -f /tmp/update_yaml.py

echo ""
echo "========================================="
echo "✅ Config file updated successfully!"
echo "========================================="
echo ""
echo "Original config backed up to: $BACKUP_FILE"
echo ""
echo "You can now run the experiment:"
echo "  python3 scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml --skip-deploy"
echo ""
