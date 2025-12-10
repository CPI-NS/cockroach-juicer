#!/bin/bash
# Setup control node with all necessary tools and code

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <control-node-public-ip>"
    exit 1
fi

CONTROL_NODE="$1"
KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"
REMOTE_USER="ubuntu"

echo "========================================="
echo "Setup Juicer Control Node"
echo "========================================="
echo ""
echo "Control Node: $CONTROL_NODE"
echo ""

# Function to run SSH commands
run_ssh() {
    ssh -i "$KEY_PATH" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR \
        "${REMOTE_USER}@${CONTROL_NODE}" "$@"
}

# Function to copy files
copy_file() {
    scp -i "$KEY_PATH" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR \
        "$1" "${REMOTE_USER}@${CONTROL_NODE}:$2"
}

echo "Step 1/6: Installing system dependencies..."
run_ssh "sudo apt-get update -qq && sudo apt-get install -y -qq git build-essential python3-pip" 2>&1 | grep -v "debconf" || true

echo "Step 2/6: Installing Go..."
run_ssh 'wget -q https://go.dev/dl/go1.21.5.linux-amd64.tar.gz && \
         sudo rm -rf /usr/local/go && \
         sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz && \
         rm go1.21.5.linux-amd64.tar.gz && \
         echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.bashrc'

echo "Step 3/6: Installing Bazelisk (Bazel version manager)..."
run_ssh 'wget -q https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64 && \
         chmod +x bazelisk-linux-amd64 && \
         sudo mv bazelisk-linux-amd64 /usr/local/bin/bazel'

echo "Step 4/6: Installing Python dependencies..."
run_ssh "pip3 install --quiet paramiko pyyaml"

echo "Step 5/6: Cloning cockroach-juicer repository from CPI-NS..."
run_ssh "git clone git@github.com:CPI-NS/cockroach-juicer.git 2>&1 | grep -v 'Cloning' || true" || \
run_ssh "cd cockroach-juicer && git pull"

echo "Step 6/6: Checking out branch juicer-v25.3.50-harris..."
run_ssh "cd cockroach-juicer && git fetch origin && git checkout juicer-v25.3.50-harris && git pull origin juicer-v25.3.50-harris"

echo ""
echo "Step 7/8: Copying SSH keys..."
# Copy AWS key for node access
copy_file "$KEY_PATH" "~/.ssh/aws-juicer-key.pem"
run_ssh "chmod 600 ~/.ssh/aws-juicer-key.pem"

# Copy GitHub SSH key for repo access
if [ -f "$HOME/.ssh/id_ed25519" ]; then
    echo "Copying GitHub SSH key..."
    copy_file "$HOME/.ssh/id_ed25519" "~/.ssh/id_ed25519"
    run_ssh "chmod 600 ~/.ssh/id_ed25519"
else
    echo "⚠️  Warning: GitHub SSH key not found at ~/.ssh/id_ed25519"
fi

echo ""
echo "========================================="
echo "✓ Control Node Setup Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. SSH to control node:"
echo "   ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@$CONTROL_NODE"
echo ""
echo "2. Build binaries:"
echo "   cd ~/cockroach-juicer"
echo "   export PATH=\$PATH:/usr/local/go/bin"
echo "   ./dev doctor"
echo "   ./dev build cockroach"
echo "   go build -o benchmark pkg/cmd/benchmark/main.go"
echo ""
echo "3. Run experiments:"
echo "   cd ~/cockroach-juicer/experiments"
echo "   python3 scripts/run_experiment.py configs/aws_multiregion_graph7_knee.yaml"
echo ""
