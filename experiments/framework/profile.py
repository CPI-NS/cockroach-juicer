"""CockroachDB Development Cluster Profile for CloudLab

This profile creates a cluster for running a modified CockroachDB (cockroach-juicer):
- 1 Control node (for building, orchestrating experiments)
- 2 Client nodes (for workload generation)
- 2 Server nodes (for running CockroachDB)

All nodes are connected via a private LAN. The control node can SSH
to all other nodes using hostnames (client0, client1, server0, server1).

Pre-installed software:
- Control node: Go 1.22.0, Bazel 7.0.0, Python 3 venv with dependencies
- All nodes: Basic utilities, system tuning for database workloads

Instructions:
1. Wait for all nodes to finish setup (check /local/setup-complete.log on each node)
2. SSH to the control node first
3. From control, run /local/setup-ssh-keys.sh to enable passwordless SSH
4. Clone your cockroach-juicer repo and build CockroachDB
5. Deploy and run your experiments from control node
"""

import geni.portal as portal
import geni.rspec.pg as pg

# Create portal context and request
pc = portal.Context()
request = pc.makeRequestRSpec()

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
# Hardware types ordered by CURRENT availability (check resinfo.php for updates)
pc.defineParameter("hardware_type", "Hardware Type",
    portal.ParameterType.STRING, "d710",
    legalValues=[
        # Emulab - Best current availability
        ("d710", "d710 - 64GB RAM, 8 cores (Emulab, ~76 free)"),
        ("d430", "d430 - 64GB RAM, 8 cores (Emulab, ~11 free)"),
        # Utah
        ("c6620", "c6620 - 128GB RAM, 28 cores, 100Gb (Utah, ~13 free)"),
        ("d6515", "d6515 - 128GB RAM, 32 cores, 100Gb (Utah, ~4 free)"),
        ("m510", "m510 - 64GB RAM, 8 cores, NVMe (Utah)"),
        ("xl170", "xl170 - 64GB RAM, 10 cores (Utah)"),
        ("c6525-25g", "c6525-25g - 128GB RAM, 16 cores (Utah)"),
        # Wisconsin
        ("c220g1", "c220g1 - 128GB RAM, 16 cores (Wisconsin, ~6 free)"),
        ("c220g2", "c220g2 - 160GB RAM, 20 cores (Wisconsin, ~5 free)"),
        ("c220g5", "c220g5 - 192GB RAM, 20 cores (Wisconsin, often busy)"),
        # Clemson
        ("c6420", "c6420 - 384GB RAM, 32 cores (Clemson)"),
        ("c8220", "c8220 - 256GB RAM, 20 cores (Clemson)"),
        ("c6320", "c6320 - 256GB RAM, 28 cores (Clemson)")
    ],
    longDescription="Hardware type. Default d710 has best availability (~76 free). " +
                    "Check https://www.cloudlab.us/resinfo.php for current availability.")

pc.defineParameter("os_image", "OS Image",
    portal.ParameterType.IMAGE,
    "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD",
    longDescription="Ubuntu 22.04 LTS - recommended for CockroachDB.")

params = pc.bindParameters()
pc.verifyParameters()

# ---------------------------------------------------------------------------
# Network Configuration
# ---------------------------------------------------------------------------
# IP assignments:
#   control:  10.10.1.1
#   client0:  10.10.1.10
#   client1:  10.10.1.11
#   server0:  10.10.1.100
#   server1:  10.10.1.101

NODE_IPS = {
    "control": "10.10.1.1",
    "client0": "10.10.1.10",
    "client1": "10.10.1.11",
    "server0": "10.10.1.100",
    "server1": "10.10.1.101",
}

NETMASK = "255.255.255.0"

# Create cluster LAN
lan = pg.LAN("cluster-lan")

# ---------------------------------------------------------------------------
# Setup Scripts
# ---------------------------------------------------------------------------

# Generate /etc/hosts entries for all nodes
HOSTS_ENTRIES = "\n".join(["{0} {1}".format(ip, name) for name, ip in NODE_IPS.items()])

# Common setup script for ALL nodes (lightweight)
COMMON_SETUP = """#!/bin/bash
set -ex

NODE_ID=$(geni-get client_id)
echo "Setting up node: $NODE_ID"

# Update system packages
sudo apt-get update
sudo apt-get install -y wget curl htop iotop sysstat net-tools git

# Add all cluster nodes to /etc/hosts
cat << 'EOF' | sudo tee -a /etc/hosts

# CockroachDB Cluster Nodes
%s
EOF

# Disable swap (critical for CockroachDB performance)
sudo swapoff -a
sudo sed -i '/swap/d' /etc/fstab

# Set transparent huge pages to madvise (CockroachDB recommendation)
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag

# Increase file descriptor limits
cat << 'EOF' | sudo tee -a /etc/security/limits.conf
* soft nofile 65535
* hard nofile 65535
* soft nproc 65535
* hard nproc 65535
EOF

# Create data directory for CockroachDB
sudo mkdir -p /data/cockroach
sudo chown -R $(whoami):$(whoami) /data

# Setup SSH directory for accepting keys from control node
mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

echo "Common setup complete on $NODE_ID at $(date)" | sudo tee /local/setup-complete.log
""" % HOSTS_ENTRIES

# Control node setup - Go, Bazel, Python venv, SSH orchestration
CONTROL_SETUP = """#!/bin/bash
set -ex

# Wait for common setup to complete
while [ ! -f /local/setup-complete.log ]; do sleep 5; done

echo "Installing development tools on control node..."

# ===========================================================================
# Install build dependencies
# ===========================================================================
sudo apt-get install -y \
    build-essential \
    autoconf \
    automake \
    cmake \
    libncurses-dev \
    libresolv-wrapper \
    libtool \
    bison \
    flex \
    apt-transport-https \
    gnupg

# ===========================================================================
# Install Go
# ===========================================================================
echo "Installing Go..."
GO_VERSION="1.22.0"
cd /tmp
wget -q https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz
rm go${GO_VERSION}.linux-amd64.tar.gz

# Add Go to PATH for all users
cat << 'EOF' | sudo tee /etc/profile.d/go.sh
export GOROOT=/usr/local/go
export GOPATH=$HOME/go
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
EOF
sudo chmod +x /etc/profile.d/go.sh
source /etc/profile.d/go.sh

# Verify Go installation
go version

# ===========================================================================
# Install Bazel
# ===========================================================================
echo "Installing Bazel..."
BAZEL_VERSION="7.0.0"

curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor | sudo tee /usr/share/keyrings/bazel-archive-keyring.gpg > /dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list

sudo apt-get update
sudo apt-get install -y bazel-${BAZEL_VERSION}
sudo ln -sf /usr/bin/bazel-${BAZEL_VERSION} /usr/bin/bazel

# Verify Bazel installation
bazel --version

# ===========================================================================
# Install Python 3 and create virtual environment
# ===========================================================================
echo "Setting up Python virtual environment..."
sudo apt-get install -y python3 python3-pip python3-venv python3-dev

# Create virtual environment
python3 -m venv ~/venv

# Install Python dependencies
~/venv/bin/pip install --upgrade pip
~/venv/bin/pip install \
    'pyyaml>=6.0' \
    'numpy>=1.24.0' \
    'matplotlib>=3.7.0' \
    'paramiko>=3.0.0'

# Add venv activation to bashrc
echo "" >> ~/.bashrc
echo "# Activate Python virtual environment" >> ~/.bashrc
echo "source ~/venv/bin/activate" >> ~/.bashrc

# Verify Python packages
~/venv/bin/python -c "import yaml, numpy, matplotlib, paramiko; print('Python packages installed successfully')"

# ===========================================================================
# SSH setup for orchestration
# ===========================================================================
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
fi

cat << 'EOF' > ~/.ssh/config
Host client0 client1 server0 server1
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
EOF
chmod 600 ~/.ssh/config

cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# ===========================================================================
# Helper scripts
# ===========================================================================

# Script to distribute SSH keys
cat << 'SCRIPT' > /local/setup-ssh-keys.sh
#!/bin/bash
echo "Distributing SSH public key to all cluster nodes..."
echo "You will be prompted for your password for each node."
echo ""

PUBKEY=$(cat ~/.ssh/id_rsa.pub)

for node in client0 client1 server0 server1; do
    echo "Copying key to $node..."
    ssh $node "mkdir -p ~/.ssh && chmod 700 ~/.ssh && echo '$PUBKEY' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
done

echo ""
echo "SSH keys distributed! You can now SSH to nodes without a password."
echo "Test with: ssh client0"
SCRIPT
chmod +x /local/setup-ssh-keys.sh

# Script to check node connectivity
cat << 'SCRIPT' > /local/check-nodes.sh
#!/bin/bash
echo "=== Checking Cluster Node Status ==="
echo ""
for node in client0 client1 server0 server1; do
    if ping -c 1 -W 2 $node > /dev/null 2>&1; then
        echo "  $node: REACHABLE"
    else
        echo "  $node: UNREACHABLE"
    fi
done
SCRIPT
chmod +x /local/check-nodes.sh

# Script to check installed versions
cat << 'SCRIPT' > /local/check-versions.sh
#!/bin/bash
source /etc/profile.d/go.sh 2>/dev/null || true
echo "=== Installed Software Versions ==="
echo "Go:     $(go version 2>/dev/null || echo 'Not installed')"
echo "Bazel:  $(bazel --version 2>/dev/null || echo 'Not installed')"
echo "Python: $(~/venv/bin/python --version 2>/dev/null || echo 'Not installed')"
echo ""
echo "=== Python Packages (in venv) ==="
~/venv/bin/pip list 2>/dev/null | grep -E "^(PyYAML|numpy|matplotlib|paramiko)" || echo "Not installed"
SCRIPT
chmod +x /local/check-versions.sh

# Script to run command on all worker nodes
cat << 'SCRIPT' > /local/run-on-all.sh
#!/bin/bash
if [ -z "$1" ]; then
    echo "Usage: /local/run-on-all.sh <command>"
    echo "Runs the given command on all client and server nodes"
    exit 1
fi

for node in client0 client1 server0 server1; do
    echo "=== $node ==="
    ssh $node "$@"
    echo ""
done
SCRIPT
chmod +x /local/run-on-all.sh

# Script to run command on server nodes only
cat << 'SCRIPT' > /local/run-on-servers.sh
#!/bin/bash
if [ -z "$1" ]; then
    echo "Usage: /local/run-on-servers.sh <command>"
    echo "Runs the given command on all server nodes"
    exit 1
fi

for node in server0 server1; do
    echo "=== $node ==="
    ssh $node "$@"
    echo ""
done
SCRIPT
chmod +x /local/run-on-servers.sh

# Script to run command on client nodes only
cat << 'SCRIPT' > /local/run-on-clients.sh
#!/bin/bash
if [ -z "$1" ]; then
    echo "Usage: /local/run-on-clients.sh <command>"
    echo "Runs the given command on all client nodes"
    exit 1
fi

for node in client0 client1; do
    echo "=== $node ==="
    ssh $node "$@"
    echo ""
done
SCRIPT
chmod +x /local/run-on-clients.sh

echo "Control node setup complete at $(date)" | sudo tee -a /local/setup-complete.log
"""

# Server node setup (minimal)
SERVER_SETUP = """#!/bin/bash
set -ex

# Wait for common setup to complete
while [ ! -f /local/setup-complete.log ]; do sleep 5; done

echo "Server node setup complete at $(date)" | sudo tee -a /local/setup-complete.log
"""

# Client node setup (minimal)
CLIENT_SETUP = """#!/bin/bash
set -ex

# Wait for common setup to complete
while [ ! -f /local/setup-complete.log ]; do sleep 5; done

echo "Client node setup complete at $(date)" | sudo tee -a /local/setup-complete.log
"""

# ---------------------------------------------------------------------------
# Node Definitions
# ---------------------------------------------------------------------------

# Control Node
control = request.RawPC("control")
control.hardware_type = params.hardware_type
control.disk_image = params.os_image
control.routable_control_ip = True

iface = control.addInterface("if-control")
iface.addAddress(pg.IPv4Address(NODE_IPS["control"], NETMASK))
lan.addInterface(iface)

control.addService(pg.Execute(shell="bash", command=COMMON_SETUP))
control.addService(pg.Execute(shell="bash", command=CONTROL_SETUP))

# Client Nodes (2)
for i in range(2):
    name = "client%d" % i
    client = request.RawPC(name)
    client.hardware_type = params.hardware_type
    client.disk_image = params.os_image
    
    iface = client.addInterface("if-%s" % name)
    iface.addAddress(pg.IPv4Address(NODE_IPS[name], NETMASK))
    lan.addInterface(iface)
    
    client.addService(pg.Execute(shell="bash", command=COMMON_SETUP))
    client.addService(pg.Execute(shell="bash", command=CLIENT_SETUP))

# Server Nodes (2)
for i in range(2):
    name = "server%d" % i
    server = request.RawPC(name)
    server.hardware_type = params.hardware_type
    server.disk_image = params.os_image
    
    iface = server.addInterface("if-%s" % name)
    iface.addAddress(pg.IPv4Address(NODE_IPS[name], NETMASK))
    lan.addInterface(iface)
    
    server.addService(pg.Execute(shell="bash", command=COMMON_SETUP))
    server.addService(pg.Execute(shell="bash", command=SERVER_SETUP))

# Add LAN to request
request.addResource(lan)

# ---------------------------------------------------------------------------
# Output RSpec
# ---------------------------------------------------------------------------
pc.printRequestRSpec()