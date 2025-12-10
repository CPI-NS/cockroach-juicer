#!/bin/bash
# Install full prereqs on all AWS instances (merge of install_dependencies.sh + pre-req.sh)

set -e

KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"
REMOTE_USER="ubuntu"

# Read instance IPs from aws_instances_multiregion.txt
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTANCES_FILE="$SCRIPT_DIR/aws_instances_multiregion.txt"

if [ ! -f "$INSTANCES_FILE" ]; then
    echo "ERROR: Instance file not found: $INSTANCES_FILE"
    echo "Please run aws_start_instances.sh first to create this file"
    exit 1
fi

# Extract public IPs from the instances file
# Format: juicer-server-east1: public=34.204.67.217  private=172.31.82.214  region=us-east-1
INSTANCES=()
while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

    # Extract public IP from "public=IP" format
    if [[ "$line" =~ public=([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+) ]]; then
        INSTANCES+=("${BASH_REMATCH[1]}")
    fi
done < "$INSTANCES_FILE"

if [ ${#INSTANCES[@]} -eq 0 ]; then
    echo "ERROR: No instances found in $INSTANCES_FILE"
    exit 1
fi

echo "Found ${#INSTANCES[@]} instances to configure"

echo "========================================="
echo "Installing prerequisites on all instances"
echo "========================================="

# Full prereq script to execute remotely (from pre-req.sh)
REMOTE_PREREQ_SCRIPT=$(cat <<'REMOTE_PREREQ'
set -e

export DEBIAN_FRONTEND=noninteractive

# Optionally prime GitHub auth for private deps if GITHUB_TOKEN is provided
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    GH_USER="${GITHUB_USER:-token}"
    echo "machine github.com login ${GH_USER} password ${GITHUB_TOKEN}" | tee -a ~/.netrc >/dev/null
    chmod 600 ~/.netrc
    git config --global credential.helper "store"
    echo "  -> GitHub credentials configured via netrc"
fi

declare -A required_packages=(
    ["libncurses5-dev"]="6.0"
    ["git"]="1.9"
    ["bash"]="4.0"
    ["make"]="4.2"
    ["cmake"]="3.20"
    ["autoconf"]="2.68"
    ["bison"]=""
    ["patch"]="2.7"
    ["build-essential"]=""
    ["g++"]="6.0"
    ["curl"]=""
    ["software-properties-common"]=""
    ["haproxy"]="2.4"
    ["python3"]="3.8.2"
    ["python3-pip"]=""
    ["python3-setuptools"]=""
)

echo "==> Updating package lists..."
sudo apt-get update -y

echo "==> Upgrading system..."
sudo apt-get upgrade -y

echo "==> Installing required packages..."
for pkg in "${!required_packages[@]}"; do
    version="${required_packages[$pkg]}"

    if [[ -z "$version" ]]; then
        echo "Installing $pkg..."
        sudo apt-get install -y "$pkg"
    else
        echo "Installing $pkg (min version $version)..."
        installed_version=$(dpkg-query -W -f='${Version}' "$pkg" 2>/dev/null || echo "")

        if [[ -n "$installed_version" ]]; then
            dpkg --compare-versions "$installed_version" ge "$version" && {
                echo "  -> Already installed ($installed_version ≥ $version)"
                continue
            }
        fi

        sudo apt-get install -y "$pkg"
    fi
done

echo "==> Ensuring Bazelisk (bazel) is installed..."
if ! command -v bazel >/dev/null 2>&1; then
    BAZELISK_VERSION="v1.19.0"
    BAZELISK_URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-amd64"
    tmp_bazelisk=$(mktemp)
    curl -sSL "$BAZELISK_URL" -o "$tmp_bazelisk"
    sudo install -m 0755 "$tmp_bazelisk" /usr/local/bin/bazel
    sudo install -m 0755 "$tmp_bazelisk" /usr/local/bin/bazelisk
    rm -f "$tmp_bazelisk"
    echo "  -> Bazelisk installed to /usr/local/bin/bazel"
else
    echo "  -> bazel already installed"
fi

echo "==> Building and installing libresolv_wrapper from source..."
if [ ! -f "/usr/lib/x86_64-linux-gnu/libresolv_wrapper.so" ]; then
    workdir="$(mktemp -d)"
    trap 'rm -rf "$workdir"' EXIT
    cd "$workdir"
    git clone https://gitlab.com/cwrap/resolv_wrapper.git
    cd resolv_wrapper
    mkdir -p build && cd build
    cmake -DCMAKE_INSTALL_PREFIX=/usr ..
    make
    sudo make install
    sudo ldconfig
    echo "  -> libresolv_wrapper installed successfully"
else
    echo "  -> libresolv_wrapper already installed"
fi

echo "==> Creating compatibility wrapper for __res_nsearch..."
cat > /tmp/res_compat.c << 'EOF'
#include <resolv.h>
#include <netinet/in.h>
#include <arpa/nameser.h>

// Provide __res_nsearch by calling the public res_nsearch
int __res_nsearch(res_state statp, const char *dname, int class, int type,
                   unsigned char *answer, int anslen)
{
    return res_nsearch(statp, dname, class, type, answer, anslen);
}

// Also provide other potentially missing internal functions
int __res_nquery(res_state statp, const char *dname, int class, int type,
                  unsigned char *answer, int anslen)
{
    return res_nquery(statp, dname, class, type, answer, anslen);
}

int __res_nquerydomain(res_state statp, const char *name, const char *domain,
                        int class, int type, unsigned char *answer, int anslen)
{
    return res_nquerydomain(statp, name, domain, class, type, answer, anslen);
}
EOF

gcc -shared -fPIC -o /tmp/libres_compat.so /tmp/res_compat.c -lresolv
sudo cp /tmp/libres_compat.so /usr/lib/x86_64-linux-gnu/
sudo ldconfig
echo "  -> Resolver compatibility library created"

echo "==> Configuring Bazel..."
cat > ~/.bazelrc << 'EOF'
build --linkopt=-L/usr/lib/x86_64-linux-gnu
build --linkopt=-Wl,-rpath,/usr/lib/x86_64-linux-gnu
build --linkopt=-lres_compat
EOF
echo "  -> Bazel configuration updated"

echo "==> All packages installed successfully!"
REMOTE_PREREQ
)

install_on_host() {
    local HOST=$1

    echo ""
    echo "Installing on $HOST..."

    ssh -i "$KEY_PATH" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR \
        "${REMOTE_USER}@${HOST}" \
        "GITHUB_USER=${GITHUB_USER:-} GITHUB_TOKEN=${GITHUB_TOKEN:-} bash -s" <<<"$REMOTE_PREREQ_SCRIPT" \
        && echo "  ✓ Finished setup on $HOST" \
        || echo "  ✗ Failed setup on $HOST"
}

for INSTANCE in "${INSTANCES[@]}"; do
    install_on_host "$INSTANCE" &
done

# Wait for all background jobs to complete
wait

echo ""
echo "========================================="
echo "✓ All instances updated!"
echo "========================================="
