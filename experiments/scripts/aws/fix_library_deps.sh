#!/bin/bash
# Fix library dependencies by copying from build server to all instances

set -e

KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"
REMOTE_USER="ubuntu"
BUILD_SERVER="54.208.110.229"

# All other instance IPs (excluding build server)
INSTANCES=(
    # Servers
    "34.204.67.217"
    "54.91.203.85"
    "18.222.126.3"
    "3.17.66.206"
    "18.118.106.162"
    "3.101.151.1"
    "54.219.238.16"
    "18.144.81.88"
    # Clients
    "44.211.216.86"
    "34.207.68.148"
    "44.201.75.196"
    "3.17.189.229"
    "18.222.218.34"
    "3.17.133.122"
    "13.57.41.216"
    "54.215.105.27"
    "54.219.155.93"
)

echo "========================================="
echo "Checking library dependencies on build server"
echo "========================================="

# Find what libraries cockroach needs
echo "Finding required libraries..."
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${BUILD_SERVER}" \
    'ldd /home/ubuntu/cockroach 2>&1 || true'

echo ""
echo "Searching for libres_compat.so on build server..."
LIB_PATH=$(ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${BUILD_SERVER}" \
    'find /home/ubuntu -name "libres_compat.so*" 2>/dev/null | head -1' || echo "")

if [ -z "$LIB_PATH" ]; then
    echo "ERROR: libres_compat.so not found on build server!"
    echo ""
    echo "The issue is that the cockroach binary has unresolved dependencies."
    echo "This happened during the build process."
    echo ""
    echo "RECOMMENDED SOLUTION:"
    echo "Rebuild cockroach with static linking or use dev build crosslinux target:"
    echo ""
    echo "  ssh -i ~/.ssh/aws-juicer-key.pem ubuntu@54.208.110.229"
    echo "  cd cockroach-juicer"
    echo "  ./dev build cockroach --cross=linux"
    echo ""
    exit 1
fi

echo "Found library at: $LIB_PATH"
echo ""
echo "WARNING: Copying shared libraries between systems is risky!"
echo "BETTER SOLUTION: Rebuild with static linking"
echo ""
read -p "Continue anyway? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

echo "Distributing library to all instances..."

for INSTANCE in "${INSTANCES[@]}"; do
    echo "  Copying to $INSTANCE..."

    # Copy from build server to this instance
    ssh -i "$KEY_PATH" "${REMOTE_USER}@${BUILD_SERVER}" \
        "scp -i ~/.ssh/aws-juicer-key.pem -o StrictHostKeyChecking=no ${LIB_PATH} ${REMOTE_USER}@${INSTANCE}:/tmp/" \
        2>/dev/null && echo "    ✓ Copied" || echo "    ✗ Failed"

    # Move to system library location
    ssh -i "$KEY_PATH" "${REMOTE_USER}@${INSTANCE}" \
        "sudo mv /tmp/$(basename $LIB_PATH) /usr/lib/x86_64-linux-gnu/ && sudo ldconfig" \
        2>/dev/null && echo "    ✓ Installed" || echo "    ✗ Failed to install"
done

echo ""
echo "========================================="
echo "Done!"
echo "========================================="
