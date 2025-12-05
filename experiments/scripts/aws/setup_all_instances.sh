#!/bin/bash
# Run pre-req.sh on all AWS instances to install missing libraries

set -e

KEY_PATH="$HOME/.ssh/aws-juicer-key.pem"
REMOTE_USER="ubuntu"

# All instance IPs
INSTANCES=(
    # Servers
    "34.204.67.217"
    "54.208.110.229"
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
echo "Installing libres_compat.so on all instances"
echo "========================================="
echo ""
echo "This will create the missing resolver compatibility library"
echo "that cockroach needs to run."
echo ""

# Create a minimal version of the script (both libraries)
cat > /tmp/install_res_compat.sh << 'EOF'
#!/bin/bash
set -e

echo "Installing required dependencies..."

# Install build tools
sudo apt-get update -qq
sudo apt-get install -y -qq gcc libc6-dev git cmake make 2>/dev/null || true

# Build and install libresolv_wrapper
echo "Building libresolv_wrapper..."
if [ ! -f "/usr/lib/x86_64-linux-gnu/libresolv_wrapper.so" ]; then
    cd /tmp
    if [ -d "resolv_wrapper" ]; then
        rm -rf resolv_wrapper
    fi
    git clone https://gitlab.com/cwrap/resolv_wrapper.git >/dev/null 2>&1
    cd resolv_wrapper
    mkdir -p build && cd build
    cmake -DCMAKE_INSTALL_PREFIX=/usr .. >/dev/null 2>&1
    make >/dev/null 2>&1
    sudo make install >/dev/null 2>&1
    sudo ldconfig
    cd ~
    echo "  ✓ libresolv_wrapper installed"
else
    echo "  ✓ libresolv_wrapper already installed"
fi

# Create the compatibility library
echo "Creating libres_compat.so..."
cat > /tmp/res_compat.c << 'CEOF'
#include <resolv.h>
#include <netinet/in.h>
#include <arpa/nameser.h>

int __res_nsearch(res_state statp, const char *dname, int class, int type,
                   unsigned char *answer, int anslen)
{
    return res_nsearch(statp, dname, class, type, answer, anslen);
}

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
CEOF

# Compile and install
gcc -shared -fPIC -o /tmp/libres_compat.so /tmp/res_compat.c -lresolv
sudo cp /tmp/libres_compat.so /usr/lib/x86_64-linux-gnu/
sudo ldconfig

echo "✓ libres_compat.so installed"
rm -f /tmp/res_compat.c /tmp/libres_compat.so
EOF

chmod +x /tmp/install_res_compat.sh

# Function to install on one host
install_on_host() {
    local HOST=$1

    echo "Installing on $HOST..."

    # Copy script to host
    scp -i "$KEY_PATH" -o StrictHostKeyChecking=no /tmp/install_res_compat.sh "${REMOTE_USER}@${HOST}:/tmp/" >/dev/null 2>&1

    # Run script on host
    ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${HOST}" \
        'bash /tmp/install_res_compat.sh' 2>&1 | sed 's/^/  /'

    echo "  ✓ Completed $HOST"
    echo ""
}

# Install on all instances in parallel
for INSTANCE in "${INSTANCES[@]}"; do
    install_on_host "$INSTANCE" &
done

# Wait for all background jobs
echo "Waiting for all installations to complete..."
wait

echo ""
echo "========================================="
echo "✓ All instances updated!"
echo "========================================="
echo ""
echo "You can now re-run the experiment:"
echo "  source venv/bin/activate"
echo "  python scripts/run_experiment.py config configs/aws_multiregion_graph7_knee.yaml"
echo ""

# Cleanup
rm -f /tmp/install_res_compat.sh
