#!/usr/bin/env bash

set -e

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
        # APT typically installs latest version available; to enforce a minimum
        # version we check first
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

echo "==> Building and installing libresolv_wrapper from source..."
if [ ! -f "/usr/lib/x86_64-linux-gnu/libresolv_wrapper.so" ]; then
    cd /tmp
    if [ -d "resolv_wrapper" ]; then
        rm -rf resolv_wrapper
    fi
    git clone https://gitlab.com/cwrap/resolv_wrapper.git
    cd resolv_wrapper
    mkdir -p build && cd build
    cmake -DCMAKE_INSTALL_PREFIX=/usr ..
    make
    sudo make install
    sudo ldconfig
    cd ~
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
