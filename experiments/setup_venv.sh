#!/bin/bash
# Simple dependency installation for experiments
set -e

cd "$(dirname "$0")"

echo "Checking for pip..."
if ! python3 -m pip --version &>/dev/null; then
    echo "⚠ pip not found. Installing pip first..."
    echo ""

    # Try to install pip using get-pip.py
    if command -v wget &>/dev/null; then
        wget -q https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
        python3 /tmp/get-pip.py --user
        rm /tmp/get-pip.py
    elif command -v curl &>/dev/null; then
        curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
        python3 /tmp/get-pip.py --user
        rm /tmp/get-pip.py
    else
        echo "ERROR: Neither wget nor curl available to download pip installer."
        echo "Please install pip manually or ask admin to install python3-pip package."
        exit 1
    fi

    # Add user bin to PATH for this session
    export PATH="$HOME/.local/bin:$PATH"
    echo "✓ pip installed to ~/.local/bin"
    echo ""
fi

echo "Attempting to create virtual environment..."
if python3 -m venv venv 2>/dev/null; then
    echo "✓ Virtual environment created."
    source venv/bin/activate
    echo "Installing dependencies in virtual environment..."
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    echo ""
    echo "✓ Setup complete. Virtual environment ready."
    echo "To activate: source venv/bin/activate"
else
    echo "⚠ Virtual environment creation failed (missing python3-venv)."
    echo "Installing dependencies to user directory instead..."
    echo ""
    python3 -m pip install --user --upgrade pip -q
    python3 -m pip install --user -r requirements.txt -q
    echo ""
    echo "✓ Setup complete. Dependencies installed to user directory (~/.local/lib/python3.x/site-packages)"
    echo ""
    echo "IMPORTANT: Add to your PATH if not already there:"
    echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "No need to activate anything - dependencies are globally available."
fi
