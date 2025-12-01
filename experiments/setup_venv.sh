#!/bin/bash
# Simple dependency installation for experiments
set -e

cd "$(dirname "$0")"

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
    echo "✓ Setup complete. Dependencies installed to user directory."
    echo "No need to activate anything - dependencies are globally available."
fi
