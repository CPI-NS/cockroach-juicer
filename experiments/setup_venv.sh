#!/bin/bash
# Simple virtual environment setup for experiments
set -e

cd "$(dirname "$0")"

if [ -d "venv" ]; then
    echo "Virtual environment already exists."
else
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Installing dependencies..."
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo "✓ Setup complete. Virtual environment ready."
echo ""
echo "To activate: source venv/bin/activate"
