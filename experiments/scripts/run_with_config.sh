#!/bin/bash
# Helper script to run experiments with correct paths

# Detect cockroach binary location
COCKROACH_BIN=""

# Check common locations
if [ -f "../../cockroach" ]; then
    COCKROACH_BIN="../../cockroach"
elif [ -f "../cockroach" ]; then
    COCKROACH_BIN="../cockroach"
elif command -v cockroach &> /dev/null; then
    COCKROACH_BIN="cockroach"
else
    echo "Error: cockroach binary not found!"
    echo ""
    echo "Please build CockroachDB first:"
    echo "  cd ../../"
    echo "  make build"
    echo ""
    exit 1
fi

echo "Using cockroach binary: $COCKROACH_BIN"
echo ""

# Set environment variable for the framework
export COCKROACH_BIN="$COCKROACH_BIN"

# Run the experiment
python3 run_experiment.py "$@"
