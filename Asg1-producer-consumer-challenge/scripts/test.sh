#!/bin/bash
# scripts/test.sh
# Run tests using pytest

# Ensure we are in the project root
cd "$(dirname "$0")/.."

# Check if venv exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Virtual environment not found. Please set it up first."
    exit 1
fi

# Run tests
pytest tests
