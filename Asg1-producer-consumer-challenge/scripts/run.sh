#!/bin/bash
# scripts/run.sh
# Run the Producer-Consumer application

# Ensure we are in the project root
cd "$(dirname "$0")/.."

# Check if venv exists (uv or standard)
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Virtual environment not found. Please set it up first."
    exit 1
fi

# Run the application
python -m src.main
