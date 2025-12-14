#!/bin/bash
# Run the streaming analytics pipeline only
# (Assumes CSV file already exists)

set -e  # Exit on error

# Load environment variables from .env if it exists
if [ -f ".env" ]; then
    echo "Loading configuration from .env file..."
    set -a  # automatically export all variables
    source .env
    set +a
    echo ""
fi

echo "=========================================="
echo "StreamSight: Analytics Pipeline"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "ERROR: Virtual environment not found"
    echo "Please run: ./scripts/setup.sh"
    exit 1
fi

# Check if CSV exists
if [ ! -f "dataset/Online_Retail.csv" ]; then
    echo "ERROR: CSV file not found at dataset/Online_Retail.csv"
    echo ""
    echo "Options:"
    echo "  1. Run full pipeline: ./scripts/run.sh"
    echo "  2. Convert Excel manually: ./scripts/convert_excel.sh"
    exit 1
fi

# Run analytics
uv run python cli/run_streaming_analytics.py

echo ""
echo "Analytics complete!"
echo ""

