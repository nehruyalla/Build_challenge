#!/bin/bash
# Run RFM whale analysis only

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
echo "StreamSight: RFM Whale Analysis"
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
    echo "Please run: ./scripts/convert_excel.sh"
    exit 1
fi

# Run RFM analysis
uv run python cli/run_rfm_whales.py

echo ""
echo "RFM analysis complete!"
echo ""

