#!/bin/bash
# Main script to run the complete StreamSight pipeline
# Converts Excel > CSV > Runs Analytics > Generates Reports

set -e  # Exit on error

# Load environment variables from .env if it exists
# Only load variables that aren't already set (environment variables take precedence)
if [ -f ".env" ]; then
    echo "Loading configuration from .env file..."
    set -a  # automatically export all variables
    source .env
    set +a
    echo ""
fi

echo "=========================================="
echo "StreamSight: Complete Analytics Pipeline"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "ERROR: Virtual environment not found"
    echo "Please run: ./scripts/setup.sh"
    exit 1
fi

# Check if dataset exists
if [ ! -f "dataset/Online Retail.xlsx" ] && [ ! -f "dataset/Online_Retail.csv" ]; then
    echo "ERROR: Dataset not found"
    echo "Please place your data file at: dataset/Online Retail.xlsx"
    exit 1
fi

# Run the complete pipeline
uv run python cli/run_all.py

echo ""
echo "=========================================="
echo "Pipeline completed!"
echo "=========================================="
echo ""
echo "View your results:"
echo "  -  Report: results/reports/SUMMARY.md"
echo "  - Figures: results/figures/"
echo "  - Data Tables: results/tables/"
echo ""

