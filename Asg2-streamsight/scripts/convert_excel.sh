#!/bin/bash
# Convert Excel file to CSV format

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
echo "Excel to CSV Converter"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "ERROR: Virtual environment not found"
    echo "Please run: ./scripts/setup.sh"
    exit 1
fi

# Check if Excel file exists
if [ ! -f "dataset/Online Retail.xlsx" ]; then
    echo "ERROR: Excel file not found"
    echo "Please place your Excel file at: dataset/Online Retail.xlsx"
    exit 1
fi

# Convert Excel to CSV
uv run python cli/convert_excel_to_csv.py "$@"

echo ""
echo "Conversion complete!"
echo "CSV file: dataset/Online_Retail.csv"
echo ""

