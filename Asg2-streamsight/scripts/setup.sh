#!/bin/bash
# Setup script for StreamSight
# This script installs dependencies and sets up the environment

set -e  # Exit on error

echo "=========================================="
echo "StreamSight Environment Setup"
echo "=========================================="
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "ERROR: uv is not installed"
    echo ""
    echo "Please install uv first:"
    echo "  macOS/Linux: curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo "  Windows: powershell -c \"irm https://astral.sh/uv/install.ps1 | iex\""
    echo "  Or with pip: pip install uv"
    exit 1
fi

echo "Found uv: $(which uv)"
echo ""

# Check Python version
echo "Checking Python version..."
PYTHON_VERSION=$(uv run python --version 2>&1 || python3 --version 2>&1)
echo "Python: $PYTHON_VERSION"
echo ""

# Install dependencies
echo "Installing dependencies..."
echo "This may take a few minutes..."
echo ""
uv sync

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Virtual environment created at: .venv"
echo ""
echo "Configuration:"
if [ -f ".env" ]; then
    echo "  - Using existing .env file for configuration"
else
    echo "  - Using default configuration"
    echo "  - Optional: Create .env file to customize parameters"
    echo "    (See .env.example for available options)"
fi
echo ""
echo "Next steps:"
echo "  1. Place your data file: dataset/Online Retail.xlsx"
echo "  2. (Optional) Customize config: cp .env.example .env"
echo "  3. Run the pipeline: ./scripts/run.sh"
echo "  4. View results: results/reports/SUMMARY.md"
echo ""

