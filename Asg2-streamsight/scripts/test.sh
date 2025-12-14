#!/bin/bash
# Run all tests for StreamSight

set -e  # Exit on error

echo "=========================================="
echo "StreamSight: Running Tests"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "ERROR: Virtual environment not found"
    echo "Please run: ./scripts/setup.sh"
    exit 1
fi

# Parse arguments
VERBOSE=""
COVERAGE="--cov=src/streamsight --cov-report=term-missing"
TEST_PATH="tests/"

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        --no-cov)
            COVERAGE=""
            shift
            ;;
        *)
            TEST_PATH="$1"
            shift
            ;;
    esac
done

# Clear PYTHONPATH to avoid conflicts with other projects
unset PYTHONPATH

# Add source directory to PYTHONPATH for tests
export PYTHONPATH="${PWD}/src:${PYTHONPATH}"

# Run tests
echo "Running pytest..."
echo ""
.venv/bin/pytest $TEST_PATH $VERBOSE $COVERAGE

echo ""
echo "=========================================="
echo "Tests completed!"
echo "=========================================="
echo ""

