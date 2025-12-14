# StreamSight: Production-Grade Streaming Analytics Pipeline

**StreamSight** is a production-grade, modular Python streaming analytics pipeline designed for processing large-scale sales data with financial accuracy, memory efficiency, and functional programming paradigms.

## Key Features

- Streaming Architecture: Generator-based pipeline that never loads the entire dataset into memory
- Financial Accuracy: Decimal-based arithmetic preventing floating-point errors
- Memory Efficient: Capable of processing 10GB files on 1GB RAM machines
- Dead Letter Queue: Robust error handling with validation error tracking
- RFM Whale Detection: Two-pass customer segmentation to identify high-value customers
- Anomaly Detection: Streaming Z-score analysis using Welford's online algorithm
- Rich Visualizations: User friendly charts and reports

---

## Setup Instructions

### Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) package manager

Install uv if you haven't already:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with pip
pip install uv
```

### Step 1: Setup

```bash
# Clone/navigate to the project directory
cd Asg2-streamsight

# Run setup script (installs dependencies and creates virtual environment)
./scripts/setup.sh
```

The setup script will:

- Create a virtual environment in `.venv/`
- Install all required dependencies
- Install the package in editable mode

### Step 2: Prepare Your Data

Place your CSV file in the `dataset/` directory:

```bash
dataset/Online Retail.csv
```

### Step 3: Run the Pipeline

```bash
# Run everything (Excel conversion + analytics + reports)
./scripts/run.sh

# OR run step-by-step:
./scripts/run_analytics.sh      # Run analytics
./scripts/run_rfm.sh            # Run RFM analysis only
```

### Step 4: View Results

```bash
# Open the  summary
open results/reports/SUMMARY.md

# View visualizations
open results/figures/
```

---

## Configuration (Optional)

Copy and edit the configuration file:

```bash
cp .env.example .env
# Edit .env to customize parameters
```

Available parameters:

- `STREAMSIGHT_INPUT_FILE`: Input CSV file path
- `STREAMSIGHT_TOP_K_PRODUCTS`: Number of top products to track (default: 10)
- `STREAMSIGHT_ZSCORE_THRESHOLD`: Anomaly detection threshold (default: 3.0)
- `STREAMSIGHT_RFM_WHALE_PERCENTILE`: Whale customer percentile (default: 99)
- `STREAMSIGHT_LOG_LEVEL`: Logging level (default: INFO)

---

## Sample Output

When you run `./scripts/run.sh`, you'll see:

```
======================================================================
               StreamSight: Complete Analytics Workflow
======================================================================

[1/4] CSV file found: dataset/Online_Retail.csv

[2/4] Running analytics pipeline...
 Analytics complete

[3/4] Generating visualizations...
 Visualizations created

[4/4] Generating summary report...
 Report generated

======================================================================
Workflow Completed Successfully!
======================================================================

  Execution Time: 45.23 seconds

 Key Metrics:
   - Net Revenue: $9,747,748.00
   - Transactions: 541,909
   - Products: 4,070
   - Countries: 38

 Whale Insights:
   - Top 43 customers (1.0%) generate 21.5% of revenue

 All Outputs:
   - Figures: results/figures
   - Data Tables: results/tables
   -  Report: results/reports/SUMMARY.md

 All done! Open SUMMARY.md for the  report.
```

### Console Output (Detailed Analytics)

```
======================================================================
StreamSight: Streaming Sales Analytics Pipeline
======================================================================

 REVENUE ANALYSIS
   Gross Revenue:        $9,747,748.00
   Net Revenue:          $9,747,748.00
   Total Transactions:   541,909
   Return Transactions:  9,288

 GEOGRAPHY ANALYSIS
   Total Countries:      38
   Top 5 Countries by Revenue:
      1. United Kingdom       $8,187,806.36 (84.00%)
      2. Netherlands            $284,661.54 ( 2.92%)
      3. EIRE                   $263,276.82 ( 2.70%)
      4. Germany                $221,698.21 ( 2.27%)
      5. France                 $197,403.90 ( 2.03%)

  PRODUCT ANALYSIS
   Total Products:       4,070
   Top 10 Products by Revenue:
      1. REGENCY CAKESTAND 3 TIER               $164,762.19
      2. WHITE HANGING HEART T-LIGHT HOLDER     $100,603.50
      3. JUMBO BAG RED RETROSPOT                 $93,603.13

  RETURNS ANALYSIS
   Return Rate:          1.71%
   Return Impact:        $-141,846.52

 DATA QUALITY
   Valid Rows:           541,909
   Completeness Rate:    75.07%

 WHALE CUSTOMER ANALYSIS
   Whale Customers:      43 (0.98%)
   Whale Revenue Share:  21.48%

   Top 5 Whale Customers:
      1. Customer 14646: $279,489.02 | 196 transactions
      2. Customer 18102: $256,438.49 | 84 transactions
      3. Customer 17450: $187,482.17 | 47 transactions

  ANOMALY DETECTION
   Anomalies Detected:    1,084 (0.200%)
   Mean Transaction:      $17.99
   Std Deviation:         $75.23

 OUTPUT FILES
   Figures:    results/figures
   Tables:     results/tables
   Report:     results/reports/SUMMARY.md
```

### Report Files

```
results/
├── figures/
│   ├── revenue_trend.png           # Daily revenue line chart
│   ├── country_performance.png     # Top 10 countries bar chart
│   ├── top_products.png            # Top products bar chart
│   └── whale_pareto.png            # Cumulative revenue distribution
├── tables/
│   ├── revenue.json                # Detailed revenue data
│   ├── geography.json              # Country-level metrics
│   ├── products.json               # Product rankings
│   ├── returns.json                # Return analysis
│   ├── anomalies.json              # Detected anomalies
│   └── rfm_whales.json             # Whale customers
├── reports/
│   └── SUMMARY.md                  #  summary report
└── errors/
    └── bad_rows.jsonl              # Validation errors (if any)
```

---

## Testing

```bash
# Run all tests with coverage
./scripts/test.sh

# Run specific test file
./scripts/test.sh tests/test_core.py

# Run tests without coverage
./scripts/test.sh --no-cov

# Run tests in verbose mode
./scripts/test.sh -v
```

### Test Coverage

The project maintains high test coverage:

- **48 test cases** covering all major components
- **75%+ code coverage** across the codebase
- Unit tests, integration tests, and property-based tests

---

## Troubleshooting

### Issue: ModuleNotFoundError: No module named 'streamsight'

If you see this error when running tests or scripts:

```
ImportError while loading conftest
E   ModuleNotFoundError: No module named 'streamsight'
```

**Solution:** Your virtual environment may be corrupted or pointing to the wrong Python interpreter. Recreate it:

```bash
# Remove the corrupted virtual environment
rm -rf .venv

# Clear Python caches
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true

# Recreate the virtual environment
./scripts/setup.sh

# Run tests to verify
./scripts/test.sh
```

### Issue: Bad interpreter error

If you see:

```
bad interpreter: No such file or directory
```

This means your `.venv` directory has references to a different project or moved Python installation. Follow the steps above to recreate the virtual environment.

### Issue: PYTHONPATH conflicts

If you have `PYTHONPATH` set in your shell configuration pointing to other projects, it may cause import conflicts. The test script automatically clears `PYTHONPATH`, but if you encounter issues:

```bash
# Temporarily unset PYTHONPATH
unset PYTHONPATH

# Run your command
./scripts/test.sh
```

### Issue: Permission denied when running scripts

Make sure all scripts are executable:

```bash
chmod +x scripts/*.sh
```
