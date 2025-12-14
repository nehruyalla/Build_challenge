#!/usr/bin/env python3
"""Master script: Convert Excel > Run Analytics > Generate Reports.

This script orchestrates the complete workflow from Excel to final reports.
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cli.convert_excel_to_csv import convert_excel_to_csv
from streamsight.config import Config
from streamsight.logging_conf import configure_logging, get_logger
from streamsight.pipeline.runner import run_pipeline
from streamsight.viz.plots import create_all_plots
from streamsight.viz.reporting import generate_summary_report


def main() -> None:
    """Main orchestration."""
    # Load configuration
    config = Config()
    configure_logging(config.log_level)
    logger = get_logger(__name__)

    print("\n" + "=" * 70)
    print(" " * 15 + "StreamSight: Complete Analytics Workflow")
    print("=" * 70)

    start_time = time.time()

    try:
        # Step 1: Check if CSV exists, if not convert from Excel
        if not config.input_file.exists():
            print("\n[1/4] Converting Excel to CSV...")
            excel_path = config.input_file.parent / "Online Retail.xlsx"

            if not excel_path.exists():
                print(f"\nError: Neither CSV nor Excel file found")
                print(f"   Expected: {config.input_file} or {excel_path}")
                sys.exit(1)

            convert_excel_to_csv(excel_path, config.input_file)
        else:
            print(f"\n[1/4] CSV file found: {config.input_file}")

        # Step 2: Run analytics pipeline
        print("\n[2/4] Running analytics pipeline...")
        results = run_pipeline(config)
        print("Analytics complete")

        # Step 3: Generate visualizations
        print("\n[3/4] Generating visualizations...")
        create_all_plots(config, results)
        print("Visualizations created")

        # Step 4: Generate report
        print("\n[4/4] Generating summary report...")
        generate_summary_report(config, results)
        print("Report generated")

        # Summary
        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print("Workflow Completed Successfully!")
        print("=" * 70)

        print(f"\nExecution Time: {elapsed:.2f} seconds")

        print(f"\nKey Metrics:")
        print(f"   - Net Revenue: ${results.revenue.net_revenue:,.2f}")
        print(f"   - Transactions: {results.revenue.transaction_count:,}")
        print(f"   - Products: {results.products.total_product_count:,}")
        print(f"   - Countries: {len(results.geography.country_revenue)}")

        if results.rfm:
            print(f"\nWhale Insights:")
            print(
                f"   - Top {results.rfm.whale_count} customers ({(results.rfm.whale_count / results.rfm.total_customers * 100):.1f}%) "
                f"generate {results.rfm.whale_revenue_share:.1f}% of revenue"
            )

        print(f"\nAll Outputs:")
        print(f"   - Figures: {config.figures_dir}")
        print(f"   - Data Tables: {config.tables_dir}")
        print(f"   -  Report: {config.reports_dir / 'SUMMARY.md'}")

        print("\nAll done! Open SUMMARY.md for the  report.")

        logger.info("workflow_completed", elapsed_seconds=elapsed)

    except Exception as e:
        logger.error("workflow_failed", error=str(e), exc_info=True)
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

