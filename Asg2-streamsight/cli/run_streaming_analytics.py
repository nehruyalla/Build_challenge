#!/usr/bin/env python3
"""Run the StreamSight analytics pipeline.

This script executes the complete streaming analytics pipeline on a CSV file.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from streamsight.config import Config
from streamsight.logging_conf import configure_logging, get_logger
from streamsight.pipeline.runner import run_pipeline
from streamsight.viz.plots import create_all_plots
from streamsight.viz.reporting import generate_summary_report


def main() -> None:
    """Main entry point."""
    # Load configuration
    config = Config()

    # Configure logging
    configure_logging(config.log_level)
    logger = get_logger(__name__)

    logger.info("streamsight_starting", version="0.1.0")

    try:
        # Check if input file exists
        if not config.input_file.exists():
            logger.error("input_file_not_found", path=str(config.input_file))
            print(f"\nError: Input file not found: {config.input_file}")
            print("\nDid you run convert_excel_to_csv.py first?")
            sys.exit(1)

        # Run the pipeline
        logger.info("starting_pipeline")
        print("\n" + "=" * 60)
        print("StreamSight: Streaming Sales Analytics Pipeline")
        print("=" * 60)
        print(f"\nInput: {config.input_file}")
        print(f"Output: {config.output_dir}")
        print("\nRunning analytics...")

        results = run_pipeline(config)

        # Generate visualizations
        print("\nGenerating visualizations...")
        create_all_plots(config, results)

        # Generate report
        print("Generating summary report...")
        generate_summary_report(config, results)

        # Print detailed summary
        print("\n" + "=" * 70)
        print("Pipeline Completed Successfully!")
        print("=" * 70)
        
        # Revenue Analysis
        print(f"\nREVENUE ANALYSIS")
        print(f"   Gross Revenue:        ${results.revenue.gross_revenue:,.2f}")
        print(f"   Net Revenue:          ${results.revenue.net_revenue:,.2f}")
        print(f"   Total Transactions:   {results.revenue.transaction_count:,}")
        print(f"   Return Transactions:  {results.revenue.return_count:,}")
        print(f"   Daily Revenue Points: {len(results.revenue.daily_revenue)}")
        print(f"   Monthly Periods:      {len(results.revenue.monthly_revenue)}")
        
        # Geography Analysis
        print(f"\nGEOGRAPHY ANALYSIS")
        print(f"   Total Countries:      {len(results.geography.country_revenue)}")
        print(f"   Top 5 Countries by Revenue:")
        top_countries = sorted(
            results.geography.country_revenue.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        for i, (country, revenue) in enumerate(top_countries, 1):
            share = results.geography.country_revenue_share.get(country, 0)
            print(f"      {i}. {country:20s} ${revenue:>12,.2f} ({share:5.2f}%)")
        
        # Product Analysis
        print(f"\nPRODUCT ANALYSIS")
        print(f"   Total Products:       {results.products.total_product_count:,}")
        print(f"   Top {len(results.products.top_products)} Products by Revenue:")
        for i, product in enumerate(results.products.top_products, 1):
            desc = product.description[:40] + "..." if len(product.description) > 40 else product.description
            print(f"      {i}. {desc:45s} ${product.revenue:>12,.2f}")
            print(f"         Qty: {product.quantity_sold:,} | Transactions: {product.transaction_count:,}")
        
        # Returns Analysis
        print(f"\nRETURNS ANALYSIS")
        print(f"   Total Transactions:   {results.returns.total_transactions:,}")
        print(f"   Return Transactions:  {results.returns.return_transactions:,}")
        print(f"   Return Rate:          {results.returns.return_rate:.2f}%")
        print(f"   Return Impact:        ${results.returns.return_revenue_impact:,.2f}")
        if results.returns.top_returned_products:
            print(f"   Most Returned Products:")
            for stock_code, count in list(results.returns.top_returned_products.items())[:5]:
                print(f"      - {stock_code}: {count} returns")
        
        # Data Quality
        print(f"\nDATA QUALITY")
        print(f"   Valid Rows:           {results.data_quality.valid_rows:,}")
        print(f"   Missing Customer ID:  {results.data_quality.missing_customer_id:,}")
        print(f"   Missing Description:  {results.data_quality.missing_description:,}")
        print(f"   Completeness Rate:    {results.data_quality.completeness_rate:.2f}%")
        print(f"   DLQ Errors:           {results.dlq_count:,}")

        # RFM Whale Analysis
        if results.rfm:
            print(f"\nWHALE CUSTOMER ANALYSIS")
            print(f"   Total Customers:      {results.rfm.total_customers:,}")
            print(f"   Whale Customers:      {results.rfm.whale_count:,} ({results.rfm.whale_count / results.rfm.total_customers * 100:.2f}%)")
            print(f"   Whale Revenue:        ${results.rfm.whale_revenue:,.2f}")
            print(f"   Whale Revenue Share:  {results.rfm.whale_revenue_share:.2f}%")
            print(f"   Total Revenue:        ${results.rfm.total_revenue:,.2f}")
            
            print(f"\n   Top 5 Whale Customers:")
            for i, whale in enumerate(results.rfm.whale_customers[:5], 1):
                print(f"      {i}. Customer {whale.customer_id}")
                print(f"         Spend: ${whale.monetary:,.2f} | Frequency: {whale.frequency} | Recency: {whale.recency_days} days")
                print(f"         RFM Score: {whale.rfm_score}")

        # Anomaly Detection
        if results.anomaly:
            print(f"\nANOMALY DETECTION")
            print(f"   Transactions Analyzed: {results.anomaly.total_transactions:,}")
            print(f"   Anomalies Detected:    {results.anomaly.anomaly_count:,} ({results.anomaly.anomaly_count / results.anomaly.total_transactions * 100:.3f}%)")
            print(f"   Mean Transaction:      ${results.anomaly.mean_transaction_value:.2f}")
            print(f"   Std Deviation:         ${results.anomaly.stddev_transaction_value:.2f}")
            
            if results.anomaly.anomalies:
                print(f"\n   Top 5 Anomalous Transactions:")
                for i, anomaly in enumerate(results.anomaly.anomalies[:5], 1):
                    print(f"      {i}. Invoice {anomaly.transaction.InvoiceNo}")
                    print(f"         Amount: ${anomaly.transaction_value:,.2f} | Z-Score: {anomaly.z_score:.2f}")
                    print(f"         Customer: {anomaly.transaction.CustomerID or 'Unknown'}")

        print(f"\nOUTPUT FILES")
        print(f"   Figures:    {config.figures_dir}")
        print(f"   Tables:     {config.tables_dir}")
        print(f"   Report:     {config.reports_dir / 'SUMMARY.md'}")
        if results.dlq_count > 0:
            print(f"   DLQ Errors: {config.errors_dir / 'bad_rows.jsonl'} ({results.dlq_count} rows)")

        print("\n" + "=" * 70)
        print("Pipeline completed successfully!")
        print("=" * 70)

        logger.info("streamsight_completed")

    except Exception as e:
        logger.error("pipeline_failed", error=str(e), exc_info=True)
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

