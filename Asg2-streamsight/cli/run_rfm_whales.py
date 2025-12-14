#!/usr/bin/env python3
"""Run RFM whale analysis only.

This script focuses on RFM analysis and whale customer identification,
skipping other analytics.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from streamsight.config import Config
from streamsight.io.csv_stream import stream_transactions
from streamsight.logging_conf import configure_logging, get_logger
from streamsight.rfm.calculator import build_customer_profiles
from streamsight.rfm.segmentation import segment_customers
from streamsight.viz.plots import plot_whale_pareto


def main() -> None:
    """Main entry point."""
    # Load configuration
    config = Config()
    config.enable_anomaly_detection = False  # Focus on RFM only

    # Configure logging
    configure_logging(config.log_level)
    logger = get_logger(__name__)

    logger.info("rfm_analysis_starting")

    try:
        # Check if input file exists
        if not config.input_file.exists():
            logger.error("input_file_not_found", path=str(config.input_file))
            print(f"\nError: Input file not found: {config.input_file}")
            sys.exit(1)

        print("\n" + "=" * 60)
        print("StreamSight: RFM Whale Analysis")
        print("=" * 60)
        print(f"\nInput: {config.input_file}")
        print("\nBuilding customer profiles...")

        # Stream transactions and build profiles
        stream = stream_transactions(config.input_file)
        valid_stream = (result for result in stream if result.is_ok())
        profiles = build_customer_profiles(valid_stream)

        print(f"Built {len(profiles):,} customer profiles")

        # Segment customers
        print("\nSegmenting customers and identifying whales...")
        rfm_result = segment_customers(
            profiles,
            reference_date=config.rfm_reference_date,
            whale_percentile=config.rfm_whale_percentile,
        )

        print(f"Analyzed {rfm_result.total_customers:,} customers")

        # Generate whale plot
        config.ensure_output_dirs()
        print("\nGenerating whale Pareto chart...")
        plot_whale_pareto(rfm_result, config.figures_dir / "whale_pareto.png")

        # Print results
        print("\n" + "=" * 60)
        print("RFM Analysis Complete")
        print("=" * 60)
        print(f"\nWhale Statistics:")
        print(f"   - Total Customers: {rfm_result.total_customers:,}")
        print(f"   - Whale Customers: {rfm_result.whale_count:,}")
        print(
            f"   - Whale Percentage: {(rfm_result.whale_count / rfm_result.total_customers * 100):.2f}%"
        )
        print(f"   - Total Revenue: ${rfm_result.total_revenue:,.2f}")
        print(f"   - Whale Revenue: ${rfm_result.whale_revenue:,.2f}")
        print(f"   - Whale Revenue Share: {rfm_result.whale_revenue_share:.2f}%")

        print(f"\nTop 5 Whale Customers:")
        for i, whale in enumerate(rfm_result.whale_customers[:5], 1):
            print(f"   {i}. Customer {whale.customer_id}:")
            print(f"      - Spend: ${whale.monetary:,.2f}")
            print(f"      - Transactions: {whale.frequency}")
            print(f"      - Last Seen: {whale.recency_days} days ago")
            print(f"      - RFM Score: {whale.rfm_score}")

        print(f"\nOutput:")
        print(f"   - Chart: {config.figures_dir / 'whale_pareto.png'}")

        print("\nRFM analysis completed successfully!")

        logger.info("rfm_analysis_completed")

    except Exception as e:
        logger.error("rfm_analysis_failed", error=str(e), exc_info=True)
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

