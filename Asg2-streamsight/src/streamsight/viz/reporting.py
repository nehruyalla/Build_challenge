"""Markdown report generation.

This module generates human-readable  summary reports.
"""

from pathlib import Path

from streamsight.config import Config
from streamsight.core.money import format_money
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


def generate_summary_report(config: Config, results: "PipelineResults") -> None:  # type: ignore[name-defined]
    """Generate an  summary report in Markdown format.

    Args:
        config: Configuration
        results: Pipeline results
    """
    logger.info("generating_summary_report")

    report_path = config.reports_dir / "SUMMARY.md"

    with open(report_path, "w") as f:
        f.write("# StreamSight Analytics Report\n\n")
        f.write("** Summary of Sales Data Analysis**\n\n")
        f.write("---\n\n")

        # Revenue Section
        f.write("## Revenue Overview\n\n")
        f.write(f"- **Gross Revenue**: {format_money(results.revenue.gross_revenue)}\n")
        f.write(f"- **Net Revenue**: {format_money(results.revenue.net_revenue)}\n")
        f.write(f"- **Total Transactions**: {results.revenue.transaction_count:,}\n")
        f.write(f"- **Return Transactions**: {results.revenue.return_count:,}\n")
        f.write(f"- **Return Rate**: {results.returns.return_rate:.2f}%\n\n")

        # Geographic Performance
        f.write("## Geographic Performance\n\n")
        f.write("### Top 5 Countries by Revenue\n\n")
        top_countries = sorted(
            results.geography.country_revenue.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        for i, (country, revenue) in enumerate(top_countries, 1):
            share = results.geography.country_revenue_share.get(country, 0)
            f.write(
                f"{i}. **{country}**: {format_money(revenue)} ({share:.1f}% of total)\n"
            )
        f.write("\n")

        # Product Performance
        f.write("## Product Performance\n\n")
        f.write(f"- **Total Unique Products**: {results.products.total_product_count:,}\n")
        f.write(f"- **Top Products Tracked**: {len(results.products.top_products)}\n\n")
        f.write("### Top 3 Products by Revenue\n\n")
        for i, product in enumerate(results.products.top_products[:3], 1):
            f.write(
                f"{i}. **{product.description}**\n"
                f"   - Revenue: {format_money(product.revenue)}\n"
                f"   - Units Sold: {product.quantity_sold:,}\n"
                f"   - Transactions: {product.transaction_count:,}\n\n"
            )

        # RFM & Whale Analysis
        if results.rfm is not None:
            f.write("## Whale Customer Analysis\n\n")
            f.write(f"- **Total Customers**: {results.rfm.total_customers:,}\n")
            f.write(f"- **Whale Customers**: {results.rfm.whale_count:,}\n")
            f.write(f"- **Whale Percentage**: {(results.rfm.whale_count / results.rfm.total_customers * 100):.2f}%\n")
            f.write(f"- **Whale Revenue**: {format_money(results.rfm.whale_revenue)}\n")
            f.write(
                f"- **Whale Revenue Share**: {results.rfm.whale_revenue_share:.2f}%\n\n"
            )

            f.write("**Key Insight**: ")
            f.write(
                f"The top {results.rfm.whale_count} customers ({(results.rfm.whale_count / results.rfm.total_customers * 100):.1f}% of the customer base) "
                f"contribute {results.rfm.whale_revenue_share:.1f}% of total revenue.\n\n"
            )

            # Top 3 Whales
            f.write("### Top 3 Whale Customers\n\n")
            for i, whale in enumerate(results.rfm.whale_customers[:3], 1):
                f.write(
                    f"{i}. **Customer {whale.customer_id}**\n"
                    f"   - Total Spend: {format_money(whale.monetary)}\n"
                    f"   - Transactions: {whale.frequency}\n"
                    f"   - Last Seen: {whale.recency_days} days ago\n"
                    f"   - RFM Score: {whale.rfm_score}\n\n"
                )

        # Anomaly Detection
        if results.anomaly is not None:
            f.write("## Anomaly Detection\n\n")
            f.write(f"- **Transactions Analyzed**: {results.anomaly.total_transactions:,}\n")
            f.write(f"- **Anomalies Detected**: {results.anomaly.anomaly_count:,}\n")
            f.write(
                f"- **Anomaly Rate**: {(results.anomaly.anomaly_count / results.anomaly.total_transactions * 100):.2f}%\n"
            )
            f.write(
                f"- **Mean Transaction Value**: ${results.anomaly.mean_transaction_value:.2f}\n"
            )
            f.write(
                f"- **Std Dev**: ${results.anomaly.stddev_transaction_value:.2f}\n\n"
            )

            if results.anomaly.anomalies:
                f.write("### Top 3 Anomalous Transactions\n\n")
                for i, anomaly in enumerate(results.anomaly.anomalies[:3], 1):
                    f.write(
                        f"{i}. **Invoice {anomaly.transaction.InvoiceNo}**\n"
                        f"   - Amount: {format_money(anomaly.transaction_value)}\n"
                        f"   - Z-Score: {anomaly.z_score:.2f}\n"
                        f"   - Customer: {anomaly.transaction.CustomerID or 'Unknown'}\n\n"
                    )

        # Data Quality
        f.write("## Data Quality\n\n")
        f.write(f"- **Total Rows Processed**: {results.data_quality.total_rows:,}\n")
        f.write(f"- **Valid Rows**: {results.data_quality.valid_rows:,}\n")
        f.write(f"- **Rows with Missing Customer ID**: {results.data_quality.missing_customer_id:,}\n")
        f.write(f"- **Data Completeness Rate**: {results.data_quality.completeness_rate:.2f}%\n")
        f.write(f"- **DLQ (Bad Rows)**: {results.dlq_count:,}\n\n")

        # Footer
        f.write("---\n\n")
        f.write("*Generated by StreamSight v0.1.0*\n")
        f.write("*Production-grade streaming analytics for sales data*\n")

    logger.info("summary_report_generated", report_path=str(report_path))

