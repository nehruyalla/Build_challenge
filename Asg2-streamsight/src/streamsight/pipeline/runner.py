"""Pipeline orchestration and execution.

This module is the heart of StreamSight, orchestrating the streaming
pipeline with broadcasting to multiple aggregators.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

from streamsight.analytics.anomaly import AnomalyResult
from streamsight.analytics.data_quality import DataQualityResult
from streamsight.analytics.geography import GeographyResult
from streamsight.analytics.products import ProductsResult
from streamsight.analytics.returns import ReturnsResult
from streamsight.analytics.revenue import RevenueResult
from streamsight.config import Config
from streamsight.core.stream_utils import broadcast, partition
from streamsight.io.csv_stream import stream_transactions, write_dlq
from streamsight.logging_conf import get_logger
from streamsight.pipeline.registry import create_default_registry
from streamsight.rfm.calculator import build_customer_profiles
from streamsight.rfm.segmentation import segment_customers, SegmentationResult

logger = get_logger(__name__)


@dataclass
class PipelineResults:
    """Complete results from the analytics pipeline.

    Attributes:
        revenue: Revenue analysis results
        geography: Geographic analysis results
        products: Product analysis results
        returns: Returns analysis results
        data_quality: Data quality results
        anomaly: Anomaly detection results (optional)
        rfm: RFM segmentation results (optional)
        dlq_count: Number of rows in Dead Letter Queue
    """

    revenue: RevenueResult
    geography: GeographyResult
    products: ProductsResult
    returns: ReturnsResult
    data_quality: DataQualityResult
    anomaly: AnomalyResult | None
    rfm: SegmentationResult | None
    dlq_count: int


def run_pipeline(config: Config) -> PipelineResults:
    """Execute the complete StreamSight analytics pipeline.

    This function orchestrates the entire streaming pipeline:
    1. Stream CSV data with validation
    2. Partition valid/invalid rows (DLQ)
    3. Broadcast valid stream to N aggregators
    4. Execute all aggregators in parallel (conceptually)
    5. Optionally run RFM analysis (two-pass on aggregates)
    6. Write results to disk

    Memory Efficiency: The pipeline uses generator broadcasting to process
    the dataset once, with O(unique_keys) memory for aggregates.

    Args:
        config: Configuration object

    Returns:
        PipelineResults with all analytics

    Raises:
        FileNotFoundError: If input CSV doesn't exist
        ValueError: If configuration is invalid

    Examples:
        >>> config = Config(input_file=Path("data.csv"))
        >>> results = run_pipeline(config)
        >>> print(f"Net Revenue: ${results.revenue.net_revenue}")
    """
    logger.info("pipeline_started", config=config.model_dump())

    # Ensure output directories exist
    config.ensure_output_dirs()

    # Step 1: Stream and validate transactions
    logger.info("step_1_streaming_data", input_file=str(config.input_file))
    stream = stream_transactions(config.input_file)

    # Step 2: Partition into valid and error streams
    logger.info("step_2_partitioning_stream")
    valid_stream, error_stream = partition(stream, lambda r: r.is_ok())

    # Convert error stream to tuples and write DLQ
    error_list = [result.unwrap_err() for result in error_stream]
    dlq_count = write_dlq(
        iter(error_list), config.errors_dir / "bad_rows.jsonl"
    )

    # Step 3: Broadcast valid stream to multiple aggregators
    # We need 5 core aggregators + optionally anomaly detection + RFM
    num_streams = 6 if config.enable_anomaly_detection else 5
    num_streams += 1 if config.enable_rfm_analysis else 0

    logger.info("step_3_broadcasting_stream", num_streams=num_streams)
    streams = broadcast(valid_stream, num_streams)

    # Step 4: Execute aggregators
    logger.info("step_4_executing_aggregators")

    stream_idx = 0

    # Core analytics (always enabled)
    from streamsight.analytics.revenue import analyze_revenue
    from streamsight.analytics.geography import analyze_geography
    from streamsight.analytics.products import analyze_products
    from streamsight.analytics.returns import analyze_returns
    from streamsight.analytics.data_quality import analyze_data_quality

    revenue_result = analyze_revenue(streams[stream_idx])
    stream_idx += 1

    geography_result = analyze_geography(streams[stream_idx])
    stream_idx += 1

    products_result = analyze_products(streams[stream_idx], top_k=config.top_k_products)
    stream_idx += 1

    returns_result = analyze_returns(streams[stream_idx])
    stream_idx += 1

    data_quality_result = analyze_data_quality(streams[stream_idx])
    stream_idx += 1

    # Optional: Anomaly detection
    anomaly_result = None
    if config.enable_anomaly_detection:
        from streamsight.analytics.anomaly import detect_anomalies

        anomaly_result = detect_anomalies(
            streams[stream_idx], threshold=config.zscore_threshold
        )
        stream_idx += 1

    # Optional: RFM analysis (two-pass on aggregates)
    rfm_result = None
    if config.enable_rfm_analysis:
        logger.info("step_5_rfm_analysis")
        profiles = build_customer_profiles(streams[stream_idx])
        rfm_result = segment_customers(
            profiles,
            reference_date=config.rfm_reference_date,
            whale_percentile=config.rfm_whale_percentile,
        )

    # Step 6: Write results to disk
    logger.info("step_6_writing_results")
    _write_results(config, revenue_result, geography_result, products_result,
                   returns_result, data_quality_result, anomaly_result, rfm_result)

    logger.info("pipeline_completed")

    return PipelineResults(
        revenue=revenue_result,
        geography=geography_result,
        products=products_result,
        returns=returns_result,
        data_quality=data_quality_result,
        anomaly=anomaly_result,
        rfm=rfm_result,
        dlq_count=dlq_count,
    )


def _write_results(
    config: Config,
    revenue: RevenueResult,
    geography: GeographyResult,
    products: ProductsResult,
    returns: ReturnsResult,
    data_quality: DataQualityResult,
    anomaly: AnomalyResult | None,
    rfm: SegmentationResult | None,
) -> None:
    """Write all results to JSON files.

    Args:
        config: Configuration
        revenue: Revenue results
        geography: Geography results
        products: Products results
        returns: Returns results
        data_quality: Data quality results
        anomaly: Anomaly results (optional)
        rfm: RFM results (optional)
    """
    tables_dir = config.tables_dir

    # Revenue
    with open(tables_dir / "revenue.json", "wb") as f:
        f.write(
            orjson.dumps(
                {
                    "gross_revenue": str(revenue.gross_revenue),
                    "net_revenue": str(revenue.net_revenue),
                    "transaction_count": revenue.transaction_count,
                    "return_count": revenue.return_count,
                    "daily_revenue": {
                        k: str(v) for k, v in revenue.daily_revenue.items()
                    },
                    "monthly_revenue": {
                        k: str(v) for k, v in revenue.monthly_revenue.items()
                    },
                },
                option=orjson.OPT_INDENT_2,
            )
        )

    # Geography
    with open(tables_dir / "geography.json", "wb") as f:
        f.write(
            orjson.dumps(
                {
                    "country_revenue": {
                        k: str(v) for k, v in geography.country_revenue.items()
                    },
                    "country_transaction_counts": geography.country_transaction_counts,
                    "country_revenue_share": geography.country_revenue_share,
                    "total_revenue": str(geography.total_revenue),
                },
                option=orjson.OPT_INDENT_2,
            )
        )

    # Products
    with open(tables_dir / "products.json", "wb") as f:
        f.write(
            orjson.dumps(
                {
                    "top_products": [
                        {
                            "stock_code": p.stock_code,
                            "description": p.description,
                            "revenue": str(p.revenue),
                            "quantity_sold": p.quantity_sold,
                            "transaction_count": p.transaction_count,
                        }
                        for p in products.top_products
                    ],
                    "total_product_count": products.total_product_count,
                    "total_revenue": str(products.total_revenue),
                },
                option=orjson.OPT_INDENT_2,
            )
        )

    # Returns
    with open(tables_dir / "returns.json", "wb") as f:
        f.write(
            orjson.dumps(
                {
                    "total_transactions": returns.total_transactions,
                    "return_transactions": returns.return_transactions,
                    "return_rate": returns.return_rate,
                    "return_revenue_impact": str(returns.return_revenue_impact),
                    "top_returned_products": returns.top_returned_products,
                },
                option=orjson.OPT_INDENT_2,
            )
        )

    # Data Quality
    with open(tables_dir / "data_quality.json", "wb") as f:
        f.write(
            orjson.dumps(
                {
                    "total_rows": data_quality.total_rows,
                    "valid_rows": data_quality.valid_rows,
                    "missing_customer_id": data_quality.missing_customer_id,
                    "missing_description": data_quality.missing_description,
                    "completeness_rate": data_quality.completeness_rate,
                },
                option=orjson.OPT_INDENT_2,
            )
        )

    # Anomaly (optional)
    if anomaly is not None:
        with open(tables_dir / "anomalies.json", "wb") as f:
            f.write(
                orjson.dumps(
                    {
                        "total_transactions": anomaly.total_transactions,
                        "anomaly_count": anomaly.anomaly_count,
                        "mean_transaction_value": anomaly.mean_transaction_value,
                        "stddev_transaction_value": anomaly.stddev_transaction_value,
                        "anomalies": [
                            {
                                "invoice_no": a.transaction.InvoiceNo,
                                "customer_id": a.transaction.CustomerID,
                                "amount": str(a.transaction_value),
                                "z_score": a.z_score,
                            }
                            for a in anomaly.anomalies[:100]  # Limit to top 100
                        ],
                    },
                    option=orjson.OPT_INDENT_2,
                )
            )

    # RFM (optional)
    if rfm is not None:
        with open(tables_dir / "rfm_whales.json", "wb") as f:
            f.write(
                orjson.dumps(
                    {
                        "total_customers": rfm.total_customers,
                        "whale_count": rfm.whale_count,
                        "whale_revenue": str(rfm.whale_revenue),
                        "whale_revenue_share": rfm.whale_revenue_share,
                        "total_revenue": str(rfm.total_revenue),
                        "whale_customers": [
                            {
                                "customer_id": w.customer_id,
                                "recency_days": w.recency_days,
                                "frequency": w.frequency,
                                "monetary": str(w.monetary),
                                "rfm_score": w.rfm_score,
                            }
                            for w in rfm.whale_customers[:50]  # Top 50 whales
                        ],
                    },
                    option=orjson.OPT_INDENT_2,
                )
            )

    logger.info("results_written", output_dir=str(config.tables_dir))

