"""Data quality analysis and reporting.

This module tracks data quality metrics like missing values and validation errors.
"""

from dataclasses import dataclass
from typing import Iterator

from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class DataQualityResult:
    """Results from data quality analysis.

    Attributes:
        total_rows: Total number of rows processed
        valid_rows: Number of valid transactions
        missing_customer_id: Number of rows with missing CustomerID
        missing_description: Number of rows with missing Description
        completeness_rate: Percentage of rows with all fields populated
    """

    total_rows: int
    valid_rows: int
    missing_customer_id: int
    missing_description: int
    completeness_rate: float


def analyze_data_quality(stream: Iterator[Ok[Transaction]]) -> DataQualityResult:
    """Analyze data quality from a stream of valid transactions.

    This function checks for missing or incomplete data in validated transactions.

    Memory Complexity: O(1) - only counters

    Args:
        stream: Iterator of Ok-wrapped Transaction objects

    Returns:
        DataQualityResult with quality metrics

    Examples:
        >>> result = analyze_data_quality(transaction_stream)
        >>> print(f"Data Completeness: {result.completeness_rate:.2f}%")
    """
    logger.info("data_quality_analysis_started")

    # Accumulators
    total_rows = 0
    missing_customer_id = 0
    missing_description = 0

    # Single pass through the stream
    for result in stream:
        tx = result.unwrap()
        total_rows += 1

        if tx.CustomerID is None:
            missing_customer_id += 1

        if not tx.Description or tx.Description.strip() == "":
            missing_description += 1

    # Calculate completeness rate
    # A row is complete if it has both CustomerID and Description
    complete_rows = total_rows - max(missing_customer_id, missing_description)
    completeness_rate = (complete_rows / total_rows * 100) if total_rows > 0 else 0.0

    logger.info(
        "data_quality_analysis_completed",
        total_rows=total_rows,
        missing_customer_id=missing_customer_id,
        missing_description=missing_description,
        completeness_rate=f"{completeness_rate:.2f}%",
    )

    return DataQualityResult(
        total_rows=total_rows,
        valid_rows=total_rows,
        missing_customer_id=missing_customer_id,
        missing_description=missing_description,
        completeness_rate=completeness_rate,
    )

