"""Returns and refund analysis.

This module analyzes return patterns and calculates return rates.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator

from streamsight.core.money import Money
from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class ReturnsResult:
    """Results from returns analysis.

    Attributes:
        total_transactions: Total number of transactions
        return_transactions: Number of return transactions
        return_rate: Percentage of transactions that are returns
        return_revenue_impact: Total monetary value of returns (negative)
        top_returned_products: Most frequently returned products
    """

    total_transactions: int
    return_transactions: int
    return_rate: float
    return_revenue_impact: Money
    top_returned_products: dict[str, int]


def analyze_returns(stream: Iterator[Ok[Transaction]]) -> ReturnsResult:
    """Analyze return patterns from a stream of transactions.

    Returns are identified by:
    - Negative quantity
    - Invoice number starting with 'C' (cancellation)

    Memory Complexity: O(unique_returned_products)

    Args:
        stream: Iterator of Ok-wrapped Transaction objects

    Returns:
        ReturnsResult with return metrics

    Examples:
        >>> result = analyze_returns(transaction_stream)
        >>> print(f"Return Rate: {result.return_rate:.2f}%")
    """
    logger.info("returns_analysis_started")

    # Accumulators
    total_transactions = 0
    return_transactions = 0
    return_revenue_impact = Decimal("0")
    returned_product_counts: dict[str, int] = {}

    # Single pass through the stream
    for result in stream:
        tx = result.unwrap()
        total_transactions += 1

        if tx.is_return:
            return_transactions += 1
            return_revenue_impact += tx.total_amount

            # Track which products are returned most
            stock_code = tx.StockCode
            returned_product_counts[stock_code] = (
                returned_product_counts.get(stock_code, 0) + 1
            )

    # Calculate return rate
    return_rate = (
        (return_transactions / total_transactions * 100)
        if total_transactions > 0
        else 0.0
    )

    # Sort returned products by frequency (top 10)
    top_returned = dict(
        sorted(
            returned_product_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:10]
    )

    logger.info(
        "returns_analysis_completed",
        total_transactions=total_transactions,
        return_transactions=return_transactions,
        return_rate=f"{return_rate:.2f}%",
        return_revenue_impact=str(return_revenue_impact),
    )

    return ReturnsResult(
        total_transactions=total_transactions,
        return_transactions=return_transactions,
        return_rate=return_rate,
        return_revenue_impact=return_revenue_impact,
        top_returned_products=top_returned,
    )

