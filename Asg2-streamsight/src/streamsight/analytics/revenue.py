"""Revenue aggregation and analysis.

This module performs single-pass revenue calculations with financial accuracy.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterator

from streamsight.core.money import Money
from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class RevenueResult:
    """Results from revenue analysis.

    Attributes:
        gross_revenue: Total revenue including returns
        net_revenue: Revenue after subtracting returns
        daily_revenue: Revenue by date
        monthly_revenue: Revenue by year-month
        transaction_count: Total number of transactions
        return_count: Number of return transactions
    """

    gross_revenue: Money
    net_revenue: Money
    daily_revenue: dict[str, Money]
    monthly_revenue: dict[str, Money]
    transaction_count: int
    return_count: int


def analyze_revenue(stream: Iterator[Ok[Transaction]]) -> RevenueResult:
    """Analyze revenue from a stream of transactions in a single pass.

    This function calculates both gross and net revenue, as well as
    daily and monthly breakdowns, all in one pass through the data.

    Memory Complexity: O(unique_dates) - only aggregates are kept

    Args:
        stream: Iterator of Ok-wrapped Transaction objects

    Returns:
        RevenueResult with all revenue metrics

    Examples:
        >>> result = analyze_revenue(transaction_stream)
        >>> print(f"Net Revenue: ${result.net_revenue}")
    """
    logger.info("revenue_analysis_started")

    # Accumulators
    gross_revenue = Decimal("0")
    net_revenue = Decimal("0")
    daily_revenue: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    monthly_revenue: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    transaction_count = 0
    return_count = 0

    # Single pass through the stream
    for result in stream:
        tx = result.unwrap()
        transaction_count += 1

        # Calculate transaction amount
        amount = tx.total_amount

        # Update gross revenue (includes all transactions)
        gross_revenue += amount

        # Update net revenue (positive for sales, negative for returns)
        # Returns have negative amounts, so they reduce net revenue
        net_revenue += amount

        # Track returns
        if tx.is_return:
            return_count += 1

        # Daily aggregation
        date_key = tx.InvoiceDate.strftime("%Y-%m-%d")
        daily_revenue[date_key] += amount

        # Monthly aggregation
        month_key = tx.InvoiceDate.strftime("%Y-%m")
        monthly_revenue[month_key] += amount

    logger.info(
        "revenue_analysis_completed",
        gross_revenue=str(gross_revenue),
        net_revenue=str(net_revenue),
        transaction_count=transaction_count,
        return_count=return_count,
    )

    return RevenueResult(
        gross_revenue=gross_revenue,
        net_revenue=net_revenue,
        daily_revenue=dict(daily_revenue),
        monthly_revenue=dict(monthly_revenue),
        transaction_count=transaction_count,
        return_count=return_count,
    )

