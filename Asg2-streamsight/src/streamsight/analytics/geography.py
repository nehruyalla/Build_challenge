"""Geographic performance analysis.

This module analyzes sales by country/region.
"""

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator

from streamsight.core.money import Money, divide_money
from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class GeographyResult:
    """Results from geographic analysis.

    Attributes:
        country_revenue: Revenue by country
        country_transaction_counts: Transaction count by country
        country_revenue_share: Revenue share percentage by country
        total_revenue: Total revenue across all countries
    """

    country_revenue: dict[str, Money]
    country_transaction_counts: dict[str, int]
    country_revenue_share: dict[str, float]
    total_revenue: Money


def analyze_geography(stream: Iterator[Ok[Transaction]]) -> GeographyResult:
    """Analyze geographic performance from a stream of transactions.

    Calculates revenue and transaction counts by country in a single pass.

    Memory Complexity: O(unique_countries) - typically < 100

    Args:
        stream: Iterator of Ok-wrapped Transaction objects

    Returns:
        GeographyResult with country-level metrics

    Examples:
        >>> result = analyze_geography(transaction_stream)
        >>> for country, revenue in result.country_revenue.items():
        ...     print(f"{country}: ${revenue}")
    """
    logger.info("geography_analysis_started")

    # Accumulators
    country_revenue: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    country_counts: dict[str, int] = defaultdict(int)
    total_revenue = Decimal("0")

    # Single pass through the stream
    for result in stream:
        tx = result.unwrap()

        amount = tx.total_amount
        country = tx.Country

        country_revenue[country] += amount
        country_counts[country] += 1
        total_revenue += amount

    # Calculate revenue share percentages
    country_revenue_share: dict[str, float] = {}
    if total_revenue > 0:
        for country, revenue in country_revenue.items():
            share = float((revenue / total_revenue) * 100)
            country_revenue_share[country] = share

    logger.info(
        "geography_analysis_completed",
        country_count=len(country_revenue),
        total_revenue=str(total_revenue),
    )

    return GeographyResult(
        country_revenue=dict(country_revenue),
        country_transaction_counts=dict(country_counts),
        country_revenue_share=country_revenue_share,
        total_revenue=total_revenue,
    )

