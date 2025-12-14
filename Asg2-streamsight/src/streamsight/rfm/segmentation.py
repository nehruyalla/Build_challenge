"""RFM scoring and whale customer segmentation.

This module performs Pass 2 of RFM analysis: scoring customer profiles
and identifying high-value "whale" customers.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np

from streamsight.core.money import Money
from streamsight.logging_conf import get_logger
from streamsight.rfm.calculator import CustomerProfile

logger = get_logger(__name__)


@dataclass
class RFMScore:
    """RFM scores for a customer.

    Attributes:
        customer_id: Customer identifier
        recency_days: Days since last purchase
        frequency: Number of transactions
        monetary: Total spend
        recency_score: Recency score (1-5, 5 is best)
        frequency_score: Frequency score (1-5, 5 is best)
        monetary_score: Monetary score (1-5, 5 is best)
        rfm_score: Combined RFM score (e.g., "555" for best customers)
        is_whale: Whether this customer is a whale (top percentile)
    """

    customer_id: str
    recency_days: int
    frequency: int
    monetary: Money
    recency_score: int
    frequency_score: int
    monetary_score: int
    rfm_score: str
    is_whale: bool


@dataclass
class SegmentationResult:
    """Results from RFM segmentation.

    Attributes:
        rfm_scores: RFM scores for all customers
        whale_customers: List of whale customers
        whale_count: Number of whale customers
        whale_revenue: Total revenue from whales
        whale_revenue_share: Percentage of revenue from whales
        total_customers: Total number of customers analyzed
        total_revenue: Total revenue across all customers
    """

    rfm_scores: list[RFMScore]
    whale_customers: list[RFMScore]
    whale_count: int
    whale_revenue: Money
    whale_revenue_share: float
    total_customers: int
    total_revenue: Money


def calculate_quintiles(values: list[float]) -> list[float]:
    """Calculate quintile boundaries for scoring.

    Args:
        values: List of values to calculate quintiles from

    Returns:
        List of 5 quintile boundaries (20th, 40th, 60th, 80th, 100th percentiles)
    """
    if not values:
        return [0.0, 0.0, 0.0, 0.0, 0.0]

    return [
        float(np.percentile(values, 20)),
        float(np.percentile(values, 40)),
        float(np.percentile(values, 60)),
        float(np.percentile(values, 80)),
        float(np.percentile(values, 100)),
    ]


def score_value(value: float, quintiles: list[float], reverse: bool = False) -> int:
    """Score a value based on quintile boundaries.

    Args:
        value: Value to score
        quintiles: Quintile boundaries
        reverse: If True, lower values get higher scores (for recency)

    Returns:
        Score from 1 to 5
    """
    for i, threshold in enumerate(quintiles, start=1):
        if value <= threshold:
            return (6 - i) if reverse else i
    return 5 if not reverse else 1


def segment_customers(
    profiles: dict[str, CustomerProfile],
    reference_date: Optional[datetime] = None,
    whale_percentile: int = 99,
) -> SegmentationResult:
    """Perform RFM scoring and whale segmentation (Pass 2).

    This function operates on the aggregated customer profiles,
    not on raw transactions, making it memory-efficient.

    Args:
        profiles: Dictionary of customer profiles from Pass 1
        reference_date: Reference date for recency calculation (None = use max date)
        whale_percentile: Percentile threshold for whale customers (1-100)

    Returns:
        SegmentationResult with RFM scores and whale identification

    Examples:
        >>> result = segment_customers(profiles, whale_percentile=99)
        >>> print(f"Whales: {result.whale_count} ({result.whale_revenue_share:.1f}%)")
    """
    logger.info(
        "rfm_segmentation_started",
        total_customers=len(profiles),
        whale_percentile=whale_percentile,
    )

    if not profiles:
        logger.warning("rfm_segmentation_skipped", reason="no_customer_profiles")
        return SegmentationResult(
            rfm_scores=[],
            whale_customers=[],
            whale_count=0,
            whale_revenue=Money("0"),
            whale_revenue_share=0.0,
            total_customers=0,
            total_revenue=Money("0"),
        )

    # Determine reference date
    if reference_date is None:
        from streamsight.rfm.calculator import calculate_max_date

        reference_date = calculate_max_date(profiles)

    # Calculate RFM values for all customers
    recency_days_list: list[float] = []
    frequency_list: list[float] = []
    monetary_list: list[float] = []

    for profile in profiles.values():
        recency_days = (reference_date - profile.last_seen).days
        frequency = profile.transaction_count
        monetary = float(profile.total_spend)

        recency_days_list.append(float(recency_days))
        frequency_list.append(float(frequency))
        monetary_list.append(monetary)

    # Calculate quintiles for scoring
    recency_quintiles = calculate_quintiles(recency_days_list)
    frequency_quintiles = calculate_quintiles(frequency_list)
    monetary_quintiles = calculate_quintiles(monetary_list)

    # Calculate whale threshold (top percentile by monetary value)
    whale_threshold = float(np.percentile(monetary_list, whale_percentile))

    # Score each customer
    rfm_scores: list[RFMScore] = []
    total_revenue = Money("0")

    for profile in profiles.values():
        recency_days = (reference_date - profile.last_seen).days
        frequency = profile.transaction_count
        monetary = float(profile.total_spend)

        # Score RFM (1-5 scale)
        # Recency: Lower is better (reverse=True)
        # Frequency: Higher is better
        # Monetary: Higher is better
        r_score = score_value(float(recency_days), recency_quintiles, reverse=True)
        f_score = score_value(float(frequency), frequency_quintiles, reverse=False)
        m_score = score_value(monetary, monetary_quintiles, reverse=False)

        # Identify whales (top percentile by monetary value)
        is_whale = monetary >= whale_threshold

        rfm_score = RFMScore(
            customer_id=profile.customer_id,
            recency_days=recency_days,
            frequency=frequency,
            monetary=profile.total_spend,
            recency_score=r_score,
            frequency_score=f_score,
            monetary_score=m_score,
            rfm_score=f"{r_score}{f_score}{m_score}",
            is_whale=is_whale,
        )

        rfm_scores.append(rfm_score)
        total_revenue += profile.total_spend

    # Extract whale customers
    whale_customers = [score for score in rfm_scores if score.is_whale]
    whale_customers.sort(key=lambda x: x.monetary, reverse=True)

    whale_revenue = sum((whale.monetary for whale in whale_customers), Money("0"))
    whale_revenue_share = (
        float(whale_revenue / total_revenue * 100) if total_revenue > 0 else 0.0
    )

    logger.info(
        "rfm_segmentation_completed",
        total_customers=len(rfm_scores),
        whale_count=len(whale_customers),
        whale_revenue=str(whale_revenue),
        whale_revenue_share=f"{whale_revenue_share:.2f}%",
    )

    return SegmentationResult(
        rfm_scores=rfm_scores,
        whale_customers=whale_customers,
        whale_count=len(whale_customers),
        whale_revenue=whale_revenue,
        whale_revenue_share=whale_revenue_share,
        total_customers=len(rfm_scores),
        total_revenue=total_revenue,
    )

