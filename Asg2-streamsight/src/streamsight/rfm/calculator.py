"""RFM (Recency, Frequency, Monetary) profile builder.

This module performs Pass 1 of RFM analysis: building customer profiles
from the transaction stream without loading all transactions into memory.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterator, Optional

from streamsight.core.money import Money
from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class CustomerProfile:
    """Aggregate profile for a single customer.

    This represents all we need to know about a customer for RFM analysis,
    without keeping individual transactions in memory.

    Attributes:
        customer_id: Unique customer identifier
        first_seen: Date of first transaction
        last_seen: Date of most recent transaction
        transaction_count: Number of transactions
        total_spend: Total monetary value of all transactions
    """

    customer_id: str
    first_seen: datetime
    last_seen: datetime
    transaction_count: int
    total_spend: Money


def build_customer_profiles(
    stream: Iterator[Ok[Transaction]],
) -> dict[str, CustomerProfile]:
    """Build customer profiles from a transaction stream (Pass 1).

    This function performs a single pass through the transactions,
    building aggregate profiles for each customer.

    Memory Complexity: O(unique_customers) - only profiles are stored

    Args:
        stream: Iterator of Ok-wrapped Transaction objects

    Returns:
        Dictionary mapping CustomerID to CustomerProfile

    Examples:
        >>> profiles = build_customer_profiles(transaction_stream)
        >>> print(f"Total customers: {len(profiles)}")
    """
    logger.info("rfm_profile_building_started")

    profiles: dict[str, CustomerProfile] = {}
    transactions_processed = 0
    skipped_no_customer_id = 0

    for result in stream:
        tx = result.unwrap()
        transactions_processed += 1

        # Skip transactions without CustomerID
        if tx.CustomerID is None:
            skipped_no_customer_id += 1
            continue

        customer_id = tx.CustomerID

        if customer_id in profiles:
            # Update existing profile
            profile = profiles[customer_id]
            profile.transaction_count += 1
            profile.total_spend += tx.total_amount

            # Update time boundaries
            if tx.InvoiceDate < profile.first_seen:
                profile.first_seen = tx.InvoiceDate
            if tx.InvoiceDate > profile.last_seen:
                profile.last_seen = tx.InvoiceDate

        else:
            # Create new profile
            profiles[customer_id] = CustomerProfile(
                customer_id=customer_id,
                first_seen=tx.InvoiceDate,
                last_seen=tx.InvoiceDate,
                transaction_count=1,
                total_spend=tx.total_amount,
            )

    logger.info(
        "rfm_profile_building_completed",
        transactions_processed=transactions_processed,
        unique_customers=len(profiles),
        skipped_no_customer_id=skipped_no_customer_id,
    )

    return profiles


def calculate_max_date(profiles: dict[str, CustomerProfile]) -> datetime:
    """Calculate the maximum date across all customer profiles.

    This is used as the reference date for recency calculations.

    Args:
        profiles: Dictionary of customer profiles

    Returns:
        Maximum transaction date

    Raises:
        ValueError: If no profiles exist
    """
    if not profiles:
        msg = "Cannot calculate max date from empty profiles"
        raise ValueError(msg)

    max_date = max(profile.last_seen for profile in profiles.values())
    logger.debug("max_date_calculated", max_date=max_date.isoformat())
    return max_date

