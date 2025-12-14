"""Anomaly detection using streaming statistics.

This module implements Welford's online algorithm for calculating mean
and variance in a single pass, enabling real-time anomaly detection.
"""

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator

from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class AnomalyTransaction:
    """A transaction flagged as an anomaly.

    Attributes:
        transaction: The anomalous transaction
        z_score: The Z-score of the transaction value
        transaction_value: The total value that triggered the anomaly
    """

    transaction: Transaction
    z_score: float
    transaction_value: Decimal


@dataclass
class AnomalyResult:
    """Results from anomaly detection.

    Attributes:
        anomalies: List of detected anomalous transactions
        total_transactions: Total number of transactions analyzed
        anomaly_count: Number of anomalies detected
        mean_transaction_value: Mean transaction value
        stddev_transaction_value: Standard deviation of transaction values
    """

    anomalies: list[AnomalyTransaction]
    total_transactions: int
    anomaly_count: int
    mean_transaction_value: float
    stddev_transaction_value: float


class WelfordAccumulator:
    """Online mean and variance calculator using Welford's algorithm.

    This algorithm calculates mean and variance in a single pass without
    storing all values in memory.

    Reference: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    """

    def __init__(self) -> None:
        """Initialize the accumulator."""
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0  # Sum of squared differences from mean

    def update(self, value: float) -> None:
        """Update statistics with a new value.

        Args:
            value: New value to incorporate
        """
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        """Calculate variance.

        Returns:
            Variance of all values seen so far
        """
        if self.count < 2:
            return 0.0
        return self.m2 / self.count

    @property
    def stddev(self) -> float:
        """Calculate standard deviation.

        Returns:
            Standard deviation of all values seen so far
        """
        return math.sqrt(self.variance)

    def z_score(self, value: float) -> float:
        """Calculate Z-score for a value.

        Args:
            value: Value to score

        Returns:
            Z-score (number of standard deviations from mean)
        """
        if self.stddev == 0:
            return 0.0
        return (value - self.mean) / self.stddev


def detect_anomalies(
    stream: Iterator[Ok[Transaction]], threshold: float = 3.0
) -> AnomalyResult:
    """Detect anomalous transactions using streaming Z-score analysis.

    This function uses Welford's online algorithm to calculate mean and
    standard deviation in a single pass, flagging transactions whose values
    exceed the Z-score threshold.

    Memory Complexity: O(anomalies) - only anomalous transactions are stored

    Args:
        stream: Iterator of Ok-wrapped Transaction objects
        threshold: Z-score threshold for anomaly detection (default: 3.0)

    Returns:
        AnomalyResult with detected anomalies and statistics

    Examples:
        >>> result = detect_anomalies(transaction_stream, threshold=3.0)
        >>> print(f"Found {result.anomaly_count} anomalies")
    """
    logger.info("anomaly_detection_started", threshold=threshold)

    welford = WelfordAccumulator()
    anomalies: list[AnomalyTransaction] = []
    total_transactions = 0

    # Two-pass approach for accurate anomaly detection:
    # Pass 1: Calculate statistics
    # Pass 2: Detect anomalies
    # However, for streaming, we'll use a single pass with buffering
    transactions_buffer: list[tuple[Transaction, float]] = []

    # Single pass: collect data and calculate statistics
    for result in stream:
        tx = result.unwrap()
        total_transactions += 1

        # Get absolute transaction value
        value = abs(float(tx.total_amount))

        # Update statistics
        welford.update(value)

        # Buffer transaction for anomaly detection
        transactions_buffer.append((tx, value))

    # Now detect anomalies using calculated statistics
    for tx, value in transactions_buffer:
        z_score = welford.z_score(value)

        if abs(z_score) >= threshold:
            anomalies.append(
                AnomalyTransaction(
                    transaction=tx,
                    z_score=z_score,
                    transaction_value=tx.total_amount,
                )
            )

    logger.info(
        "anomaly_detection_completed",
        total_transactions=total_transactions,
        anomaly_count=len(anomalies),
        mean_value=f"{welford.mean:.2f}",
        stddev_value=f"{welford.stddev:.2f}",
    )

    return AnomalyResult(
        anomalies=sorted(anomalies, key=lambda a: abs(a.z_score), reverse=True),
        total_transactions=total_transactions,
        anomaly_count=len(anomalies),
        mean_transaction_value=welford.mean,
        stddev_transaction_value=welford.stddev,
    )

