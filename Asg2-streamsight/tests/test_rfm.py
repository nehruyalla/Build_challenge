"""Tests for RFM analysis."""

from datetime import datetime
from typing import Iterator

import pytest

from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.rfm.calculator import build_customer_profiles, calculate_max_date
from streamsight.rfm.segmentation import segment_customers


class TestRFMCalculator:
    """Tests for RFM profile building."""

    def test_build_profiles(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test building customer profiles."""
        transactions = list(sample_stream)
        profiles = build_customer_profiles(iter(transactions))

        assert len(profiles) == 2  # CUST001 and CUST002
        assert "CUST001" in profiles
        assert "CUST002" in profiles

    def test_profile_aggregation(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test that profiles correctly aggregate transactions."""
        transactions = list(sample_stream)
        profiles = build_customer_profiles(iter(transactions))

        cust001 = profiles["CUST001"]
        assert cust001.transaction_count == 2  # Two transactions
        assert cust001.total_spend == (5 * 10 + 3 * 20)  # 50 + 60 = 110

        cust002 = profiles["CUST002"]
        assert cust002.transaction_count == 2  # Two transactions
        assert cust002.total_spend == (2 * 10 - 1 * 10)  # 20 - 10 = 10

    def test_calculate_max_date(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test calculating max date from profiles."""
        transactions = list(sample_stream)
        profiles = build_customer_profiles(iter(transactions))
        max_date = calculate_max_date(profiles)

        # Latest transaction is 2023-01-18
        assert max_date == datetime(2023, 1, 18, 13, 0, 0)

    def test_empty_profiles(self) -> None:
        """Test that empty profiles raise error."""
        with pytest.raises(ValueError):
            calculate_max_date({})


class TestRFMSegmentation:
    """Tests for RFM segmentation and whale detection."""

    def test_segment_customers(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test customer segmentation."""
        transactions = list(sample_stream)
        profiles = build_customer_profiles(iter(transactions))
        result = segment_customers(profiles, whale_percentile=50)

        assert result.total_customers == 2
        assert len(result.rfm_scores) == 2

    def test_whale_identification(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test whale customer identification."""
        transactions = list(sample_stream)
        profiles = build_customer_profiles(iter(transactions))

        # With 50th percentile, top 50% are whales (1 customer)
        result = segment_customers(profiles, whale_percentile=50)

        assert result.whale_count == 1
        # CUST001 should be the whale (110 > 10)
        assert result.whale_customers[0].customer_id == "CUST001"

    def test_rfm_scores(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test that RFM scores are calculated."""
        transactions = list(sample_stream)
        profiles = build_customer_profiles(iter(transactions))
        result = segment_customers(profiles)

        for score in result.rfm_scores:
            assert 1 <= score.recency_score <= 5
            assert 1 <= score.frequency_score <= 5
            assert 1 <= score.monetary_score <= 5
            assert len(score.rfm_score) == 3  # e.g., "555"

    def test_empty_segmentation(self) -> None:
        """Test segmentation with no customers."""
        result = segment_customers({})

        assert result.total_customers == 0
        assert result.whale_count == 0
        assert len(result.rfm_scores) == 0

