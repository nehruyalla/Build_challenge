"""Tests for analytics modules."""

from decimal import Decimal
from typing import Iterator

import pytest

from streamsight.analytics.data_quality import analyze_data_quality
from streamsight.analytics.geography import analyze_geography
from streamsight.analytics.products import analyze_products
from streamsight.analytics.returns import analyze_returns
from streamsight.analytics.revenue import analyze_revenue
from streamsight.core.types import Ok
from streamsight.io.schema import Transaction


class TestRevenueAnalytics:
    """Tests for revenue analysis."""

    def test_revenue_analysis(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test basic revenue analysis."""
        # Need to materialize stream for testing
        transactions = list(sample_stream)
        result = analyze_revenue(iter(transactions))

        # Expected: 5*10 + 3*20 + 2*10 + (-1)*10 = 50 + 60 + 20 - 10 = 120
        assert result.net_revenue == Decimal("120.00")
        assert result.transaction_count == 4
        assert result.return_count == 1

    def test_daily_revenue(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test daily revenue breakdown."""
        transactions = list(sample_stream)
        result = analyze_revenue(iter(transactions))

        assert "2023-01-15" in result.daily_revenue
        assert "2023-01-16" in result.daily_revenue
        assert result.daily_revenue["2023-01-15"] == Decimal("50.00")

    def test_monthly_revenue(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test monthly revenue breakdown."""
        transactions = list(sample_stream)
        result = analyze_revenue(iter(transactions))

        assert "2023-01" in result.monthly_revenue
        assert result.monthly_revenue["2023-01"] == Decimal("120.00")


class TestGeographyAnalytics:
    """Tests for geography analysis."""

    def test_geography_analysis(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test geographic analysis."""
        transactions = list(sample_stream)
        result = analyze_geography(iter(transactions))

        assert "UK" in result.country_revenue
        assert "France" in result.country_revenue
        assert result.country_revenue["UK"] == Decimal("110.00")  # 50 + 60
        assert result.country_revenue["France"] == Decimal("10.00")  # 20 - 10

    def test_revenue_share(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test revenue share calculation."""
        transactions = list(sample_stream)
        result = analyze_geography(iter(transactions))

        # UK should have ~91.67% share (110/120)
        assert abs(result.country_revenue_share["UK"] - 91.67) < 0.1
        # France should have ~8.33% share (10/120)
        assert abs(result.country_revenue_share["France"] - 8.33) < 0.1


class TestProductsAnalytics:
    """Tests for product analysis."""

    def test_products_analysis(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test product analysis."""
        transactions = list(sample_stream)
        result = analyze_products(iter(transactions), top_k=2)

        assert result.total_product_count == 2  # ABC123 and ABC124
        assert len(result.top_products) == 2

    def test_top_products_order(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test that top products are sorted by revenue."""
        transactions = list(sample_stream)
        result = analyze_products(iter(transactions), top_k=10)

        # Product A (ABC123): 5*10 + 2*10 - 1*10 = 60
        # Product B (ABC124): 3*20 = 60
        # Both should be in top products
        stock_codes = [p.stock_code for p in result.top_products]
        assert "ABC123" in stock_codes
        assert "ABC124" in stock_codes


class TestReturnsAnalytics:
    """Tests for returns analysis."""

    def test_returns_analysis(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test returns analysis."""
        transactions = list(sample_stream)
        result = analyze_returns(iter(transactions))

        assert result.total_transactions == 4
        assert result.return_transactions == 1
        assert result.return_rate == 25.0  # 1 out of 4

    def test_return_revenue_impact(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test return revenue impact calculation."""
        transactions = list(sample_stream)
        result = analyze_returns(iter(transactions))

        assert result.return_revenue_impact == Decimal("-10.00")


class TestDataQuality:
    """Tests for data quality analysis."""

    def test_data_quality(self, sample_stream: Iterator[Ok[Transaction]]) -> None:
        """Test data quality analysis."""
        transactions = list(sample_stream)
        result = analyze_data_quality(iter(transactions))

        assert result.total_rows == 4
        assert result.valid_rows == 4
        assert result.missing_customer_id == 0

