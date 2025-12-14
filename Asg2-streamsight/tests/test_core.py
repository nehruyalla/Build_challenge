"""Tests for core types and utilities."""

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from streamsight.core.money import (
    parse_money,
    sum_money,
    multiply_money,
    divide_money,
    format_money,
)
from streamsight.core.stream_utils import broadcast, partition, fold, take, drop
from streamsight.core.types import Ok, Err


class TestResultTypes:
    """Tests for Ok and Err result types."""

    def test_ok_creation(self) -> None:
        """Test creating an Ok result."""
        result = Ok(42)
        assert result.is_ok()
        assert not result.is_err()
        assert result.unwrap() == 42

    def test_err_creation(self) -> None:
        """Test creating an Err result."""
        result = Err("error message")
        assert not result.is_ok()
        assert result.is_err()
        assert result.unwrap_err() == "error message"


class TestMoneyOperations:
    """Tests for Decimal-based money operations."""

    def test_parse_money_from_string(self) -> None:
        """Test parsing money from string."""
        result = parse_money("10.50")
        assert result == Decimal("10.50")

    def test_parse_money_from_float(self) -> None:
        """Test parsing money from float (with rounding)."""
        result = parse_money(10.5)
        assert result == Decimal("10.50")

    def test_parse_money_from_int(self) -> None:
        """Test parsing money from integer."""
        result = parse_money(10)
        assert result == Decimal("10.00")

    def test_parse_money_invalid(self) -> None:
        """Test parsing invalid money raises error."""
        with pytest.raises(Exception):
            parse_money("invalid")

    def test_sum_money(self) -> None:
        """Test summing money values."""
        values = [Decimal("10.50"), Decimal("20.25"), Decimal("5.00")]
        result = sum_money(iter(values))
        assert result == Decimal("35.75")

    def test_multiply_money(self) -> None:
        """Test multiplying money by quantity."""
        result = multiply_money(Decimal("10.50"), 3)
        assert result == Decimal("31.50")

    def test_divide_money(self) -> None:
        """Test dividing money."""
        result = divide_money(Decimal("100.00"), 3)
        assert result == Decimal("33.33")

    def test_divide_by_zero(self) -> None:
        """Test that dividing by zero raises error."""
        with pytest.raises(ZeroDivisionError):
            divide_money(Decimal("100.00"), 0)

    def test_format_money(self) -> None:
        """Test formatting money for display."""
        result = format_money(Decimal("1234.56"))
        assert result == "$1,234.56"

    @given(
        st.lists(
            st.decimals(
                min_value=Decimal("0.01"),
                max_value=Decimal("10000.00"),
                places=2,
            ),
            min_size=1,
            max_size=100,
        )
    )
    def test_sum_associativity(self, values: list[Decimal]) -> None:
        """Property test: Sum should be associative.

        This proves that summing in chunks gives the same result as
        summing all at once, which is critical for streaming aggregation.
        """
        total = sum_money(iter(values))

        # Split into chunks and sum separately
        mid = len(values) // 2
        chunk1_sum = sum_money(iter(values[:mid])) if mid > 0 else Decimal("0")
        chunk2_sum = sum_money(iter(values[mid:]))
        chunked_total = chunk1_sum + chunk2_sum

        assert abs(total - chunked_total) < Decimal("0.01")


class TestStreamUtils:
    """Tests for streaming utility functions."""

    def test_broadcast(self) -> None:
        """Test broadcasting a stream to multiple consumers."""
        source = iter([1, 2, 3])
        s1, s2 = broadcast(source, 2)

        assert list(s1) == [1, 2, 3]
        assert list(s2) == [1, 2, 3]

    def test_partition(self) -> None:
        """Test partitioning a stream by predicate."""
        source = iter([1, 2, 3, 4, 5])
        evens, odds = partition(source, lambda x: x % 2 == 0)

        assert list(evens) == [2, 4]
        assert list(odds) == [1, 3, 5]

    def test_fold(self) -> None:
        """Test folding (reducing) a stream."""
        source = iter([1, 2, 3, 4])
        result = fold(source, lambda acc, x: acc + x, 0)
        assert result == 10

    def test_take(self) -> None:
        """Test taking N items from stream."""
        source = iter(range(100))
        result = list(take(source, 5))
        assert result == [0, 1, 2, 3, 4]

    def test_drop(self) -> None:
        """Test dropping N items from stream."""
        source = iter(range(10))
        result = list(drop(source, 5))
        assert result == [5, 6, 7, 8, 9]

