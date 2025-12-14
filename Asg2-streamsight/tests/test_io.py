"""Tests for I/O modules (CSV streaming and schema validation)."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from streamsight.io.csv_stream import stream_transactions, write_dlq
from streamsight.io.schema import Transaction


class TestTransactionSchema:
    """Tests for Transaction schema validation."""

    def test_valid_transaction(self, sample_transaction: Transaction) -> None:
        """Test that valid transaction is created correctly."""
        assert sample_transaction.InvoiceNo == "12345"
        assert sample_transaction.Quantity == 5
        assert sample_transaction.UnitPrice == Decimal("10.50")

    def test_transaction_total_amount(self, sample_transaction: Transaction) -> None:
        """Test transaction total amount calculation."""
        total = sample_transaction.total_amount
        assert total == Decimal("52.50")  # 5 * 10.50

    def test_is_return_negative_quantity(self, return_transaction: Transaction) -> None:
        """Test return detection with negative quantity."""
        assert return_transaction.is_return

    def test_is_return_c_prefix(self) -> None:
        """Test return detection with C prefix."""
        tx = Transaction(
            InvoiceNo="C12345",
            StockCode="ABC",
            Description="Product",
            Quantity=1,
            InvoiceDate=datetime.now(),
            UnitPrice=Decimal("10.00"),
            CustomerID="CUST001",
            Country="UK",
        )
        assert tx.is_return

    def test_invalid_unit_price(self) -> None:
        """Test that invalid unit price raises validation error."""
        with pytest.raises(Exception):
            Transaction(
                InvoiceNo="12345",
                StockCode="ABC",
                Description="Product",
                Quantity=1,
                InvoiceDate=datetime.now(),
                UnitPrice="invalid",  # type: ignore
                CustomerID="CUST001",
                Country="UK",
            )

    def test_missing_customer_id_allowed(self) -> None:
        """Test that missing CustomerID is allowed (None)."""
        tx = Transaction(
            InvoiceNo="12345",
            StockCode="ABC",
            Description="Product",
            Quantity=1,
            InvoiceDate=datetime.now(),
            UnitPrice=Decimal("10.00"),
            CustomerID=None,
            Country="UK",
        )
        assert tx.CustomerID is None


class TestCSVStreaming:
    """Tests for CSV streaming functionality."""

    def test_stream_valid_csv(self, temp_csv_file: Path) -> None:
        """Test streaming a valid CSV file."""
        results = list(stream_transactions(temp_csv_file))

        # Count Ok and Err results
        ok_count = sum(1 for r in results if r.is_ok())
        err_count = sum(1 for r in results if r.is_err())

        assert ok_count == 4  # All 4 transactions are valid
        assert err_count == 0

    def test_stream_invalid_csv(self, invalid_csv_file: Path) -> None:
        """Test streaming CSV with invalid rows (DLQ)."""
        results = list(stream_transactions(invalid_csv_file))

        ok_count = sum(1 for r in results if r.is_ok())
        err_count = sum(1 for r in results if r.is_err())

        assert ok_count == 1  # Only first row is valid
        assert err_count == 2  # Two invalid rows

    def test_write_dlq(self, invalid_csv_file: Path, tmp_path: Path) -> None:
        """Test writing DLQ entries."""
        results = list(stream_transactions(invalid_csv_file))
        errors = [r.unwrap_err() for r in results if r.is_err()]

        dlq_path = tmp_path / "dlq.jsonl"
        count = write_dlq(iter(errors), dlq_path)

        assert count == 2
        assert dlq_path.exists()

        # Verify DLQ file content
        with open(dlq_path, "r") as f:
            lines = f.readlines()
            assert len(lines) == 2

    def test_stream_nonexistent_file(self) -> None:
        """Test streaming nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            list(stream_transactions(Path("nonexistent.csv")))

