"""Pytest configuration and fixtures for StreamSight tests."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import pytest

from streamsight.core.types import Ok
from streamsight.io.schema import Transaction


@pytest.fixture
def sample_transaction() -> Transaction:
    """Create a sample valid transaction for testing."""
    return Transaction(
        InvoiceNo="12345",
        StockCode="ABC123",
        Description="Test Product",
        Quantity=5,
        InvoiceDate=datetime(2023, 1, 15, 10, 30, 0),
        UnitPrice=Decimal("10.50"),
        CustomerID="CUST001",
        Country="United Kingdom",
    )


@pytest.fixture
def return_transaction() -> Transaction:
    """Create a sample return transaction."""
    return Transaction(
        InvoiceNo="C12346",
        StockCode="ABC123",
        Description="Test Product",
        Quantity=-2,
        InvoiceDate=datetime(2023, 1, 16, 14, 0, 0),
        UnitPrice=Decimal("10.50"),
        CustomerID="CUST001",
        Country="United Kingdom",
    )


@pytest.fixture
def sample_transactions() -> list[Transaction]:
    """Create a list of sample transactions for testing."""
    return [
        Transaction(
            InvoiceNo="12345",
            StockCode="ABC123",
            Description="Product A",
            Quantity=5,
            InvoiceDate=datetime(2023, 1, 15, 10, 0, 0),
            UnitPrice=Decimal("10.00"),
            CustomerID="CUST001",
            Country="UK",
        ),
        Transaction(
            InvoiceNo="12346",
            StockCode="ABC124",
            Description="Product B",
            Quantity=3,
            InvoiceDate=datetime(2023, 1, 16, 11, 0, 0),
            UnitPrice=Decimal("20.00"),
            CustomerID="CUST001",
            Country="UK",
        ),
        Transaction(
            InvoiceNo="12347",
            StockCode="ABC123",
            Description="Product A",
            Quantity=2,
            InvoiceDate=datetime(2023, 1, 17, 12, 0, 0),
            UnitPrice=Decimal("10.00"),
            CustomerID="CUST002",
            Country="France",
        ),
        Transaction(
            InvoiceNo="C12348",
            StockCode="ABC123",
            Description="Product A",
            Quantity=-1,
            InvoiceDate=datetime(2023, 1, 18, 13, 0, 0),
            UnitPrice=Decimal("10.00"),
            CustomerID="CUST002",
            Country="France",
        ),
    ]


@pytest.fixture
def sample_stream(sample_transactions: list[Transaction]) -> Iterator[Ok[Transaction]]:
    """Create an iterator stream of Ok-wrapped transactions."""
    return (Ok(tx) for tx in sample_transactions)


@pytest.fixture
def temp_csv_file(tmp_path: Path, sample_transactions: list[Transaction]) -> Path:
    """Create a temporary CSV file with sample data."""
    csv_path = tmp_path / "test_data.csv"

    with open(csv_path, "w") as f:
        # Write header
        f.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n")

        # Write data
        for tx in sample_transactions:
            customer_id = tx.CustomerID or ""
            f.write(
                f"{tx.InvoiceNo},{tx.StockCode},{tx.Description},{tx.Quantity},"
                f"{tx.InvoiceDate.strftime('%Y-%m-%d %H:%M:%S')},{tx.UnitPrice},"
                f"{customer_id},{tx.Country}\n"
            )

    return csv_path


@pytest.fixture
def invalid_csv_file(tmp_path: Path) -> Path:
    """Create a CSV file with invalid data for DLQ testing."""
    csv_path = tmp_path / "invalid_data.csv"

    with open(csv_path, "w") as f:
        f.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n")
        # Valid row
        f.write("12345,ABC123,Product,5,2023-01-15 10:00:00,10.00,CUST001,UK\n")
        # Invalid UnitPrice
        f.write("12346,ABC124,Product,3,2023-01-16 11:00:00,INVALID,CUST002,UK\n")
        # Missing required field
        f.write("12347,ABC125,,2,2023-01-17 12:00:00,15.00,CUST003,France\n")

    return csv_path

