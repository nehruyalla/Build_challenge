"""Pydantic schema for transaction validation.

This module defines the Transaction model with proper validation,
ensuring data quality at the ingestion layer.
"""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from pydantic import BaseModel, Field, field_validator, ConfigDict

from streamsight.core.money import parse_money, Money


class Transaction(BaseModel):
    """A single transaction record from the sales data.

    All monetary values are stored as Decimal for financial accuracy.
    This model validates incoming CSV data and rejects invalid rows.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    InvoiceNo: str = Field(
        ...,
        description="Invoice number (unique identifier)",
        min_length=1,
    )
    StockCode: str = Field(
        ...,
        description="Product/stock code",
        min_length=1,
    )
    Description: str = Field(
        ...,
        description="Product description",
        min_length=1,
    )
    Quantity: int = Field(
        ...,
        description="Quantity purchased (negative for returns)",
    )
    InvoiceDate: datetime = Field(
        ...,
        description="Invoice date and time",
    )
    UnitPrice: Money = Field(
        ...,
        description="Unit price in currency",
    )
    CustomerID: Optional[str] = Field(
        default=None,
        description="Customer identifier (may be missing)",
    )
    Country: str = Field(
        ...,
        description="Country of customer",
        min_length=1,
    )

    @field_validator("UnitPrice", mode="before")
    @classmethod
    def validate_unit_price(cls, v: object) -> Money:
        """Validate and convert unit price to Decimal.

        Args:
            v: Raw value from CSV

        Returns:
            Parsed Money (Decimal) value

        Raises:
            ValueError: If value cannot be parsed as currency
        """
        if v is None or v == "":
            msg = "UnitPrice cannot be empty"
            raise ValueError(msg)

        try:
            return parse_money(v)
        except (InvalidOperation, ValueError) as e:
            msg = f"Invalid UnitPrice: {v!r}"
            raise ValueError(msg) from e

    @field_validator("InvoiceDate", mode="before")
    @classmethod
    def validate_invoice_date(cls, v: object) -> datetime:
        """Validate and parse invoice date.

        Supports multiple date formats commonly found in CSV files.

        Args:
            v: Raw value from CSV

        Returns:
            Parsed datetime

        Raises:
            ValueError: If date cannot be parsed
        """
        if v is None or v == "":
            msg = "InvoiceDate cannot be empty"
            raise ValueError(msg)

        if isinstance(v, datetime):
            return v

        if isinstance(v, str):
            # Try common date formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%d/%m/%Y %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue

        msg = f"Cannot parse date: {v!r}"
        raise ValueError(msg)

    @field_validator("CustomerID", mode="before")
    @classmethod
    def validate_customer_id(cls, v: object) -> Optional[str]:
        """Validate customer ID, allowing None for missing values.

        Args:
            v: Raw value from CSV

        Returns:
            Customer ID string or None
        """
        if v is None or v == "" or (isinstance(v, str) and v.strip() == ""):
            return None
        return str(v).strip()

    @property
    def is_return(self) -> bool:
        """Check if this transaction is a return.

        Returns are identified by:
        - Negative quantity
        - Invoice number starting with 'C' (cancellation)
        """
        return self.Quantity < 0 or self.InvoiceNo.startswith("C")

    @property
    def total_amount(self) -> Money:
        """Calculate the total transaction amount.

        Returns:
            Quantity * UnitPrice (negative for returns)
        """
        from streamsight.core.money import multiply_money

        return multiply_money(self.UnitPrice, self.Quantity)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"Transaction(InvoiceNo={self.InvoiceNo!r}, "
            f"StockCode={self.StockCode!r}, "
            f"Quantity={self.Quantity}, "
            f"UnitPrice={self.UnitPrice}, "
            f"CustomerID={self.CustomerID!r})"
        )

