"""Financial calculations with Decimal precision.

This module ensures that all monetary calculations maintain accuracy
by using Decimal instead of float, preventing floating-point errors.

Why Decimal?
------------
Floating-point arithmetic can introduce errors in financial calculations:
>>> 0.1 + 0.2 == 0.3
False
>>> float(0.1) + float(0.2)
0.30000000000000004

Decimal provides exact decimal arithmetic:
>>> Decimal('0.1') + Decimal('0.2') == Decimal('0.3')
True
"""

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Union, Iterator

from streamsight.core.types import Money


# Standard rounding mode for financial calculations (Banker's Rounding alternative)
FINANCIAL_ROUNDING = ROUND_HALF_UP

# Standard precision for currency (2 decimal places)
CURRENCY_PRECISION = Decimal("0.01")


def parse_money(value: Union[str, int, float, Decimal]) -> Money:
    """Parse a value into a Money (Decimal) type with financial precision.

    Args:
        value: The value to parse (string, int, float, or Decimal)

    Returns:
        Decimal value rounded to 2 decimal places

    Raises:
        InvalidOperation: If the value cannot be parsed as a decimal

    Examples:
        >>> parse_money("10.50")
        Decimal('10.50')
        >>> parse_money(10.5)
        Decimal('10.50')
        >>> parse_money(10)
        Decimal('10.00')
    """
    try:
        if isinstance(value, Decimal):
            decimal_value = value
        else:
            # Convert to string first to avoid float precision issues
            decimal_value = Decimal(str(value))

        # Round to standard currency precision
        return decimal_value.quantize(CURRENCY_PRECISION, rounding=FINANCIAL_ROUNDING)
    except (InvalidOperation, ValueError) as e:
        msg = f"Cannot parse '{value}' as Money: {e}"
        raise InvalidOperation(msg) from e


def sum_money(values: Iterator[Money]) -> Money:
    """Sum monetary values with proper precision.

    Args:
        values: Iterator of Money (Decimal) values

    Returns:
        Sum of all values, rounded to currency precision

    Examples:
        >>> sum_money(iter([Decimal('10.50'), Decimal('20.25')]))
        Decimal('30.75')
    """
    total = Decimal("0")
    for value in values:
        total += value
    return total.quantize(CURRENCY_PRECISION, rounding=FINANCIAL_ROUNDING)


def multiply_money(amount: Money, quantity: Union[int, Decimal]) -> Money:
    """Multiply a monetary amount by a quantity.

    Args:
        amount: The unit amount
        quantity: The quantity to multiply by

    Returns:
        Product rounded to currency precision

    Examples:
        >>> multiply_money(Decimal('10.50'), 3)
        Decimal('31.50')
    """
    if isinstance(quantity, int):
        quantity_decimal = Decimal(quantity)
    else:
        quantity_decimal = quantity

    result = amount * quantity_decimal
    return result.quantize(CURRENCY_PRECISION, rounding=FINANCIAL_ROUNDING)


def divide_money(amount: Money, divisor: Union[int, Decimal]) -> Money:
    """Divide a monetary amount.

    Args:
        amount: The amount to divide
        divisor: The divisor

    Returns:
        Quotient rounded to currency precision

    Raises:
        ZeroDivisionError: If divisor is zero

    Examples:
        >>> divide_money(Decimal('100.00'), 3)
        Decimal('33.33')
    """
    if divisor == 0:
        msg = "Cannot divide money by zero"
        raise ZeroDivisionError(msg)

    if isinstance(divisor, int):
        divisor_decimal = Decimal(divisor)
    else:
        divisor_decimal = divisor

    result = amount / divisor_decimal
    return result.quantize(CURRENCY_PRECISION, rounding=FINANCIAL_ROUNDING)


def format_money(amount: Money, currency_symbol: str = "$") -> str:
    """Format a monetary amount for display.

    Args:
        amount: The amount to format
        currency_symbol: Currency symbol to use (default: $)

    Returns:
        Formatted string representation

    Examples:
        >>> format_money(Decimal('1234.56'))
        '$1,234.56'
        >>> format_money(Decimal('1234.56'), 'EUR')
        'EUR1,234.56'
    """
    # Format with thousands separator
    formatted = f"{amount:,.2f}"
    return f"{currency_symbol}{formatted}"

