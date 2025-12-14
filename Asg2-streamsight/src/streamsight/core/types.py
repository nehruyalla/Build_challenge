"""Core type definitions for StreamSight.

This module provides type aliases and Result types for functional error handling.
"""

from decimal import Decimal
from typing import Generic, TypeVar, Union

# Financial type - NEVER use float for money
Money = Decimal

# Generic type variables for Result type
T = TypeVar("T")
E = TypeVar("E")


class Ok(Generic[T]):
    """Success result containing a value."""

    __slots__ = ("_value",)

    def __init__(self, value: T) -> None:
        """Initialize Ok with a value.

        Args:
            value: The success value to wrap
        """
        self._value = value

    @property
    def value(self) -> T:
        """Get the wrapped value."""
        return self._value

    def is_ok(self) -> bool:
        """Check if this is an Ok result."""
        return True

    def is_err(self) -> bool:
        """Check if this is an Err result."""
        return False

    def unwrap(self) -> T:
        """Unwrap the Ok value."""
        return self._value

    def __repr__(self) -> str:
        """String representation."""
        return f"Ok({self._value!r})"


class Err(Generic[E]):
    """Error result containing an error value."""

    __slots__ = ("_error",)

    def __init__(self, error: E) -> None:
        """Initialize Err with an error.

        Args:
            error: The error value to wrap
        """
        self._error = error

    @property
    def error(self) -> E:
        """Get the wrapped error."""
        return self._error

    def is_ok(self) -> bool:
        """Check if this is an Ok result."""
        return False

    def is_err(self) -> bool:
        """Check if this is an Err result."""
        return True

    def unwrap_err(self) -> E:
        """Unwrap the Err value."""
        return self._error

    def __repr__(self) -> str:
        """String representation."""
        return f"Err({self._error!r})"


# Result type for functional error handling
Result = Union[Ok[T], Err[E]]

