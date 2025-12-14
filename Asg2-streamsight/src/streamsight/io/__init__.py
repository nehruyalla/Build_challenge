"""I/O modules for streaming CSV data and schema validation."""

from streamsight.io.csv_stream import stream_transactions
from streamsight.io.schema import Transaction

__all__ = ["stream_transactions", "Transaction"]

