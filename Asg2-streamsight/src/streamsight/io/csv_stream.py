"""Generator-based CSV streaming with validation and Dead Letter Queue.

This module provides memory-efficient streaming of CSV data, validating
each row and routing invalid rows to a Dead Letter Queue (DLQ).
"""

import csv
from pathlib import Path
from typing import Iterator, Any

from pydantic import ValidationError

from streamsight.core.types import Result, Ok, Err
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


def stream_transactions(filepath: Path) -> Iterator[Result[Transaction, tuple[int, dict[str, Any], ValidationError]]]:
    """Stream transactions from a CSV file with lazy evaluation.

    This generator reads the CSV line-by-line, never loading the entire
    dataset into memory. Invalid rows are yielded as Err results containing
    the row number, raw data, and validation error.

    Memory Complexity: O(1) - only one row in memory at a time

    Args:
        filepath: Path to the CSV file

    Yields:
        Result[Transaction, Error] - Ok(Transaction) or Err((row_num, row_data, error))

    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        IOError: If the file cannot be read

    Examples:
        >>> for result in stream_transactions(Path("data.csv")):
        ...     if result.is_ok():
        ...         tx = result.unwrap()
        ...         # Process valid transaction
        ...     else:
        ...         row_num, data, error = result.unwrap_err()
        ...         # Handle validation error
    """
    if not filepath.exists():
        msg = f"CSV file not found: {filepath}"
        raise FileNotFoundError(msg)

    logger.info("streaming_started", filepath=str(filepath))

    valid_count = 0
    error_count = 0

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            # Use DictReader for automatic header parsing
            reader = csv.DictReader(f)

            # Validate that we have a header
            if reader.fieldnames is None:
                msg = "CSV file is empty or has no header"
                raise ValueError(msg)

            logger.debug(
                "csv_header_parsed",
                fields=reader.fieldnames,
                field_count=len(reader.fieldnames),
            )

            # Stream each row
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
                try:
                    # Validate and parse the transaction
                    tx = Transaction.model_validate(row)
                    valid_count += 1
                    yield Ok(tx)

                except ValidationError as e:
                    # Route to DLQ
                    error_count += 1
                    logger.debug(
                        "validation_error",
                        row_num=row_num,
                        error_count=error_count,
                        errors=e.error_count(),
                    )
                    yield Err((row_num, row, e))

    except Exception as e:
        logger.error("streaming_failed", error=str(e), filepath=str(filepath))
        raise

    logger.info(
        "streaming_completed",
        filepath=str(filepath),
        valid_count=valid_count,
        error_count=error_count,
        total_count=valid_count + error_count,
        error_rate=f"{error_count / (valid_count + error_count) * 100:.2f}%"
        if (valid_count + error_count) > 0
        else "0.00%",
    )


def write_dlq(
    dlq_stream: Iterator[tuple[int, dict[str, Any], ValidationError]],
    output_path: Path,
) -> int:
    """Write Dead Letter Queue entries to a JSONL file.

    Args:
        dlq_stream: Iterator of (row_number, row_data, error) tuples
        output_path: Path to write DLQ JSONL file

    Returns:
        Number of DLQ entries written

    Examples:
        >>> dlq_count = write_dlq(error_stream, Path("errors/bad_rows.jsonl"))
    """
    import orjson

    logger.info("dlq_write_started", output_path=str(output_path))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with open(output_path, "wb") as f:
        for row_num, row_data, error in dlq_stream:
            # Format error details
            error_details = {
                "row_number": row_num,
                "data": row_data,
                "errors": [
                    {
                        "field": ".".join(str(loc) for loc in err["loc"]),
                        "message": err["msg"],
                        "type": err["type"],
                    }
                    for err in error.errors()
                ],
            }

            # Write as JSONL (one JSON object per line)
            f.write(orjson.dumps(error_details))
            f.write(b"\n")
            count += 1

    logger.info("dlq_write_completed", output_path=str(output_path), count=count)
    return count

