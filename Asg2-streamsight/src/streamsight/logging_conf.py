"""Structured logging configuration using structlog.

This module sets up structured logging for the entire application,
providing context-rich, machine-readable logs suitable for production.
"""

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor


def add_log_level(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    """Add log level to the event dict.

    Args:
        logger: The logger instance
        method_name: The log method name
        event_dict: The event dictionary

    Returns:
        Updated event dictionary with level
    """
    event_dict["level"] = method_name.upper()
    return event_dict


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the application.

    Args:
        log_level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
    )

    # Configure structlog
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # Use JSON for production, colored output for development
    if log_level == "DEBUG":
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Name of the logger (typically __name__)

    Returns:
        Configured structlog logger

    Examples:
        >>> logger = get_logger(__name__)
        >>> logger.info("processing_started", file="data.csv", rows=1000)
    """
    return structlog.get_logger(name)

