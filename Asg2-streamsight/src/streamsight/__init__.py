"""StreamSight: Production-grade streaming analytics pipeline for sales data.

This package provides a memory-efficient, generator-based streaming architecture
for processing large-scale sales data with financial accuracy (Decimal-based)
and functional programming paradigms.
"""

__version__ = "0.1.0"
__author__ = "StreamSight Team"

from streamsight.config import Config

__all__ = ["Config", "__version__"]

