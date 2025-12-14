"""Configuration management for StreamSight.

All configurable parameters are centralized here using Pydantic Settings
for type safety and validation.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Main configuration for StreamSight pipeline.

    Attributes:
        input_file: Path to the input CSV file
        output_dir: Base directory for all outputs
        top_k_products: Number of top products to track
        zscore_threshold: Z-score threshold for anomaly detection
        rfm_whale_percentile: Percentile threshold for whale customers
        rfm_reference_date: Reference date for RFM recency calculation
        chunk_size: Size of chunks for streaming operations
        enable_anomaly_detection: Whether to run anomaly detection
        enable_rfm_analysis: Whether to run RFM analysis
        log_level: Logging level
    """

    model_config = SettingsConfigDict(
        env_prefix="STREAMSIGHT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Input/Output paths
    input_file: Path = Field(
        default=Path("dataset/Online_Retail.csv"),
        description="Path to input CSV file",
    )
    output_dir: Path = Field(
        default=Path("results"),
        description="Base directory for outputs",
    )

    # Analytics parameters
    top_k_products: int = Field(
        default=10,
        description="Number of top products to track",
        ge=1,
        le=100,
    )
    zscore_threshold: float = Field(
        default=3.0,
        description="Z-score threshold for anomaly detection",
        gt=0.0,
    )
    rfm_whale_percentile: int = Field(
        default=99,
        description="Percentile threshold for whale customers (1-100)",
        ge=1,
        le=100,
    )
    rfm_reference_date: Optional[datetime] = Field(
        default=None,
        description="Reference date for RFM recency (None = use max date in data)",
    )

    # Performance tuning
    chunk_size: int = Field(
        default=1000,
        description="Chunk size for streaming operations",
        ge=100,
    )

    # Feature flags
    enable_anomaly_detection: bool = Field(
        default=True,
        description="Enable anomaly detection",
    )
    enable_rfm_analysis: bool = Field(
        default=True,
        description="Enable RFM whale analysis",
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    @property
    def figures_dir(self) -> Path:
        """Directory for figure outputs."""
        return self.output_dir / "figures"

    @property
    def tables_dir(self) -> Path:
        """Directory for table outputs."""
        return self.output_dir / "tables"

    @property
    def reports_dir(self) -> Path:
        """Directory for report outputs."""
        return self.output_dir / "reports"

    @property
    def errors_dir(self) -> Path:
        """Directory for error/DLQ outputs."""
        return self.output_dir / "errors"

    def ensure_output_dirs(self) -> None:
        """Create all output directories if they don't exist."""
        self.figures_dir.mkdir(parents=True, exist_ok=True)
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.errors_dir.mkdir(parents=True, exist_ok=True)

