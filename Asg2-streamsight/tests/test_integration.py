"""Integration tests for the complete pipeline."""

from pathlib import Path

import pytest

from streamsight.config import Config
from streamsight.pipeline.runner import run_pipeline


class TestPipelineIntegration:
    """Integration tests for the complete analytics pipeline."""

    def test_full_pipeline(self, temp_csv_file: Path, tmp_path: Path) -> None:
        """Test running the complete pipeline end-to-end."""
        # Configure to use temp file
        config = Config(
            input_file=temp_csv_file,
            output_dir=tmp_path / "results",
            enable_anomaly_detection=True,
            enable_rfm_analysis=True,
            top_k_products=5,
            zscore_threshold=3.0,
            rfm_whale_percentile=50,
        )

        # Run pipeline
        results = run_pipeline(config)

        # Verify core analytics
        assert results.revenue is not None
        assert results.geography is not None
        assert results.products is not None
        assert results.returns is not None
        assert results.data_quality is not None

        # Verify outputs were written
        assert (config.tables_dir / "revenue.json").exists()
        assert (config.tables_dir / "geography.json").exists()
        assert (config.tables_dir / "products.json").exists()

        # Verify RFM results
        assert results.rfm is not None
        assert results.rfm.total_customers == 2
        assert (config.tables_dir / "rfm_whales.json").exists()

    def test_pipeline_with_dlq(self, invalid_csv_file: Path, tmp_path: Path) -> None:
        """Test pipeline with invalid data (DLQ handling)."""
        config = Config(
            input_file=invalid_csv_file,
            output_dir=tmp_path / "results",
            enable_anomaly_detection=False,
            enable_rfm_analysis=False,
        )

        results = run_pipeline(config)

        # Verify DLQ captured errors
        assert results.dlq_count == 2
        assert (config.errors_dir / "bad_rows.jsonl").exists()

        # Verify valid row was processed
        assert results.revenue.transaction_count == 1

    def test_pipeline_minimal_config(self, temp_csv_file: Path, tmp_path: Path) -> None:
        """Test pipeline with minimal configuration (no optional features)."""
        config = Config(
            input_file=temp_csv_file,
            output_dir=tmp_path / "results",
            enable_anomaly_detection=False,
            enable_rfm_analysis=False,
        )

        results = run_pipeline(config)

        # Core analytics should run
        assert results.revenue is not None
        assert results.geography is not None

        # Optional features should be None
        assert results.anomaly is None
        assert results.rfm is None

