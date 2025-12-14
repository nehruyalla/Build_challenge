"""Analytics registry for managing and configuring aggregators.

This module provides a registry pattern for organizing analytics functions
and making the pipeline extensible.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Iterator

from streamsight.core.types import Ok
from streamsight.io.schema import Transaction


@dataclass
class Aggregator:
    """Metadata for an analytics aggregator.

    Attributes:
        name: Unique name of the aggregator
        function: The aggregation function
        description: Human-readable description
        enabled: Whether this aggregator is enabled
        requires_config: Whether this aggregator needs config parameters
    """

    name: str
    function: Callable[[Iterator[Ok[Transaction]], Any], Any]
    description: str
    enabled: bool = True
    requires_config: bool = False


class AnalyticsRegistry:
    """Registry for analytics aggregators.

    This provides a central place to register and manage all analytics
    functions in the pipeline.
    """

    def __init__(self) -> None:
        """Initialize the registry."""
        self._aggregators: dict[str, Aggregator] = {}

    def register(
        self,
        name: str,
        function: Callable[[Iterator[Ok[Transaction]], Any], Any],
        description: str,
        enabled: bool = True,
        requires_config: bool = False,
    ) -> None:
        """Register an aggregator.

        Args:
            name: Unique name for the aggregator
            function: The aggregation function
            description: Human-readable description
            enabled: Whether the aggregator is enabled by default
            requires_config: Whether the aggregator requires config parameters
        """
        if name in self._aggregators:
            msg = f"Aggregator '{name}' is already registered"
            raise ValueError(msg)

        self._aggregators[name] = Aggregator(
            name=name,
            function=function,
            description=description,
            enabled=enabled,
            requires_config=requires_config,
        )

    def get(self, name: str) -> Aggregator:
        """Get an aggregator by name.

        Args:
            name: Name of the aggregator

        Returns:
            Aggregator metadata

        Raises:
            KeyError: If aggregator not found
        """
        if name not in self._aggregators:
            msg = f"Aggregator '{name}' not found"
            raise KeyError(msg)
        return self._aggregators[name]

    def list_enabled(self) -> list[Aggregator]:
        """List all enabled aggregators.

        Returns:
            List of enabled aggregators
        """
        return [agg for agg in self._aggregators.values() if agg.enabled]

    def enable(self, name: str) -> None:
        """Enable an aggregator.

        Args:
            name: Name of the aggregator
        """
        self.get(name).enabled = True

    def disable(self, name: str) -> None:
        """Disable an aggregator.

        Args:
            name: Name of the aggregator
        """
        self.get(name).enabled = False


def create_default_registry() -> AnalyticsRegistry:
    """Create a registry with all default aggregators.

    Returns:
        Configured AnalyticsRegistry
    """
    from streamsight.analytics.data_quality import analyze_data_quality
    from streamsight.analytics.geography import analyze_geography
    from streamsight.analytics.products import analyze_products
    from streamsight.analytics.returns import analyze_returns
    from streamsight.analytics.revenue import analyze_revenue
    from streamsight.analytics.anomaly import detect_anomalies

    registry = AnalyticsRegistry()

    # Register core analytics
    registry.register(
        name="revenue",
        function=analyze_revenue,
        description="Revenue aggregation (gross, net, daily, monthly)",
        enabled=True,
        requires_config=False,
    )

    registry.register(
        name="geography",
        function=analyze_geography,
        description="Geographic performance analysis by country",
        enabled=True,
        requires_config=False,
    )

    registry.register(
        name="products",
        function=analyze_products,
        description="Product performance and Top-K analysis",
        enabled=True,
        requires_config=True,  # Requires top_k parameter
    )

    registry.register(
        name="returns",
        function=analyze_returns,
        description="Returns and refund analysis",
        enabled=True,
        requires_config=False,
    )

    registry.register(
        name="data_quality",
        function=analyze_data_quality,
        description="Data quality metrics and completeness",
        enabled=True,
        requires_config=False,
    )

    registry.register(
        name="anomaly",
        function=detect_anomalies,
        description="Anomaly detection using Z-score",
        enabled=True,
        requires_config=True,  # Requires threshold parameter
    )

    return registry

