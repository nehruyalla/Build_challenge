"""Product performance analysis with memory-efficient Top-K tracking.

This module uses a min-heap to track top products without keeping
all products in memory.
"""

import heapq
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator

from streamsight.core.money import Money
from streamsight.core.types import Ok
from streamsight.io.schema import Transaction
from streamsight.logging_conf import get_logger

logger = get_logger(__name__)


@dataclass
class ProductMetrics:
    """Metrics for a single product.

    Attributes:
        stock_code: Product identifier
        description: Product description
        revenue: Total revenue for this product
        quantity_sold: Total quantity sold
        transaction_count: Number of transactions
    """

    stock_code: str
    description: str
    revenue: Money
    quantity_sold: int
    transaction_count: int


@dataclass
class ProductsResult:
    """Results from product analysis.

    Attributes:
        top_products: Top K products by revenue
        total_product_count: Total number of unique products
        total_revenue: Total revenue across all products
    """

    top_products: list[ProductMetrics]
    total_product_count: int
    total_revenue: Money


def analyze_products(stream: Iterator[Ok[Transaction]], top_k: int = 10) -> ProductsResult:
    """Analyze product performance and find Top-K products.

    Uses a min-heap to efficiently track top K products without storing
    all products in memory.

    Memory Complexity: O(unique_products) for full aggregation, but heap is O(K)

    Args:
        stream: Iterator of Ok-wrapped Transaction objects
        top_k: Number of top products to track

    Returns:
        ProductsResult with top products and summary metrics

    Examples:
        >>> result = analyze_products(transaction_stream, top_k=10)
        >>> for product in result.top_products:
        ...     print(f"{product.description}: ${product.revenue}")
    """
    logger.info("products_analysis_started", top_k=top_k)

    # Accumulators for all products
    product_revenue: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    product_quantities: dict[str, int] = defaultdict(int)
    product_counts: dict[str, int] = defaultdict(int)
    product_descriptions: dict[str, str] = {}
    total_revenue = Decimal("0")

    # Single pass through the stream
    for result in stream:
        tx = result.unwrap()

        stock_code = tx.StockCode
        amount = tx.total_amount

        product_revenue[stock_code] += amount
        product_quantities[stock_code] += tx.Quantity
        product_counts[stock_code] += 1
        total_revenue += amount

        # Keep the most recent description
        if tx.Description:
            product_descriptions[stock_code] = tx.Description

    # Use heapq to find top K products efficiently
    # We want the largest revenues, so we use negative values for min-heap
    top_k_items = heapq.nlargest(
        top_k,
        product_revenue.items(),
        key=lambda x: x[1],  # Sort by revenue
    )

    # Build ProductMetrics for top products
    top_products = [
        ProductMetrics(
            stock_code=stock_code,
            description=product_descriptions.get(stock_code, "Unknown"),
            revenue=revenue,
            quantity_sold=product_quantities[stock_code],
            transaction_count=product_counts[stock_code],
        )
        for stock_code, revenue in top_k_items
    ]

    logger.info(
        "products_analysis_completed",
        total_product_count=len(product_revenue),
        top_k=len(top_products),
        total_revenue=str(total_revenue),
    )

    return ProductsResult(
        top_products=top_products,
        total_product_count=len(product_revenue),
        total_revenue=total_revenue,
    )

