"""Visualization module for creating plots and charts.

This module uses Pandas for data formatting and Matplotlib/Seaborn for plotting.
This is the ONLY place in the codebase where Pandas is used.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from streamsight.analytics.geography import GeographyResult
from streamsight.analytics.products import ProductsResult
from streamsight.analytics.revenue import RevenueResult
from streamsight.config import Config
from streamsight.logging_conf import get_logger
from streamsight.rfm.segmentation import SegmentationResult

logger = get_logger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["font.size"] = 10


def plot_revenue_trend(revenue: RevenueResult, output_path: Path) -> None:
    """Plot daily revenue trend as a line chart.

    Args:
        revenue: Revenue analysis results
        output_path: Path to save the plot
    """
    logger.info("plotting_revenue_trend", output_path=str(output_path))

    # Convert to DataFrame (Pandas allowed here for visualization)
    df = pd.DataFrame(
        [
            {"date": date, "revenue": float(amount)}
            for date, amount in revenue.daily_revenue.items()
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Create plot
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(df["date"], df["revenue"], linewidth=2, color="#2E86AB")
    ax.fill_between(df["date"], df["revenue"], alpha=0.3, color="#2E86AB")

    ax.set_title("Daily Revenue Trend", fontsize=16, fontweight="bold")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Revenue ($)", fontsize=12)
    ax.grid(True, alpha=0.3)

    # Format y-axis as currency
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${x:,.0f}"))

    # Rotate x-axis labels
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("revenue_trend_plotted", output_path=str(output_path))


def plot_country_performance(geography: GeographyResult, output_path: Path) -> None:
    """Plot country performance as a horizontal bar chart.

    Args:
        geography: Geography analysis results
        output_path: Path to save the plot
    """
    logger.info("plotting_country_performance", output_path=str(output_path))

    # Convert to DataFrame and get top 10 countries
    df = pd.DataFrame(
        [
            {"country": country, "revenue": float(revenue)}
            for country, revenue in geography.country_revenue.items()
        ]
    )
    df = df.sort_values("revenue", ascending=False).head(10)

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df["country"], df["revenue"], color="#A23B72")

    ax.set_title("Top 10 Countries by Revenue", fontsize=16, fontweight="bold")
    ax.set_xlabel("Revenue ($)", fontsize=12)
    ax.set_ylabel("Country", fontsize=12)
    ax.grid(True, alpha=0.3, axis="x")

    # Format x-axis as currency
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${x:,.0f}"))

    # Add value labels on bars
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width,
            bar.get_y() + bar.get_height() / 2,
            f"${width:,.0f}",
            ha="left",
            va="center",
            fontsize=9,
            fontweight="bold",
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("country_performance_plotted", output_path=str(output_path))


def plot_top_products(products: ProductsResult, output_path: Path) -> None:
    """Plot top products as a bar chart.

    Args:
        products: Products analysis results
        output_path: Path to save the plot
    """
    logger.info("plotting_top_products", output_path=str(output_path))

    # Convert to DataFrame
    df = pd.DataFrame(
        [
            {
                "description": p.description[:40] + "..."
                if len(p.description) > 40
                else p.description,
                "revenue": float(p.revenue),
            }
            for p in products.top_products
        ]
    )

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df["description"], df["revenue"], color="#F18F01")

    ax.set_title("Top Products by Revenue", fontsize=16, fontweight="bold")
    ax.set_xlabel("Revenue ($)", fontsize=12)
    ax.set_ylabel("Product", fontsize=12)
    ax.grid(True, alpha=0.3, axis="x")

    # Format x-axis as currency
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${x:,.0f}"))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("top_products_plotted", output_path=str(output_path))


def plot_whale_pareto(rfm: SegmentationResult, output_path: Path) -> None:
    """Plot whale customer Pareto chart (cumulative % of customers vs % of revenue).

    This visualizes the concentration of revenue among top customers.

    Args:
        rfm: RFM segmentation results
        output_path: Path to save the plot
    """
    logger.info("plotting_whale_pareto", output_path=str(output_path))

    # Sort customers by monetary value
    sorted_customers = sorted(rfm.rfm_scores, key=lambda x: x.monetary, reverse=True)

    # Calculate cumulative percentages
    total_revenue = sum(c.monetary for c in sorted_customers)
    cumulative_revenue = 0
    data = []

    for i, customer in enumerate(sorted_customers, 1):
        cumulative_revenue += customer.monetary
        customer_pct = (i / len(sorted_customers)) * 100
        revenue_pct = (float(cumulative_revenue) / float(total_revenue)) * 100
        data.append({"customer_pct": customer_pct, "revenue_pct": revenue_pct})

    df = pd.DataFrame(data)

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.plot(df["customer_pct"], df["revenue_pct"], linewidth=2, color="#6A4C93")
    ax.plot([0, 100], [0, 100], "k--", alpha=0.3, label="Perfect Equality")

    # Highlight whale zone
    whale_pct = (100 - rfm.whale_count / rfm.total_customers * 100)
    ax.axvline(x=whale_pct, color="red", linestyle="--", alpha=0.5, label="Whale Threshold")
    ax.fill_between(
        df[df["customer_pct"] >= whale_pct]["customer_pct"],
        df[df["customer_pct"] >= whale_pct]["revenue_pct"],
        alpha=0.2,
        color="red",
    )

    ax.set_title("Customer Revenue Concentration (Pareto Chart)", fontsize=16, fontweight="bold")
    ax.set_xlabel("Cumulative % of Customers", fontsize=12)
    ax.set_ylabel("Cumulative % of Revenue", fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend()

    # Add annotation for whale contribution
    ax.text(
        whale_pct + 2,
        rfm.whale_revenue_share - 5,
        f"Top {100 - whale_pct:.0f}% customers\ngenerate {rfm.whale_revenue_share:.1f}% revenue",
        fontsize=10,
        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("whale_pareto_plotted", output_path=str(output_path))


def create_all_plots(config: Config, results: "PipelineResults") -> None:  # type: ignore[name-defined]
    """Create all visualization plots.

    Args:
        config: Configuration
        results: Pipeline results
    """
    logger.info("creating_all_plots")

    figures_dir = config.figures_dir

    # Revenue trend
    plot_revenue_trend(results.revenue, figures_dir / "revenue_trend.png")

    # Country performance
    plot_country_performance(results.geography, figures_dir / "country_performance.png")

    # Top products
    plot_top_products(results.products, figures_dir / "top_products.png")

    # Whale Pareto (if RFM analysis was run)
    if results.rfm is not None:
        plot_whale_pareto(results.rfm, figures_dir / "whale_pareto.png")

    logger.info("all_plots_created", output_dir=str(figures_dir))

