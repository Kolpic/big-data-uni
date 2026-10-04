#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
STAGE 5 - VISUALIZATION (matplotlib / seaborn)
═══════════════════════════════════════════════════════════════════════════

Turns the aggregated results (CSV files written by the Spark, Dask and Ray
stages) into publication-quality PNG charts saved under reports/charts/.

The assignment requires at least 4 visualizations; this stage produces 6:

  1. Revenue distribution histogram        (Ray output)   - sequential hue
  2. Top 10 products by revenue            (Spark)        - horizontal bars
  3. Revenue share by category (Pareto)    (Spark)        - long-tail view
  4. Monthly revenue trend                 (Spark)        - line chart
  5. Orders by hour of day                 (Spark)        - bar chart
  6. Order-value-category distribution     (Spark)        - bar chart

Colors use the Okabe-Ito palette, which is colorblind-safe; identity is never
carried by color alone (every chart is also directly labeled).

Run:
    python -m src.visualize
═══════════════════════════════════════════════════════════════════════════
"""

import time

import matplotlib
matplotlib.use("Agg")  # headless backend (no display needed)
import matplotlib.pyplot as plt
import pandas as pd

from config import SPARK_AGG_DIR, RAY_STATS_DIR, CHARTS_DIR
from logger_config import setup_logger

logger = setup_logger(__name__, log_file="visualize.log")

# Okabe-Ito colorblind-safe palette.
BLUE, ORANGE, GREEN, VERMILLION = "#0072B2", "#E69F00", "#009E73", "#D55E00"
PURPLE, SKY, YELLOW = "#CC79A7", "#56B4E9", "#F0E442"
INK, MUTED, GRID = "#1a1a1a", "#555555", "#dddddd"

plt.rcParams.update({
    "figure.dpi": 110,
    "savefig.dpi": 130,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.edgecolor": MUTED,
    "axes.labelcolor": INK,
    "text.color": INK,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "figure.autolayout": True,
})


def _style(ax):
    """Common axis clean-up: light grid behind, no top/right spines."""
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def chart_revenue_histogram() -> str:
    """1) Distribution of transaction revenue (from the Ray map-reduce)."""
    df = pd.read_csv(RAY_STATS_DIR / "revenue_histogram.csv")
    centers = (df["bin_left"] + df["bin_right"]) / 2
    width = (df["bin_right"] - df["bin_left"]).median()

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(centers, df["count"], width=width * 0.95, color=BLUE, edgecolor="white", linewidth=0.3)
    ax.set_title("Distribution of Transaction Revenue")
    ax.set_xlabel("Revenue per transaction ($)")
    ax.set_ylabel("Number of transactions")
    _style(ax)
    out = CHARTS_DIR / "01_revenue_distribution.png"
    fig.savefig(out); plt.close(fig)
    return out.name


def chart_top_products() -> str:
    """2) Top 10 products by total revenue."""
    df = pd.read_csv(SPARK_AGG_DIR / "top_products.csv").head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.barh(df["product_name"], df["total_revenue"] / 1e6, color=GREEN)
    ax.bar_label(bars, fmt="$%.2fM", padding=3, fontsize=9, color=INK)
    ax.set_title("Top 10 Products by Revenue")
    ax.set_xlabel("Total revenue ($ millions)")
    ax.margins(x=0.12)
    _style(ax)
    out = CHARTS_DIR / "02_top_products.png"
    fig.savefig(out); plt.close(fig)
    return out.name


def chart_category_pareto() -> str:
    """3) Revenue share by category (Pareto / long-tail), single axis."""
    df = pd.read_csv(SPARK_AGG_DIR / "revenue_by_category.csv").sort_values(
        "total_revenue", ascending=False
    )
    total = df["total_revenue"].sum()
    share = df["total_revenue"] / total * 100
    cum = share.cumsum()

    fig, ax = plt.subplots(figsize=(10, 5.5))
    bars = ax.bar(df["product_category"], share, color=ORANGE)
    ax.bar_label(bars, labels=[f"{s:.1f}%" for s in share], padding=3, fontsize=9, color=INK)
    # Cumulative share as a line on the SAME percentage axis (no dual axis).
    # The line + legend convey the cumulative curve; per-point numbers are
    # omitted to avoid colliding with the bar labels.
    ax.plot(df["product_category"], cum, color=VERMILLION, marker="o",
            markersize=6, linewidth=2, label="Cumulative share")
    ax.set_title("Revenue Share by Category (Pareto)")
    ax.set_ylabel("Share of total revenue (%)")
    ax.set_ylim(0, 110)
    ax.legend(frameon=False, loc="center right")
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    _style(ax)
    out = CHARTS_DIR / "03_category_pareto.png"
    fig.savefig(out); plt.close(fig)
    return out.name


def chart_monthly_trend() -> str:
    """4) Monthly revenue trend over time (line chart)."""
    df = pd.read_csv(SPARK_AGG_DIR / "monthly_revenue.csv")
    df["period"] = (df["year"].astype(int).astype(str) + "-"
                    + df["month"].astype(int).astype(str).str.zfill(2))
    df = df.sort_values(["year", "month"])
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df["period"], df["monthly_revenue"] / 1e6, color=BLUE,
            marker="o", markersize=7, linewidth=2.2)
    ax.set_title("Monthly Revenue Trend")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue ($ millions)")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    _style(ax)
    out = CHARTS_DIR / "04_monthly_trend.png"
    fig.savefig(out); plt.close(fig)
    return out.name


def chart_orders_by_hour() -> str:
    """5) Number of orders by hour of day (bar chart)."""
    df = pd.read_csv(SPARK_AGG_DIR / "hourly_orders.csv").sort_values("hour")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(df["hour"], df["orders_count"], color=SKY, edgecolor="white", linewidth=0.3)
    ax.set_title("Orders by Hour of Day")
    ax.set_xlabel("Hour (0-23)")
    ax.set_ylabel("Number of orders")
    ax.set_xticks(range(0, 24))
    _style(ax)
    out = CHARTS_DIR / "05_orders_by_hour.png"
    fig.savefig(out); plt.close(fig)
    return out.name


def chart_order_value_categories() -> str:
    """6) Distribution of orders across order-value categories (bar chart)."""
    df = pd.read_csv(SPARK_AGG_DIR / "order_value_categories.csv")
    order = ["Small", "Medium", "Large", "Very Large"]
    df["order_value_category"] = pd.Categorical(df["order_value_category"], order, ordered=True)
    df = df.sort_values("order_value_category")
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(df["order_value_category"], df["orders_count"], color=PURPLE)
    ax.bar_label(bars, fmt="%d", padding=3, fontsize=10, color=INK)
    ax.set_title("Orders by Value Category")
    ax.set_xlabel("Order value category")
    ax.set_ylabel("Number of orders")
    ax.margins(y=0.12)
    _style(ax)
    out = CHARTS_DIR / "06_order_value_categories.png"
    fig.savefig(out); plt.close(fig)
    return out.name


def run_visualizations() -> dict:
    """Generate all charts; return the list of produced PNG file names."""
    logger.info("=" * 70)
    logger.info("STAGE 5 - VISUALIZATION")
    logger.info("=" * 70)
    t = time.time()

    charts = []
    for fn in (
        chart_revenue_histogram,
        chart_top_products,
        chart_category_pareto,
        chart_monthly_trend,
        chart_orders_by_hour,
        chart_order_value_categories,
    ):
        try:
            name = fn()
            charts.append(name)
            logger.info(f"   saved {name}")
        except Exception as exc:  # keep going if one chart fails
            logger.error(f"   chart {fn.__name__} failed: {exc}")

    elapsed = round(time.time() - t, 2)
    logger.info("-" * 70)
    logger.info(f"Visualization completed in {elapsed:.2f}s - {len(charts)} charts")
    logger.info(f"Charts saved to: {CHARTS_DIR}")
    logger.info("=" * 70)
    return {"charts": charts, "elapsed_seconds": elapsed}


if __name__ == "__main__":
    run_visualizations()
