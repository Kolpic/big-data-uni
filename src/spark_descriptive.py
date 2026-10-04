#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
STAGE 3 - DISTRIBUTED DESCRIPTIVE ANALYSIS (PySpark)
═══════════════════════════════════════════════════════════════════════════

This is the core of the project: the *descriptive* analysis
("What happened?") performed with Apache Spark on the cleaned dataset that
the Dask stage produced (Parquet, columnar & compressed).

Spark runs the heavy group-by / aggregation work in parallel across all CPU
cores (local[*]).  We compute:

  * Overall KPIs      - total revenue, avg order value, #orders, unique users.
  * Distribution stats- mean, stddev, min, max, median & quartiles,
                        skewness, kurtosis for the monetary columns.
  * Top-N products    - by total revenue (the "long tail" / Pareto picture).
  * Revenue by category, by city, by month, by hour.
  * Payment-method and order-value-category breakdowns.

Each aggregation is written to a CSV (Excel-friendly) via the plain Python
`csv` module - the same Windows-safe approach used in the course exercises
(avoids the `winutils.exe` requirement of `spark.write.csv`).

Run:
    python -m src.spark_descriptive
═══════════════════════════════════════════════════════════════════════════
"""

import csv
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import (
    spark_config, CLEANED_PARQUET_DIR, RAW_DATA_DIR, SPARK_AGG_DIR,
)
from logger_config import setup_logger

logger = setup_logger(__name__, log_file="spark_descriptive.log")


def create_spark_session() -> SparkSession:
    """Create and configure the Spark session."""
    logger.info("=" * 70)
    logger.info("STAGE 3 - DESCRIPTIVE ANALYSIS (PySpark)")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(spark_config.APP_NAME)
        .master(spark_config.MASTER)
        .config("spark.driver.memory", spark_config.DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", spark_config.SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created (version {spark.version}, "
                f"master {spark.sparkContext.master})")
    return spark


def save_rows_to_csv(rows: list, fieldnames: list, filepath: Path) -> None:
    """Write a list of dict rows to CSV with the plain csv module (Windows-safe)."""
    with open(filepath, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row[k]) for k in fieldnames})


def load_cleaned(spark: SparkSession) -> DataFrame:
    """Load the cleaned Parquet produced by the Dask stage."""
    logger.info("Loading cleaned Parquet dataset...")
    if any(CLEANED_PARQUET_DIR.glob("*.parquet")):
        df = spark.read.parquet(str(CLEANED_PARQUET_DIR))
    else:
        # Fallback: read raw CSVs directly and clean minimally with Spark.
        logger.warning("No cleaned Parquet found - falling back to raw CSVs.")
        df = (
            spark.read.option("header", "true").option("inferSchema", "true")
            .csv([str(f) for f in RAW_DATA_DIR.glob("*.csv")])
            .filter((F.col("price") > 0) & (F.col("quantity") > 0) & (F.col("total_amount") > 0))
            .withColumn("revenue", F.col("total_amount"))
        )
    df = df.cache()
    logger.info(f"Loaded {df.count():,} rows. Schema:")
    df.printSchema()
    return df


def analyze(df: DataFrame) -> dict:
    """Run the descriptive analysis and return a results dict."""
    results: dict = {}

    # ── Derived columns: order-value category & age group ────────────────
    df = df.withColumn(
        "order_value_category",
        F.when(F.col("total_amount") < 50, "Small")
        .when(F.col("total_amount") < 200, "Medium")
        .when(F.col("total_amount") < 500, "Large")
        .otherwise("Very Large"),
    ).withColumn(
        "age_group",
        F.when(F.col("user_age") < 25, "18-24")
        .when(F.col("user_age") < 35, "25-34")
        .when(F.col("user_age") < 50, "35-49")
        .when(F.col("user_age") < 65, "50-64")
        .otherwise("65+"),
    )

    # ── 1) Overall KPIs ──────────────────────────────────────────────────
    logger.info("1) Overall KPIs")
    agg = df.agg(
        F.sum("revenue").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.count("*").alias("total_orders"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("product_name").alias("unique_products"),
        F.sum("quantity").alias("total_items_sold"),
    ).collect()[0].asDict()
    results["kpis"] = agg
    logger.info(f"   total_revenue=${agg['total_revenue']:,.2f}  "
                f"orders={agg['total_orders']:,}  users={agg['unique_users']:,}")

    # ── 2) Distribution statistics (mean/std/min/max/skew/kurtosis) ──────
    logger.info("2) Distribution statistics")
    dist_rows = []
    for col in ["price", "quantity", "total_amount", "revenue"]:
        stat = df.agg(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("std"),
            F.min(col).alias("min"),
            F.max(col).alias("max"),
            F.skewness(col).alias("skewness"),
            F.kurtosis(col).alias("kurtosis"),
        ).collect()[0].asDict()
        # Median + quartiles via approxQuantile (distributed, fast).
        q1, med, q3 = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.001)
        stat.update({"metric": col, "q25": q1, "median": med, "q75": q3})
        dist_rows.append(stat)
        logger.info(f"   {col:<13} mean={stat['mean']:.2f} median={med:.2f} "
                    f"skew={stat['skewness']:.3f}")
    results["distribution_stats"] = dist_rows
    save_rows_to_csv(
        dist_rows,
        ["metric", "mean", "std", "min", "q25", "median", "q75", "max", "skewness", "kurtosis"],
        SPARK_AGG_DIR / "distribution_statistics.csv",
    )

    # ── 3) Top 10 products by revenue ────────────────────────────────────
    logger.info("3) Top products by revenue")
    top_products = (
        df.groupBy("product_name", "product_category")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("orders_count"),
            F.avg("price").alias("avg_price"),
            F.sum("quantity").alias("items_sold"),
        )
        .orderBy(F.desc("total_revenue"))
        .limit(10)
    )
    results["top_products"] = [r.asDict() for r in top_products.collect()]
    save_rows_to_csv(
        results["top_products"],
        ["product_name", "product_category", "total_revenue", "orders_count", "avg_price", "items_sold"],
        SPARK_AGG_DIR / "top_products.csv",
    )

    # ── 4) Revenue by category ───────────────────────────────────────────
    logger.info("4) Revenue by category")
    by_cat = (
        df.groupBy("product_category")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("orders_count"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("items_sold"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    results["revenue_by_category"] = [r.asDict() for r in by_cat.collect()]
    save_rows_to_csv(
        results["revenue_by_category"],
        ["product_category", "total_revenue", "orders_count", "avg_order_value", "items_sold"],
        SPARK_AGG_DIR / "revenue_by_category.csv",
    )

    # ── 5) Revenue by city (Top 15) ──────────────────────────────────────
    logger.info("5) Revenue by city")
    by_city = (
        df.groupBy("user_city")
        .agg(F.sum("revenue").alias("total_revenue"), F.count("*").alias("orders_count"))
        .orderBy(F.desc("total_revenue"))
        .limit(15)
    )
    results["revenue_by_city"] = [r.asDict() for r in by_city.collect()]
    save_rows_to_csv(
        results["revenue_by_city"],
        ["user_city", "total_revenue", "orders_count"],
        SPARK_AGG_DIR / "revenue_by_city.csv",
    )

    # ── 6) Monthly revenue trend ─────────────────────────────────────────
    logger.info("6) Revenue by month")
    monthly = (
        df.groupBy("year", "month")
        .agg(F.sum("revenue").alias("monthly_revenue"), F.count("*").alias("orders_count"))
        .orderBy("year", "month")
    )
    results["monthly"] = [r.asDict() for r in monthly.collect()]
    save_rows_to_csv(
        results["monthly"],
        ["year", "month", "monthly_revenue", "orders_count"],
        SPARK_AGG_DIR / "monthly_revenue.csv",
    )

    # ── 7) Orders by hour of day ─────────────────────────────────────────
    logger.info("7) Orders by hour")
    hourly = (
        df.groupBy("hour")
        .agg(F.count("*").alias("orders_count"), F.sum("revenue").alias("revenue"))
        .orderBy("hour")
    )
    results["hourly"] = [r.asDict() for r in hourly.collect()]
    save_rows_to_csv(
        results["hourly"],
        ["hour", "orders_count", "revenue"],
        SPARK_AGG_DIR / "hourly_orders.csv",
    )

    # ── 8) Payment-method distribution ───────────────────────────────────
    logger.info("8) Payment-method distribution")
    by_pay = (
        df.groupBy("payment_method")
        .agg(F.count("*").alias("orders_count"), F.sum("revenue").alias("total_revenue"))
        .orderBy(F.desc("orders_count"))
    )
    results["payment_methods"] = [r.asDict() for r in by_pay.collect()]
    save_rows_to_csv(
        results["payment_methods"],
        ["payment_method", "orders_count", "total_revenue"],
        SPARK_AGG_DIR / "payment_methods.csv",
    )

    # ── 9) Order-value-category distribution ─────────────────────────────
    logger.info("9) Order-value-category distribution")
    by_ovc = (
        df.groupBy("order_value_category")
        .agg(F.count("*").alias("orders_count"), F.sum("revenue").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
    )
    results["order_value_categories"] = [r.asDict() for r in by_ovc.collect()]
    save_rows_to_csv(
        results["order_value_categories"],
        ["order_value_category", "orders_count", "total_revenue"],
        SPARK_AGG_DIR / "order_value_categories.csv",
    )

    return results


def run_spark_analysis() -> dict:
    """Execute the full Spark descriptive-analysis pipeline."""
    t_total = time.time()
    spark = None
    try:
        spark = create_spark_session()
        df = load_cleaned(spark)
        results = analyze(df)
        results["elapsed_seconds"] = round(time.time() - t_total, 2)
        logger.info("-" * 70)
        logger.info(f"Spark stage completed in {results['elapsed_seconds']:.2f}s")
        logger.info(f"Aggregated CSVs written to: {SPARK_AGG_DIR}")
        logger.info("=" * 70)
        return results
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    run_spark_analysis()
