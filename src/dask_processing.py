#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
STAGE 2 - DATA CLEANING & DESCRIPTIVE STATS (Dask)
═══════════════════════════════════════════════════════════════════════════

Dask is used here for out-of-core / parallel data *processing*:

  1. Reads ALL raw CSV part-files at once with a single glob pattern
     (`dask.dataframe.read_csv`), splitting them into partitions.
  2. Cleans the data with a lazily-built task graph:
       - drop rows with invalid price / quantity / total_amount,
       - fill the remaining missing (NULL) values with defaults,
       - recompute `total_amount` and add a `revenue` column,
       - drop duplicate transaction_ids.
  3. Computes descriptive statistics (count, mean, std, min, max,
     median & quartiles) - all evaluated in parallel on `.compute()`.
  4. Writes the cleaned dataset to partitioned Parquet (columnar, compressed)
     which the Spark stage can read very quickly.

Nothing actually runs until `.compute()` / `.to_parquet()` is called - this
is Dask's lazy evaluation, the same idea Spark uses.

Run:
    python -m src.dask_processing
═══════════════════════════════════════════════════════════════════════════
"""

import shutil
import time

import dask
import dask.dataframe as dd
import pandas as pd

from config import (
    data_config, dask_config,
    RAW_DATA_DIR, CLEANED_PARQUET_DIR, DASK_STATS_DIR,
)
from logger_config import setup_logger

logger = setup_logger(__name__, log_file="dask_processing.log")

# Numeric columns we describe statistically.
NUMERIC_COLS = ["price", "quantity", "total_amount", "revenue"]

# Explicit dtypes so Dask does not mis-infer columns that contain NULLs.
READ_DTYPES = {
    "transaction_id": "object",
    "user_id": "object",
    "user_name": "object",
    "user_email": "object",
    "user_age": "float64",
    "user_country": "object",
    "user_city": "object",
    "product_name": "object",
    "product_category": "object",
    "price": "float64",
    "quantity": "float64",
    "total_amount": "float64",
    "payment_method": "object",
    "order_status": "object",
    "year": "float64",
    "month": "float64",
    "day": "float64",
    "hour": "float64",
}


def run_dask_processing() -> dict:
    """Clean the raw data with Dask and compute descriptive statistics."""
    logger.info("=" * 70)
    logger.info("STAGE 2 - CLEANING & DESCRIPTIVE STATS (Dask)")
    logger.info("=" * 70)

    # Use the multiprocessing scheduler for real parallelism across cores.
    dask.config.set(scheduler="processes", num_workers=dask_config.N_WORKERS)
    logger.info(f"Dask scheduler: processes, workers={dask_config.N_WORKERS}")

    stats: dict = {}
    t_total = time.time()

    # ── 1) Read all raw CSV files with one glob pattern ──────────────────
    t = time.time()
    pattern = str(RAW_DATA_DIR / "transactions_part_*.csv")
    ddf = dd.read_csv(
        pattern,
        dtype=READ_DTYPES,
        blocksize=dask_config.BLOCKSIZE,
        parse_dates=["timestamp"],
        assume_missing=True,
    )
    n_partitions = ddf.npartitions
    logger.info(f"Built lazy graph over {n_partitions} partitions ({time.time() - t:.2f}s)")
    stats["partitions"] = n_partitions

    # ── 2) Clean the data (lazy transformations) ─────────────────────────
    # Drop invalid rows: null or non-positive price / quantity / total_amount.
    cleaned = ddf[
        (ddf["price"] > 0)
        & (ddf["quantity"] > 0)
        & (ddf["total_amount"] > 0)
    ]

    # Fill remaining missing categorical/user values with sensible defaults.
    fill_values = {
        "user_id": "unknown_user",
        "user_name": "Unknown User",
        "user_email": "unknown@example.com",
        "user_age": 0,
        "user_country": "Unknown",
        "user_city": "Unknown",
        "product_name": "Unknown Product",
        "product_category": "Uncategorized",
        "payment_method": "Unknown",
        "order_status": "Unknown",
    }
    cleaned = cleaned.fillna(fill_values)

    # Recompute total_amount for consistency and add an explicit `revenue`
    # column (revenue == money brought in by the transaction).
    cleaned["total_amount"] = (cleaned["price"] * cleaned["quantity"]).round(2)
    cleaned["revenue"] = cleaned["total_amount"]

    # Remove duplicate transactions.
    cleaned = cleaned.drop_duplicates(subset=["transaction_id"])

    # ── 3) Build ALL descriptive aggregations lazily, compute in ONE pass ─
    # This is the key Dask idea: we declare every statistic we want, then call
    # a single dask.compute() so the whole task graph is optimised and the
    # data is read/processed only once, in parallel across the partitions.
    t = time.time()
    lazy: dict = {
        "raw_count": ddf.shape[0],
        "clean_count": cleaned.shape[0],
        "by_category": (
            cleaned.groupby("product_category")["revenue"].agg(["sum", "count", "mean"])
        ),
    }
    for col in NUMERIC_COLS:
        lazy[f"{col}__count"] = cleaned[col].count()
        lazy[f"{col}__mean"] = cleaned[col].mean()
        lazy[f"{col}__std"] = cleaned[col].std()
        lazy[f"{col}__min"] = cleaned[col].min()
        lazy[f"{col}__max"] = cleaned[col].max()
        lazy[f"{col}__q"] = cleaned[col].quantile([0.25, 0.5, 0.75])

    (res,) = dask.compute(lazy)  # single, optimised, parallel execution
    logger.info(f"All descriptive aggregations computed in ONE pass "
                f"({time.time() - t:.2f}s)")

    raw_count = int(res["raw_count"])
    clean_count = int(res["clean_count"])
    stats["raw_count"] = raw_count
    stats["clean_count"] = clean_count
    stats["removed_count"] = raw_count - clean_count
    logger.info(f"Raw rows: {raw_count:,} | cleaned rows: {clean_count:,} "
                f"(removed {raw_count - clean_count:,})")

    desc_rows = []
    for col in NUMERIC_COLS:
        q = res[f"{col}__q"]
        row = {
            "metric": col,
            "count": int(res[f"{col}__count"]),
            "mean": float(res[f"{col}__mean"]),
            "std": float(res[f"{col}__std"]),
            "min": float(res[f"{col}__min"]),
            "q25": float(q.loc[0.25]),
            "median": float(q.loc[0.50]),
            "q75": float(q.loc[0.75]),
            "max": float(res[f"{col}__max"]),
        }
        desc_rows.append(row)
        logger.info(f"   {col:<13} mean={row['mean']:.2f} "
                    f"median={row['median']:.2f} std={row['std']:.2f}")

    pd.DataFrame(desc_rows).to_csv(
        DASK_STATS_DIR / "descriptive_statistics.csv", index=False
    )
    stats["descriptive_statistics"] = desc_rows

    by_cat = res["by_category"].sort_values("sum", ascending=False).reset_index()
    by_cat.columns = ["product_category", "total_revenue", "orders_count", "avg_revenue"]
    by_cat.to_csv(DASK_STATS_DIR / "revenue_by_category.csv", index=False)
    stats["revenue_by_category"] = by_cat.to_dict("records")

    # ── 4) Write cleaned data to partitioned Parquet ─────────────────────
    t = time.time()
    if CLEANED_PARQUET_DIR.exists():
        shutil.rmtree(CLEANED_PARQUET_DIR)
    CLEANED_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(
        str(CLEANED_PARQUET_DIR),
        engine="pyarrow",
        compression="snappy",
        write_index=False,
        # Spark 3.5 cannot read nanosecond Parquet timestamps; write them as
        # microseconds so the Spark stage can load the file without errors.
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    logger.info(f"Cleaned data written to Parquet in {time.time() - t:.2f}s "
                f"-> {CLEANED_PARQUET_DIR}")

    stats["elapsed_seconds"] = round(time.time() - t_total, 2)
    logger.info("-" * 70)
    logger.info(f"Dask stage completed in {stats['elapsed_seconds']:.2f}s")
    logger.info("=" * 70)
    return stats


if __name__ == "__main__":
    run_dask_processing()
