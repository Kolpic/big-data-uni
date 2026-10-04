#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
STAGE 4 - PARALLEL DESCRIPTIVE STATS, MAP-REDUCE STYLE (Ray)
═══════════════════════════════════════════════════════════════════════════

Ray is used for *parallel execution of computations*.  Unlike Dask/Spark
(which parallelise dataframes), Ray parallelises arbitrary Python functions:
we turn a plain "compute stats for one file" function into a remote task with
the `@ray.remote` decorator and run one task per raw CSV file concurrently
across all CPUs - a classic Map -> Reduce pattern:

    MAP     : each worker reads ONE file and returns partial statistics
              (n, sum, sum of squares, min, max, per-category sums).
    REDUCE  : the driver combines the partial results into global statistics
              (mean, std via the sum-of-squares trick, global min/max,
               revenue per category).

A second parallel pass builds a global histogram of the revenue distribution
(each worker bins its own file against shared bin edges, counts are summed).

This is exactly how distributed descriptive analysis works at scale, shown
on a small local Ray cluster.

Run:
    python -m src.ray_parallel
═══════════════════════════════════════════════════════════════════════════
"""

import csv
import math
import time

import numpy as np
import pandas as pd
import ray

from config import ray_config, RAW_DATA_DIR, RAY_STATS_DIR
from logger_config import setup_logger

logger = setup_logger(__name__, log_file="ray_parallel.log")


def _read_clean(path: str) -> pd.DataFrame:
    """Read one raw CSV and keep only valid rows (positive price/qty/amount)."""
    df = pd.read_csv(path, usecols=["product_category", "price", "quantity", "total_amount"])
    df = df[(df["price"] > 0) & (df["quantity"] > 0) & (df["total_amount"] > 0)].copy()
    df["revenue"] = (df["price"] * df["quantity"]).round(2)
    return df


@ray.remote
def partial_stats(path: str) -> dict:
    """MAP task: partial descriptive statistics for a single file."""
    df = _read_clean(path)
    rev = df["revenue"].to_numpy(dtype="float64")
    by_cat = df.groupby("product_category")["revenue"].agg(["sum", "count"])
    return {
        "n": int(rev.size),
        "sum": float(rev.sum()),
        "sumsq": float(np.square(rev).sum()),
        "min": float(rev.min()) if rev.size else math.inf,
        "max": float(rev.max()) if rev.size else -math.inf,
        "cat_sum": by_cat["sum"].to_dict(),
        "cat_count": by_cat["count"].astype(int).to_dict(),
    }


@ray.remote
def partial_histogram(path: str, edges: np.ndarray) -> np.ndarray:
    """MAP task: histogram counts for a single file against shared bin edges."""
    df = _read_clean(path)
    counts, _ = np.histogram(df["revenue"].to_numpy(dtype="float64"), bins=edges)
    return counts


def _save_csv(rows: list, fieldnames: list, filename: str) -> None:
    with open(RAY_STATS_DIR / filename, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_ray_analysis() -> dict:
    """Run the two-pass parallel map-reduce descriptive analysis with Ray."""
    logger.info("=" * 70)
    logger.info("STAGE 4 - PARALLEL DESCRIPTIVE STATS (Ray)")
    logger.info("=" * 70)

    files = sorted(str(f) for f in RAW_DATA_DIR.glob("transactions_part_*.csv"))
    if not files:
        logger.error("No raw CSV files found - run the generator first.")
        return {}

    ray.init(
        num_cpus=ray_config.NUM_CPUS,
        ignore_reinit_error=True,
        include_dashboard=False,
        log_to_driver=False,
        configure_logging=False,
    )
    logger.info(f"Ray initialised with {ray_config.NUM_CPUS} CPUs for "
                f"{len(files)} files")

    stats: dict = {}
    t_total = time.time()

    # ── PASS 1: partial stats per file (MAP), combined on the driver (REDUCE)
    t = time.time()
    partials = ray.get([partial_stats.remote(f) for f in files])
    map_seconds = time.time() - t
    logger.info(f"MAP pass 1 (per-file stats) finished in {map_seconds:.2f}s")

    n = sum(p["n"] for p in partials)
    total = sum(p["sum"] for p in partials)
    sumsq = sum(p["sumsq"] for p in partials)
    gmin = min(p["min"] for p in partials)
    gmax = max(p["max"] for p in partials)
    mean = total / n if n else 0.0
    # Population variance via E[x^2] - E[x]^2, then sample std.
    variance = max(sumsq / n - mean ** 2, 0.0) if n else 0.0
    std = math.sqrt(variance * n / (n - 1)) if n > 1 else 0.0

    # Reduce per-category sums/counts.
    cat_sum: dict = {}
    cat_count: dict = {}
    for p in partials:
        for cat, val in p["cat_sum"].items():
            cat_sum[cat] = cat_sum.get(cat, 0.0) + val
            cat_count[cat] = cat_count.get(cat, 0) + p["cat_count"].get(cat, 0)

    logger.info(f"REDUCE: n={n:,}  mean={mean:.2f}  std={std:.2f}  "
                f"min={gmin:.2f}  max={gmax:.2f}")

    stats["global"] = {
        "count": n, "revenue_sum": total, "mean": mean, "std": std,
        "min": gmin, "max": gmax,
    }
    stats["processed_records"] = n

    # ── PASS 2: global revenue histogram (MAP counts, REDUCE sum) ────────
    t = time.time()
    edges = np.linspace(gmin, gmax, ray_config.HIST_BINS + 1)
    hist_parts = ray.get([partial_histogram.remote(f, edges) for f in files])
    total_hist = np.sum(hist_parts, axis=0)
    hist_seconds = time.time() - t
    logger.info(f"MAP pass 2 (histogram, {ray_config.HIST_BINS} bins) "
                f"finished in {hist_seconds:.2f}s")

    hist_rows = [
        {"bin_left": round(float(edges[i]), 2),
         "bin_right": round(float(edges[i + 1]), 2),
         "count": int(total_hist[i])}
        for i in range(len(total_hist))
    ]
    stats["histogram"] = hist_rows
    _save_csv(hist_rows, ["bin_left", "bin_right", "count"], "revenue_histogram.csv")

    # Save global stats + per-category.
    _save_csv(
        [{"metric": "revenue", **stats["global"]}],
        ["metric", "count", "revenue_sum", "mean", "std", "min", "max"],
        "global_statistics.csv",
    )
    cat_rows = sorted(
        ({"product_category": c, "total_revenue": round(cat_sum[c], 2),
          "orders_count": cat_count[c]} for c in cat_sum),
        key=lambda r: r["total_revenue"], reverse=True,
    )
    stats["revenue_by_category"] = cat_rows
    _save_csv(cat_rows, ["product_category", "total_revenue", "orders_count"],
              "revenue_by_category.csv")

    ray.shutdown()

    stats["map_seconds"] = round(map_seconds, 2)
    stats["histogram_seconds"] = round(hist_seconds, 2)
    stats["elapsed_seconds"] = round(time.time() - t_total, 2)
    stats["num_cpus"] = ray_config.NUM_CPUS
    logger.info("-" * 70)
    logger.info(f"Ray stage completed in {stats['elapsed_seconds']:.2f}s")
    logger.info(f"Ray outputs written to: {RAY_STATS_DIR}")
    logger.info("=" * 70)
    return stats


if __name__ == "__main__":
    run_ray_analysis()
