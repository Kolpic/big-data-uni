#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
MAIN ORCHESTRATOR - Big Data Descriptive Analysis (Variant 1)
═══════════════════════════════════════════════════════════════════════════

Runs the whole pipeline end to end and measures the execution time of every
main operation (required by the assignment):

    1. generate   - Faker        -> raw CSV files
    2. dask        - Dask         -> cleaned Parquet + descriptive stats
    3. spark       - PySpark      -> aggregations (the descriptive analysis)
    4. ray         - Ray          -> parallel map-reduce statistics
    5. visualize   - matplotlib   -> 6 PNG charts
    6. report      - .txt + .html -> automatic report with conclusions & timings

Usage:
    python main.py                 # run every stage
    python main.py --skip-generate # reuse existing raw data
    python main.py --only spark     # run a single stage (generate first!)

Configuration (dataset size, etc.) lives in config.py / .env.
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import time

from logger_config import setup_logger

logger = setup_logger("main", log_file="pipeline.log")

STAGES = ["generate", "dask", "spark", "ray", "visualize", "report"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Big Data descriptive-analysis pipeline")
    parser.add_argument("--skip-generate", action="store_true",
                        help="reuse existing raw data (skip stage 1)")
    parser.add_argument("--only", choices=STAGES,
                        help="run only one stage (data must already exist)")
    args = parser.parse_args()

    results: dict = {}
    pipeline_start = time.time()

    logger.info("#" * 70)
    logger.info("BIG DATA PROJECT - DESCRIPTIVE ANALYSIS PIPELINE")
    logger.info("#" * 70)

    def should_run(stage: str) -> bool:
        if args.only:
            return stage == args.only
        if stage == "generate" and args.skip_generate:
            return False
        return True

    # ── 1) Generate ──────────────────────────────────────────────────────
    if should_run("generate"):
        from src.generator import generate_dataset
        results["generation"] = generate_dataset()

    # ── 2) Dask processing ───────────────────────────────────────────────
    if should_run("dask"):
        from src.dask_processing import run_dask_processing
        results["dask"] = run_dask_processing()

    # ── 3) Spark descriptive analysis ────────────────────────────────────
    if should_run("spark"):
        from src.spark_descriptive import run_spark_analysis
        results["spark"] = run_spark_analysis()

    # ── 4) Ray parallel statistics ───────────────────────────────────────
    if should_run("ray"):
        from src.ray_parallel import run_ray_analysis
        results["ray"] = run_ray_analysis()

    # ── 5) Visualization ─────────────────────────────────────────────────
    if should_run("visualize"):
        from src.visualize import run_visualizations
        results["visualize"] = run_visualizations()

    # ── 6) Report ────────────────────────────────────────────────────────
    if should_run("report"):
        from src.report import build_report
        results["report"] = build_report(results)

    total = time.time() - pipeline_start
    logger.info("#" * 70)
    logger.info(f"PIPELINE FINISHED in {total:.2f} s ({total / 60:.1f} min)")
    if results.get("report"):
        logger.info(f"Report (txt) : {results['report']['txt']}")
        logger.info(f"Report (html): {results['report']['html']}")
    logger.info("#" * 70)


if __name__ == "__main__":
    main()
