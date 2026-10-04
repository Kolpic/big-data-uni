#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
STAGE 1 - SYNTHETIC DATA GENERATION (Faker)
═══════════════════════════════════════════════════════════════════════════

Generates a large e-commerce transaction dataset and writes it to several
CSV files inside `data/raw/` (simulating a data lake / HDFS with many
part-files).  The data is intentionally "dirty":

  * ~NULL_RATE of the fields are written as empty (missing values),
  * ~CORRUPTION_RATE of the rows get an invalid price/quantity,

so the later cleaning step (Dask / Spark) has something real to do.

Each record describes one purchase:
    transaction_id, user_id, user_name, user_email, user_age,
    user_country, user_city, product_name, product_category,
    price, quantity, total_amount, payment_method, order_status,
    timestamp, year, month, day, hour

Run:
    python -m src.generator
═══════════════════════════════════════════════════════════════════════════
"""

import csv
import json
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
from faker import Faker

from config import data_config as cfg, RAW_DATA_DIR, METADATA_DIR
from logger_config import setup_logger

# ═══════════════════════════════════════════════════════════════════════════
# INITIALISATION
# ═══════════════════════════════════════════════════════════════════════════

logger = setup_logger(__name__, log_file="generator.log")

# Reproducibility: seed every random source.
fake = Faker(cfg.LOCALE)
Faker.seed(cfg.SEED)
random.seed(cfg.SEED)
np.random.seed(cfg.SEED)

# Reference catalogues (kept small so group-by / Top-N analysis is meaningful).
PRODUCT_CATALOG = [
    # (product_name, category, base_price)
    ("Laptop Pro 15", "Electronics", 1299.99),
    ("Smartphone X", "Electronics", 899.99),
    ("Wireless Headphones", "Electronics", 149.99),
    ("Smart Watch", "Electronics", 249.99),
    ("Bluetooth Speaker", "Electronics", 79.99),
    ("Running Shoes", "Sports", 119.99),
    ("Yoga Mat", "Sports", 29.99),
    ("Mountain Bike", "Sports", 549.99),
    ("Coffee Maker", "Home & Garden", 89.99),
    ("Robot Vacuum", "Home & Garden", 299.99),
    ("Desk Lamp", "Home & Garden", 34.99),
    ("Air Fryer", "Home & Garden", 129.99),
    ('Novel "The Story"', "Books", 19.99),
    ("Cookbook Deluxe", "Books", 39.99),
    ("Backpack", "Clothing", 59.99),
    ("Winter Jacket", "Clothing", 199.99),
    ("Action Camera", "Electronics", 329.99),
    ("Drone Mini", "Electronics", 459.99),
    ("Board Game", "Toys", 44.99),
    ("Building Blocks Set", "Toys", 69.99),
]

PAYMENT_METHODS = ["Credit Card", "PayPal", "Bank Transfer", "Cash", "Crypto"]
ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]

# A fixed pool of users (so a user can appear in many transactions).
logger.info(f"Building a pool of {cfg.NUM_USERS:,} users...")
USER_POOL = [
    {
        "user_id": fake.uuid4(),
        "user_name": fake.name(),
        "user_email": fake.email(),
        "user_age": random.randint(18, 80),
        "user_country": fake.country(),
        "user_city": fake.city(),
    }
    for _ in range(cfg.NUM_USERS)
]


CSV_FIELDS = [
    "transaction_id", "user_id", "user_name", "user_email", "user_age",
    "user_country", "user_city", "product_name", "product_category",
    "price", "quantity", "total_amount", "payment_method", "order_status",
    "timestamp", "year", "month", "day", "hour",
]


def _maybe_null(value: Any) -> Any:
    """Return the value, or None with probability NULL_RATE (missing value)."""
    if random.random() < cfg.NULL_RATE:
        return None
    return value


def generate_record(record_id: int, start_date: datetime) -> Dict[str, Any]:
    """Generate a single transaction record."""
    user = random.choice(USER_POOL)
    product_name, category, base_price = random.choice(PRODUCT_CATALOG)

    # Price varies a little around the product's base price.
    price = round(base_price * random.uniform(0.85, 1.15), 2)
    quantity = random.randint(1, 10)

    # Inject corrupt rows: negative price or quantity (to be filtered later).
    if random.random() < cfg.CORRUPTION_RATE:
        if random.random() < 0.5:
            price = -price
        else:
            quantity = -quantity

    total_amount = round(price * quantity, 2)

    # Random timestamp within the configured history window.
    delta = timedelta(
        days=random.randint(0, cfg.HISTORY_DAYS),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    ts = start_date - delta

    return {
        "transaction_id": f"TXN_{record_id:010d}",
        "user_id": _maybe_null(user["user_id"]),
        "user_name": _maybe_null(user["user_name"]),
        "user_email": _maybe_null(user["user_email"]),
        "user_age": _maybe_null(user["user_age"]),
        "user_country": _maybe_null(user["user_country"]),
        "user_city": _maybe_null(user["user_city"]),
        "product_name": _maybe_null(product_name),
        "product_category": _maybe_null(category),
        "price": _maybe_null(price),
        "quantity": _maybe_null(quantity),
        "total_amount": _maybe_null(total_amount),
        "payment_method": _maybe_null(random.choice(PAYMENT_METHODS)),
        "order_status": _maybe_null(random.choice(ORDER_STATUSES)),
        "timestamp": ts.isoformat(timespec="seconds"),
        "year": ts.year,
        "month": ts.month,
        "day": ts.day,
        "hour": ts.hour,
    }


def generate_dataset() -> dict:
    """
    Generate the full dataset across several CSV files.

    Returns:
        A dict with summary information (records, files, elapsed time, size).
    """
    logger.info("=" * 70)
    logger.info("STAGE 1 - DATA GENERATION (Faker)")
    logger.info("=" * 70)
    logger.info(f"Total records     : {cfg.TOTAL_RECORDS:,}")
    logger.info(f"Number of files   : {cfg.NUM_FILES}")
    logger.info(f"Records per file  : {cfg.RECORDS_PER_FILE:,}")
    logger.info(f"NULL rate         : {cfg.NULL_RATE * 100:.1f}%")
    logger.info(f"Corruption rate   : {cfg.CORRUPTION_RATE * 100:.1f}%")
    logger.info(f"Output directory  : {RAW_DATA_DIR}")
    logger.info("-" * 70)

    start_time = time.time()
    start_date = datetime.now()
    total_generated = 0

    for file_idx in range(cfg.NUM_FILES):
        file_start = time.time()
        filename = f"transactions_part_{file_idx + 1:03d}.csv"
        filepath = RAW_DATA_DIR / filename
        logger.info(f"Generating file {file_idx + 1}/{cfg.NUM_FILES}: {filename}")

        with open(filepath, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for record_idx in range(cfg.RECORDS_PER_FILE):
                global_id = file_idx * cfg.RECORDS_PER_FILE + record_idx + 1
                writer.writerow(generate_record(global_id, start_date))

                if (record_idx + 1) % 100_000 == 0:
                    progress = (record_idx + 1) / cfg.RECORDS_PER_FILE * 100
                    logger.info(f"   progress: {progress:.0f}% ({record_idx + 1:,} rows)")

        total_generated += cfg.RECORDS_PER_FILE
        size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"   done in {time.time() - file_start:.2f}s, size: {size_mb:.2f} MB")

    elapsed = time.time() - start_time
    total_size_mb = sum(f.stat().st_size for f in RAW_DATA_DIR.glob("*.csv")) / (1024 * 1024)

    # Persist dataset metadata (schema + counts) for later stages / the report.
    metadata = {
        "field_names": CSV_FIELDS,
        "total_records": total_generated,
        "num_files": cfg.NUM_FILES,
        "records_per_file": cfg.RECORDS_PER_FILE,
        "null_rate": cfg.NULL_RATE,
        "corruption_rate": cfg.CORRUPTION_RATE,
        "locale": cfg.LOCALE,
        "seed": cfg.SEED,
        "total_size_mb": round(total_size_mb, 2),
        "generation_seconds": round(elapsed, 2),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "description": "Synthetic e-commerce transactions for descriptive big-data analysis",
    }
    with open(METADATA_DIR / "dataset_metadata.json", "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)

    logger.info("-" * 70)
    logger.info("DATA GENERATION COMPLETED")
    logger.info(f"Total records : {total_generated:,}")
    logger.info(f"Total size    : {total_size_mb:.2f} MB ({total_size_mb / 1024:.2f} GB)")
    logger.info(f"Elapsed time  : {elapsed:.2f} s")
    logger.info(f"Metadata      : {METADATA_DIR / 'dataset_metadata.json'}")
    logger.info("=" * 70)

    return metadata


if __name__ == "__main__":
    generate_dataset()
