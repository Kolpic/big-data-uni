"""
Project configuration - Big Data Descriptive Analysis (Variant 1).

Settings are loaded from a `.env` file when present (see `.env.example`),
otherwise sensible defaults are used.  All directories are created
automatically on import so the rest of the code can assume they exist.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load variables from a local .env file, if one exists.
load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════
# Base directories
# ═══════════════════════════════════════════════════════════════════════════
BASE_DIR = Path(__file__).parent

# Raw and processed data
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"                 # Raw CSV files from the generator
METADATA_DIR = DATA_DIR / "metadata"            # Dataset metadata (schema, counts)
PROCESSED_DIR = DATA_DIR / "processed"          # Cleaned data (Parquet) from Dask
CLEANED_PARQUET_DIR = PROCESSED_DIR / "cleaned_parquet"

# Analysis outputs
OUTPUT_DIR = BASE_DIR / "output"
SPARK_AGG_DIR = OUTPUT_DIR / "spark_aggregations"   # Aggregated CSVs from Spark
DASK_STATS_DIR = OUTPUT_DIR / "dask_stats"          # Descriptive stats from Dask
RAY_STATS_DIR = OUTPUT_DIR / "ray_stats"            # Parallel stats from Ray

# Deliverables (what the assignment asks to submit)
REPORTS_DIR = BASE_DIR / "reports"              # Automatic .txt / .html reports
CHARTS_DIR = REPORTS_DIR / "charts"             # Generated PNG visualizations

# Create every directory up-front.
for _dir in [
    RAW_DATA_DIR, METADATA_DIR, PROCESSED_DIR, CLEANED_PARQUET_DIR,
    OUTPUT_DIR, SPARK_AGG_DIR, DASK_STATS_DIR, RAY_STATS_DIR,
    REPORTS_DIR, CHARTS_DIR,
]:
    _dir.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# Data generation settings (Faker)
# ═══════════════════════════════════════════════════════════════════════════
class DataGenConfig:
    """Configuration for the synthetic data generator."""

    # Total number of records to generate.
    # The assignment recommends a minimum of 500,000 records.
    TOTAL_RECORDS: int = int(os.getenv("TOTAL_RECORDS", "1000000"))  # 1 million

    # Split the dataset across several CSV files (simulates a data lake / HDFS).
    NUM_FILES: int = int(os.getenv("NUM_FILES", "10"))

    # Records per file (derived).
    @property
    def RECORDS_PER_FILE(self) -> int:
        return self.TOTAL_RECORDS // self.NUM_FILES

    # Probability that a field is written as NULL (to demonstrate data cleaning).
    NULL_RATE: float = float(os.getenv("NULL_RATE", "0.05"))  # 5%

    # Probability that a whole record is "corrupt" (negative price/quantity),
    # so the cleaning step has something invalid to filter out.
    CORRUPTION_RATE: float = float(os.getenv("CORRUPTION_RATE", "0.02"))  # 2%

    # Faker locale and random seed (reproducibility).
    LOCALE: str = os.getenv("FAKER_LOCALE", "en_US")
    SEED: int = int(os.getenv("RANDOM_SEED", "42"))

    # Number of distinct users/products to draw from (keeps joins/group-bys
    # meaningful instead of every row being unique).
    NUM_USERS: int = int(os.getenv("NUM_USERS", "20000"))

    # Time span of the generated transactions (days back from "now").
    HISTORY_DAYS: int = int(os.getenv("HISTORY_DAYS", "180"))


# ═══════════════════════════════════════════════════════════════════════════
# Spark settings
# ═══════════════════════════════════════════════════════════════════════════
class SparkConfig:
    """Configuration for the Spark session."""

    APP_NAME: str = os.getenv("SPARK_APP_NAME", "BigDataDescriptiveAnalysis")
    MASTER: str = os.getenv("SPARK_MASTER", "local[*]")        # use all cores
    DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    SHUFFLE_PARTITIONS: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))


# ═══════════════════════════════════════════════════════════════════════════
# Dask settings
# ═══════════════════════════════════════════════════════════════════════════
class DaskConfig:
    """Configuration for the Dask local cluster."""

    # Number of worker processes. "0" => let Dask decide (os.cpu_count()).
    N_WORKERS: int = int(os.getenv("DASK_N_WORKERS", "4"))
    THREADS_PER_WORKER: int = int(os.getenv("DASK_THREADS_PER_WORKER", "1"))
    # CSV blocksize for partitioning large files.
    BLOCKSIZE: str = os.getenv("DASK_BLOCKSIZE", "32MB")


# ═══════════════════════════════════════════════════════════════════════════
# Ray settings
# ═══════════════════════════════════════════════════════════════════════════
class RayConfig:
    """Configuration for the Ray runtime."""

    NUM_CPUS: int = int(os.getenv("RAY_NUM_CPUS", "4"))
    # Number of histogram bins used by the parallel distribution analysis.
    HIST_BINS: int = int(os.getenv("RAY_HIST_BINS", "50"))


# Instantiate the configuration objects used across the project.
data_config = DataGenConfig()
spark_config = SparkConfig()
dask_config = DaskConfig()
ray_config = RayConfig()
