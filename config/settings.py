"""Centralised runtime configuration for the Lambda architecture.

Values are read from the environment (``.env``) with safe local defaults, so
the same code runs on a laptop, in Docker and in CI without edits.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

_BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(_BASE_DIR / ".env")

# ============================================================
# KAFKA — one topic for the single canonical event contract
# ============================================================
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_EVENTS: str = os.getenv("KAFKA_TOPIC_EVENTS", "ecommerce_events")

# ============================================================
# SPARK
# ============================================================
SPARK_MASTER_URL: str = os.getenv("SPARK_MASTER_URL", "spark://spark:7077")
SPARK_KAFKA_PACKAGE: str = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

# ============================================================
# MINIO — S3-compatible data lake (authoritative warehouse)
# ============================================================
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_BRONZE: str = os.getenv("MINIO_BUCKET_BRONZE", "ecommerce-bronze")
MINIO_BUCKET_SILVER: str = os.getenv("MINIO_BUCKET_SILVER", "ecommerce-silver")
MINIO_BUCKET_GOLD: str = os.getenv("MINIO_BUCKET_GOLD", "ecommerce-gold")

# "s3a" uses MinIO; "local" writes parquet under DATA_LAKE_LOCAL_ROOT (tests/dev).
DATA_LAKE_MODE: str = os.getenv("DATA_LAKE_MODE", "local").lower()
DATA_LAKE_LOCAL_ROOT: Path = Path(
    os.getenv("DATA_LAKE_LOCAL_ROOT", str(_BASE_DIR / "data" / "lakehouse"))
)


def data_lake_uri(zone: str, dataset: str = "") -> str:
    """Return the URI for a medallion-zone dataset (S3A or local file)."""
    buckets = {
        "bronze": MINIO_BUCKET_BRONZE,
        "silver": MINIO_BUCKET_SILVER,
        "gold": MINIO_BUCKET_GOLD,
    }
    zone_name = zone.lower()
    if zone_name not in buckets:
        raise ValueError(f"Unknown data-lake zone: {zone}")
    if DATA_LAKE_MODE == "s3a":
        base = f"s3a://{buckets[zone_name]}"
    else:
        base = (DATA_LAKE_LOCAL_ROOT / zone_name).resolve().as_uri()
    suffix = dataset.strip("/")
    return f"{base}/{suffix}" if suffix else base


# ============================================================
# POSTGRES — BI serving cache, NOT the system of record
# The lake (MinIO gold) is the authoritative warehouse; PostgreSQL only holds
# compact BI marts + ML outputs so Superset always has data to show.
# Host access uses 5433 (mapped); in-container jobs override POSTGRES_PORT=5432.
# ============================================================
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "admin123")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "data_warehouse")

POSTGRES_URL: str = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ============================================================
# REDIS — realtime KPI serving (speed layer)
# ============================================================
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))

# ============================================================
# ELASTICSEARCH — realtime search/analytics (Kibana source)
# ============================================================
ES_HOST: str = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX_EVENTS: str = os.getenv("ES_INDEX_EVENTS", "ecommerce-events")
ES_INDEX_METRICS: str = os.getenv("ES_INDEX_METRICS", "ecommerce-metrics")

# ============================================================
# PATHS
# ============================================================
BASE_DIR: Path = _BASE_DIR
CHECKPOINTS_DIR: Path = _BASE_DIR / "data" / "checkpoints"
