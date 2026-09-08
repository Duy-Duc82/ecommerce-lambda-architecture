"""Centralised runtime configuration for the Lambda architecture.

Values are read from the environment (``.env``) with safe local defaults, so
the same code runs on a laptop, in Docker and in CI without edits.
"""

import os
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

from config.storage import active_profile

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

MINIO_BUCKETS: dict[str, str] = {
    "bronze": MINIO_BUCKET_BRONZE,
    "silver": MINIO_BUCKET_SILVER,
    "gold": MINIO_BUCKET_GOLD,
}

# "s3a" uses MinIO; "local" writes parquet under DATA_LAKE_LOCAL_ROOT (tests/dev).
# Kept for backward compatibility with docker-compose and scripts/*.ps1; which
# object store an "s3a" lake actually points at is now chosen by
# DATA_LAKE_PROFILE — see config/storage.py.
DATA_LAKE_MODE: str = os.getenv("DATA_LAKE_MODE", "local").lower()
DATA_LAKE_LOCAL_ROOT: Path = Path(
    os.getenv("DATA_LAKE_LOCAL_ROOT", str(_BASE_DIR / "data" / "lakehouse"))
)


def data_lake_uri(zone: str, dataset: str = "") -> str:
    """Return the URI for a medallion-zone dataset (S3A or local file)."""
    zone_name = zone.lower()
    if zone_name not in MINIO_BUCKETS:
        raise ValueError(f"Unknown data-lake zone: {zone}")
    if not active_profile().is_local:
        base = f"s3a://{MINIO_BUCKETS[zone_name]}"
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
# CRAWLER — multi-site product/price snapshots (separate from behavior events;
# a crawler can only observe public catalog/price state, not real user
# view/cart/purchase behavior — see config.schema price-snapshot contract)
# ============================================================
KAFKA_TOPIC_PRICE_SNAPSHOTS: str = os.getenv(
    "KAFKA_TOPIC_PRICE_SNAPSHOTS", "ecommerce_price_snapshots"
)
CRAWL_REQUEST_DELAY_SECONDS: float = float(os.getenv("CRAWL_REQUEST_DELAY_SECONDS", "2.0"))
CRAWL_JITTER_SECONDS: float = float(os.getenv("CRAWL_JITTER_SECONDS", "1.0"))
CRAWL_HTTP_TIMEOUT_SECONDS: float = float(os.getenv("CRAWL_HTTP_TIMEOUT_SECONDS", "10"))
CRAWL_USER_AGENT: str = os.getenv(
    "CRAWL_USER_AGENT", "EcommercePriceResearchBot/0.1 (+thesis project; rate-limited)"
)
# Pages requested per category before moving on. The adapter stops earlier when
# the site reports its last page, so this is a safety ceiling, not a target.
CRAWL_MAX_PAGES: int = int(os.getenv("CRAWL_MAX_PAGES", "50"))
# Comma-separated Tiki category ids, e.g. "1846,1789". The default nine were
# each probed live (2026-08-16): all return 200; seven cap at total=2000
# (50 pages), 1789 has 116 products and 17166 has 307.
TIKI_CATEGORIES: list[str] = [
    c.strip()
    for c in os.getenv(
        "TIKI_CATEGORIES", "1846,8322,1882,1520,931,915,4384,1789,17166"
    ).split(",")
    if c.strip()
]

# ============================================================
# SPEED LAYER — marketplace change detection (Phase 5)
# A rule-version bump changes emitted change identity, so it must come with a
# new checkpoint directory: reusing the old one would silently mix two rule
# versions inside one stream.
# ============================================================
CHANGE_RULE_VERSION: str = os.getenv("CHANGE_RULE_VERSION", "marketplace-change-rules.v1")

KAFKA_MARKETPLACE_SPEED_CONSUMER_GROUP: str = os.getenv(
    "KAFKA_MARKETPLACE_SPEED_CONSUMER_GROUP", "marketplace-speed-v1"
)
SPEED_CHECKPOINT_DIR: Path = Path(
    os.getenv("SPEED_CHECKPOINT_DIR", str(_BASE_DIR / "data" / "checkpoints" / "marketplace_speed_v1"))
)
SPEED_MAX_OFFSETS_PER_TRIGGER: int = int(os.getenv("SPEED_MAX_OFFSETS_PER_TRIGGER", "5000"))
SPEED_TRIGGER_INTERVAL_SECONDS: int = int(os.getenv("SPEED_TRIGGER_INTERVAL_SECONDS", "30"))

# Decimal, never float: these thresholds decide whether a change event exists.
SPEED_LARGE_DROP_ABSOLUTE: Decimal = Decimal(os.getenv("SPEED_LARGE_DROP_ABSOLUTE", "500000"))
SPEED_LARGE_DROP_PERCENT: Decimal = Decimal(os.getenv("SPEED_LARGE_DROP_PERCENT", "15"))
# Softer threshold, used for the freshness/health document only.
SPEED_FRESHNESS_THRESHOLD_MINUTES: int = int(os.getenv("SPEED_FRESHNESS_THRESHOLD_MINUTES", "360"))
# Harder threshold: crossing it emits an OFFER_STALE change.
SPEED_STALE_THRESHOLD_MINUTES: int = int(os.getenv("SPEED_STALE_THRESHOLD_MINUTES", "1440"))

REDIS_MARKETPLACE_NAMESPACE: str = os.getenv("REDIS_MARKETPLACE_NAMESPACE", "rt")
SPEED_RECENT_CHANGES_MAX: int = int(os.getenv("SPEED_RECENT_CHANGES_MAX", "1000"))
SPEED_CHANGE_DOC_TTL_SECONDS: int = int(os.getenv("SPEED_CHANGE_DOC_TTL_SECONDS", "604800"))
SPEED_STALE_SWEEP_LIMIT: int = int(os.getenv("SPEED_STALE_SWEEP_LIMIT", "5000"))

ES_INDEX_MARKETPLACE_OBSERVATIONS: str = os.getenv(
    "ES_INDEX_MARKETPLACE_OBSERVATIONS", "marketplace-observations-v1"
)
ES_INDEX_MARKETPLACE_CHANGES: str = os.getenv(
    "ES_INDEX_MARKETPLACE_CHANGES", "marketplace-changes-v1"
)

# ============================================================
# BATCH TEMPORAL WAREHOUSE — marketplace Gold marts (Phase 6)
# ============================================================
GOLD_RULE_VERSION: str = os.getenv("GOLD_RULE_VERSION", "marketplace-gold-rules.v1")
MARKETPLACE_FRESHNESS_THRESHOLD_MINUTES: int = int(
    os.getenv("MARKETPLACE_FRESHNESS_THRESHOLD_MINUTES", "360")
)
MARKETPLACE_STALE_THRESHOLD_MINUTES: int = int(
    os.getenv("MARKETPLACE_STALE_THRESHOLD_MINUTES", "1440")
)
COUNTER_DELTA_MAX_GAP_MINUTES: int = int(os.getenv("COUNTER_DELTA_MAX_GAP_MINUTES", "2880"))
GOLD_MIN_OBSERVATIONS_PER_DAY: int = int(os.getenv("GOLD_MIN_OBSERVATIONS_PER_DAY", "1"))
GOLD_DEFAULT_WINDOW_DAYS: int = int(os.getenv("GOLD_DEFAULT_WINDOW_DAYS", "30"))
# percentile_approx accuracy; quantile marts must declare this alongside values.
GOLD_QUANTILE_ACCURACY: int = int(os.getenv("GOLD_QUANTILE_ACCURACY", "10000"))
POSTGRES_MARKETPLACE_SCHEMA: str = os.getenv("POSTGRES_MARKETPLACE_SCHEMA", "marketplace_gold")
POSTGRES_MARKETPLACE_STAGING_SCHEMA: str = os.getenv(
    "POSTGRES_MARKETPLACE_STAGING_SCHEMA", "marketplace_gold_staging"
)
MARKETPLACE_DECIMAL_PRECISION: int = int(os.getenv("MARKETPLACE_DECIMAL_PRECISION", "38"))
MARKETPLACE_DECIMAL_SCALE: int = int(os.getenv("MARKETPLACE_DECIMAL_SCALE", "6"))

# ============================================================
# PATHS
# ============================================================
BASE_DIR: Path = _BASE_DIR
CHECKPOINTS_DIR: Path = _BASE_DIR / "data" / "checkpoints"
