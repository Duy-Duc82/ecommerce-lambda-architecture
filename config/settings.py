"""Centralised runtime configuration for the Lambda architecture.

Values are read from the environment (``.env``) with safe local defaults, so
the same code runs on a laptop, in Docker and in CI without edits.
"""

import os
from decimal import Decimal, InvalidOperation
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
# Spark 4 is built on Scala 2.13 (pyspark is pinned to 4.x, PROGRESS §6), and
# the connector must match the runtime's version. Set it empty inside an
# image whose connector jars are already on the classpath.
SPARK_KAFKA_PACKAGE: str = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
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
# The listing API endpoint. Overridable so the offline stub source can stand in
# for the live site; robots.txt is then read from this URL's host (RFC 9309
# scopes it to the host actually fetched), so there is no separate setting.
TIKI_LISTING_URL: str = os.getenv("TIKI_LISTING_URL", "https://tiki.vn/api/personalish/v1/blocks/listings")
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
# PATHS
# ============================================================
BASE_DIR: Path = _BASE_DIR
CHECKPOINTS_DIR: Path = _BASE_DIR / "data" / "checkpoints"

# ============================================================
# MARKETPLACE SPEED / TEMPORAL WAREHOUSE
# ============================================================
def _env_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name, default)
    try:
        value = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a decimal") from exc
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


MARKETPLACE_CHANGE_RULE_VERSION = os.getenv("MARKETPLACE_CHANGE_RULE_VERSION", "speed-rules.v1")
MARKETPLACE_LARGE_DROP_ABSOLUTE = _env_decimal("MARKETPLACE_LARGE_DROP_ABSOLUTE", "100000")
MARKETPLACE_LARGE_DROP_RELATIVE = _env_decimal("MARKETPLACE_LARGE_DROP_RELATIVE", "0.20")
MARKETPLACE_STALE_AFTER_SECONDS = int(os.getenv("MARKETPLACE_STALE_AFTER_SECONDS", "21600"))
MARKETPLACE_STREAM_WATERMARK = os.getenv("MARKETPLACE_STREAM_WATERMARK", "2 hours")
MARKETPLACE_STREAM_TRIGGER = os.getenv("MARKETPLACE_STREAM_TRIGGER", "30 seconds")
# Where the speed query's checkpoint lives; empty means data/checkpoints/
# marketplace_speed. The version directory is appended either way, so bumping
# MARKETPLACE_STREAM_CHECKPOINT_VERSION always starts a fresh checkpoint.
MARKETPLACE_SPEED_CHECKPOINT_ROOT = os.getenv("MARKETPLACE_SPEED_CHECKPOINT_ROOT", "").strip()
# Shuffle partitions for the speed query. Spark fixes the stateful operator's
# partition count in the checkpoint on its first run: changing this later
# needs a new checkpoint version (and a replay). Spark's default of 200 ran
# 200+ tasks per micro-batch for a few dozen records.
MARKETPLACE_SPEED_SHUFFLE_PARTITIONS = int(os.getenv("MARKETPLACE_SPEED_SHUFFLE_PARTITIONS", "4"))
MARKETPLACE_STREAM_CHECKPOINT_VERSION = os.getenv("MARKETPLACE_STREAM_CHECKPOINT_VERSION", "v1")
# ============================================================
# CRAWL SCHEDULER - frontier cadence, retry and circuit (Phase 3)
# Values are read here and validated inside the policy dataclasses, so a test
# can monkeypatch a setting without tripping an import-time check.
# ============================================================
CRAWL_ACTIVE_CADENCE_MINUTES = int(os.getenv("CRAWL_ACTIVE_CADENCE_MINUTES", "60"))
CRAWL_NORMAL_CADENCE_MINUTES = int(os.getenv("CRAWL_NORMAL_CADENCE_MINUTES", "240"))
CRAWL_COLD_CADENCE_MINUTES = int(os.getenv("CRAWL_COLD_CADENCE_MINUTES", "720"))
CRAWL_LEASE_SECONDS = int(os.getenv("CRAWL_LEASE_SECONDS", "300"))
CRAWL_MAX_ATTEMPTS = int(os.getenv("CRAWL_MAX_ATTEMPTS", "5"))
CRAWL_RETRY_BASE_SECONDS = int(os.getenv("CRAWL_RETRY_BASE_SECONDS", "30"))
CRAWL_RETRY_MAX_SECONDS = int(os.getenv("CRAWL_RETRY_MAX_SECONDS", "1800"))
CRAWL_RETRY_JITTER_RATIO = Decimal(os.getenv("CRAWL_RETRY_JITTER_RATIO", "0.20"))
CRAWL_CIRCUIT_FAILURE_THRESHOLD = int(os.getenv("CRAWL_CIRCUIT_FAILURE_THRESHOLD", "5"))
CRAWL_CIRCUIT_OPEN_SECONDS = int(os.getenv("CRAWL_CIRCUIT_OPEN_SECONDS", "900"))
CRAWL_WORKER_POLL_SECONDS = int(os.getenv("CRAWL_WORKER_POLL_SECONDS", "5"))
CRAWL_WORKER_BATCH_SIZE = int(os.getenv("CRAWL_WORKER_BATCH_SIZE", "10"))
# Phase 8 crawl service. An empty worker ID means "<hostname>:<pid>". The idle
# wait applies only after a cycle that leased nothing; a busy worker waits
# CRAWL_WORKER_POLL_SECONDS.
CRAWL_SERVICE_WORKER_ID = os.getenv("CRAWL_SERVICE_WORKER_ID", "").strip()
CRAWL_SERVICE_IDLE_SECONDS = int(os.getenv("CRAWL_SERVICE_IDLE_SECONDS", "30"))
# Touched once per loop iteration by the Python services; their container
# healthcheck fails when it goes stale. Empty disables it (tests, host runs).
SERVICE_HEARTBEAT_FILE = os.getenv("SERVICE_HEARTBEAT_FILE", "")

KAFKA_CHANGE_ACK_TIMEOUT_SECONDS = int(os.getenv("KAFKA_CHANGE_ACK_TIMEOUT_SECONDS", "30"))
ES_INDEX_MARKETPLACE_CHANGES = os.getenv("ES_INDEX_MARKETPLACE_CHANGES", "marketplace-changes-v1")
ES_INDEX_MARKETPLACE_OFFERS = os.getenv("ES_INDEX_MARKETPLACE_OFFERS", "marketplace-offers-current-v1")
REDIS_MARKETPLACE_OFFER_TTL_SECONDS = int(os.getenv("REDIS_MARKETPLACE_OFFER_TTL_SECONDS", "86400"))
REDIS_MARKETPLACE_RECENT_CHANGES_MAX = int(os.getenv("REDIS_MARKETPLACE_RECENT_CHANGES_MAX", "5000"))
MARKETPLACE_SPEED_QUERY_NAME = os.getenv("MARKETPLACE_SPEED_QUERY_NAME", "marketplace-speed-v1")

# Phase 8 WP8. Four operational indices, written by ops/es_projector.py, and
# read by the Kibana source-health dashboard. They hold operational state
# only: no raw body, no cookie, no header — the same rule as the audit tables.
ES_INDEX_MARKETPLACE_SOURCE_HEALTH = os.getenv("ES_INDEX_MARKETPLACE_SOURCE_HEALTH", "marketplace-source-health-v1")
ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS = os.getenv("ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS", "marketplace-crawl-attempts-v1")
ES_INDEX_MARKETPLACE_SPEED_BATCHES = os.getenv("ES_INDEX_MARKETPLACE_SPEED_BATCHES", "marketplace-speed-batches-v1")
ES_INDEX_MARKETPLACE_DLQ = os.getenv("ES_INDEX_MARKETPLACE_DLQ", "marketplace-dlq-v1")
# The projector re-reads the last OVERLAP seconds of settled rows every pass,
# so a row that was UPDATEd after it was first projected, or a pass that died
# halfway, is repaired by the next pass. The overlap must exceed the interval,
# or a gap opens between two passes that nothing ever re-reads.
MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS = int(os.getenv("MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS", "30"))
MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS = int(os.getenv("MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS", "900"))
KAFKA_DLQ_PROJECTOR_GROUP = os.getenv("KAFKA_DLQ_PROJECTOR_GROUP", "marketplace-dlq-projector-v1")

MARKETPLACE_SILVER_DATASET = os.getenv("MARKETPLACE_SILVER_DATASET", "marketplace/offer_observations")
MARKETPLACE_GOLD_DATASET = os.getenv("MARKETPLACE_GOLD_DATASET", "marketplace")
MARKETPLACE_FRESHNESS_SECONDS = int(os.getenv("MARKETPLACE_FRESHNESS_SECONDS", "21600"))
MARKETPLACE_FRESHNESS_RULE_VERSION = os.getenv("MARKETPLACE_FRESHNESS_RULE_VERSION", "freshness-rules.v1")
MARKETPLACE_COUNTER_RULE_VERSION = os.getenv("MARKETPLACE_COUNTER_RULE_VERSION", "counter-rules.v1")
MARKETPLACE_COUNTER_MAX_GAP_SECONDS = int(os.getenv("MARKETPLACE_COUNTER_MAX_GAP_SECONDS", "86400"))
MARKETPLACE_PERCENTILE_ACCURACY = int(os.getenv("MARKETPLACE_PERCENTILE_ACCURACY", "10000"))
MARKETPLACE_BATCH_SHUFFLE_PARTITIONS = int(os.getenv("MARKETPLACE_BATCH_SHUFFLE_PARTITIONS", "8"))
MARKETPLACE_BATCH_APP_NAME = os.getenv("MARKETPLACE_BATCH_APP_NAME", "MarketplaceTemporalWarehouse")
# Phase 8 batch scheduler. Each window closes on an interval boundary and is
# cut only once the lag has passed, which leaves crawl runs time to settle
# before Phase 7 check 8 reconciles them. The key names the PostgreSQL
# advisory lock that keeps two batch runs from overlapping.
MARKETPLACE_BATCH_INTERVAL_SECONDS = int(os.getenv("MARKETPLACE_BATCH_INTERVAL_SECONDS", "86400"))
MARKETPLACE_BATCH_AS_OF_LAG_SECONDS = int(os.getenv("MARKETPLACE_BATCH_AS_OF_LAG_SECONDS", "1800"))
MARKETPLACE_BATCH_LOCK_KEY = int(os.getenv("MARKETPLACE_BATCH_LOCK_KEY", "820801"))

# Phase 7 robust price anomaly. The baseline is one offer's own recent observed
# price history, so these tune a per-offer comparison and nothing wider.
# 0.6745 inside the score is the normal consistency constant that puts a MAD
# score on a z-score scale; 3.5 is its conventional companion threshold.
MARKETPLACE_ANOMALY_RULE_VERSION = os.getenv("MARKETPLACE_ANOMALY_RULE_VERSION", "anomaly-rules.v1")
MARKETPLACE_ANOMALY_WINDOW_DAYS = int(os.getenv("MARKETPLACE_ANOMALY_WINDOW_DAYS", "14"))
MARKETPLACE_ANOMALY_MIN_SAMPLES = int(os.getenv("MARKETPLACE_ANOMALY_MIN_SAMPLES", "7"))
MARKETPLACE_ANOMALY_MAD_THRESHOLD = _env_decimal("MARKETPLACE_ANOMALY_MAD_THRESHOLD", "3.5")
MARKETPLACE_ANOMALY_IQR_MULTIPLIER = _env_decimal("MARKETPLACE_ANOMALY_IQR_MULTIPLIER", "1.5")

# Phase 7 quality gate. The tolerance absorbs clock skew between the fetch of
# a response and the observation it yields; anything beyond it is a real
# ordering defect, not jitter. v2: check 6 judges a row against its own fetch,
# and check 8 reconciles only settled crawl runs inside the lookback.
MARKETPLACE_QUALITY_RULE_VERSION = os.getenv("MARKETPLACE_QUALITY_RULE_VERSION", "quality-rules.v2")
MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS = int(os.getenv("MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS", "300"))
# A crawl run is reconciled against Silver only once it has been finished for
# the settle delay, which covers Kafka-to-Silver lag, and only while it is
# inside the lookback. The lookback must exceed the gap between batch runs plus
# the settle delay, or a run could settle and age out between two batches
# without ever being reconciled. It also bounds how long one discrepancy can
# hold publication back.
MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS = int(os.getenv("MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS", "900"))
MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS = int(os.getenv("MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS", "172800"))
# A failure sample exists to point a human at the first few offending rows, so
# it stays small and carries identifiers only.
MARKETPLACE_QUALITY_SAMPLE_LIMIT = int(os.getenv("MARKETPLACE_QUALITY_SAMPLE_LIMIT", "10"))
MARKETPLACE_MANIFEST_SCHEMA_VERSION = os.getenv("MARKETPLACE_MANIFEST_SCHEMA_VERSION", "marketplace-gold-manifest.v1")
MARKETPLACE_ALLOWED_CURRENCIES = tuple(
    sorted({c.strip().upper() for c in os.getenv("MARKETPLACE_ALLOWED_CURRENCIES", "VND,USD").split(",") if c.strip()})
)

# Phase 4 topic settings are kept separate from the legacy behavioral topic.
KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS = os.getenv("KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS", "marketplace.observations.v1")
KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS_DLQ = os.getenv("KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS_DLQ", "marketplace.observations.v1.dlq")
KAFKA_TOPIC_MARKETPLACE_CHANGES = os.getenv("KAFKA_TOPIC_MARKETPLACE_CHANGES", "marketplace.changes.v1")
KAFKA_MARKETPLACE_PARTITIONS = int(os.getenv("KAFKA_MARKETPLACE_PARTITIONS", "3"))
KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS = int(os.getenv("KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS", "30"))
KAFKA_SILVER_CONSUMER_GROUP = os.getenv("KAFKA_SILVER_CONSUMER_GROUP", "marketplace-silver-v1")
# Phase 8 Silver sink service. A record that fails is retried at the same
# offset, waiting base, then double, up to max.
MARKETPLACE_SILVER_POLL_TIMEOUT_MS = int(os.getenv("MARKETPLACE_SILVER_POLL_TIMEOUT_MS", "1000"))
MARKETPLACE_SILVER_RETRY_BASE_SECONDS = int(os.getenv("MARKETPLACE_SILVER_RETRY_BASE_SECONDS", "2"))
MARKETPLACE_SILVER_RETRY_MAX_SECONDS = int(os.getenv("MARKETPLACE_SILVER_RETRY_MAX_SECONDS", "60"))


def validate_marketplace_settings() -> None:
    if MARKETPLACE_LARGE_DROP_ABSOLUTE < 0:
        raise ValueError("MARKETPLACE_LARGE_DROP_ABSOLUTE must be non-negative")
    if not Decimal("0") <= MARKETPLACE_LARGE_DROP_RELATIVE <= Decimal("1"):
        raise ValueError("MARKETPLACE_LARGE_DROP_RELATIVE must be between 0 and 1")
    positive = {
        "CRAWL_SERVICE_IDLE_SECONDS": CRAWL_SERVICE_IDLE_SECONDS,
        "MARKETPLACE_SILVER_POLL_TIMEOUT_MS": MARKETPLACE_SILVER_POLL_TIMEOUT_MS,
        "MARKETPLACE_SILVER_RETRY_BASE_SECONDS": MARKETPLACE_SILVER_RETRY_BASE_SECONDS,
        "MARKETPLACE_SILVER_RETRY_MAX_SECONDS": MARKETPLACE_SILVER_RETRY_MAX_SECONDS,
        "MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS": MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS,
        "MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS": MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS,
        "MARKETPLACE_STALE_AFTER_SECONDS": MARKETPLACE_STALE_AFTER_SECONDS,
        "KAFKA_CHANGE_ACK_TIMEOUT_SECONDS": KAFKA_CHANGE_ACK_TIMEOUT_SECONDS,
        "REDIS_MARKETPLACE_OFFER_TTL_SECONDS": REDIS_MARKETPLACE_OFFER_TTL_SECONDS,
        "REDIS_MARKETPLACE_RECENT_CHANGES_MAX": REDIS_MARKETPLACE_RECENT_CHANGES_MAX,
        "MARKETPLACE_FRESHNESS_SECONDS": MARKETPLACE_FRESHNESS_SECONDS,
        "MARKETPLACE_COUNTER_MAX_GAP_SECONDS": MARKETPLACE_COUNTER_MAX_GAP_SECONDS,
        "MARKETPLACE_PERCENTILE_ACCURACY": MARKETPLACE_PERCENTILE_ACCURACY,
        "MARKETPLACE_BATCH_SHUFFLE_PARTITIONS": MARKETPLACE_BATCH_SHUFFLE_PARTITIONS,
        "MARKETPLACE_BATCH_INTERVAL_SECONDS": MARKETPLACE_BATCH_INTERVAL_SECONDS,
        "MARKETPLACE_BATCH_AS_OF_LAG_SECONDS": MARKETPLACE_BATCH_AS_OF_LAG_SECONDS,
        "MARKETPLACE_BATCH_LOCK_KEY": MARKETPLACE_BATCH_LOCK_KEY,
        "MARKETPLACE_SPEED_SHUFFLE_PARTITIONS": MARKETPLACE_SPEED_SHUFFLE_PARTITIONS,
        "KAFKA_MARKETPLACE_PARTITIONS": KAFKA_MARKETPLACE_PARTITIONS,
        "KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS": KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS,
        "MARKETPLACE_ANOMALY_WINDOW_DAYS": MARKETPLACE_ANOMALY_WINDOW_DAYS,
        "MARKETPLACE_ANOMALY_MIN_SAMPLES": MARKETPLACE_ANOMALY_MIN_SAMPLES,
        "MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS": MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS,
        "MARKETPLACE_QUALITY_SAMPLE_LIMIT": MARKETPLACE_QUALITY_SAMPLE_LIMIT,
        "MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS,
        "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS": MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS,
    }
    if any(value <= 0 for value in positive.values()):
        bad = next(name for name, value in positive.items() if value <= 0)
        raise ValueError(f"{bad} must be positive")
    if MARKETPLACE_ANOMALY_MIN_SAMPLES > MARKETPLACE_ANOMALY_WINDOW_DAYS:
        # Otherwise no row could ever reach the minimum and the whole mart would
        # read INSUFFICIENT_HISTORY without anything looking broken.
        raise ValueError("MARKETPLACE_ANOMALY_MIN_SAMPLES cannot exceed MARKETPLACE_ANOMALY_WINDOW_DAYS")
    if MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS <= MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS:
        # An empty window would reconcile nothing and pass every run.
        raise ValueError("MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS must exceed MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS")
    if MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS <= MARKETPLACE_BATCH_INTERVAL_SECONDS + MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS:
        # A crawl run could otherwise settle and age out between two scheduled
        # batches without ever being reconciled.
        raise ValueError("MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS must exceed MARKETPLACE_BATCH_INTERVAL_SECONDS plus MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS")
    if MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS <= MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS:
        # The pass after an outage must re-read everything the outage hid.
        raise ValueError("MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS must exceed MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS")
    if MARKETPLACE_ANOMALY_MAD_THRESHOLD <= 0:
        raise ValueError("MARKETPLACE_ANOMALY_MAD_THRESHOLD must be positive")
    if MARKETPLACE_ANOMALY_IQR_MULTIPLIER <= 0:
        raise ValueError("MARKETPLACE_ANOMALY_IQR_MULTIPLIER must be positive")
    for name in (
        "MARKETPLACE_CHANGE_RULE_VERSION", "MARKETPLACE_STREAM_CHECKPOINT_VERSION",
        "MARKETPLACE_SPEED_QUERY_NAME", "MARKETPLACE_FRESHNESS_RULE_VERSION",
        "MARKETPLACE_COUNTER_RULE_VERSION", "MARKETPLACE_SILVER_DATASET",
        "MARKETPLACE_GOLD_DATASET", "MARKETPLACE_BATCH_APP_NAME",
        "MARKETPLACE_ANOMALY_RULE_VERSION", "MARKETPLACE_QUALITY_RULE_VERSION",
        "MARKETPLACE_MANIFEST_SCHEMA_VERSION", "KAFKA_DLQ_PROJECTOR_GROUP",
        "ES_INDEX_MARKETPLACE_SOURCE_HEALTH", "ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS",
        "ES_INDEX_MARKETPLACE_SPEED_BATCHES", "ES_INDEX_MARKETPLACE_DLQ",
    ):
        if not globals()[name].strip():
            raise ValueError(f"{name} must be non-empty")
    if not MARKETPLACE_ALLOWED_CURRENCIES:
        raise ValueError("MARKETPLACE_ALLOWED_CURRENCIES must list at least one code")
    for code in MARKETPLACE_ALLOWED_CURRENCIES:
        if len(code) != 3 or not code.isalpha():
            raise ValueError(f"MARKETPLACE_ALLOWED_CURRENCIES holds a non ISO-4217 code: {code}")


validate_marketplace_settings()
