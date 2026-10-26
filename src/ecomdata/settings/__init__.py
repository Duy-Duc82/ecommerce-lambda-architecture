"""Runtime configuration, one frozen dataclass per concern.

Importing this package reads nothing: no environment variable, no ``.env``
file, no validation. A composition root (``bootstrap.py``, ``service.py``,
``cli.py``, ``__main__.py``, an Airflow DAG) calls :func:`load_settings` once
and hands each component the part it needs; nothing below a composition root
imports this package (``tests/architecture`` checks it).

``load_settings(env)`` takes the mapping to read, so a test builds settings
from a dict instead of patching module attributes. With ``dotenv=True`` the
repository ``.env`` is read as a fallback under the process environment, via
``dotenv_values`` (``os.environ`` is never modified).
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from decimal import Decimal, InvalidOperation
from pathlib import Path

from ecomdata.domain.sources import SOURCES

REPO_ROOT = Path(__file__).resolve().parents[3]

# Nearest AWS/GCP region to Vietnam: a crawl load of many small objects pays
# the round trip on every request.
DEFAULT_REGION = "ap-southeast-1"
STORAGE_PROFILES = ("local", "minio", "s3", "r2", "b2")
ZONES = ("bronze", "silver", "gold")


class SettingsError(ValueError):
    """A configuration value is missing, malformed or inconsistent."""


class _Env:
    """Typed reads from one mapping; every error names the variable."""

    def __init__(self, values: Mapping[str, str]):
        self.values = values

    def text(self, name: str, default: str) -> str:
        value = self.values.get(name)
        return default if value is None else value

    def first(self, *names: str, default: str = "") -> str:
        """First non-empty value among ``names``."""
        for name in names:
            value = self.values.get(name)
            if value:
                return value
        return default

    def integer(self, name: str, default: int) -> int:
        raw = self.text(name, str(default)).strip()
        try:
            return int(raw)
        except ValueError:
            raise SettingsError(f"{name} must be an integer, got {raw!r}") from None

    def number(self, name: str, default: float) -> float:
        raw = self.text(name, str(default)).strip()
        try:
            return float(raw)
        except ValueError:
            raise SettingsError(f"{name} must be a number, got {raw!r}") from None

    def decimal(self, name: str, default: str) -> Decimal:
        raw = self.text(name, default).strip()
        try:
            value = Decimal(raw)
        except (InvalidOperation, ValueError):
            raise SettingsError(f"{name} must be a decimal, got {raw!r}") from None
        if not value.is_finite():
            raise SettingsError(f"{name} must be finite")
        return value

    def flag(self, name: str, default: bool) -> bool:
        raw = self.values.get(name)
        if raw is None or not raw.strip():
            return default
        return raw.strip().lower() in ("1", "true", "yes", "on")

    def items(self, name: str, default: str) -> tuple[str, ...]:
        return tuple(item.strip() for item in self.text(name, default).split(",") if item.strip())


# ---------------------------------------------------------------- storage
@dataclass(frozen=True)
class StorageSettings:
    """Where the lake lives and how to reach it.

    Every layer addresses the lake as ``s3a://`` whoever hosts it, so MinIO,
    AWS S3, Cloudflare R2 and Backblaze B2 differ only in endpoint, TLS and
    bucket addressing. ``profile`` picks one; ``local`` writes files under
    ``local_root`` (tests, development).
    """

    profile: str = "local"
    local_root: Path = REPO_ROOT / "data" / "lakehouse"
    bucket_bronze: str = "ecommerce-bronze"
    bucket_silver: str = "ecommerce-silver"
    bucket_gold: str = "ecommerce-gold"
    endpoint: str | None = None
    use_ssl: bool = True
    path_style_access: bool = False
    region: str | None = None
    access_key: str = ""
    secret_key: str = ""

    @property
    def is_local(self) -> bool:
        return self.profile == "local"

    @property
    def buckets(self) -> dict[str, str]:
        return {"bronze": self.bucket_bronze, "silver": self.bucket_silver, "gold": self.bucket_gold}

    @property
    def scheme(self) -> str:
        return "file" if self.is_local else "s3a"

    def endpoint_url(self) -> str:
        if self.is_local:
            raise SettingsError(f"storage profile {self.profile!r} is local; it has no endpoint")
        host = self.endpoint or f"s3.{self.region or DEFAULT_REGION}.amazonaws.com"
        if host.startswith(("http://", "https://")):
            return host
        return f"{'https' if self.use_ssl else 'http'}://{host}"

    def endpoint_host(self) -> str:
        """Endpoint as bare ``host[:port]``, for SDKs that reject a scheme (minio)."""
        return self.endpoint_url().split("://", 1)[1].rstrip("/")


def _storage(env: _Env) -> StorageSettings:
    # DATA_LAKE_MODE predates profiles and is still set by older env files, so
    # it decides when no profile is named.
    profile = env.first("DATA_LAKE_PROFILE").lower() or (
        "minio" if env.text("DATA_LAKE_MODE", "local").lower() == "s3a" else "local"
    )
    if profile not in STORAGE_PROFILES:
        raise SettingsError(f"unknown DATA_LAKE_PROFILE {profile!r} (expected one of {', '.join(STORAGE_PROFILES)})")
    common = {
        "local_root": Path(env.text("DATA_LAKE_LOCAL_ROOT", str(REPO_ROOT / "data" / "lakehouse"))),
        "bucket_bronze": env.text("MINIO_BUCKET_BRONZE", "ecommerce-bronze"),
        "bucket_silver": env.text("MINIO_BUCKET_SILVER", "ecommerce-silver"),
        "bucket_gold": env.text("MINIO_BUCKET_GOLD", "ecommerce-gold"),
        # DATA_LAKE_* wins, then the AWS names every cloud SDK honours, then
        # the MinIO defaults of the docker stack.
        "access_key": env.first("DATA_LAKE_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY", default="minioadmin"),
        "secret_key": env.first(
            "DATA_LAKE_SECRET_KEY", "AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY", default="minioadmin"
        ),
    }
    region = env.first("DATA_LAKE_REGION", "AWS_REGION", "AWS_DEFAULT_REGION", default=DEFAULT_REGION)
    endpoint = env.first("DATA_LAKE_ENDPOINT")
    if profile == "local":
        return StorageSettings(profile="local", **{**common, "access_key": "", "secret_key": ""})
    if profile == "minio":
        return StorageSettings(
            profile="minio",
            endpoint=endpoint or env.first("MINIO_ENDPOINT", default="localhost:9000"),
            use_ssl=env.flag("MINIO_USE_SSL", False),
            path_style_access=True,
            region=None,
            **common,
        )
    if profile == "s3":
        return StorageSettings(
            profile="s3", endpoint=endpoint or None, use_ssl=True, path_style_access=False, region=region, **common
        )
    # R2 and B2 are S3-compatible but neither endpoint can be derived (an
    # account id, a region): fail rather than guess.
    if not endpoint:
        raise SettingsError(f"DATA_LAKE_PROFILE={profile} requires DATA_LAKE_ENDPOINT")
    return StorageSettings(
        profile=profile,
        endpoint=endpoint,
        use_ssl=True,
        path_style_access=True,
        region="auto" if profile == "r2" else region,
        **common,
    )


# ---------------------------------------------------------------- services
@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:9092"
    # Partitions a marketplace topic is created with (ops create-topics).
    partitions: int = 3
    producer_ack_timeout_seconds: int = 30
    change_ack_timeout_seconds: int = 30
    dlq_projector_group: str = "marketplace-dlq-projector-v1"
    # The connector must match the Spark runtime (4.x, Scala 2.13). Empty
    # inside an image whose connector jars are already on the classpath.
    spark_kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


@dataclass(frozen=True)
class PostgresSettings:
    # The BI cache and audit store, not the system of record. Host access uses
    # the mapped port; in-container jobs set POSTGRES_PORT=5432.
    host: str = "localhost"
    port: int = 5433
    user: str = "admin"
    password: str = "admin123"
    db: str = "data_warehouse"


@dataclass(frozen=True)
class RedisSettings:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    offer_ttl_seconds: int = 86400
    recent_changes_max: int = 5000


@dataclass(frozen=True)
class ElasticsearchSettings:
    host: str = "http://localhost:9200"
    index_changes: str = "marketplace-changes-v1"
    index_offers: str = "marketplace-offers-current-v1"
    # Operational indices written by the ES projector; state only, never a
    # raw body, cookie or header.
    index_source_health: str = "marketplace-source-health-v1"
    index_crawl_attempts: str = "marketplace-crawl-attempts-v1"
    index_speed_batches: str = "marketplace-speed-batches-v1"
    index_dlq: str = "marketplace-dlq-v1"


# ---------------------------------------------------------------- crawl
@dataclass(frozen=True)
class CrawlSettings:
    request_delay_seconds: float = 2.0
    jitter_seconds: float = 1.0
    http_timeout_seconds: float = 10.0
    user_agent: str = "EcommercePriceResearchBot/0.1 (+thesis project; rate-limited)"
    # A fetched robots.txt is trusted this long, per host.
    robots_ttl_seconds: int = 3600
    # Listing pages per category in a one-shot crawl; a source stops earlier
    # at its last page.
    max_pages: int = 50
    # A frontier task's target per source when seeding without --category.
    default_categories: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    # Where a source's pages are fetched from, when not its public site: the
    # stub source of the smoke and the drills (<CODE>_BASE_URL).
    base_url_overrides: Mapping[str, str] = field(default_factory=dict)
    # Browser fetcher (Playwright).
    browser_timeout_seconds: float = 45.0
    browser_settle_seconds: float = 3.0
    # Frontier cadence per tier, minutes.
    cadence_minutes: Mapping[str, int] = field(
        default_factory=lambda: {"ACTIVE": 60, "NORMAL": 240, "COLD": 720, "HOT": 120, "SWEEP": 1440}
    )
    lease_seconds: int = 300
    max_attempts: int = 5
    retry_base_seconds: int = 30
    retry_max_seconds: int = 1800
    retry_jitter_ratio: Decimal = Decimal("0.20")
    circuit_failure_threshold: int = 5
    circuit_open_seconds: int = 900
    worker_poll_seconds: int = 5
    worker_batch_size: int = 10
    # Empty means "<hostname>:<pid>".
    service_worker_id: str = ""
    # The wait after a cycle that leased nothing; a busy worker waits
    # worker_poll_seconds.
    service_idle_seconds: int = 30


def _crawl(env: _Env) -> CrawlSettings:
    defaults = CrawlSettings()
    cadence = dict(defaults.cadence_minutes)
    for tier in cadence:
        cadence[tier] = env.integer(f"CRAWL_{tier}_CADENCE_MINUTES", cadence[tier])
    categories = {
        spec.code: env.items(f"{spec.code.upper()}_CATEGORIES", ",".join(spec.default_targets)) for spec in SOURCES
    }
    return CrawlSettings(
        request_delay_seconds=env.number("CRAWL_REQUEST_DELAY_SECONDS", defaults.request_delay_seconds),
        jitter_seconds=env.number("CRAWL_JITTER_SECONDS", defaults.jitter_seconds),
        http_timeout_seconds=env.number("CRAWL_HTTP_TIMEOUT_SECONDS", defaults.http_timeout_seconds),
        user_agent=env.text("CRAWL_USER_AGENT", defaults.user_agent),
        robots_ttl_seconds=env.integer("CRAWL_ROBOTS_TTL_SECONDS", defaults.robots_ttl_seconds),
        max_pages=env.integer("CRAWL_MAX_PAGES", defaults.max_pages),
        default_categories={code: items for code, items in categories.items() if items},
        base_url_overrides={
            spec.code: env.text(f"{spec.code.upper()}_BASE_URL", "").strip()
            for spec in SOURCES
            if env.text(f"{spec.code.upper()}_BASE_URL", "").strip()
        },
        browser_timeout_seconds=env.number("CRAWL_BROWSER_TIMEOUT_SECONDS", defaults.browser_timeout_seconds),
        browser_settle_seconds=env.number("CRAWL_BROWSER_SETTLE_SECONDS", defaults.browser_settle_seconds),
        cadence_minutes=cadence,
        lease_seconds=env.integer("CRAWL_LEASE_SECONDS", defaults.lease_seconds),
        max_attempts=env.integer("CRAWL_MAX_ATTEMPTS", defaults.max_attempts),
        retry_base_seconds=env.integer("CRAWL_RETRY_BASE_SECONDS", defaults.retry_base_seconds),
        retry_max_seconds=env.integer("CRAWL_RETRY_MAX_SECONDS", defaults.retry_max_seconds),
        retry_jitter_ratio=env.decimal("CRAWL_RETRY_JITTER_RATIO", str(defaults.retry_jitter_ratio)),
        circuit_failure_threshold=env.integer("CRAWL_CIRCUIT_FAILURE_THRESHOLD", defaults.circuit_failure_threshold),
        circuit_open_seconds=env.integer("CRAWL_CIRCUIT_OPEN_SECONDS", defaults.circuit_open_seconds),
        worker_poll_seconds=env.integer("CRAWL_WORKER_POLL_SECONDS", defaults.worker_poll_seconds),
        worker_batch_size=env.integer("CRAWL_WORKER_BATCH_SIZE", defaults.worker_batch_size),
        service_worker_id=env.text("CRAWL_SERVICE_WORKER_ID", "").strip(),
        service_idle_seconds=env.integer("CRAWL_SERVICE_IDLE_SECONDS", defaults.service_idle_seconds),
    )


# ---------------------------------------------------------------- processing
@dataclass(frozen=True)
class SparkSettings:
    master: str | None = None
    # Set explicitly so a job never runs on Spark's 1g default (the 8 h run of
    # 2026-10-24 needed 3g for the batch).
    driver_memory: str = "3g"
    batch_shuffle_partitions: int = 8
    speed_shuffle_partitions: int = 4
    batch_app_name: str = "MarketplaceTemporalWarehouse"
    speed_app_name: str = "MarketplaceSpeedLayer"


@dataclass(frozen=True)
class BatchSettings:
    silver_dataset: str = "marketplace/offer_observations"
    # Where the Silver v2 datasets (offer_observations_v2, quarantine, page_context, ...) live in the silver bucket.
    silver_root: str = "marketplace"
    records_dataset: str = "marketplace/records"
    freshness_seconds: int = 21600
    freshness_rule_version: str = "freshness-rules.v1"
    counter_rule_version: str = "counter-rules.v1"
    counter_max_gap_seconds: int = 86400
    percentile_accuracy: int = 10000
    # Scheduler loop (legacy-scheduler profile): windows close on interval
    # boundaries and are cut once the lag has passed.
    interval_seconds: int = 86400
    as_of_lag_seconds: int = 1800
    # PostgreSQL advisory lock that keeps two stages from running at once.
    lock_key: int = 820801
    # Per-offer robust price anomaly over its own recent history.
    anomaly_rule_version: str = "anomaly-rules.v1"
    anomaly_window_days: int = 14
    anomaly_min_samples: int = 7
    anomaly_mad_threshold: Decimal = Decimal("3.5")
    anomaly_iqr_multiplier: Decimal = Decimal("1.5")
    # Bronze/raw objects older than this are removed by the maintenance DAG.
    raw_retention_days: int = 30


@dataclass(frozen=True)
class QualitySettings:
    rule_version: str = "quality-rules.v2"
    # Clock skew tolerated between a fetch and the observation it yields.
    future_tolerance_seconds: int = 300
    # A crawl run is reconciled against Silver once settled, while inside the
    # lookback. The lookback must exceed the batch interval plus the settle
    # delay, or a run could age out between two batches unreconciled.
    reconciliation_settle_seconds: int = 900
    reconciliation_lookback_seconds: int = 172800
    sample_limit: int = 10
    allowed_currencies: tuple[str, ...] = ("USD", "VND")


@dataclass(frozen=True)
class SpeedSettings:
    change_rule_version: str = "speed-rules.v1"
    large_drop_absolute: Decimal = Decimal("100000")
    large_drop_relative: Decimal = Decimal("0.20")
    stale_after_seconds: int = 21600
    trigger: str = "30 seconds"
    # Upper bound on Kafka records one micro-batch reads; 0 means no bound
    # (Spark's default). The speed-cost benchmark sets it to hold the batch
    # size fixed while it measures what a batch of that size costs.
    max_offsets_per_trigger: int = 0
    # Empty: <repo>/data/checkpoints/marketplace_speed. The version is always
    # the last path component, so bumping it starts a fresh checkpoint.
    checkpoint_root: str = ""
    checkpoint_version: str = "v1"
    query_name: str = "marketplace-speed-v1"

    def checkpoint_path(self) -> str:
        root = Path(self.checkpoint_root) if self.checkpoint_root else REPO_ROOT / "data" / "checkpoints" / "marketplace_speed"
        return str((root / self.checkpoint_version).resolve())


@dataclass(frozen=True)
class ServiceSettings:
    # Touched once per loop by the long-running services; the container
    # healthcheck fails when it goes stale. Empty disables it.
    heartbeat_file: str = ""
    # ES projector passes. The overlap re-reads settled rows so an UPDATE or a
    # pass that died halfway is repaired; it must exceed the interval.
    project_interval_seconds: int = 30
    project_overlap_seconds: int = 900
    # Silver sink: a failed record is retried at the same offset, waiting
    # base, then double, up to max.
    silver_poll_timeout_ms: int = 1000
    silver_retry_base_seconds: int = 2
    silver_retry_max_seconds: int = 60


@dataclass(frozen=True)
class AirflowSettings:
    # Image every DockerOperator task runs, and the network it joins.
    spark_image: str = "ecommerce/spark-marketplace:4.0.1-r2"
    python_image: str = "ecommerce/marketplace-python:2"
    docker_network: str = "mp-refactor_default"
    docker_url: str = "unix://var/run/docker.sock"
    # Host paths mounted into the task containers.
    env_file: str = "/opt/airflow/env/stack.env"
    source_mount: str = ""
    discovery_limit: int = 50


@dataclass(frozen=True)
class Settings:
    storage: StorageSettings = field(default_factory=StorageSettings)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    es: ElasticsearchSettings = field(default_factory=ElasticsearchSettings)
    crawl: CrawlSettings = field(default_factory=CrawlSettings)
    spark: SparkSettings = field(default_factory=SparkSettings)
    batch: BatchSettings = field(default_factory=BatchSettings)
    quality: QualitySettings = field(default_factory=QualitySettings)
    speed: SpeedSettings = field(default_factory=SpeedSettings)
    service: ServiceSettings = field(default_factory=ServiceSettings)
    airflow: AirflowSettings = field(default_factory=AirflowSettings)


def _read(env: _Env) -> Settings:
    batch_defaults, quality_defaults, speed_defaults = BatchSettings(), QualitySettings(), SpeedSettings()
    return Settings(
        storage=_storage(env),
        kafka=KafkaSettings(
            bootstrap_servers=env.text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            partitions=env.integer("KAFKA_MARKETPLACE_PARTITIONS", 3),
            producer_ack_timeout_seconds=env.integer("KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS", 30),
            change_ack_timeout_seconds=env.integer("KAFKA_CHANGE_ACK_TIMEOUT_SECONDS", 30),
            dlq_projector_group=env.text("KAFKA_DLQ_PROJECTOR_GROUP", "marketplace-dlq-projector-v1"),
            spark_kafka_package=env.text(
                "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
            ),
        ),
        postgres=PostgresSettings(
            host=env.text("POSTGRES_HOST", "localhost"),
            port=env.integer("POSTGRES_PORT", 5433),
            user=env.text("POSTGRES_USER", "admin"),
            password=env.text("POSTGRES_PASSWORD", "admin123"),
            db=env.text("POSTGRES_DB", "data_warehouse"),
        ),
        redis=RedisSettings(
            host=env.text("REDIS_HOST", "localhost"),
            port=env.integer("REDIS_PORT", 6379),
            db=env.integer("REDIS_DB", 0),
            offer_ttl_seconds=env.integer("REDIS_MARKETPLACE_OFFER_TTL_SECONDS", 86400),
            recent_changes_max=env.integer("REDIS_MARKETPLACE_RECENT_CHANGES_MAX", 5000),
        ),
        es=ElasticsearchSettings(
            host=env.text("ES_HOST", "http://localhost:9200"),
            index_changes=env.text("ES_INDEX_MARKETPLACE_CHANGES", "marketplace-changes-v1"),
            index_offers=env.text("ES_INDEX_MARKETPLACE_OFFERS", "marketplace-offers-current-v1"),
            index_source_health=env.text("ES_INDEX_MARKETPLACE_SOURCE_HEALTH", "marketplace-source-health-v1"),
            index_crawl_attempts=env.text("ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS", "marketplace-crawl-attempts-v1"),
            index_speed_batches=env.text("ES_INDEX_MARKETPLACE_SPEED_BATCHES", "marketplace-speed-batches-v1"),
            index_dlq=env.text("ES_INDEX_MARKETPLACE_DLQ", "marketplace-dlq-v1"),
        ),
        crawl=_crawl(env),
        spark=SparkSettings(
            master=env.text("SPARK_MASTER", "").strip() or None,
            driver_memory=env.text("SPARK_DRIVER_MEMORY", "3g"),
            batch_shuffle_partitions=env.integer("MARKETPLACE_BATCH_SHUFFLE_PARTITIONS", 8),
            speed_shuffle_partitions=env.integer("MARKETPLACE_SPEED_SHUFFLE_PARTITIONS", 4),
            batch_app_name=env.text("MARKETPLACE_BATCH_APP_NAME", "MarketplaceTemporalWarehouse"),
            speed_app_name=env.text("MARKETPLACE_SPEED_APP_NAME", "MarketplaceSpeedLayer"),
        ),
        batch=BatchSettings(
            silver_dataset=env.text("MARKETPLACE_SILVER_DATASET", batch_defaults.silver_dataset),
            records_dataset=env.text("MARKETPLACE_RECORDS_DATASET", batch_defaults.records_dataset),
            silver_root=env.text("MARKETPLACE_SILVER_ROOT", batch_defaults.silver_root),
            freshness_seconds=env.integer("MARKETPLACE_FRESHNESS_SECONDS", batch_defaults.freshness_seconds),
            freshness_rule_version=env.text("MARKETPLACE_FRESHNESS_RULE_VERSION", batch_defaults.freshness_rule_version),
            counter_rule_version=env.text("MARKETPLACE_COUNTER_RULE_VERSION", batch_defaults.counter_rule_version),
            counter_max_gap_seconds=env.integer(
                "MARKETPLACE_COUNTER_MAX_GAP_SECONDS", batch_defaults.counter_max_gap_seconds
            ),
            percentile_accuracy=env.integer("MARKETPLACE_PERCENTILE_ACCURACY", batch_defaults.percentile_accuracy),
            interval_seconds=env.integer("MARKETPLACE_BATCH_INTERVAL_SECONDS", batch_defaults.interval_seconds),
            as_of_lag_seconds=env.integer("MARKETPLACE_BATCH_AS_OF_LAG_SECONDS", batch_defaults.as_of_lag_seconds),
            lock_key=env.integer("MARKETPLACE_BATCH_LOCK_KEY", batch_defaults.lock_key),
            anomaly_rule_version=env.text("MARKETPLACE_ANOMALY_RULE_VERSION", batch_defaults.anomaly_rule_version),
            anomaly_window_days=env.integer("MARKETPLACE_ANOMALY_WINDOW_DAYS", batch_defaults.anomaly_window_days),
            anomaly_min_samples=env.integer("MARKETPLACE_ANOMALY_MIN_SAMPLES", batch_defaults.anomaly_min_samples),
            anomaly_mad_threshold=env.decimal("MARKETPLACE_ANOMALY_MAD_THRESHOLD", "3.5"),
            anomaly_iqr_multiplier=env.decimal("MARKETPLACE_ANOMALY_IQR_MULTIPLIER", "1.5"),
            raw_retention_days=env.integer("MARKETPLACE_RAW_RETENTION_DAYS", batch_defaults.raw_retention_days),
        ),
        quality=QualitySettings(
            rule_version=env.text("MARKETPLACE_QUALITY_RULE_VERSION", quality_defaults.rule_version),
            future_tolerance_seconds=env.integer(
                "MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS", quality_defaults.future_tolerance_seconds
            ),
            reconciliation_settle_seconds=env.integer(
                "MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS", quality_defaults.reconciliation_settle_seconds
            ),
            reconciliation_lookback_seconds=env.integer(
                "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS", quality_defaults.reconciliation_lookback_seconds
            ),
            sample_limit=env.integer("MARKETPLACE_QUALITY_SAMPLE_LIMIT", quality_defaults.sample_limit),
            allowed_currencies=tuple(
                sorted({code.upper() for code in env.items("MARKETPLACE_ALLOWED_CURRENCIES", "VND,USD")})
            ),
        ),
        speed=SpeedSettings(
            change_rule_version=env.text("MARKETPLACE_CHANGE_RULE_VERSION", speed_defaults.change_rule_version),
            large_drop_absolute=env.decimal("MARKETPLACE_LARGE_DROP_ABSOLUTE", "100000"),
            large_drop_relative=env.decimal("MARKETPLACE_LARGE_DROP_RELATIVE", "0.20"),
            stale_after_seconds=env.integer("MARKETPLACE_STALE_AFTER_SECONDS", speed_defaults.stale_after_seconds),
            trigger=env.text("MARKETPLACE_STREAM_TRIGGER", speed_defaults.trigger),
            max_offsets_per_trigger=env.integer(
                "MARKETPLACE_SPEED_MAX_OFFSETS_PER_TRIGGER", speed_defaults.max_offsets_per_trigger
            ),
            checkpoint_root=env.text("MARKETPLACE_SPEED_CHECKPOINT_ROOT", "").strip(),
            checkpoint_version=env.text(
                "MARKETPLACE_STREAM_CHECKPOINT_VERSION", speed_defaults.checkpoint_version
            ),
            query_name=env.text("MARKETPLACE_SPEED_QUERY_NAME", speed_defaults.query_name),
        ),
        service=ServiceSettings(
            heartbeat_file=env.text("SERVICE_HEARTBEAT_FILE", ""),
            project_interval_seconds=env.integer("MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS", 30),
            project_overlap_seconds=env.integer("MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS", 900),
            silver_poll_timeout_ms=env.integer("MARKETPLACE_SILVER_POLL_TIMEOUT_MS", 1000),
            silver_retry_base_seconds=env.integer("MARKETPLACE_SILVER_RETRY_BASE_SECONDS", 2),
            silver_retry_max_seconds=env.integer("MARKETPLACE_SILVER_RETRY_MAX_SECONDS", 60),
        ),
        airflow=AirflowSettings(
            spark_image=env.text("AIRFLOW_SPARK_IMAGE", AirflowSettings.spark_image),
            python_image=env.text("AIRFLOW_PYTHON_IMAGE", AirflowSettings.python_image),
            docker_network=env.text("AIRFLOW_DOCKER_NETWORK", AirflowSettings.docker_network),
            docker_url=env.text("AIRFLOW_DOCKER_URL", AirflowSettings.docker_url),
            env_file=env.text("AIRFLOW_TASK_ENV_FILE", AirflowSettings.env_file),
            source_mount=env.text("AIRFLOW_SOURCE_MOUNT", ""),
            discovery_limit=env.integer("AIRFLOW_DISCOVERY_LIMIT", AirflowSettings.discovery_limit),
        ),
    )


def validate(settings: Settings) -> Settings:
    """Cross-field checks; raise :class:`SettingsError` naming the variable."""
    speed, batch, quality, crawl, service = (
        settings.speed,
        settings.batch,
        settings.quality,
        settings.crawl,
        settings.service,
    )
    if speed.large_drop_absolute < 0:
        raise SettingsError("MARKETPLACE_LARGE_DROP_ABSOLUTE must be non-negative")
    if not Decimal("0") <= speed.large_drop_relative <= Decimal("1"):
        raise SettingsError("MARKETPLACE_LARGE_DROP_RELATIVE must be between 0 and 1")
    if speed.max_offsets_per_trigger < 0:
        raise SettingsError("MARKETPLACE_SPEED_MAX_OFFSETS_PER_TRIGGER must be 0 (no bound) or positive")
    positive = {
        "CRAWL_SERVICE_IDLE_SECONDS": crawl.service_idle_seconds,
        "CRAWL_WORKER_POLL_SECONDS": crawl.worker_poll_seconds,
        "CRAWL_WORKER_BATCH_SIZE": crawl.worker_batch_size,
        "CRAWL_LEASE_SECONDS": crawl.lease_seconds,
        "CRAWL_MAX_ATTEMPTS": crawl.max_attempts,
        "CRAWL_HTTP_TIMEOUT_SECONDS": crawl.http_timeout_seconds,
        "CRAWL_ROBOTS_TTL_SECONDS": crawl.robots_ttl_seconds,
        "CRAWL_MAX_PAGES": crawl.max_pages,
        "MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS": service.project_interval_seconds,
        "MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS": service.project_overlap_seconds,
        "MARKETPLACE_SILVER_POLL_TIMEOUT_MS": service.silver_poll_timeout_ms,
        "MARKETPLACE_SILVER_RETRY_BASE_SECONDS": service.silver_retry_base_seconds,
        "MARKETPLACE_SILVER_RETRY_MAX_SECONDS": service.silver_retry_max_seconds,
        "MARKETPLACE_STALE_AFTER_SECONDS": speed.stale_after_seconds,
        "KAFKA_CHANGE_ACK_TIMEOUT_SECONDS": settings.kafka.change_ack_timeout_seconds,
        "KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS": settings.kafka.producer_ack_timeout_seconds,
        "KAFKA_MARKETPLACE_PARTITIONS": settings.kafka.partitions,
        "REDIS_MARKETPLACE_OFFER_TTL_SECONDS": settings.redis.offer_ttl_seconds,
        "REDIS_MARKETPLACE_RECENT_CHANGES_MAX": settings.redis.recent_changes_max,
        "MARKETPLACE_FRESHNESS_SECONDS": batch.freshness_seconds,
        "MARKETPLACE_COUNTER_MAX_GAP_SECONDS": batch.counter_max_gap_seconds,
        "MARKETPLACE_PERCENTILE_ACCURACY": batch.percentile_accuracy,
        "MARKETPLACE_BATCH_SHUFFLE_PARTITIONS": settings.spark.batch_shuffle_partitions,
        "MARKETPLACE_SPEED_SHUFFLE_PARTITIONS": settings.spark.speed_shuffle_partitions,
        "MARKETPLACE_BATCH_INTERVAL_SECONDS": batch.interval_seconds,
        "MARKETPLACE_BATCH_AS_OF_LAG_SECONDS": batch.as_of_lag_seconds,
        "MARKETPLACE_BATCH_LOCK_KEY": batch.lock_key,
        "MARKETPLACE_ANOMALY_WINDOW_DAYS": batch.anomaly_window_days,
        "MARKETPLACE_ANOMALY_MIN_SAMPLES": batch.anomaly_min_samples,
        "MARKETPLACE_ANOMALY_MAD_THRESHOLD": batch.anomaly_mad_threshold,
        "MARKETPLACE_ANOMALY_IQR_MULTIPLIER": batch.anomaly_iqr_multiplier,
        "MARKETPLACE_RAW_RETENTION_DAYS": batch.raw_retention_days,
        "MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS": quality.future_tolerance_seconds,
        "MARKETPLACE_QUALITY_SAMPLE_LIMIT": quality.sample_limit,
        "MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": quality.reconciliation_settle_seconds,
        "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS": quality.reconciliation_lookback_seconds,
    }
    for name, value in positive.items():
        if value <= 0:
            raise SettingsError(f"{name} must be positive")
    for tier, minutes in crawl.cadence_minutes.items():
        if minutes <= 0:
            raise SettingsError(f"CRAWL_{tier}_CADENCE_MINUTES must be positive")
    if crawl.request_delay_seconds < 0 or crawl.jitter_seconds < 0:
        raise SettingsError("CRAWL_REQUEST_DELAY_SECONDS and CRAWL_JITTER_SECONDS must not be negative")
    if not Decimal("0") <= crawl.retry_jitter_ratio <= Decimal("1"):
        raise SettingsError("CRAWL_RETRY_JITTER_RATIO must be between 0 and 1")
    if batch.anomaly_min_samples > batch.anomaly_window_days:
        # No row could ever reach the minimum: the whole mart would read
        # INSUFFICIENT_HISTORY without anything looking broken.
        raise SettingsError("MARKETPLACE_ANOMALY_MIN_SAMPLES cannot exceed MARKETPLACE_ANOMALY_WINDOW_DAYS")
    if quality.reconciliation_lookback_seconds <= quality.reconciliation_settle_seconds:
        raise SettingsError(
            "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS must exceed "
            "MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS"
        )
    if quality.reconciliation_lookback_seconds <= batch.interval_seconds + quality.reconciliation_settle_seconds:
        raise SettingsError(
            "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS must exceed "
            "MARKETPLACE_BATCH_INTERVAL_SECONDS plus MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS"
        )
    if service.project_overlap_seconds <= service.project_interval_seconds:
        raise SettingsError("MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS must exceed MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS")
    required_text = {
        "MARKETPLACE_CHANGE_RULE_VERSION": speed.change_rule_version,
        "MARKETPLACE_STREAM_CHECKPOINT_VERSION": speed.checkpoint_version,
        "MARKETPLACE_SPEED_QUERY_NAME": speed.query_name,
        "MARKETPLACE_FRESHNESS_RULE_VERSION": batch.freshness_rule_version,
        "MARKETPLACE_COUNTER_RULE_VERSION": batch.counter_rule_version,
        "MARKETPLACE_SILVER_DATASET": batch.silver_dataset,
        "MARKETPLACE_RECORDS_DATASET": batch.records_dataset,
        "MARKETPLACE_SILVER_ROOT": batch.silver_root,
        "MARKETPLACE_BATCH_APP_NAME": settings.spark.batch_app_name,
        "MARKETPLACE_ANOMALY_RULE_VERSION": batch.anomaly_rule_version,
        "MARKETPLACE_QUALITY_RULE_VERSION": quality.rule_version,
        "SPARK_DRIVER_MEMORY": settings.spark.driver_memory,
        "KAFKA_DLQ_PROJECTOR_GROUP": settings.kafka.dlq_projector_group,
        "ES_INDEX_MARKETPLACE_SOURCE_HEALTH": settings.es.index_source_health,
        "ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS": settings.es.index_crawl_attempts,
        "ES_INDEX_MARKETPLACE_SPEED_BATCHES": settings.es.index_speed_batches,
        "ES_INDEX_MARKETPLACE_DLQ": settings.es.index_dlq,
    }
    for name, value in required_text.items():
        if not value.strip():
            raise SettingsError(f"{name} must be non-empty")
    if not quality.allowed_currencies:
        raise SettingsError("MARKETPLACE_ALLOWED_CURRENCIES must list at least one code")
    for code in quality.allowed_currencies:
        if len(code) != 3 or not code.isalpha():
            raise SettingsError(f"MARKETPLACE_ALLOWED_CURRENCIES holds a non ISO-4217 code: {code}")
    return settings


def environment(*, dotenv: bool = False, dotenv_path: Path | None = None) -> dict[str, str]:
    """The process environment, over the repository ``.env`` when asked."""
    values: dict[str, str] = {}
    if dotenv:
        from dotenv import dotenv_values

        path = dotenv_path or REPO_ROOT / ".env"
        if path.exists():
            values.update({key: value for key, value in dotenv_values(path).items() if value is not None})
    values.update(os.environ)
    return values


def load_settings(env: Mapping[str, str] | None = None, *, dotenv: bool = False) -> Settings:
    """Read and validate every setting from ``env`` (default: the process environment)."""
    values = environment(dotenv=dotenv) if env is None else env
    return validate(_read(_Env(values)))


def describe(settings: Settings) -> dict[str, dict[str, object]]:
    """Every value by concern, secrets masked, for a startup log line."""
    secret = {"password", "secret_key", "access_key"}
    out: dict[str, dict[str, object]] = {}
    for concern in fields(settings):
        section = getattr(settings, concern.name)
        out[concern.name] = {
            item.name: ("***" if item.name in secret and getattr(section, item.name) else str(getattr(section, item.name)))
            for item in fields(section)
        }
    return out


__all__ = [
    "AirflowSettings",
    "BatchSettings",
    "CrawlSettings",
    "ElasticsearchSettings",
    "KafkaSettings",
    "PostgresSettings",
    "QualitySettings",
    "RedisSettings",
    "ServiceSettings",
    "Settings",
    "SettingsError",
    "SparkSettings",
    "SpeedSettings",
    "StorageSettings",
    "describe",
    "environment",
    "load_settings",
    "validate",
]
