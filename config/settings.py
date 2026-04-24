"""
Cau hinh tap trung cho toan bo Lambda Architecture.
Doc tu bien moi truong (.env) voi gia tri mac dinh an toan.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# ---------- Load .env ----------
_BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(_BASE_DIR / ".env")


# ============================================================
# KAFKA
# ============================================================
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS_INTERNAL: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "kafka:19092"
)
KAFKA_TOPIC_EVENTS: str = os.getenv("KAFKA_TOPIC_EVENTS", "ecommerce_events")
KAFKA_TOPIC_ORDERS: str = os.getenv("KAFKA_TOPIC_ORDERS", "ecommerce_orders")
KAFKA_TOPIC_PRICES: str = os.getenv("KAFKA_TOPIC_PRICES", "ecommerce_prices")

# ============================================================
# SPARK
# ============================================================
SPARK_MASTER_URL: str = os.getenv("SPARK_MASTER_URL", "spark://spark:7077")
SPARK_KAFKA_PACKAGE: str = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
)

# ============================================================
# MINIO  (S3-compatible Data Lake)
# ============================================================
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_RAW: str = os.getenv("MINIO_BUCKET_RAW", "ecommerce-raw")
MINIO_BUCKET_PROCESSED: str = os.getenv("MINIO_BUCKET_PROCESSED", "ecommerce-processed")

# ============================================================
# POSTGRES  (Data Warehouse)
# ============================================================
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "data_warehouse")

POSTGRES_URL: str = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ============================================================
# REDIS  (Real-time Serving)
# ============================================================
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))

# ============================================================
# ELASTICSEARCH
# ============================================================
ES_HOST: str = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX_EVENTS: str = os.getenv("ES_INDEX_EVENTS", "ecommerce-events")
ES_INDEX_ANOMALIES: str = os.getenv("ES_INDEX_ANOMALIES", "ecommerce-anomalies")

# ============================================================
# DATA SIMULATOR DEFAULTS
# ============================================================
SIMULATOR_PRODUCTS_COUNT: int = int(os.getenv("SIMULATOR_PRODUCTS_COUNT", "100"))
SIMULATOR_USERS_COUNT: int = int(os.getenv("SIMULATOR_USERS_COUNT", "1000"))
SIMULATOR_EVENTS_PER_SECOND: int = int(os.getenv("SIMULATOR_EVENTS_PER_SECOND", "50"))

# ============================================================
# PATHS
# ============================================================
BASE_DIR: Path = _BASE_DIR
CHECKPOINTS_DIR: Path = _BASE_DIR / "checkpoints"
SAMPLE_DATA_DIR: Path = _BASE_DIR / "data_ingestion" / "sample_data"
