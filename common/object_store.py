"""Lightweight object writes into the medallion data lake, outside Spark.

Mirrors the local-vs-s3a duality of ``config.settings.data_lake_uri`` so any
plain-Python writer (the crawler, for now) can drop raw snapshots into Bronze
without spinning up a Spark session.
"""

from __future__ import annotations

import io
from functools import lru_cache

from config.settings import (
    DATA_LAKE_LOCAL_ROOT,
    DATA_LAKE_MODE,
    MINIO_ACCESS_KEY,
    MINIO_BUCKETS,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)


@lru_cache(maxsize=1)
def _minio_client():
    from minio import Minio

    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def put_bytes(zone: str, relative_path: str, data: bytes) -> str:
    """Write ``data`` under a medallion zone; returns the location written to."""
    zone_name = zone.lower()
    if zone_name not in MINIO_BUCKETS:
        raise ValueError(f"Unknown data-lake zone: {zone}")
    key = relative_path.strip("/")

    if DATA_LAKE_MODE == "s3a":
        bucket = MINIO_BUCKETS[zone_name]
        client = _minio_client()
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        client.put_object(bucket, key, io.BytesIO(data), length=len(data))
        return f"s3a://{bucket}/{key}"

    path = DATA_LAKE_LOCAL_ROOT / zone_name / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path.resolve().as_uri()
