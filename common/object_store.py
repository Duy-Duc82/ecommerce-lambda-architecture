"""Lightweight object writes into the medallion data lake, outside Spark.

Mirrors the local-vs-object-store duality of ``config.settings.data_lake_uri``
so any plain-Python writer (the crawler, for now) can drop raw snapshots into
Bronze without spinning up a Spark session.

Which object store that is — laptop MinIO, S3, R2, B2 — comes from the active
storage profile, so this module never hardcodes an endpoint or TLS setting.
"""

from __future__ import annotations

import io
from functools import lru_cache

from config.settings import DATA_LAKE_LOCAL_ROOT, MINIO_BUCKETS
from config.storage import StorageProfile, active_profile


@lru_cache(maxsize=4)
def _client(profile: StorageProfile):
    from minio import Minio

    return Minio(
        profile.endpoint_host(),
        access_key=profile.access_key,
        secret_key=profile.secret_key,
        secure=profile.use_ssl,
        region=profile.region,
    )


def put_bytes(zone: str, relative_path: str, data: bytes) -> str:
    """Write ``data`` under a medallion zone; returns the location written to."""
    zone_name = zone.lower()
    if zone_name not in MINIO_BUCKETS:
        raise ValueError(f"Unknown data-lake zone: {zone}")
    key = relative_path.strip("/")
    profile = active_profile()

    if not profile.is_local:
        bucket = MINIO_BUCKETS[zone_name]
        client = _client(profile)
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        client.put_object(bucket, key, io.BytesIO(data), length=len(data))
        return f"s3a://{bucket}/{key}"

    path = DATA_LAKE_LOCAL_ROOT / zone_name / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path.resolve().as_uri()
