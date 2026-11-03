"""How Spark and the object-store SDKs reach the lake.

The values come from a storage configuration (``StorageSettings`` at the
composition root); this module only turns them into client options. MinIO,
AWS S3, Cloudflare R2 and Backblaze B2 differ in endpoint, TLS and bucket
addressing, never in job code.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class StorageTarget(Protocol):
    """The shape of ``ecomdata.settings.StorageSettings`` this layer reads."""

    profile: str
    local_root: Path
    endpoint: str | None
    use_ssl: bool
    path_style_access: bool
    region: str | None
    access_key: str
    secret_key: str
    table_format: str

    @property
    def is_local(self) -> bool: ...

    @property
    def buckets(self) -> dict[str, str]: ...

    def endpoint_url(self) -> str: ...

    def endpoint_host(self) -> str: ...


def spark_hadoop_options(storage: StorageTarget) -> dict[str, str]:
    """S3A settings for ``spark.hadoop.*``; empty for a local lake."""
    if storage.is_local:
        return {}
    options = {
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.access.key": storage.access_key,
        "fs.s3a.secret.key": storage.secret_key,
        "fs.s3a.path.style.access": str(storage.path_style_access).lower(),
        "fs.s3a.connection.ssl.enabled": str(storage.use_ssl).lower(),
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # Object stores have no atomic rename; the rename-based v2 committer is
        # slow and unsafe against them.
        "fs.s3a.committer.name": "directory",
        "fs.s3a.fast.upload": "true",
    }
    if storage.endpoint:
        options["fs.s3a.endpoint"] = storage.endpoint_url()
    if storage.region:
        options["fs.s3a.endpoint.region"] = storage.region
    return options
