"""Storage profiles — the single place that knows how to reach an object store.

Every layer addresses the lake as ``s3a://`` no matter who hosts it, so MinIO,
AWS S3, Cloudflare R2 and Backblaze B2 differ only in endpoint, TLS and bucket
addressing — never in job code. Moving the warehouse to the cloud is therefore
a deployment decision (``DATA_LAKE_PROFILE``), not a rewrite.

That portability was theoretical until now: ``warehouse_job.build_spark`` hard
coded two MinIO-only settings — path-style addressing and
``connection.ssl.enabled=false`` — which are correct for MinIO and wrong for
every real cloud endpoint. Both now come from the active profile, so the same
image runs against a laptop MinIO or an S3 bucket in Singapore unchanged.

Selecting a profile::

    DATA_LAKE_PROFILE=local   # parquet under DATA_LAKE_LOCAL_ROOT (tests/dev)
    DATA_LAKE_PROFILE=minio   # local docker MinIO (the current default stack)
    DATA_LAKE_PROFILE=s3      # AWS S3
    DATA_LAKE_PROFILE=r2      # Cloudflare R2   (S3-compatible, zero egress fee)
    DATA_LAKE_PROFILE=b2      # Backblaze B2    (S3-compatible)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

# Read .env here too: this module is imported by config.settings, so it must not
# depend on settings having loaded the environment first.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Nearest AWS/GCP region to Vietnam. Crossing the Pacific to us-east-1 adds
# roughly 200ms per request, which dominates a many-small-objects crawl load.
DEFAULT_REGION = "ap-southeast-1"


@dataclass(frozen=True)
class StorageProfile:
    """How to talk to one object store. Everything a job needs, nothing more."""

    name: str
    is_local: bool = False
    endpoint: str | None = None          # None -> provider default (AWS regional)
    use_ssl: bool = True
    path_style_access: bool = False      # MinIO/R2/B2 need True; AWS S3 wants False
    region: str | None = None
    access_key: str = ""
    secret_key: str = ""

    @property
    def scheme(self) -> str:
        return "file" if self.is_local else "s3a"

    def endpoint_url(self) -> str:
        """Endpoint as a full URL, resolving the AWS default when unset."""
        if self.is_local:
            raise ValueError(f"profile {self.name!r} is local; it has no endpoint")
        host = self.endpoint or f"s3.{self.region or DEFAULT_REGION}.amazonaws.com"
        if host.startswith(("http://", "https://")):
            return host
        return f"{'https' if self.use_ssl else 'http'}://{host}"

    def endpoint_host(self) -> str:
        """Endpoint as bare host[:port], for SDKs that reject a scheme (minio)."""
        return self.endpoint_url().split("://", 1)[1].rstrip("/")


def _env(*names: str, default: str = "") -> str:
    """First non-empty value among ``names``; lets cloud vars shadow MinIO ones."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _credentials() -> tuple[str, str]:
    # DATA_LAKE_* wins, then the AWS conventions every cloud SDK already honours,
    # then the MinIO defaults so the existing docker stack keeps working untouched.
    access = _env("DATA_LAKE_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY", default="minioadmin")
    secret = _env("DATA_LAKE_SECRET_KEY", "AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY", default="minioadmin")
    return access, secret


def _build(name: str) -> StorageProfile:
    access, secret = _credentials()
    region = _env("DATA_LAKE_REGION", "AWS_REGION", "AWS_DEFAULT_REGION", default=DEFAULT_REGION)
    endpoint = _env("DATA_LAKE_ENDPOINT")

    if name == "local":
        return StorageProfile(name="local", is_local=True)
    if name == "minio":
        return StorageProfile(
            name="minio",
            endpoint=endpoint or _env("MINIO_ENDPOINT", default="localhost:9000"),
            use_ssl=_env("MINIO_USE_SSL", default="false").lower() in ("1", "true", "yes"),
            path_style_access=True,
            region=None,
            access_key=access,
            secret_key=secret,
        )
    if name == "s3":
        return StorageProfile(
            name="s3",
            endpoint=endpoint or None,   # let the SDK pick the regional endpoint
            use_ssl=True,
            path_style_access=False,
            region=region,
            access_key=access,
            secret_key=secret,
        )
    if name in ("r2", "b2"):
        # Both are S3-compatible but neither publishes a derivable endpoint:
        # R2 embeds an account id, B2 a region. Fail loudly rather than guess.
        if not endpoint:
            raise ValueError(
                f"DATA_LAKE_PROFILE={name} requires DATA_LAKE_ENDPOINT "
                f"(e.g. https://<account>.r2.cloudflarestorage.com)"
            )
        return StorageProfile(
            name=name,
            endpoint=endpoint,
            use_ssl=True,
            path_style_access=True,
            region="auto" if name == "r2" else region,
            access_key=access,
            secret_key=secret,
        )
    raise ValueError(f"Unknown DATA_LAKE_PROFILE: {name!r} (expected one of {', '.join(PROFILE_NAMES)})")


PROFILE_NAMES = ("local", "minio", "s3", "r2", "b2")


def resolve_profile_name() -> str:
    """Profile name from the environment.

    ``DATA_LAKE_MODE`` predates profiles and is still set by docker-compose and
    the run scripts, so it stays authoritative when no profile is named.
    """
    explicit = _env("DATA_LAKE_PROFILE").lower()
    if explicit:
        return explicit
    return "minio" if _env("DATA_LAKE_MODE", default="local").lower() == "s3a" else "local"


@lru_cache(maxsize=1)
def active_profile() -> StorageProfile:
    return _build(resolve_profile_name())


def spark_hadoop_options(profile: StorageProfile | None = None) -> dict[str, str]:
    """S3A settings for ``spark.sparkContext.hadoopConfiguration``.

    Empty for local profiles — a file:// lake needs no Hadoop tuning.
    """
    profile = profile or active_profile()
    if profile.is_local:
        return {}

    options = {
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.access.key": profile.access_key,
        "fs.s3a.secret.key": profile.secret_key,
        "fs.s3a.path.style.access": str(profile.path_style_access).lower(),
        "fs.s3a.connection.ssl.enabled": str(profile.use_ssl).lower(),
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # Cloud object stores have no atomic rename; the v2 committer's
        # rename-based commit is both slow and unsafe against them.
        "fs.s3a.committer.name": "directory",
        "fs.s3a.fast.upload": "true",
    }
    if profile.endpoint:
        options["fs.s3a.endpoint"] = profile.endpoint_url()
    if profile.region:
        options["fs.s3a.endpoint.region"] = profile.region
    return options
