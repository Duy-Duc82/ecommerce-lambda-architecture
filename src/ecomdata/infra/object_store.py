"""Object writes and reads in the medallion lake, outside Spark.

:class:`ObjectStore` is the port: ``put``/``get``/``list``/``delete`` by zone
and key, and ``uri`` for the location Spark reads. Two adapters implement it:
:class:`LocalObjectStore` (files under a root, tests and development) and
:class:`S3ObjectStore` (MinIO, S3, R2 or B2 through the ``minio`` SDK).
:func:`object_store_for` picks one from a storage configuration; it is the
only place that decides.

``put_gzip``/``get_gunzip`` store a body gzip-compressed. Compression is
deterministic (``mtime=0``), so the same bytes always give the same object.
"""

from __future__ import annotations

import gzip
import io
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import unquote, urlparse

from ecomdata.infra.storage import StorageTarget

ZONES = ("bronze", "silver", "gold")


def gzip_bytes(data: bytes) -> bytes:
    return gzip.compress(data, compresslevel=6, mtime=0)


def gunzip_bytes(data: bytes) -> bytes:
    return gzip.decompress(data)


@dataclass(frozen=True)
class StoredObject:
    key: str
    size: int
    last_modified: Any = None


def _zone(zone: str) -> str:
    name = zone.lower()
    if name not in ZONES:
        raise ValueError(f"Unknown data-lake zone: {zone}")
    return name


def _key(key: str) -> str:
    key = key.strip("/")
    if not key or ".." in key.split("/"):
        raise ValueError(f"invalid object key: {key!r}")
    return key


class ObjectStore(Protocol):
    def put(self, zone: str, key: str, data: bytes) -> str: ...

    def get(self, zone: str, key: str) -> bytes | None: ...

    def list(self, zone: str, prefix: str = "") -> list[str]: ...

    def stat(self, zone: str, prefix: str = "") -> Iterator[StoredObject]: ...

    def delete(self, zone: str, key: str) -> None: ...

    def uri(self, zone: str, key: str = "") -> str: ...

    def key_of(self, zone: str, uri: str) -> str: ...


class _GzipMixin:
    def put_gzip(self, zone: str, key: str, data: bytes) -> str:
        return self.put(zone, key, gzip_bytes(data))  # type: ignore[attr-defined]

    def get_gunzip(self, zone: str, key: str) -> bytes | None:
        payload = self.get(zone, key)  # type: ignore[attr-defined]
        return None if payload is None else gunzip_bytes(payload)


class LocalObjectStore(_GzipMixin):
    """Files under ``root/<zone>/<key>``; URIs are ``file://``."""

    def __init__(self, root: Path | str):
        self.root = Path(root)

    def _path(self, zone: str, key: str) -> Path:
        return self.root / _zone(zone) / _key(key)

    def put(self, zone: str, key: str, data: bytes) -> str:
        path = self._path(zone, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path.resolve().as_uri()

    def get(self, zone: str, key: str) -> bytes | None:
        path = self._path(zone, key)
        return path.read_bytes() if path.is_file() else None

    def stat(self, zone: str, prefix: str = "") -> Iterator[StoredObject]:
        base = self.root / _zone(zone)
        prefix = prefix.lstrip("/")
        # Walk from the deepest directory the prefix names; the rest of the
        # prefix may end mid-name, as an S3 prefix can.
        head = prefix.rsplit("/", 1)[0] if "/" in prefix else ""
        start = base / head if head else base
        if not start.is_dir():
            return
        for path in sorted(start.rglob("*")):
            if not path.is_file():
                continue
            key = path.relative_to(base).as_posix()
            if key.startswith(prefix):
                info = path.stat()
                yield StoredObject(key, info.st_size, info.st_mtime)

    def list(self, zone: str, prefix: str = "") -> list[str]:
        return [item.key for item in self.stat(zone, prefix)]

    def delete(self, zone: str, key: str) -> None:
        path = self._path(zone, key)
        if path.is_file():
            path.unlink()

    def uri(self, zone: str, key: str = "") -> str:
        base = (self.root / _zone(zone)).resolve().as_uri()
        key = key.strip("/")
        return f"{base}/{key}" if key else base

    def key_of(self, zone: str, uri: str) -> str:
        base = self.uri(zone).rstrip("/")
        if uri.startswith(base):
            return unquote(uri[len(base):].lstrip("/"))
        return unquote(urlparse(uri).path).lstrip("/")


class S3ObjectStore(_GzipMixin):
    """One bucket per zone on an S3-compatible endpoint; URIs are ``s3a://``."""

    def __init__(self, storage: StorageTarget, client: Any = None):
        self.storage = storage
        self.buckets = dict(storage.buckets)
        self._client = client
        self._known_buckets: set[str] = set()

    @property
    def client(self) -> Any:
        if self._client is None:
            from minio import Minio

            self._client = Minio(
                self.storage.endpoint_host(),
                access_key=self.storage.access_key,
                secret_key=self.storage.secret_key,
                secure=self.storage.use_ssl,
                region=self.storage.region,
            )
        return self._client

    def _bucket(self, zone: str) -> str:
        return self.buckets[_zone(zone)]

    def put(self, zone: str, key: str, data: bytes) -> str:
        bucket, key = self._bucket(zone), _key(key)
        if bucket not in self._known_buckets:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
            self._known_buckets.add(bucket)
        self.client.put_object(bucket, key, io.BytesIO(data), length=len(data))
        return f"s3a://{bucket}/{key}"

    def get(self, zone: str, key: str) -> bytes | None:
        """The object, or ``None`` only when it does not exist.

        A transport or permission failure raises: a caller that read "no
        current version" out of a network error would overwrite a good pointer.
        """
        from minio.error import S3Error

        response = None
        try:
            response = self.client.get_object(self._bucket(zone), _key(key))
            return response.read()
        except S3Error as error:
            if error.code in ("NoSuchKey", "NoSuchBucket"):
                return None
            raise
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def stat(self, zone: str, prefix: str = "") -> Iterator[StoredObject]:
        from minio.error import S3Error

        try:
            objects = self.client.list_objects(self._bucket(zone), prefix=prefix.strip("/") or None, recursive=True)
            for item in objects:
                yield StoredObject(item.object_name, item.size or 0, item.last_modified)
        except S3Error as error:
            if error.code != "NoSuchBucket":
                raise

    def list(self, zone: str, prefix: str = "") -> list[str]:
        return sorted(item.key for item in self.stat(zone, prefix))

    def delete(self, zone: str, key: str) -> None:
        self.client.remove_object(self._bucket(zone), _key(key))

    def uri(self, zone: str, key: str = "") -> str:
        base = f"s3a://{self._bucket(zone)}"
        key = key.strip("/")
        return f"{base}/{key}" if key else base

    def key_of(self, zone: str, uri: str) -> str:
        parsed = urlparse(uri)
        return unquote(parsed.path).lstrip("/")


def object_store_for(storage: StorageTarget) -> LocalObjectStore | S3ObjectStore:
    """The adapter for a storage configuration: the one place that chooses."""
    if storage.is_local:
        return LocalObjectStore(storage.local_root)
    return S3ObjectStore(storage)
