"""Bronze/records: the extracted records of one page, one gzip JSON-lines file.

``marketplace/records/source=<code>/observed_date=<d>/hour=<h>/raw_artifact_id=<id>.jsonl.gz``

The file is written, and the write acknowledged, before any record is published
to Kafka: Bronze is the source of truth, and the stream only a faster copy of it.
The name carries the raw artifact id, so re-extracting an artifact rewrites the
same file with the same content rather than adding a second one.
"""
from __future__ import annotations

import gzip
from collections.abc import Callable, Iterable
from urllib.parse import quote

from ecomdata.domain import validation as v
from ecomdata.domain.acquisition import RawPersistenceError
from ecomdata.domain.records import SourceRecord, record_from_wire, record_json
from ecomdata.infra.object_store import gunzip_bytes

BRONZE_ZONE = "bronze"
RECORDS_ROOT = "marketplace/records"

Writer = Callable[[str, str, bytes], str]
Reader = Callable[[str, str], "bytes | None"]


def records_path(source_code: str, fetched_at, raw_artifact_id: str) -> str:
    return (
        f"{RECORDS_ROOT}/"
        f"source={quote(v.require_text(source_code, 'source_code'), safe='-_.~')}/"
        f"observed_date={fetched_at.strftime('%Y-%m-%d')}/"
        f"hour={fetched_at.strftime('%H')}/"
        f"raw_artifact_id={quote(v.require_text(raw_artifact_id, 'raw_artifact_id'), safe='-_.~')}.jsonl.gz"
    )


def encode_records(records: Iterable[SourceRecord]) -> bytes:
    """Canonical JSON lines, gzipped with a zero mtime so identical records give identical bytes."""
    text = "".join(record_json(record) + "\n" for record in records)
    return gzip.compress(text.encode("utf-8"), compresslevel=6, mtime=0)


def decode_records(data: bytes) -> tuple[SourceRecord, ...]:
    import json

    lines = gunzip_bytes(data).decode("utf-8").splitlines()
    return tuple(record_from_wire(json.loads(line)) for line in lines if line.strip())


def persist_records(records: tuple[SourceRecord, ...], *, writer: Writer) -> str:
    """Write one page's records; return the URI the writer reports."""
    if not records:
        raise ValueError("a page always has at least its page-context record")
    first = records[0]
    if len({record.raw_artifact_id for record in records}) != 1:
        raise ValueError("records of one file must share one raw artifact")
    path = records_path(first.source_code, first.fetched_at, first.raw_artifact_id)
    try:
        return v.require_text(writer(BRONZE_ZONE, path, encode_records(records)), "records_uri")
    except Exception as exc:
        raise RawPersistenceError(f"could not persist the records: {exc}", write_stage="RECORDS") from exc


def read_records(path: str, *, reader: Reader) -> tuple[SourceRecord, ...] | None:
    data = reader(BRONZE_ZONE, path)
    return None if data is None else decode_records(data)
