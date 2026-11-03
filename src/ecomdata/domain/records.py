"""The source record envelope (``marketplace-record.v1``).

A record is one listing row, or the page around the rows, exactly as the source
sent it. The envelope adds identity and lineage; the payload is the source's own
JSON object, with only the fields the source's spec lists as personal data
removed (their names are kept in ``dropped_fields``). Nothing is typed,
renamed, filtered or validated here: that belongs to the Silver normaliser.
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from ecomdata.domain import validation as v
from ecomdata.domain.identity import deterministic_id
from ecomdata.domain.serialization import serialize_for_wire

RECORD_SCHEMA_VERSION = "marketplace-record.v1"


class RecordKind(str, Enum):
    LISTING_ROW = "listing_row"
    PAGE_CONTEXT = "page_context"


def make_record_id(raw_artifact_id: str, record_kind: RecordKind | str, record_index: int) -> str:
    kind = RecordKind(record_kind)
    return deterministic_id("rec", v.require_text(raw_artifact_id, "raw_artifact_id"), kind.value, record_index)


def drop_fields(payload: Mapping[str, Any], names: Iterable[str]) -> tuple[dict[str, Any], tuple[str, ...]]:
    """``payload`` without the keys named in ``names``, and which of them were present.

    A name is a top-level key, or a dotted path into nested objects
    (``mainInfo.userId``). The input is never modified: only the objects along a
    dropped path are copied.
    """
    kept = dict(payload)
    dropped = []
    for name in sorted(set(names)):
        path = name.split(".")
        parents = []
        node: Any = kept
        for key in path[:-1]:
            child = node.get(key) if isinstance(node, dict) else None
            if not isinstance(child, Mapping):
                node = None
                break
            copy = dict(child)
            node[key] = copy
            parents.append(copy)
            node = copy
        if isinstance(node, dict) and path[-1] in node:
            del node[path[-1]]
            dropped.append(name)
    return kept, tuple(dropped)


@dataclass(frozen=True)
class SourceRecord:
    record_id: str
    source_code: str
    marketplace_id: str
    record_kind: RecordKind
    record_index: int
    raw_artifact_id: str
    raw_uri: str
    body_sha256: str
    crawl_run_id: str
    adapter_version: str
    extractor_version: str
    request_target: str
    request_page: int
    fetched_at: datetime
    http_status: int | None
    payload: Mapping[str, Any]
    dropped_fields: tuple[str, ...] = ()
    schema_version: str = RECORD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECORD_SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {RECORD_SCHEMA_VERSION}")
        for name in (
            "record_id",
            "source_code",
            "marketplace_id",
            "raw_artifact_id",
            "crawl_run_id",
            "adapter_version",
            "extractor_version",
            "request_target",
        ):
            object.__setattr__(self, name, v.require_text(getattr(self, name), name))
        object.__setattr__(self, "source_code", self.source_code.lower())
        if not isinstance(self.record_kind, RecordKind):
            raise ValueError("record_kind must be a RecordKind")
        v.non_bool_int(self.record_index, "record_index")
        v.non_bool_int(self.request_page, "request_page", minimum=1)
        object.__setattr__(self, "raw_uri", v.raw_uri(self.raw_uri))
        object.__setattr__(self, "body_sha256", v.sha256_hex(self.body_sha256, "body_sha256"))
        object.__setattr__(self, "fetched_at", v.aware_utc(self.fetched_at, "fetched_at"))
        if self.http_status is not None:
            v.http_status(self.http_status)
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be an object")
        object.__setattr__(self, "dropped_fields", tuple(sorted(set(self.dropped_fields))))
        expected = make_record_id(self.raw_artifact_id, self.record_kind, self.record_index)
        if self.record_id != expected:
            raise ValueError("record_id does not follow from raw_artifact_id, record_kind and record_index")

    @property
    def partition_key(self) -> str:
        """Kafka key: every record of one category page lands in one partition, in order."""
        return f"{self.source_code}|{self.request_target}"


def record_to_wire(record: SourceRecord) -> dict[str, Any]:
    wire = serialize_for_wire(record)
    wire["dropped_fields"] = list(record.dropped_fields)
    return wire


def record_json(record: SourceRecord) -> str:
    """The canonical one-line JSON of a record (sorted keys, no spaces)."""
    return json.dumps(record_to_wire(record), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


_WIRE_FIELDS = frozenset(
    {
        "schema_version",
        "record_id",
        "source_code",
        "marketplace_id",
        "record_kind",
        "record_index",
        "raw_artifact_id",
        "raw_uri",
        "body_sha256",
        "crawl_run_id",
        "adapter_version",
        "extractor_version",
        "request_target",
        "request_page",
        "fetched_at",
        "http_status",
        "payload",
        "dropped_fields",
    }
)


class RecordWireError(ValueError):
    """A decoded object is not a ``marketplace-record.v1``."""


def record_from_wire(wire: Any) -> SourceRecord:
    if not isinstance(wire, Mapping):
        raise RecordWireError("record must be an object")
    missing, unknown = sorted(_WIRE_FIELDS - set(wire)), sorted(set(wire) - _WIRE_FIELDS)
    if missing or unknown:
        raise RecordWireError(f"record fields: missing={missing} unknown={unknown}")
    try:
        fetched_at = datetime.fromisoformat(str(wire["fetched_at"]).replace("Z", "+00:00"))
        dropped = wire["dropped_fields"]
        if not isinstance(dropped, list):
            raise RecordWireError("dropped_fields must be a list")
        return SourceRecord(
            record_id=wire["record_id"],
            source_code=wire["source_code"],
            marketplace_id=wire["marketplace_id"],
            record_kind=RecordKind(wire["record_kind"]),
            record_index=wire["record_index"],
            raw_artifact_id=wire["raw_artifact_id"],
            raw_uri=wire["raw_uri"],
            body_sha256=wire["body_sha256"],
            crawl_run_id=wire["crawl_run_id"],
            adapter_version=wire["adapter_version"],
            extractor_version=wire["extractor_version"],
            request_target=wire["request_target"],
            request_page=wire["request_page"],
            fetched_at=fetched_at,
            http_status=wire["http_status"],
            payload=wire["payload"],
            dropped_fields=tuple(dropped),
            schema_version=wire["schema_version"],
        )
    except RecordWireError:
        raise
    except (ValueError, TypeError) as exc:
        raise RecordWireError(str(exc)) from exc


def build_records(
    spec: Any,
    *,
    request: Any,
    raw_artifact: Any,
    rows: Iterable[Mapping[str, Any]],
    page_context: Mapping[str, Any],
) -> tuple[SourceRecord, ...]:
    """Wrap an extracted page: the page-context record, then one record per row in display order.

    ``spec`` is the source's :class:`~ecomdata.domain.sources.SourceSpec`; its
    personal-data lists decide which top-level keys are dropped.
    """

    def make(kind: RecordKind, index: int, payload: Mapping[str, Any], pii: tuple[str, ...]) -> SourceRecord:
        kept, dropped = drop_fields(payload, pii)
        return SourceRecord(
            record_id=make_record_id(raw_artifact.raw_artifact_id, kind, index),
            source_code=spec.code,
            marketplace_id=spec.marketplace_id,
            record_kind=kind,
            record_index=index,
            raw_artifact_id=raw_artifact.raw_artifact_id,
            raw_uri=raw_artifact.raw_uri,
            body_sha256=raw_artifact.body_sha256,
            crawl_run_id=raw_artifact.crawl_run_id,
            adapter_version=raw_artifact.adapter_version,
            extractor_version=spec.extractor_version,
            request_target=request.target,
            request_page=request.page,
            fetched_at=raw_artifact.fetched_at,
            http_status=raw_artifact.http_status,
            payload=kept,
            dropped_fields=dropped,
        )

    records = [make(RecordKind.PAGE_CONTEXT, 0, page_context, tuple(spec.pii_context_fields))]
    records.extend(make(RecordKind.LISTING_ROW, index, row, tuple(spec.pii_row_fields)) for index, row in enumerate(rows))
    return tuple(records)
