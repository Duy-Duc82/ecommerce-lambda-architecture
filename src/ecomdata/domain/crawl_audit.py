"""Where the crawl audit lives in the lake, and what one line of it holds.

The crawl worker keeps the audit's operational copy in PostgreSQL (the source
circuit breaker reads it, Grafana charts it). The copy of record is in the lake:
one gzip JSON-lines object per crawl run, written when the run finishes, under

``<CRAWL_AUDIT_ROOT>/observed_date=<started day>/hour=<HH>/crawl_run_id=<id>.jsonl.gz``

in the bronze zone. The object holds one ``run`` line (the run as it finished)
and one ``attempt`` line per request attempt. An attempt line carries the
marketplace code, the target and the resource type of its task itself, so the
batch reads the audit without the frontier (which stays PostgreSQL's: it is the
crawler's work queue, not data).

Pure data: no I/O here.
"""
from __future__ import annotations

CRAWL_AUDIT_ROOT = "marketplace/crawl_audit"
SCHEMA_VERSION = "marketplace-crawl-audit.v1"

KIND_RUN = "run"
KIND_ATTEMPT = "attempt"

# (name, Spark type) of every field a line may hold; a run line leaves the
# attempt fields null and the other way round.
RUN_FIELDS: tuple[tuple[str, str], ...] = (
    ("crawl_run_id", "string"),
    ("marketplace_id", "string"),
    ("started_at", "timestamp"),
    ("completed_at", "timestamp"),
    ("status", "string"),
    ("requested", "bigint"),
    ("succeeded", "bigint"),
    ("failed", "bigint"),
    ("adapter_version", "string"),
    ("error_summary", "string"),
)

ATTEMPT_FIELDS: tuple[tuple[str, str], ...] = (
    ("crawl_run_id", "string"),
    ("task_id", "string"),
    ("marketplace_code", "string"),
    ("request_target", "string"),
    ("resource_type", "string"),
    ("attempt_number", "int"),
    ("started_at", "timestamp"),
    ("completed_at", "timestamp"),
    ("status", "string"),
    ("http_status", "int"),
    ("latency_ms", "bigint"),
    ("raw_artifact_id", "string"),
    ("raw_uri", "string"),
    ("raw_bytes", "bigint"),
    ("parsed_count", "bigint"),
    ("rejected_count", "bigint"),
    ("error_kind", "string"),
    ("error_message", "string"),
)


def line_fields() -> tuple[tuple[str, str], ...]:
    """Every field of a line, in a fixed order: the kind and schema version first, then run and attempt fields."""
    seen: dict[str, str] = {"kind": "string", "schema_version": "string"}
    for name, dtype in RUN_FIELDS + ATTEMPT_FIELDS:
        if seen.setdefault(name, dtype) != dtype:
            raise ValueError(f"field {name} has two types")
    return tuple(seen.items())


def audit_path(started_day: str, hour: str, crawl_run_id: str) -> str:
    from urllib.parse import quote

    return (
        f"{CRAWL_AUDIT_ROOT}/observed_date={started_day}/hour={hour}/"
        f"crawl_run_id={quote(crawl_run_id, safe='-_.~')}.jsonl.gz"
    )


__all__ = [
    "ATTEMPT_FIELDS",
    "CRAWL_AUDIT_ROOT",
    "KIND_ATTEMPT",
    "KIND_RUN",
    "RUN_FIELDS",
    "SCHEMA_VERSION",
    "audit_path",
    "line_fields",
]
