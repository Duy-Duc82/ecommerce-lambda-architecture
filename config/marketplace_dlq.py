"""Versioned, evidence-preserving marketplace observation quarantine record."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from common.identity import deterministic_id
from urllib.parse import urlparse

MARKETPLACE_DLQ_SCHEMA_VERSION = "marketplace-observation-dlq.v1"

class DlqStage(str, Enum):
    DECODE = "DECODE"
    CONTRACT_VALIDATION = "CONTRACT_VALIDATION"

@dataclass(frozen=True)
class MarketplaceObservationDlqV1:
    dlq_id: str; schema_version: str; failed_at: datetime; stage: DlqStage
    source_topic: str; source_partition: int; source_offset: int; source_key: str | None
    marketplace: str | None; crawl_run_id: str | None; raw_artifact_id: str | None; raw_uri: str | None
    error_type: str; error_message: str; payload_text: str

    def __post_init__(self):
        if self.schema_version != MARKETPLACE_DLQ_SCHEMA_VERSION: raise ValueError("invalid DLQ schema version")
        if self.failed_at.tzinfo is None or self.failed_at.utcoffset() is None: raise ValueError("failed_at must be timezone-aware")
        if not isinstance(self.stage, DlqStage): raise ValueError("stage must be DlqStage")
        if self.source_partition < 0 or self.source_offset < 0: raise ValueError("source coordinates must be non-negative")
        if not self.source_topic.strip() or not self.error_type.strip() or not self.payload_text: raise ValueError("DLQ source/error/payload is required")
        if self.raw_uri is not None and urlparse(self.raw_uri).scheme.lower() not in {"s3", "s3a", "file"}: raise ValueError("raw_uri must use s3, s3a, or file scheme")
        if len(self.error_message) > 2000: object.__setattr__(self, "error_message", self.error_message[:2000])
        if len(self.payload_text.encode("utf-8")) > 1048576: object.__setattr__(self, "payload_text", self.payload_text.encode("utf-8")[:1048576].decode("utf-8", "ignore"))

def create_observation_dlq(*, failed_at, stage, source_topic, source_partition, source_offset, source_key, payload_text, error, marketplace=None, crawl_run_id=None, raw_artifact_id=None, raw_uri=None):
    if failed_at.tzinfo is None or failed_at.utcoffset() is None: raise ValueError("failed_at must be timezone-aware")
    return MarketplaceObservationDlqV1(deterministic_id("dlq", source_topic, source_partition, source_offset, stage), MARKETPLACE_DLQ_SCHEMA_VERSION, failed_at.astimezone(timezone.utc), stage, source_topic, source_partition, source_offset, source_key, marketplace, crawl_run_id, raw_artifact_id, raw_uri, type(error).__name__, str(error)[:2000], payload_text)
