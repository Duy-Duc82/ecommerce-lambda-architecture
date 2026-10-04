"""Phase 8 plan section 11.2: the operational Elasticsearch projector (tests 27-28).

Offline throughout. The projector reaches PostgreSQL, Redis and Kafka only
through its ``sources``, and Elasticsearch only through ``bulk``; both are
fakes here.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from ops import es_projector as p

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)
OVERLAP = 900


def _attempt_row(attempt_id=417, *, completed_at=NOW - timedelta(minutes=5), status="SUCCEEDED",
                 error_kind=None, parsed_count=7):
    return (attempt_id, "crawl-1", "task-1", "tiki", "LISTING_PAGE", 1,
            completed_at - timedelta(seconds=2), completed_at, status, 200,
            1_234, "raw-1", "s3a://ecommerce-bronze/raw/a.bin", 2_048, parsed_count,
            0, error_kind, None)


def _batch_row(batch_id=3, *, query_id="q-1", completed_at=NOW - timedelta(minutes=2), status="SUCCEEDED"):
    started = completed_at - timedelta(milliseconds=1500) if completed_at else NOW
    return ("marketplace-speed-v1", query_id, batch_id, status, started, completed_at,
            9, 0, 7, 1, 1, 2, 2, 9, 9, None)


def _source_row(code="tiki", *, opened_until=None, consecutive_failures=0):
    return (code, consecutive_failures, opened_until, NOW - timedelta(hours=3),
            NOW - timedelta(minutes=5), NOW - timedelta(minutes=5))


def _dlq_record(dlq_id="dlq-1"):
    return {"dlq_id": dlq_id, "schema_version": "marketplace-observation-dlq.v1",
            "failed_at": (NOW - timedelta(minutes=1)).isoformat(), "stage": "DECODE",
            "source_topic": "marketplace.observations.v1", "source_partition": 1, "source_offset": 42,
            "source_key": "tiki|L-1", "marketplace": "tiki", "crawl_run_id": "crawl-1",
            "raw_artifact_id": "raw-1", "raw_uri": "s3a://ecommerce-bronze/raw/a.bin",
            "error_type": "JSONDecodeError", "error_message": "line 1",
            # Never projected: up to a megabyte of raw response body.
            "payload_text": "{" * 50}


class FakeSources:
    """A healthy stack with one of each row, and a record of every query made."""

    def __init__(self, *, sources=None, attempts=None, batches=None, dlq=None):
        self.sources = [_source_row()] if sources is None else sources
        self.attempts = [_attempt_row()] if attempts is None else attempts
        self.batches = [_batch_row()] if batches is None else batches
        self.dlq_records = [_dlq_record()] if dlq is None else dlq
        self.redis = {"rt:source:tiki:last_observation": (NOW - timedelta(minutes=4)).isoformat()}
        self.queries: list[tuple[str, tuple]] = []
        self.commits = 0
        self.drained_from_beginning: list[bool] = []

    def query(self, sql, params=()):
        self.queries.append((sql, tuple(params)))
        if "crawl_source_state" in sql:
            return list(self.sources)
        if "crawl_request_attempt" in sql:
            return list(self.attempts)
        if "marketplace_speed_batch" in sql:
            return list(self.batches)
        raise AssertionError(f"unexpected query: {sql}")

    def redis_get(self, key):
        return self.redis.get(key)

    def drain_dlq(self, *, from_beginning=False):
        self.drained_from_beginning.append(from_beginning)
        records, self.dlq_records = self.dlq_records, []
        return records

    def commit_dlq(self):
        self.commits += 1


class FakeBulk:
    def __init__(self):
        self.bodies: list[list[dict]] = []

    def __call__(self, operations):
        self.bodies.append(operations)


def _projector(sources, bulk, *, clock=lambda: NOW, watermark=None):
    return p.Projector(sources=sources, bulk=bulk, overlap_seconds=OVERLAP, clock=clock,
                       watermark=watermark)


def _actions(body):
    """The bulk body as (index, _id, document) triples."""
    return [(body[i]["index"]["_index"], body[i]["index"]["_id"], body[i + 1])
            for i in range(0, len(body), 2)]


# --- test 27: deterministic ids, identical bulk bodies -----------------------

def test_every_document_carries_the_documented_deterministic_id():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    by_index = {index: (doc_id, doc) for index, doc_id, doc in _actions(bulk.bodies[0])}

    assert by_index["marketplace-source-health-v1"][0] == "tiki"
    assert by_index["marketplace-crawl-attempts-v1"][0] == "417"
    # query_name:query_id:batch_id — the audit table's primary key. Without
    # query_id a replay's batch 0 would overwrite the previous run's batch 0.
    assert by_index["marketplace-speed-batches-v1"][0] == "marketplace-speed-v1:q-1:3"
    assert by_index["marketplace-dlq-v1"][0] == "dlq-1"


def test_two_passes_over_the_same_rows_build_identical_bulk_bodies():
    first_sources, first_bulk = FakeSources(), FakeBulk()
    _projector(first_sources, first_bulk).project_once()
    # A second projector over the same rows: a restart re-reads the overlap
    # window and must rewrite the same documents, not new ones.
    second_sources, second_bulk = FakeSources(), FakeBulk()
    _projector(second_sources, second_bulk).project_once()

    assert first_bulk.bodies == second_bulk.bodies


def test_a_later_clock_changes_only_the_clock_derived_fields():
    stable = {}
    for clock_now in (NOW, NOW + timedelta(minutes=10)):
        sources, bulk = FakeSources(), FakeBulk()
        _projector(sources, bulk, clock=lambda now=clock_now: now).project_once()
        for index, doc_id, document in _actions(bulk.bodies[0]):
            stable.setdefault((index, doc_id), []).append(
                {k: v for k, v in document.items() if k not in ("freshness_seconds", "projected_at")})

    for (index, doc_id), versions in stable.items():
        assert versions[0] == versions[1], f"{index}/{doc_id} is not stable across clocks"


def test_the_dlq_document_leaves_the_raw_payload_behind():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    document = next(doc for index, _, doc in _actions(bulk.bodies[0]) if index == "marketplace-dlq-v1")

    assert "payload_text" not in document
    assert document["raw_uri"] == "s3a://ecommerce-bronze/raw/a.bin"


def test_the_circuit_is_open_only_while_opened_until_is_still_ahead():
    open_sources = FakeSources(sources=[_source_row(opened_until=NOW + timedelta(minutes=5),
                                                    consecutive_failures=5)])
    closed_sources = FakeSources(sources=[_source_row(opened_until=NOW - timedelta(minutes=5),
                                                      consecutive_failures=5)])
    documents = []
    for sources in (open_sources, closed_sources):
        bulk = FakeBulk()
        _projector(sources, bulk).project_once()
        documents.append(next(doc for index, _, doc in _actions(bulk.bodies[0])
                              if index == "marketplace-source-health-v1"))

    assert [doc["circuit_open"] for doc in documents] == [True, False]
    # Both rows carry the same failure count: the verdict comes from the
    # instant, never from "has it failed a lot".
    assert {doc["consecutive_failures"] for doc in documents} == {5}


def test_freshness_is_the_gap_between_now_and_the_sources_last_observation():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    document = next(doc for index, _, doc in _actions(bulk.bodies[0])
                    if index == "marketplace-source-health-v1")

    assert document["freshness_seconds"] == 240


def test_a_source_that_never_observed_anything_has_no_freshness():
    sources = FakeSources()
    sources.redis = {}
    bulk = FakeBulk()
    _projector(sources, bulk).project_once()

    document = next(doc for index, _, doc in _actions(bulk.bodies[0])
                    if index == "marketplace-source-health-v1")

    # Not zero, and not "now": nothing is known, and the panel must show that.
    assert document["last_observation_at"] is None and document["freshness_seconds"] is None


def test_micro_batch_duration_is_completed_minus_started():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    document = next(doc for index, _, doc in _actions(bulk.bodies[0])
                    if index == "marketplace-speed-batches-v1")

    assert document["duration_ms"] == 1500


def test_a_running_batch_is_projected_without_a_duration():
    sources = FakeSources(batches=[_batch_row(completed_at=None, status="RUNNING")])
    bulk = FakeBulk()
    _projector(sources, bulk).project_once()

    document = next(doc for index, _, doc in _actions(bulk.bodies[0])
                    if index == "marketplace-speed-batches-v1")

    assert document["status"] == "RUNNING"
    assert document["completed_at"] is None and document["duration_ms"] is None


def test_a_dlq_record_without_an_id_is_refused_rather_than_given_one():
    record = _dlq_record()
    del record["dlq_id"]

    with pytest.raises(ValueError, match="deterministic document id"):
        p.dlq_document(record)


# --- test 28: the overlap window ---------------------------------------------

def _since_params(sources, fragment):
    return [params[0] for sql, params in sources.queries if fragment in sql and params]


def test_the_first_pass_reads_back_one_overlap_from_the_clock():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    # A restart keeps no watermark, so the first pass must re-read the window
    # the previous process was working on.
    assert _since_params(sources, "crawl_request_attempt") == [NOW - timedelta(seconds=OVERLAP)]
    assert _since_params(sources, "marketplace_speed_batch") == [NOW - timedelta(seconds=OVERLAP)]


def test_each_pass_re_reads_the_overlap_behind_the_newest_row_it_saw():
    sources, bulk = FakeSources(), FakeBulk()
    # Started two hours ago, so what moves the watermark is the data, not the
    # clock it was seeded with.
    projector = _projector(sources, bulk, watermark=NOW - timedelta(hours=2))
    projector.project_once()
    newest = NOW - timedelta(minutes=2)          # the speed batch's completed_at
    assert projector.watermark == newest

    sources.queries.clear()
    projector.project_once()

    assert _since_params(sources, "crawl_request_attempt") == [newest - timedelta(seconds=OVERLAP)]


def test_the_watermark_never_moves_backwards():
    sources = FakeSources(attempts=[_attempt_row(completed_at=NOW - timedelta(hours=6))], batches=[])
    projector = _projector(sources, FakeBulk())

    projector.project_once()

    # An old row re-read inside the overlap must not drag the window back to
    # where it was six hours ago and re-project everything since.
    assert projector.watermark == NOW


def test_a_row_updated_after_it_was_projected_is_re_read_by_the_next_pass():
    completed = NOW - timedelta(minutes=3)
    sources = FakeSources(attempts=[_attempt_row(status="PARTIAL", completed_at=completed)],
                          batches=[], dlq=[])
    bulk = FakeBulk()
    projector = _projector(sources, bulk)
    projector.project_once()

    # The same attempt, settled differently, still inside the overlap.
    sources.attempts = [_attempt_row(status="FAILED", error_kind="PARSE_ERROR", completed_at=completed)]
    projector.project_once()

    first, second = (next(doc for index, _, doc in _actions(body) if index == "marketplace-crawl-attempts-v1")
                     for body in bulk.bodies)
    assert first["status"] == "PARTIAL" and second["status"] == "FAILED"
    # Same id, so the second projection overwrites the first rather than
    # leaving two documents for one attempt.
    ids = {doc_id for body in bulk.bodies for index, doc_id, _ in _actions(body)
           if index == "marketplace-crawl-attempts-v1"}
    assert ids == {"417"}


def test_a_pass_that_saw_nothing_leaves_the_watermark_where_it_was():
    sources = FakeSources(attempts=[], batches=[], dlq=[])
    projector = _projector(sources, FakeBulk(), watermark=NOW - timedelta(hours=1))
    projector.project_once()

    # Moving it to "now" would skip the window whose rows had not landed yet.
    assert projector.watermark == NOW - timedelta(hours=1)


def test_rebuild_drops_the_window_and_rewinds_the_dlq_for_one_pass_only():
    sources, bulk = FakeSources(), FakeBulk()
    projector = _projector(sources, bulk)
    projector.project_once(rebuild=True)

    assert _since_params(sources, "crawl_request_attempt") == []
    assert sources.drained_from_beginning == [True]

    sources.queries.clear()
    projector.project_once()

    assert _since_params(sources, "crawl_request_attempt") != []
    assert sources.drained_from_beginning == [True, False]


def test_source_health_is_projected_whole_every_pass_not_by_watermark():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    # One row per marketplace, rewritten each pass: a watermark on it would
    # leave a quiet source's row frozen and its freshness wrong.
    health_queries = [sql for sql, _ in sources.queries if "crawl_source_state" in sql]
    assert health_queries == [p.SOURCE_HEALTH_SQL] and "WHERE" not in p.SOURCE_HEALTH_SQL


# --- the DLQ offsets ---------------------------------------------------------

def test_the_dlq_offsets_are_committed_only_after_the_records_are_indexed():
    order: list[str] = []
    sources = FakeSources()
    sources.commit_dlq = lambda: order.append("commit")  # type: ignore[method-assign]
    _projector(sources, lambda operations: order.append("bulk")).project_once()

    assert order == ["bulk", "commit"]


def test_a_failed_bulk_leaves_the_dlq_offsets_uncommitted():
    sources = FakeSources()

    def explode(operations):
        raise RuntimeError("Elasticsearch item failure (1 item(s)): mapper_parsing_exception: ")

    with pytest.raises(RuntimeError, match="item failure"):
        _projector(sources, explode).project_once()

    # Uncommitted, so the next pass re-reads the record rather than losing it.
    assert sources.commits == 0


def test_a_pass_with_no_dlq_record_commits_nothing():
    sources = FakeSources(dlq=[])
    _projector(sources, FakeBulk()).project_once()

    assert sources.commits == 0


def test_a_pass_with_nothing_at_all_writes_no_bulk_request():
    sources = FakeSources(sources=[], attempts=[], batches=[], dlq=[])
    bulk = FakeBulk()
    _projector(sources, bulk).project_once()

    assert bulk.bodies == []


# --- the bulk writer ---------------------------------------------------------

def test_an_item_level_elasticsearch_failure_raises_rather_than_counting_green():
    class FakeEs:
        def bulk(self, operations):
            return {"errors": True, "items": [{"index": {"status": 400, "error": {
                "type": "strict_dynamic_mapping_exception", "reason": "mapping set to strict"}}}]}

    with pytest.raises(RuntimeError, match="strict_dynamic_mapping_exception"):
        p.elasticsearch_bulk(FakeEs())([{"index": {"_index": "x", "_id": "1"}}, {}])


def test_a_clean_bulk_response_raises_nothing():
    class FakeEs:
        def bulk(self, operations):
            return {"errors": False, "items": [{"index": {"status": 200}}]}

    p.elasticsearch_bulk(FakeEs())([{"index": {"_index": "x", "_id": "1"}}, {}])


# --- the service entry point -------------------------------------------------

def test_the_projector_module_answers_help():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "ops.es_projector", "--help"],
                            capture_output=True, text=True, timeout=120)

    assert result.returncode == 0
    assert "--rebuild" in result.stdout and "--once" in result.stdout


def test_an_overlap_that_is_not_positive_is_refused():
    with pytest.raises(ValueError, match="overlap_seconds must be positive"):
        p.Projector(sources=FakeSources(), bulk=FakeBulk(), overlap_seconds=0)


def test_the_configured_overlap_exceeds_the_configured_interval():
    from config.settings import (
        MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS, MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS,
    )

    # Otherwise a pass that failed leaves a window nothing ever re-reads.
    assert MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS > MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS


def test_the_documents_carry_only_fields_the_strict_templates_declare():
    from display.kibana.marketplace_index_templates import INDEX_TEMPLATES, PROJECTOR_TEMPLATES

    declared = {}
    for name in PROJECTOR_TEMPLATES:
        pattern = INDEX_TEMPLATES[name]["index_patterns"][0]
        declared[pattern] = set(INDEX_TEMPLATES[name]["template"]["mappings"]["properties"])

    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    for index, _, document in _actions(bulk.bodies[0]):
        pattern = next(p for p in declared if index.startswith(p[:-1]))
        # dynamic: strict — a field the template does not declare is a 400,
        # not a new column, so the whole pass would fail on it.
        assert set(document) <= declared[pattern], f"{index} writes undeclared fields"


def test_every_projected_instant_is_utc_text_elasticsearch_accepts():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()

    for _, _, document in _actions(bulk.bodies[0]):
        for key, value in document.items():
            if key.endswith("_at") and value is not None:
                assert value.endswith("Z"), f"{key} is not UTC text: {value}"
                datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_a_naive_timestamp_from_the_driver_is_read_as_utc():
    naive = (NOW - timedelta(minutes=5)).replace(tzinfo=None)
    sources = FakeSources(attempts=[_attempt_row(completed_at=naive)], batches=[], dlq=[])
    bulk = FakeBulk()
    _projector(sources, bulk).project_once()

    document = next(doc for index, _, doc in _actions(bulk.bodies[0])
                    if index == "marketplace-crawl-attempts-v1")

    assert document["completed_at"] == "2026-10-04T11:55:00Z"


def test_the_bulk_body_alternates_an_action_line_and_a_document():
    sources, bulk = FakeSources(), FakeBulk()
    _projector(sources, bulk).project_once()
    body = bulk.bodies[0]

    assert len(body) % 2 == 0
    for index in range(0, len(body), 2):
        assert set(body[index]) == {"index"}
        assert set(body[index]["index"]) == {"_index", "_id"}
        # And the document must survive json serialisation, which is what the
        # client does to it.
        json.dumps(body[index + 1])
