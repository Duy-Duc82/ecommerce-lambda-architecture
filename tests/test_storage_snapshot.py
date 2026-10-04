"""Daily storage snapshot — Phase 9 plan section 6.3. Offline: fakes only."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

from ops import storage
from ops.storage import Measure

NOW = datetime(2026, 10, 4, 23, 30, tzinfo=timezone.utc)


class Table:
    """audit.storage_snapshot with its primary key."""

    def __init__(self):
        self.rows = {}

    def factory(self):
        table = self

        class Cursor:
            rowcount = 0

            def execute(self, sql, params):
                assert "ON CONFLICT (snapshot_date, component, scope) DO NOTHING" in sql
                key = (params[0], params[2], params[3])
                self.rowcount = 0 if key in table.rows else 1
                table.rows.setdefault(key, params)

        @contextmanager
        def connection():
            @contextmanager
            def cursor():
                yield Cursor()
            yield SimpleNamespace(cursor=cursor)

        return connection


def test_a_minio_scope_is_a_dataset_never_a_partition():
    assert storage.minio_scope("ecommerce-silver",
                               "marketplace/offer_observations/marketplace=tiki/observed_date=2026-10-04/x.json") \
        == "ecommerce-silver/marketplace/offer_observations"
    assert storage.minio_scope("ecommerce-bronze", "tiki/listing=1846/raw.json") == "ecommerce-bronze/tiki"
    assert storage.minio_scope("ecommerce-gold", "top.json") == "ecommerce-gold"


def test_minio_is_summed_per_scope():
    measures = storage.measure_minio({
        "ecommerce-silver": [("marketplace/offer_observations/marketplace=tiki/a.json", 100),
                             ("marketplace/offer_observations/marketplace=tiki/b.json", 50),
                             ("quarantine/offer_observations/observed_date=2026-10-04/c.json", 7)],
    })

    assert measures == [Measure("minio", "ecommerce-silver/marketplace/offer_observations", 150, 2),
                        Measure("minio", "ecommerce-silver/quarantine/offer_observations", 7, 1)]


def test_postgres_is_per_schema_with_a_row_estimate():
    assert storage.measure_postgres([("audit", 81920, 476.0), ("cache", 16384, 0.0)]) == [
        Measure("postgres", "audit", 81920, 476), Measure("postgres", "cache", 16384, 0)]


def test_elasticsearch_leaves_out_its_own_indices():
    stats = {"indices": {
        "marketplace-changes-v1": {"total": {"store": {"size_in_bytes": 4096}, "docs": {"count": 600}}},
        ".kibana_8.18.1_001": {"total": {"store": {"size_in_bytes": 999}, "docs": {"count": 9}}},
    }}

    assert storage.measure_elasticsearch(stats) == [Measure("elasticsearch", "marketplace-changes-v1", 4096, 600)]


def test_kafka_sums_partition_directories_per_topic():
    dirs = [("marketplace.observations.v1-0", 1000), ("marketplace.observations.v1-1", 500),
            ("__consumer_offsets-7", 30), ("marketplace.changes.v1-0.abc123-delete", 10**9),
            ("meta.properties", 90)]

    assert storage.measure_kafka(dirs) == [Measure("kafka", "__consumer_offsets", 30, 1),
                                           Measure("kafka", "marketplace.observations.v1", 1500, 2)]


def test_a_partition_directory_is_sized_by_its_files(tmp_path):
    partition = tmp_path / "marketplace.observations.v1-0"
    partition.mkdir()
    (partition / "00000000000000000000.log").write_bytes(b"x" * 300)
    (partition / "00000000000000000000.index").write_bytes(b"x" * 20)
    (tmp_path / "meta.properties").write_text("version=1")

    apparent = lambda path: path.stat().st_size  # noqa: E731 - st_blocks differs per filesystem

    assert storage.kafka_partition_dirs(tmp_path, size=apparent) == [("marketplace.observations.v1-0", 320)]


def test_a_second_snapshot_on_the_same_date_writes_nothing():
    table = Table()
    collectors = {"postgres": lambda: [Measure("postgres", "audit", 10, 1)],
                  "kafka": lambda: [Measure("kafka", "t", 5, 1)]}

    first = storage.take_snapshot(collectors, table.factory(), now=NOW)
    second = storage.take_snapshot(collectors, table.factory(), now=NOW.replace(hour=23, minute=59))

    assert (first["written"], second["written"]) == (2, 0)
    assert set(table.rows) == {(date(2026, 10, 4), "postgres", "audit"), (date(2026, 10, 4), "kafka", "t")}


def test_one_unreadable_store_does_not_cost_the_others_their_rows():
    table = Table()

    def broken():
        raise ConnectionError("kafka down")

    report = storage.take_snapshot({"kafka": broken, "postgres": lambda: [Measure("postgres", "audit", 10, 1)]},
                                   table.factory(), now=NOW)

    assert report["written"] == 1
    assert report["failed"] == {"kafka": "ConnectionError: kafka down"}


def test_the_daily_snapshot_runs_once_per_utc_date_and_retries_a_total_failure():
    calls = []
    outcomes = iter([{"measured": 0}, {"measured": 3}, {"measured": 3}])
    daily = storage.DailySnapshot(lambda now: calls.append(now) or next(outcomes))

    daily.maybe(NOW)                               # nothing measured: retried
    daily.maybe(NOW.replace(minute=31))
    daily.maybe(NOW.replace(minute=59))            # same date: skipped
    daily.maybe(datetime(2026, 10, 5, 0, 1, tzinfo=timezone.utc))  # a new date: taken

    assert [c.minute for c in calls] == [30, 31, 1]
