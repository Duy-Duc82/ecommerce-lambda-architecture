"""AC5: one definition of each shared piece, found by scanning the source."""
from __future__ import annotations

import re

from tests.paths import REPO_ROOT

SRC = REPO_ROOT / "src" / "ecomdata"


def _matches(pattern: str) -> list[str]:
    regex = re.compile(pattern, re.MULTILINE)
    return [
        f"{path.relative_to(SRC).as_posix()}"
        for path in sorted(SRC.rglob("*.py"))
        if regex.search(path.read_text(encoding="utf-8"))
    ]


def test_one_source_registry_class():
    assert _matches(r"^class SourceRegistry\b") == ["domain/sources.py"]


def test_one_spark_session_builder():
    assert _matches(r"SparkSession\.builder") == ["infra/spark.py"]


def test_one_jdbc_url_builder():
    assert _matches(r"jdbc:postgresql://") == ["infra/postgres.py"]
    assert _matches(r"^def jdbc_url\b") == ["infra/postgres.py"]


def test_one_kafka_producer_factory():
    assert _matches(r"KafkaProducer\(") == ["infra/kafka.py"]


def test_one_psycopg2_connect():
    assert _matches(r"psycopg2\.connect\(") == ["infra/postgres.py"]


def test_the_value_check_helpers_are_defined_once():
    helpers = r"^def (_?require_text|_?require_aware|_?aware_utc|_?utc|_?non_bool_int)\("
    assert _matches(helpers) == ["domain/validation.py"]


def test_one_ordering_rule():
    assert _matches(r"^ORDER_COLUMNS\b") == ["domain/ordering.py"]
