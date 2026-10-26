"""Topic registry tests against the brief section 11 topic design."""
import dataclasses

import pytest

from ecomdata.domain.topics import (
    MARKETPLACE_CHANGES,
    MARKETPLACE_OBSERVATIONS,
    MARKETPLACE_OBSERVATIONS_DLQ,
    MARKETPLACE_TOPIC_SPECS,
    TopicSpec,
    with_partitions,
)


def test_registry_declares_the_record_observation_and_change_topics():
    assert [spec.name for spec in MARKETPLACE_TOPIC_SPECS] == [
        "marketplace.records.v1",
        "marketplace.records.v1.dlq",
        "marketplace.observations.v1",
        "marketplace.observations.v1.dlq",
        "marketplace.changes.v1",
    ]


def test_registry_names_are_unique():
    names = {spec.name for spec in MARKETPLACE_TOPIC_SPECS}

    assert len(names) == len(MARKETPLACE_TOPIC_SPECS)


def test_every_topic_carries_the_configured_partition_count():
    for spec in with_partitions(5):
        assert spec.partitions == 5
        assert spec.replication_factor == 1
    assert [spec.name for spec in with_partitions(5)] == [spec.name for spec in MARKETPLACE_TOPIC_SPECS]


def test_registry_exposes_each_topic_individually():
    assert MARKETPLACE_OBSERVATIONS.name.endswith(".observations.v1")
    assert MARKETPLACE_OBSERVATIONS_DLQ.name.endswith(".dlq")
    assert MARKETPLACE_CHANGES.name.endswith(".changes.v1")


def test_topic_spec_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        MARKETPLACE_OBSERVATIONS.name = "other"


@pytest.mark.parametrize(
    "name, partitions, replication",
    [
        (" ", 1, 1),
        ("topic", 0, 1),
        ("topic", -1, 1),
        ("topic", 1, 0),
    ],
)
def test_topic_spec_rejects_invalid_values(name, partitions, replication):
    with pytest.raises(ValueError):
        TopicSpec(name, partitions, replication)
