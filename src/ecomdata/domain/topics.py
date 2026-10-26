"""The marketplace Kafka topics.

A topic name is part of a versioned contract (``.v1``), so it is a constant
here, not a setting. How many partitions a topic is created with is a
deployment choice: ``partitions`` is only the default, and ``ops
create-topics`` takes the configured count.
"""
from dataclasses import dataclass

DEFAULT_PARTITIONS = 3


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int = DEFAULT_PARTITIONS
    replication_factor: int = 1

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("topic name is required")
        if self.partitions <= 0 or self.replication_factor <= 0:
            raise ValueError("topic partitions and replication_factor must be positive")


# Source records (ELT envelope), published once a page's records file is in Bronze.
MARKETPLACE_RECORDS = TopicSpec("marketplace.records.v1")
MARKETPLACE_RECORDS_DLQ = TopicSpec("marketplace.records.v1.dlq")
# Canonical observations of the pre-ELT path; no producer since the ELT switch.
MARKETPLACE_OBSERVATIONS = TopicSpec("marketplace.observations.v1")
MARKETPLACE_OBSERVATIONS_DLQ = TopicSpec("marketplace.observations.v1.dlq")
MARKETPLACE_CHANGES = TopicSpec("marketplace.changes.v1")
MARKETPLACE_TOPIC_SPECS = (
    MARKETPLACE_RECORDS,
    MARKETPLACE_RECORDS_DLQ,
    MARKETPLACE_OBSERVATIONS,
    MARKETPLACE_OBSERVATIONS_DLQ,
    MARKETPLACE_CHANGES,
)
if len({topic.name for topic in MARKETPLACE_TOPIC_SPECS}) != len(MARKETPLACE_TOPIC_SPECS):
    raise ValueError("marketplace topic names must be unique")


def with_partitions(partitions: int) -> tuple[TopicSpec, ...]:
    """Every marketplace topic, with the configured partition count."""
    return tuple(TopicSpec(topic.name, partitions, topic.replication_factor) for topic in MARKETPLACE_TOPIC_SPECS)
