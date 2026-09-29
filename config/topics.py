"""Central registry for marketplace Kafka topics."""
from dataclasses import dataclass

from config.settings import (
    KAFKA_MARKETPLACE_PARTITIONS,
    KAFKA_TOPIC_MARKETPLACE_CHANGES,
    KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS,
    KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS_DLQ,
)


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int = 1

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("topic name is required")
        if self.partitions <= 0 or self.replication_factor <= 0:
            raise ValueError("topic partitions and replication_factor must be positive")


MARKETPLACE_OBSERVATIONS = TopicSpec(KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS, KAFKA_MARKETPLACE_PARTITIONS)
MARKETPLACE_OBSERVATIONS_DLQ = TopicSpec(KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS_DLQ, KAFKA_MARKETPLACE_PARTITIONS)
MARKETPLACE_CHANGES = TopicSpec(KAFKA_TOPIC_MARKETPLACE_CHANGES, KAFKA_MARKETPLACE_PARTITIONS)
MARKETPLACE_TOPIC_SPECS = (MARKETPLACE_OBSERVATIONS, MARKETPLACE_OBSERVATIONS_DLQ, MARKETPLACE_CHANGES)
if len({topic.name for topic in MARKETPLACE_TOPIC_SPECS}) != len(MARKETPLACE_TOPIC_SPECS):
    raise ValueError("marketplace topic names must be unique")
