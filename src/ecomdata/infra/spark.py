"""The one place a SparkSession is configured.

Every job (batch stages, speed query, tools) asks :class:`SparkSessionFactory`
for its session, so UTC, shuffle partitions, driver memory, S3A credentials
and the Kafka connector are set the same way everywhere.

``spark.driver.memory`` only takes effect if it is set before the JVM starts,
which is the case when the session is created from Python (``python -m``):
pyspark launches the JVM with this configuration.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ecomdata.infra.storage import StorageTarget, spark_hadoop_options


@dataclass(frozen=True)
class SparkSessionFactory:
    app_name: str
    shuffle_partitions: int = 8
    driver_memory: str | None = "3g"
    master: str | None = None
    hadoop_options: Mapping[str, str] = field(default_factory=dict)
    packages: str | None = None
    extra: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def for_storage(cls, storage: StorageTarget, **kwargs: Any) -> SparkSessionFactory:
        return cls(hadoop_options=spark_hadoop_options(storage), **kwargs)

    def config(self) -> dict[str, str]:
        """Everything the builder is given, as plain strings (testable without Spark)."""
        conf = {
            "spark.app.name": self.app_name,
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.ui.showConsoleProgress": "false",
        }
        if self.driver_memory:
            conf["spark.driver.memory"] = self.driver_memory
        if self.packages:
            conf["spark.jars.packages"] = self.packages
        for key, value in self.hadoop_options.items():
            conf[f"spark.hadoop.{key}"] = value
        conf.update({key: str(value) for key, value in self.extra.items()})
        return conf

    def create(self) -> Any:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder
        if self.master:
            builder = builder.master(self.master)
        for key, value in self.config().items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
