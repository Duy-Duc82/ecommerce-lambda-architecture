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

# Delta Lake on a session this project builds: the SQL extension and the catalog
# that makes ``format("delta")`` and ``MERGE`` work. Databricks sets both itself.
DELTA_SESSION_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


@dataclass(frozen=True)
class SparkSessionFactory:
    app_name: str
    shuffle_partitions: int = 8
    driver_memory: str | None = "3g"
    master: str | None = None
    hadoop_options: Mapping[str, str] = field(default_factory=dict)
    packages: str | None = None
    extra: Mapping[str, str] = field(default_factory=dict)
    table_format: str = "parquet"
    # "databricks": a session already exists and its configuration belongs to the
    # cluster (a serverless session rejects most settings), so none is applied.
    runtime: str = "local"

    @classmethod
    def for_storage(cls, storage: StorageTarget, **kwargs: Any) -> SparkSessionFactory:
        return cls(hadoop_options=spark_hadoop_options(storage), table_format=storage.table_format, **kwargs)

    def config(self) -> dict[str, str]:
        """Everything the builder is given, as plain strings (testable without Spark)."""
        if self.runtime == "databricks":
            return {}
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
        if self.table_format == "delta":
            conf.update(DELTA_SESSION_CONFIG)
        conf.update({key: str(value) for key, value in self.extra.items()})
        return conf

    def create(self) -> Any:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder
        if self.runtime == "databricks":
            return builder.getOrCreate()
        if self.master:
            builder = builder.master(self.master)
        for key, value in self.config().items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
