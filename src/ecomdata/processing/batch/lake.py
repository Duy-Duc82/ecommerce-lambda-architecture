"""The Spark implementation of the lake port: Bronze records in, Silver and Gold out, as Parquet or Delta (``table_format``).

Datasets live under ``<silver root>/<name>/``,
``<gold root>/runs/run_id=<run>/<mart>/`` and, for the star, ``<gold root>/dw/<table>/``
(merged, not replaced: see ``merge_dw``). Writing a dataset replaces it (for a
partitioned dataset, the partitions it has rows for, because the session sets
dynamic partition overwrite), and a dataset that was written empty reads back
empty: reads carry an explicit schema, so a directory holding only ``_SUCCESS``
is not an error.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from ecomdata.domain.datasets import dataset
from ecomdata.processing.batch.context import DwWriteResult, GoldWriteResult, MarketplaceBatchContext
from ecomdata.processing.normalize import common


class SparkLakeIO:
    def __init__(
        self,
        spark_provider: Callable[[], SparkSession],
        *,
        records_uri: str,
        silver_root_uri: str,
        silver_schemas: Mapping[str, StructType] | None = None,
        table_format: str = "parquet",
    ) -> None:
        if table_format not in ("parquet", "delta"):
            raise ValueError(f"table_format must be parquet or delta, not {table_format!r}")
        self.table_format = table_format
        self._spark_provider = spark_provider
        self.records_uri = records_uri.rstrip("/")
        self.silver_root_uri = silver_root_uri.rstrip("/")
        self.silver_schemas = dict(silver_schemas or {})

    @property
    def spark(self) -> SparkSession:
        return self._spark_provider()

    def silver_uri(self, name: str) -> str:
        return f"{self.silver_root_uri}/{name}"

    def read_records(self, *, since_date: date | None = None) -> DataFrame:
        records = common.decode_records(self.spark.read.option("recursiveFileLookup", "true").text(self.records_uri))
        if since_date is not None:
            records = records.filter(F.to_date("fetched_at") >= F.lit(since_date))
        return records

    def write_silver(self, name: str, frame: DataFrame, *, partition_by: Sequence[str] = ()) -> str:
        uri = self.silver_uri(name)
        writer = frame.write.mode("overwrite")
        if partition_by:
            writer = writer.option("partitionOverwriteMode", "dynamic").partitionBy(*partition_by)
        writer.format(self.table_format).save(uri)
        return uri

    def read_silver(self, name: str) -> DataFrame:
        reader = self.spark.read
        schema = self.silver_schemas.get(name)
        if schema is not None:
            reader = reader.schema(schema)
        return reader.format(self.table_format).load(self.silver_uri(name))

    def write_gold(self, context: MarketplaceBatchContext, name: str, frame: DataFrame) -> GoldWriteResult:
        uri = context.gold_dataset_uri(name)
        # Static, whatever the session says: a run directory belongs wholly to this
        # run, so a rerun must replace it, not only the partitions it still has rows for.
        writer = frame.write.mode("overwrite").option("partitionOverwriteMode", "static")
        partitions = dataset(name).partition_columns
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.format(self.table_format).save(uri)
        return GoldWriteResult(name, uri, frame.count())

    def read_gold(self, context: MarketplaceBatchContext, name: str) -> DataFrame:
        return self.spark.read.format(self.table_format).load(context.gold_dataset_uri(name))

    def merge_dw(self, context: MarketplaceBatchContext, name: str, frame: DataFrame) -> DwWriteResult:
        entry = dataset(name)
        uri = context.dw_dataset_uri(name)
        frame = frame.select(*entry.columns)
        if self.table_format != "delta":
            self._create(frame, uri, entry.partition_columns, format_="parquet")
            return DwWriteResult(name, uri, self.read_dw(context, name).count(), None)
        current = self._existing_delta(uri)
        if current is None:
            self._create(frame, uri, entry.partition_columns, format_="delta")
        elif _shape(current.schema) != _shape(frame.schema):
            # The registry changed the table's columns: a MERGE cannot, so the
            # table is replaced as a new version and the old one stays readable.
            self._create(frame, uri, entry.partition_columns, format_="delta", overwrite_schema=True)
        else:
            self.spark.sql(merge_statement(uri, frame, entry.key, _source_view(frame, name)))
        history = self.spark.sql(f"DESCRIBE HISTORY delta.`{uri}` LIMIT 1").first()
        return DwWriteResult(name, uri, self.read_dw(context, name).count(), int(history["version"]))

    def read_dw(self, context: MarketplaceBatchContext, name: str) -> DataFrame:
        return self.spark.read.format(self.table_format).load(context.dw_dataset_uri(name))

    def _existing_delta(self, uri: str) -> DataFrame | None:
        from pyspark.errors import AnalysisException

        try:
            return self.spark.read.format("delta").load(uri)
        except AnalysisException:
            return None

    @staticmethod
    def _create(
        frame: DataFrame, uri: str, partitions: Sequence[str], *, format_: str, overwrite_schema: bool = False
    ) -> None:
        writer = frame.write.mode("overwrite").option("partitionOverwriteMode", "static")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.format(format_).save(uri)


def _shape(schema: StructType) -> list[tuple[str, str]]:
    # Nullability differs between a built frame and the stored table; names and types decide.
    return [(field.name, field.dataType.simpleString()) for field in schema.fields]


def _source_view(frame: DataFrame, name: str) -> str:
    view = f"_dw_merge_source_{name}"
    frame.createOrReplaceTempView(view)
    return view


def merge_statement(uri: str, frame: DataFrame, key: Sequence[str], source: str) -> str:
    """The MERGE that makes the Delta table at ``uri`` hold exactly ``source``, matched on ``key``.

    A matched row is rewritten only if a column changed (compared null-safely),
    so a rerun on the same data rewrites nothing; a key the source no longer has
    is deleted, so the table never keeps a row the builders did not produce.
    """
    on = " AND ".join(f"t.`{column}` <=> s.`{column}`" for column in key)
    values = [column for column in frame.columns if column not in key]
    changed = " OR ".join(f"NOT (t.`{column}` <=> s.`{column}`)" for column in values) or "FALSE"
    return (
        f"MERGE INTO delta.`{uri}` AS t USING {source} AS s ON {on} "
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT * "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    )


def read_crawl_audit(spark: SparkSession, jdbc_url: str, properties: Mapping[str, str]) -> tuple[DataFrame, DataFrame]:
    """The crawl audit tables the marts and the quality gates read: attempts (with their marketplace code) and runs."""
    from ecomdata.processing.batch.marts import attach_marketplace_code

    props = dict(properties)
    attempts = spark.read.jdbc(jdbc_url, "audit.crawl_request_attempt", properties=props)
    runs = spark.read.jdbc(jdbc_url, "audit.crawl_run", properties=props)
    # Neither attempt nor run rows name a marketplace code; the frontier does.
    frontier = spark.read.jdbc(jdbc_url, "audit.crawl_frontier", properties=props)
    return attach_marketplace_code(attempts, frontier), runs


def silver_schemas() -> dict[str, StructType]:
    """Explicit read schemas of the Silver datasets later stages read back."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        TimestampType,
    )

    spark_types = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "boolean": BooleanType(),
        "array<string>": ArrayType(StringType()),
        common.MONEY: DecimalType(38, 6),
    }

    from ecomdata.processing.batch.stages.silver import OBSERVATIONS, QUARANTINE

    def declared(columns: Sequence[tuple[str, str]]) -> StructType:
        # Built from the types, not parsed from DDL: parsing needs a running JVM.
        return StructType([StructField(name, spark_types[dtype]) for name, dtype in columns])

    quarantine = StructType(
        [
            StructField("record_id", StringType()),
            StructField("source_code", StringType()),
            StructField("raw_artifact_id", StringType()),
            StructField("crawl_run_id", StringType()),
            StructField("request_target", StringType()),
            StructField("request_page", IntegerType()),
            StructField("fetched_at", TimestampType()),
            StructField("reason", StringType()),
            StructField("payload_json", StringType()),
        ]
    )
    return {OBSERVATIONS: declared(common.CANONICAL_COLUMNS), QUARANTINE: quarantine}


__all__ = ["SparkLakeIO", "merge_statement", "read_crawl_audit", "silver_schemas"]
