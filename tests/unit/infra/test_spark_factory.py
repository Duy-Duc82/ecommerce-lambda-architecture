"""The one SparkSession factory: UTC, shuffle partitions, driver memory, S3A."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from ecomdata.infra import spark as spark_module
from ecomdata.infra.spark import SparkSessionFactory
from ecomdata.settings import load_settings


def factory_for(env):
    return SparkSessionFactory.for_storage(load_settings(env).storage, app_name="test-app", shuffle_partitions=5)


def test_every_session_is_pinned_to_utc_with_explicit_memory_and_partitions():
    conf = factory_for({}).config()

    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.sql.shuffle.partitions"] == "5"
    assert conf["spark.driver.memory"] == "3g"
    assert conf["spark.sql.sources.partitionOverwriteMode"] == "dynamic"
    assert conf["spark.app.name"] == "test-app"


def test_the_local_profile_adds_no_s3a_option():
    assert not [key for key in factory_for({}).config() if "fs.s3a" in key]


def test_a_minio_profile_adds_the_s3a_options():
    env = {"DATA_LAKE_PROFILE": "minio", "MINIO_ENDPOINT": "minio:9000"}
    conf = factory_for(env).config()

    assert conf["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"


def test_the_kafka_package_and_master_reach_the_builder(monkeypatch):
    class Builder:
        def __init__(self):
            self.options, self.master_url = {}, None

        def master(self, url):
            self.master_url = url
            return self

        def config(self, key, value):
            self.options[key] = value
            return self

        def getOrCreate(self):
            return self

    builder = Builder()
    import pyspark.sql

    monkeypatch.setattr(pyspark.sql, "SparkSession", SimpleNamespace(builder=builder))
    session = SparkSessionFactory(app_name="x", packages="org.apache.spark:kafka:1", master="local[1]").create()

    assert session is builder
    assert builder.master_url == "local[1]"
    assert builder.options["spark.jars.packages"] == "org.apache.spark:kafka:1"


def test_the_factory_is_the_only_place_that_builds_a_session():
    import pathlib

    root = pathlib.Path(spark_module.__file__).parents[1]
    offenders = [
        str(path.relative_to(root))
        for path in root.rglob("*.py")
        if "SparkSession.builder" in path.read_text(encoding="utf-8") and path.name != "spark.py"
    ]
    assert offenders == []


def test_settings_default_driver_memory_is_three_gigabytes():
    assert load_settings({}).spark.driver_memory == "3g"
    with pytest.raises(ValueError, match="SPARK_DRIVER_MEMORY"):
        load_settings({"SPARK_DRIVER_MEMORY": " "})


# --- Delta Lake and the Databricks runtime -------------------------------------------------------
def test_the_default_table_format_is_parquet_and_adds_no_delta_option():
    settings = load_settings({})

    assert settings.storage.table_format == "parquet"
    assert "spark.sql.extensions" not in factory_for({}).config()


def test_delta_adds_the_extension_and_the_catalog_to_a_session_this_project_builds():
    conf = factory_for({"DATA_LAKE_TABLE_FORMAT": "delta"}).config()

    assert conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert conf["spark.sql.catalog.spark_catalog"] == "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def test_an_unknown_table_format_or_runtime_is_refused():
    from ecomdata.settings import SettingsError

    with pytest.raises(SettingsError, match="DATA_LAKE_TABLE_FORMAT"):
        load_settings({"DATA_LAKE_TABLE_FORMAT": "iceberg"})
    with pytest.raises(SettingsError, match="SPARK_RUNTIME"):
        load_settings({"SPARK_RUNTIME": "emr"})


def test_on_databricks_the_factory_sets_nothing_and_takes_the_platform_session(monkeypatch):
    import pyspark.sql

    class Builder:
        def __init__(self):
            self.touched = []

        def master(self, url):
            self.touched.append("master")
            return self

        def config(self, key, value):
            self.touched.append(key)
            return self

        def getOrCreate(self):
            return "platform-session"

    builder = Builder()
    monkeypatch.setattr(pyspark.sql, "SparkSession", SimpleNamespace(builder=builder))
    storage = load_settings({"DATA_LAKE_PROFILE": "s3", "DATA_LAKE_TABLE_FORMAT": "delta"}).storage
    factory = SparkSessionFactory.for_storage(
        storage, app_name="x", master="local[1]", driver_memory="3g", packages="a:b:1", runtime="databricks"
    )

    assert factory.config() == {}
    assert factory.create() == "platform-session"
    assert builder.touched == []


def test_the_spark_runtime_comes_from_the_environment():
    assert load_settings({}).spark.runtime == "local"
    assert load_settings({"SPARK_RUNTIME": "Databricks"}).spark.runtime == "databricks"
