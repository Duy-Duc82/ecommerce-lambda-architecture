import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

from batch_layer.warehouse_job import (
    build_dimensions,
    build_fact,
    build_marts,
    normalize_events,
    quality_checks,
    read_source,
    split_valid_events,
)


class WarehouseTransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("WarehouseTransformTest")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        source = str(Path(__file__).parent / "fixtures" / "events.csv")
        raw = read_source(cls.spark, source)
        normalized = normalize_events(raw, "test-run", datetime(2026, 1, 1, tzinfo=timezone.utc))
        cls.events, cls.rejected = split_valid_events(normalized)
        cls.events.cache()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_normalization_preserves_source_grain(self):
        self.assertEqual(self.events.count(), 10)
        self.assertEqual(self.rejected.count(), 0)
        self.assertEqual(self.events.select("event_key").distinct().count(), 10)
        self.assertEqual(
            {row.event_type for row in self.events.select("event_type").distinct().collect()},
            {"view", "cart", "purchase"},
        )

    def test_star_schema_has_no_orphans(self):
        dimensions = build_dimensions(self.events)
        fact = build_fact(self.events, dimensions)
        self.assertEqual(fact.count(), 10)
        self.assertEqual(dimensions["dim_product"].count(), 3)
        self.assertEqual(dimensions["dim_user"].count(), 5)
        self.assertTrue(all(result.passed for result in quality_checks(self.events, fact, dimensions)))

    def test_funnel_and_session_marts(self):
        marts = build_marts(self.events)
        first_day = (
            marts["funnel_daily"].filter("event_date = DATE '2019-10-01'").collect()[0]
        )
        self.assertEqual((first_day.viewers, first_day.cart_users, first_day.buyers), (3, 2, 1))

        outcomes = {
            row.session_id: row.session_outcome
            for row in marts["session_daily"].select("session_id", "session_outcome").collect()
        }
        self.assertEqual(outcomes["s1"], "converted")
        self.assertEqual(outcomes["s2"], "abandoned_cart")
        self.assertEqual(outcomes["s3"], "browsing")


if __name__ == "__main__":
    unittest.main()
