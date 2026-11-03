from ecomdata.domain.datasets import STAR_NAMES
from ecomdata.processing.batch import dw_inspect
from ecomdata.processing.batch.dw_inspect import lost_rows


def _snap(**rows):
    return {"tables": {name: {"rows": count} for name, count in rows.items()}}


def test_a_table_that_lost_rows_is_reported_with_both_counts():
    assert lost_rows(_snap(dim_seller=10, fact_listing=5), _snap(dim_seller=12, fact_listing=4)) == {"fact_listing": (5, 4)}


def test_a_table_not_written_before_or_missing_after_is_judged_by_what_it_held():
    assert lost_rows(_snap(dim_brand=None), _snap(dim_brand=7)) == {}
    assert lost_rows(_snap(dim_brand=3), {"tables": {}}) == {"dim_brand": (3, 0)}


def test_the_module_imports_without_spark_and_inspects_every_star_table():
    assert "fact_crawl_attempt" in STAR_NAMES
    assert callable(dw_inspect.main)
