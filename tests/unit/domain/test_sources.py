"""The one source registry."""
import dataclasses

import pytest

from ecomdata.domain.sources import BROWSER, HTTP, LAZADA, SOURCES, TIKI, SourceRegistry, SourceSpec, resolve


def test_registry_holds_tiki_and_lazada():
    assert SOURCES.codes() == ("lazada", "tiki")
    assert SOURCES.get("TIKI") is TIKI
    assert SOURCES.by_marketplace_id("marketplace-lazada") is LAZADA
    assert TIKI.fetcher == HTTP and LAZADA.fetcher == BROWSER


def test_politeness_floors_match_the_crawl_rules():
    assert TIKI.min_delay_seconds >= 2.0
    assert LAZADA.min_delay_seconds >= 5.0


def test_unknown_source_is_an_error_not_a_default():
    with pytest.raises(ValueError, match="unknown source"):
        SOURCES.get("shopee")
    assert SOURCES.find("shopee") is None
    with pytest.raises(ValueError, match="unknown marketplace_id"):
        SOURCES.by_marketplace_id("marketplace-shopee")


def test_registration_refuses_duplicates():
    with pytest.raises(ValueError, match="already registered"):
        SourceRegistry([TIKI, TIKI])
    clone = dataclasses.replace(TIKI, code="tiki2")
    with pytest.raises(ValueError, match="marketplace_id already registered"):
        SourceRegistry([TIKI, clone])


def test_with_spec_adds_a_source_without_touching_the_original():
    fake = SourceSpec(code="fake", marketplace_id="marketplace-fake", name="Fake", base_url="https://fake.example",
                      fetcher=HTTP, extractor=object, normalizer=object, currency="vnd")
    extended = SOURCES.with_spec(fake)
    assert "fake" in extended and "fake" not in SOURCES
    assert extended.get("fake").currency == "VND"
    assert extended.extractor("fake") is object


@pytest.mark.parametrize("field, value", [("fetcher", "ftp"), ("source_type", "shop"), ("currency", "VN"),
                                          ("base_url", "tiki.vn"), ("min_delay_seconds", -1)])
def test_spec_validates_its_fields(field, value):
    with pytest.raises(ValueError):
        dataclasses.replace(TIKI, **{field: value})


def test_counter_semantics_come_from_the_spec():
    names = {c.field_name for c in TIKI.counter_semantics()}
    assert names == {"rating_count", "review_count", "sold_count"}
    assert TIKI.counter_semantic("sold_count").semantic_version == "v1"
    assert LAZADA.counter_semantic("sold_count") is None


def test_resolve_loads_dotted_paths_and_passes_objects_through():
    assert resolve("json:dumps").__name__ == "dumps"
    assert resolve("json").__name__ == "json"
    marker = object()
    assert resolve(marker) is marker


def test_adapter_version_names_source_fetcher_and_extractor_version():
    assert TIKI.adapter_version == "tiki-http-extractor-v1"
    assert LAZADA.adapter_version == "lazada-browser-extractor-v1"
