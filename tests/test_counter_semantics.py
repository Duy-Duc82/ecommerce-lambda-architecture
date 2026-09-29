from config.counter_semantics import counter_semantic, counter_semantics_for


def test_registry_is_explicit_and_unknown_is_untrusted():
    assert {x.field_name for x in counter_semantics_for("tiki")} == {"rating_count", "review_count", "sold_count"}
    assert counter_semantic("unknown", "sold_count") is None
    assert len({(x.marketplace, x.field_name) for x in counter_semantics_for("tiki")}) == 3
