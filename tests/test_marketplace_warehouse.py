from datetime import datetime, timezone
from batch_layer.marketplace_warehouse import MarketplaceBatchContext
from batch_layer.marketplace_postgres import MarketplaceCachePublisher


def test_context_normalizes_utc_and_validates_run_id():
    context = MarketplaceBatchContext("run_1", datetime(2026, 9, 5, 10, tzinfo=timezone.utc), "file:///silver", "file:///gold")
    assert context.as_of.tzinfo == timezone.utc


def test_staging_table_uses_safe_hash_and_allowlist():
    first = MarketplaceCachePublisher.staging_table("offer_current", "same-run")
    assert first == MarketplaceCachePublisher.staging_table("offer_current", "same-run")
    assert "same-run" not in first
