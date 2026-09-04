import pytest

import common.object_store as object_store
from config import storage


@pytest.fixture(autouse=True)
def _local_profile(tmp_path, monkeypatch):
    """Force the local profile so no test ever reaches a real object store."""
    monkeypatch.delenv("DATA_LAKE_MODE", raising=False)
    monkeypatch.setenv("DATA_LAKE_PROFILE", "local")
    monkeypatch.setattr(object_store, "DATA_LAKE_LOCAL_ROOT", tmp_path)
    storage.active_profile.cache_clear()
    yield
    storage.active_profile.cache_clear()


def test_put_bytes_local_mode_writes_file(tmp_path):
    location = object_store.put_bytes("bronze", "crawl_raw/tiki/2026-07-26/p1_1.json", b'{"id": "p1"}')

    written = tmp_path / "bronze" / "crawl_raw" / "tiki" / "2026-07-26" / "p1_1.json"
    assert written.exists()
    assert written.read_bytes() == b'{"id": "p1"}'
    assert location == written.resolve().as_uri()


def test_put_bytes_rejects_unknown_zone(tmp_path):
    with pytest.raises(ValueError):
        object_store.put_bytes("platinum", "x.json", b"{}")
