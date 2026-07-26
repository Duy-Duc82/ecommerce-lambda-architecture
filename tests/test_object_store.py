import common.object_store as object_store


def test_put_bytes_local_mode_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr(object_store, "DATA_LAKE_MODE", "local")
    monkeypatch.setattr(object_store, "DATA_LAKE_LOCAL_ROOT", tmp_path)

    location = object_store.put_bytes("bronze", "crawl_raw/tiki/2026-07-26/p1_1.json", b'{"id": "p1"}')

    written = tmp_path / "bronze" / "crawl_raw" / "tiki" / "2026-07-26" / "p1_1.json"
    assert written.exists()
    assert written.read_bytes() == b'{"id": "p1"}'
    assert location == written.resolve().as_uri()


def test_put_bytes_rejects_unknown_zone(tmp_path, monkeypatch):
    monkeypatch.setattr(object_store, "DATA_LAKE_MODE", "local")
    monkeypatch.setattr(object_store, "DATA_LAKE_LOCAL_ROOT", tmp_path)
    try:
        object_store.put_bytes("platinum", "x.json", b"{}")
        assert False, "expected ValueError"
    except ValueError:
        pass
