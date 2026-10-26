import gzip

import pytest

from ecomdata.infra.object_store import (
    LocalObjectStore,
    S3ObjectStore,
    gunzip_bytes,
    gzip_bytes,
    object_store_for,
)
from ecomdata.settings import load_settings


@pytest.fixture
def store(tmp_path):
    return LocalObjectStore(tmp_path)


def test_put_writes_a_file_under_the_zone(store, tmp_path):
    location = store.put("bronze", "crawl_raw/tiki/2026-07-26/p1_1.json", b'{"id": "p1"}')

    written = tmp_path / "bronze" / "crawl_raw" / "tiki" / "2026-07-26" / "p1_1.json"
    assert written.read_bytes() == b'{"id": "p1"}'
    assert location == written.resolve().as_uri()
    assert store.get("bronze", "crawl_raw/tiki/2026-07-26/p1_1.json") == b'{"id": "p1"}'


def test_an_unknown_zone_and_a_traversing_key_are_rejected(store):
    with pytest.raises(ValueError):
        store.put("platinum", "x.json", b"{}")
    with pytest.raises(ValueError):
        store.put("bronze", "../escape.json", b"{}")


def test_absent_objects_read_as_none_and_listing_is_sorted(store):
    assert store.get("silver", "missing.json") is None
    store.put("silver", "a/2.json", b"2")
    store.put("silver", "a/1.json", b"1")
    store.put("silver", "b/1.json", b"1")

    assert store.list("silver", "a/") == ["a/1.json", "a/2.json"]
    assert store.list("silver") == ["a/1.json", "a/2.json", "b/1.json"]
    assert store.list("gold") == []


def test_uri_and_key_of_are_inverse(store):
    uri = store.put("gold", "marketplace/current.json", b"{}")

    assert uri == store.uri("gold", "marketplace/current.json")
    assert store.key_of("gold", uri) == "marketplace/current.json"


def test_delete_removes_the_object(store):
    store.put("bronze", "x/y.bin", b"1")
    store.delete("bronze", "x/y.bin")
    assert store.get("bronze", "x/y.bin") is None


def test_gzip_is_deterministic_and_round_trips(store):
    body = "<html>giá 1.000 ₫</html>".encode()

    assert gzip_bytes(body) == gzip_bytes(body)
    assert gunzip_bytes(gzip_bytes(body)) == body
    store.put_gzip("bronze", "raw/part-0.bin.gz", body)
    assert gzip.decompress(store.get("bronze", "raw/part-0.bin.gz")) == body
    assert store.get_gunzip("bronze", "raw/part-0.bin.gz") == body


def test_the_factory_chooses_by_profile(tmp_path):
    local = object_store_for(load_settings({"DATA_LAKE_PROFILE": "local", "DATA_LAKE_LOCAL_ROOT": str(tmp_path)}).storage)
    minio = object_store_for(load_settings({"DATA_LAKE_PROFILE": "minio", "MINIO_ENDPOINT": "minio:9000"}).storage)

    assert isinstance(local, LocalObjectStore) and local.root == tmp_path
    assert isinstance(minio, S3ObjectStore)
    assert minio.uri("silver", "marketplace/x") == "s3a://ecommerce-silver/marketplace/x"
    assert minio.key_of("silver", "s3a://ecommerce-silver/marketplace/x") == "marketplace/x"


class _FakeMinio:
    def __init__(self):
        self.objects, self.buckets = {}, set()

    def bucket_exists(self, bucket):
        return bucket in self.buckets

    def make_bucket(self, bucket):
        self.buckets.add(bucket)

    def put_object(self, bucket, key, data, length):
        self.objects[(bucket, key)] = data.read(length)


def test_the_s3_store_creates_its_bucket_once_and_writes_under_it():
    client = _FakeMinio()
    store = S3ObjectStore(load_settings({"DATA_LAKE_PROFILE": "minio"}).storage, client=client)

    assert store.put("bronze", "/a/b.bin", b"x") == "s3a://ecommerce-bronze/a/b.bin"
    store.put("bronze", "a/c.bin", b"y")

    assert client.buckets == {"ecommerce-bronze"}
    assert client.objects == {("ecommerce-bronze", "a/b.bin"): b"x", ("ecommerce-bronze", "a/c.bin"): b"y"}
