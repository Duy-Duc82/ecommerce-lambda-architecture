"""Storage-profile contract.

The point of these tests is that the two MinIO-only settings which used to be
hardcoded in ``build_spark`` — path-style addressing and disabled TLS — are
correct for MinIO and *wrong* for every cloud endpoint. Each profile asserts
both, so a regression cannot silently ship a job that only works locally.
"""

from __future__ import annotations

import pytest

from config import storage


@pytest.fixture(autouse=True)
def _clear_profile_cache():
    storage.active_profile.cache_clear()
    yield
    storage.active_profile.cache_clear()


def _use(monkeypatch, **env):
    for name in ("DATA_LAKE_PROFILE", "DATA_LAKE_MODE", "DATA_LAKE_ENDPOINT", "DATA_LAKE_REGION"):
        monkeypatch.delenv(name, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def test_local_profile_has_no_hadoop_options(monkeypatch):
    _use(monkeypatch, DATA_LAKE_PROFILE="local")
    profile = storage.active_profile()

    assert profile.is_local
    assert profile.scheme == "file"
    assert storage.spark_hadoop_options(profile) == {}


def test_minio_profile_uses_path_style_and_plain_http(monkeypatch):
    _use(monkeypatch, DATA_LAKE_PROFILE="minio", MINIO_ENDPOINT="minio:9000")
    options = storage.spark_hadoop_options(storage.active_profile())

    assert options["fs.s3a.path.style.access"] == "true"
    assert options["fs.s3a.connection.ssl.enabled"] == "false"
    assert options["fs.s3a.endpoint"] == "http://minio:9000"


def test_s3_profile_inverts_both_minio_settings(monkeypatch):
    _use(monkeypatch, DATA_LAKE_PROFILE="s3", DATA_LAKE_REGION="ap-southeast-1")
    profile = storage.active_profile()
    options = storage.spark_hadoop_options(profile)

    # Exactly the two values that were previously hardcoded to MinIO's needs.
    assert options["fs.s3a.path.style.access"] == "false"
    assert options["fs.s3a.connection.ssl.enabled"] == "true"
    assert options["fs.s3a.endpoint.region"] == "ap-southeast-1"
    # No explicit endpoint: the SDK resolves the regional one itself.
    assert "fs.s3a.endpoint" not in options
    assert profile.endpoint_host() == "s3.ap-southeast-1.amazonaws.com"


def test_r2_profile_requires_an_endpoint(monkeypatch):
    _use(monkeypatch, DATA_LAKE_PROFILE="r2")
    with pytest.raises(ValueError, match="DATA_LAKE_ENDPOINT"):
        storage.active_profile()


def test_r2_profile_keeps_tls_with_path_style(monkeypatch):
    _use(
        monkeypatch,
        DATA_LAKE_PROFILE="r2",
        DATA_LAKE_ENDPOINT="https://acct123.r2.cloudflarestorage.com",
    )
    profile = storage.active_profile()
    options = storage.spark_hadoop_options(profile)

    assert options["fs.s3a.path.style.access"] == "true"
    assert options["fs.s3a.connection.ssl.enabled"] == "true"
    assert profile.endpoint_host() == "acct123.r2.cloudflarestorage.com"


def test_data_lake_mode_still_selects_minio(monkeypatch):
    """docker-compose and scripts/*.ps1 set DATA_LAKE_MODE, not a profile."""
    _use(monkeypatch, DATA_LAKE_MODE="s3a")
    assert storage.resolve_profile_name() == "minio"

    _use(monkeypatch, DATA_LAKE_MODE="local")
    assert storage.resolve_profile_name() == "local"


def test_explicit_profile_overrides_legacy_mode(monkeypatch):
    _use(monkeypatch, DATA_LAKE_MODE="s3a", DATA_LAKE_PROFILE="s3")
    assert storage.resolve_profile_name() == "s3"


def test_unknown_profile_is_rejected(monkeypatch):
    _use(monkeypatch, DATA_LAKE_PROFILE="dropbox")
    with pytest.raises(ValueError, match="Unknown DATA_LAKE_PROFILE"):
        storage.active_profile()
