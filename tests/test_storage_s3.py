"""Tests for the boto3-backed S3 storage backend, tested against moto."""

import boto3
import pytest
from moto import mock_aws

from app.storage.base import StorageBackend
from app.storage.errors import ObjectNotFound, StorageError
from app.storage.s3 import S3Backend
from app.utils.config import Settings

BUCKET = "test-bucket"


@pytest.fixture
def s3_backend():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield S3Backend(client, BUCKET)


def test_put_then_get_round_trips_bytes(s3_backend):
    s3_backend.put_bytes("crates/foo.zip", b"payload")
    assert s3_backend.get_bytes("crates/foo.zip") == b"payload"


def test_get_missing_key_raises_object_not_found(s3_backend):
    with pytest.raises(ObjectNotFound):
        s3_backend.get_bytes("crates/missing.zip")


def test_stat_returns_size_for_existing_object(s3_backend):
    s3_backend.put_bytes("crates/foo.zip", b"12345")
    stat = s3_backend.stat("crates/foo.zip")
    assert stat.key == "crates/foo.zip"
    assert stat.size == 5


def test_stat_missing_key_raises_object_not_found(s3_backend):
    with pytest.raises(ObjectNotFound):
        s3_backend.stat("crates/missing.zip")


def test_list_returns_only_keys_under_prefix_sorted(s3_backend):
    s3_backend.put_bytes("crates/a/ro-crate-metadata.json", b"{}")
    s3_backend.put_bytes("crates/a/data.csv", b"x")
    s3_backend.put_bytes("crates/b.zip", b"y")
    s3_backend.put_bytes("results/a.json", b"z")

    assert s3_backend.list("crates/a/") == [
        "crates/a/data.csv",
        "crates/a/ro-crate-metadata.json",
    ]


def test_list_paginates_beyond_one_thousand_objects(s3_backend):
    for i in range(1500):
        s3_backend.put_bytes(f"many/{i:04d}.txt", b"x")
    assert len(s3_backend.list("many/")) == 1500


def test_download_tree_preserves_relative_structure(s3_backend, tmp_path):
    s3_backend.put_bytes("crates/a/ro-crate-metadata.json", b"{}")
    s3_backend.put_bytes("crates/a/sub/data.csv", b"col\n1\n")

    s3_backend.download_tree("crates/a/", str(tmp_path))

    assert (tmp_path / "ro-crate-metadata.json").read_bytes() == b"{}"
    assert (tmp_path / "sub" / "data.csv").read_bytes() == b"col\n1\n"


def test_non_missing_client_error_becomes_storage_error(s3_backend):
    """A failure other than a missing key surfaces as StorageError, not ObjectNotFound."""
    broken = S3Backend(s3_backend._client, "nonexistent-bucket")
    with pytest.raises(StorageError) as exc_info:
        broken.get_bytes("whatever")
    assert not isinstance(exc_info.value, ObjectNotFound)


def test_s3_backend_satisfies_protocol(s3_backend):
    assert isinstance(s3_backend, StorageBackend)


def test_from_settings_builds_backend_for_s3_compatible_endpoint():
    env = {
        "STORAGE_ENABLED": "true",
        "S3_ENDPOINT": "minio:9000",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin",
        "S3_BUCKET": "ro-crates",
        "CELERY_BROKER_URL": "redis://redis:6379/0",
        "CELERY_RESULT_BACKEND": "redis://redis:6379/1",
    }
    backend = S3Backend.from_settings(Settings.from_env(env))
    assert isinstance(backend, StorageBackend)
    assert backend.bucket == "ro-crates"
