"""Tests for the storage abstraction and its in-memory fake."""

import pytest

from app.storage.base import ObjectStat, StorageBackend
from app.storage.errors import ObjectNotFound
from app.storage.memory import InMemoryStorage


@pytest.fixture
def storage() -> InMemoryStorage:
    return InMemoryStorage()


def test_put_then_get_round_trips_bytes(storage):
    storage.put_bytes("crates/foo.zip", b"payload")
    assert storage.get_bytes("crates/foo.zip") == b"payload"


def test_get_missing_key_raises_object_not_found(storage):
    with pytest.raises(ObjectNotFound):
        storage.get_bytes("crates/missing.zip")


def test_stat_returns_size_for_existing_object(storage):
    storage.put_bytes("crates/foo.zip", b"12345")
    stat = storage.stat("crates/foo.zip")
    assert isinstance(stat, ObjectStat)
    assert stat.key == "crates/foo.zip"
    assert stat.size == 5


def test_stat_missing_key_raises_object_not_found(storage):
    with pytest.raises(ObjectNotFound):
        storage.stat("crates/missing.zip")


def test_list_returns_only_keys_under_prefix(storage):
    storage.put_bytes("crates/a/ro-crate-metadata.json", b"{}")
    storage.put_bytes("crates/a/data.csv", b"x")
    storage.put_bytes("crates/b.zip", b"y")
    storage.put_bytes("results/a.json", b"z")

    assert storage.list("crates/a/") == [
        "crates/a/data.csv",
        "crates/a/ro-crate-metadata.json",
    ]


def test_download_tree_preserves_relative_structure(storage, tmp_path):
    storage.put_bytes("crates/a/ro-crate-metadata.json", b"{}")
    storage.put_bytes("crates/a/sub/data.csv", b"col\n1\n")

    storage.download_tree("crates/a/", str(tmp_path))

    assert (tmp_path / "ro-crate-metadata.json").read_bytes() == b"{}"
    assert (tmp_path / "sub" / "data.csv").read_bytes() == b"col\n1\n"


def test_in_memory_storage_satisfies_protocol(storage):
    assert isinstance(storage, StorageBackend)
