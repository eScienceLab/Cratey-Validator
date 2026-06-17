"""Tests for deterministic crate resolution over a StorageBackend."""

import pytest

from app.crates.ids import InvalidCrateId
from app.crates.resolver import (
    AmbiguousCrate,
    CrateNotFound,
    ResolvedCrate,
    resolve_crate,
)
from app.storage.memory import InMemoryStorage

PREFIX = "crates"


@pytest.fixture
def storage() -> InMemoryStorage:
    return InMemoryStorage()


def test_resolves_zip_crate(storage):
    storage.put_bytes("crates/foo.zip", b"PK...")

    resolved = resolve_crate(storage, "foo", PREFIX)

    assert isinstance(resolved, ResolvedCrate)
    assert resolved.crate_id == "foo"
    assert resolved.is_zip is True
    assert resolved.key == "crates/foo.zip"


def test_resolves_directory_crate_with_metadata(storage):
    storage.put_bytes("crates/foo/ro-crate-metadata.json", b"{}")
    storage.put_bytes("crates/foo/data.csv", b"x")

    resolved = resolve_crate(storage, "foo", PREFIX)

    assert resolved.is_zip is False
    assert resolved.key == "crates/foo/"


def test_directory_without_metadata_is_not_a_crate(storage):
    """A directory lacking ro-crate-metadata.json must not resolve (old TODO)."""
    storage.put_bytes("crates/foo/data.csv", b"x")

    with pytest.raises(CrateNotFound):
        resolve_crate(storage, "foo", PREFIX)


def test_ambiguous_when_both_zip_and_directory_exist(storage):
    storage.put_bytes("crates/foo.zip", b"PK...")
    storage.put_bytes("crates/foo/ro-crate-metadata.json", b"{}")

    with pytest.raises(AmbiguousCrate):
        resolve_crate(storage, "foo", PREFIX)


def test_missing_crate_raises_not_found(storage):
    with pytest.raises(CrateNotFound):
        resolve_crate(storage, "absent", PREFIX)


def test_sibling_prefix_does_not_false_match(storage):
    """Resolving 'foo' must not match 'foobar' (old prefix-substring bug)."""
    storage.put_bytes("crates/foobar.zip", b"PK...")

    with pytest.raises(CrateNotFound):
        resolve_crate(storage, "foo", PREFIX)


def test_zip_suffix_in_id_resolves_as_directory(storage):
    """An ID containing '.zip' is opaque: a directory crate named 'data.zip' resolves."""
    storage.put_bytes("crates/data.zip/ro-crate-metadata.json", b"{}")

    resolved = resolve_crate(storage, "data.zip", PREFIX)

    assert resolved.is_zip is False
    assert resolved.key == "crates/data.zip/"


def test_invalid_id_propagates(storage):
    with pytest.raises(InvalidCrateId):
        resolve_crate(storage, "../etc", PREFIX)


def test_result_object_does_not_satisfy_crate_resolution(storage):
    """A stored result under a separate prefix never counts as the crate itself."""
    storage.put_bytes("validation-results/foo.json", b"{}")

    with pytest.raises(CrateNotFound):
        resolve_crate(storage, "foo", PREFIX)
