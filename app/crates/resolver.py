"""Deterministic crate resolution over a StorageBackend.

Resolution is by direct existence checks on canonical keys, never by listing a
prefix and substring-matching. This removes the previous fragilities: it cannot
false-match a sibling prefix, it confirms a directory crate actually contains
``ro-crate-metadata.json``, and it treats the ID as opaque (so a ``.zip`` in the
ID is harmless). Ambiguity and absence are reported explicitly.
"""

from dataclasses import dataclass

from app.crates.ids import validate_crate_id
from app.crates.layout import crate_dir_prefix, crate_metadata_key, crate_zip_key
from app.storage.base import StorageBackend
from app.storage.errors import ObjectNotFound


@dataclass(frozen=True)
class ResolvedCrate:
    """A crate located in storage.

    ``key`` is the zip object key for a zip crate, or the directory prefix
    (with trailing slash) for a directory crate.
    """

    crate_id: str
    key: str
    is_zip: bool


class CrateNotFound(Exception):
    """Raised when no crate exists for the given ID."""


class AmbiguousCrate(Exception):
    """Raised when both a zip and a directory crate exist for the same ID."""


def _object_exists(storage: StorageBackend, key: str) -> bool:
    try:
        storage.stat(key)
        return True
    except ObjectNotFound:
        return False


def resolve_crate(
    storage: StorageBackend, crate_id: str, crate_prefix: str
) -> ResolvedCrate:
    """Resolve ``crate_id`` to a concrete crate object.

    :raises InvalidCrateId: If the ID is malformed.
    :raises AmbiguousCrate: If both zip and directory forms exist.
    :raises CrateNotFound: If neither form exists.
    """
    validate_crate_id(crate_id)

    zip_key = crate_zip_key(crate_prefix, crate_id)
    metadata_key = crate_metadata_key(crate_prefix, crate_id)

    zip_exists = _object_exists(storage, zip_key)
    directory_exists = _object_exists(storage, metadata_key)

    if zip_exists and directory_exists:
        raise AmbiguousCrate(
            f"Crate {crate_id!r} exists as both a zip and a directory; refusing to guess."
        )
    if zip_exists:
        return ResolvedCrate(crate_id=crate_id, key=zip_key, is_zip=True)
    if directory_exists:
        return ResolvedCrate(
            crate_id=crate_id,
            key=crate_dir_prefix(crate_prefix, crate_id),
            is_zip=False,
        )
    raise CrateNotFound(f"No crate found for ID {crate_id!r}")
