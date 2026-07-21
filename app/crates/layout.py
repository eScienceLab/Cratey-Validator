"""Canonical object-key layout for crates and their validation results.

A single place defines where crates and results live in the bucket. Crates and
results use *separate* prefixes so a result object can never collide with, or be
mistaken for, a crate object.

Layout (given a ``crate_prefix`` and ``results_prefix``):

- Crate (zip):       ``{crate_prefix}/{id}.zip``
- Crate (directory): ``{crate_prefix}/{id}/`` containing ``ro-crate-metadata.json``
- Validation result: ``{results_prefix}/{id}.json``
"""

METADATA_FILENAME = "ro-crate-metadata.json"


def _join(prefix: str, suffix: str) -> str:
    """Join an optional prefix and a suffix with a single separator."""
    prefix = prefix.strip("/")
    return f"{prefix}/{suffix}" if prefix else suffix


def crate_zip_key(crate_prefix: str, crate_id: str) -> str:
    """Object key for a crate stored as a zip archive."""
    return _join(crate_prefix, f"{crate_id}.zip")


def crate_dir_prefix(crate_prefix: str, crate_id: str) -> str:
    """Key prefix (with trailing slash) for a crate stored as a directory."""
    return _join(crate_prefix, f"{crate_id}/")


def crate_metadata_key(crate_prefix: str, crate_id: str) -> str:
    """Object key for the metadata file inside a directory-style crate."""
    return _join(crate_prefix, f"{crate_id}/{METADATA_FILENAME}")


def result_key(results_prefix: str, crate_id: str) -> str:
    """Object key for a crate's stored validation result."""
    return _join(results_prefix, f"{crate_id}.json")
