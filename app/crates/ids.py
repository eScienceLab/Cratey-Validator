"""Strict validation for crate identifiers.

A crate ID is treated as a single-segment label. Constraining it to a safe charset
means it can be composed into object keys and local paths without risk of collisions
or traversal, and removes the need to parse meaning (such as a ``.zip`` suffix) back
out of it.
"""

import re

# Start with an alphanumeric (so no leading dot/dash), then up to 127 more of a
# restricted set. No slashes, whitespace, or non-ASCII; max length 128.
_CRATE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class InvalidCrateId(ValueError):
    """Raised when a crate ID does not meet the required format."""


def is_valid_crate_id(crate_id: object) -> bool:
    """Return whether ``crate_id`` is a well-formed crate identifier."""
    if not isinstance(crate_id, str):
        return False
    if ".." in crate_id:
        return False
    return _CRATE_ID_PATTERN.match(crate_id) is not None


def validate_crate_id(crate_id: object) -> str:
    """Return ``crate_id`` unchanged if valid, else raise :class:`InvalidCrateId`."""
    if not is_valid_crate_id(crate_id):
        raise InvalidCrateId(
            f"Invalid crate ID: {crate_id!r}. Crate IDs must start with a letter "
            "or digit and contain only letters, digits, '.', '_' or '-' "
            "(max 128 characters, no '/' or '..')."
        )
    return crate_id
