"""Tests for strict crate-ID validation."""

import pytest

from app.crates.ids import validate_crate_id, is_valid_crate_id, InvalidCrateId


@pytest.mark.parametrize(
    "crate_id",
    [
        "a",
        "crate-123",
        "my_crate.v2",
        "ABC.def-123_456",
        "release.zip",          # ".zip" in the ID is harmless now: IDs are opaque
        "x" * 128,              # max length
    ],
)
def test_valid_ids_are_accepted(crate_id):
    assert validate_crate_id(crate_id) == crate_id
    assert is_valid_crate_id(crate_id) is True


@pytest.mark.parametrize(
    "crate_id",
    [
        "",                     # empty
        ".hidden",              # leading dot
        "-leading-dash",        # must start alphanumeric
        "a/b",                  # path separator
        "../etc/passwd",        # traversal
        "a..b",                 # parent-dir sequence
        "with space",           # whitespace
        "tab\tchar",            # control char
        "x" * 129,              # too long
        "unicodé",              # non-ASCII
    ],
)
def test_invalid_ids_are_rejected(crate_id):
    assert is_valid_crate_id(crate_id) is False
    with pytest.raises(InvalidCrateId):
        validate_crate_id(crate_id)


def test_non_string_is_rejected():
    assert is_valid_crate_id(None) is False
    with pytest.raises(InvalidCrateId):
        validate_crate_id(None)


def test_error_message_names_the_offending_id():
    with pytest.raises(InvalidCrateId) as exc_info:
        validate_crate_id("a/b")
    assert "a/b" in str(exc_info.value)
