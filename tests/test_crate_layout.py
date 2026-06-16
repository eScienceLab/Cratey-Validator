"""Tests for canonical crate/result key construction."""

import pytest

from app.crates.layout import (
    crate_zip_key,
    crate_dir_prefix,
    crate_metadata_key,
    result_key,
)


def test_crate_zip_key_under_prefix():
    assert crate_zip_key("crates", "foo") == "crates/foo.zip"


def test_crate_dir_prefix_under_prefix():
    assert crate_dir_prefix("crates", "foo") == "crates/foo/"


def test_crate_metadata_key_under_prefix():
    assert crate_metadata_key("crates", "foo") == "crates/foo/ro-crate-metadata.json"


def test_result_key_uses_separate_results_prefix():
    assert result_key("validation-results", "foo") == "validation-results/foo.json"


@pytest.mark.parametrize("prefix", ["", "crates/"])
def test_prefix_edge_cases_are_normalised(prefix):
    """An empty prefix maps to the bucket root; a trailing slash is not doubled."""
    expected = "foo.zip" if prefix == "" else "crates/foo.zip"
    assert crate_zip_key(prefix, "foo") == expected
