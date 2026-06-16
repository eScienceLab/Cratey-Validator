"""Tests for the validation runner that wraps rocrate_validator."""

import json

import pytest

from app.validation import runner
from app.validation.results import ValidationStatus


class FakeResult:
    def __init__(self, has_issues: bool):
        self._has_issues = has_issues

    def has_issues(self) -> bool:
        return self._has_issues

    def to_json(self) -> str:
        return json.dumps({"issues": self._has_issues})


class FakeServices:
    """A stand-in for rocrate_validator.services."""

    def __init__(self, result=None, raises=None):
        self._result = result
        self._raises = raises
        self.last_settings = None

    def ValidationSettings(self, **kwargs):  # noqa: N802 - mirrors the real API
        self.last_settings = kwargs
        return kwargs

    def validate(self, settings):
        if self._raises is not None:
            raise self._raises
        return self._result


def test_validate_metadata_success_is_valid(monkeypatch):
    fake = FakeServices(result=FakeResult(has_issues=False))
    monkeypatch.setattr(runner, "services", fake)

    outcome = runner.validate_metadata({"@graph": []}, profile_name="ro-crate")

    assert outcome.status is ValidationStatus.VALID
    assert outcome.profile == "ro-crate"
    assert fake.last_settings["metadata_only"] is True
    assert fake.last_settings["metadata_dict"] == {"@graph": []}


def test_validate_metadata_with_issues_is_invalid(monkeypatch):
    monkeypatch.setattr(runner, "services", FakeServices(result=FakeResult(True)))
    outcome = runner.validate_metadata({"@graph": []})
    assert outcome.status is ValidationStatus.INVALID


def test_validate_metadata_exception_becomes_error_outcome(monkeypatch):
    monkeypatch.setattr(runner, "services", FakeServices(raises=RuntimeError("kaboom")))
    outcome = runner.validate_metadata({"@graph": []}, profile_name="ro-crate")
    assert outcome.status is ValidationStatus.ERROR
    assert "kaboom" in outcome.error
    assert outcome.profile == "ro-crate"


def test_validate_crate_path_success(monkeypatch):
    fake = FakeServices(result=FakeResult(has_issues=False))
    monkeypatch.setattr(runner, "services", fake)

    outcome = runner.validate_crate_path("/tmp/crate", profile_name="ro-crate")

    assert outcome.status is ValidationStatus.VALID
    assert fake.last_settings["rocrate_uri"] == "/tmp/crate"


def test_validate_crate_path_exception_becomes_error_outcome(monkeypatch):
    monkeypatch.setattr(runner, "services", FakeServices(raises=ValueError("bad crate")))
    outcome = runner.validate_crate_path("/tmp/crate")
    assert outcome.status is ValidationStatus.ERROR
    assert "bad crate" in outcome.error
