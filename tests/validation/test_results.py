"""Tests for the ValidationOutcome result type."""

import json

from app.validation.results import ValidationOutcome, ValidationStatus


class FakeResult:
    """Stand-in for a rocrate_validator ValidationResult."""

    def __init__(self, has_issues: bool, report: dict):
        self._has_issues = has_issues
        self._report = report

    def has_issues(self) -> bool:
        return self._has_issues

    def to_json(self) -> str:
        return json.dumps(self._report)


def test_from_validator_result_without_issues_is_valid():
    outcome = ValidationOutcome.from_validator_result(
        FakeResult(False, {"report": "ok"}), profile="ro-crate"
    )
    assert outcome.status is ValidationStatus.VALID
    assert outcome.is_valid is True
    assert outcome.profile == "ro-crate"
    assert outcome.detail == {"report": "ok"}
    assert outcome.error is None


def test_from_validator_result_with_issues_is_invalid():
    outcome = ValidationOutcome.from_validator_result(FakeResult(True, {"issues": [1]}))
    assert outcome.status is ValidationStatus.INVALID
    assert outcome.is_valid is False
    assert outcome.detail == {"issues": [1]}


def test_from_error_records_message_and_has_no_detail():
    outcome = ValidationOutcome.from_error("boom", profile="ro-crate")
    assert outcome.status is ValidationStatus.ERROR
    assert outcome.is_valid is False
    assert outcome.error == "boom"
    assert outcome.detail is None


def test_to_dict_serialises_status_as_string_and_omits_absent_fields():
    outcome = ValidationOutcome.from_validator_result(FakeResult(False, {"r": 1}))
    data = outcome.to_dict()
    assert data["status"] == "valid"
    assert data["detail"] == {"r": 1}
    assert "error" not in data


def test_to_json_round_trips():
    outcome = ValidationOutcome.from_error("nope")
    parsed = json.loads(outcome.to_json())
    assert parsed["status"] == "error"
    assert parsed["error"] == "nope"


def test_created_at_is_propagated_when_provided():
    outcome = ValidationOutcome.from_error("x", created_at="2026-06-16T00:00:00Z")
    assert outcome.created_at == "2026-06-16T00:00:00Z"
    assert outcome.to_dict()["created_at"] == "2026-06-16T00:00:00Z"
