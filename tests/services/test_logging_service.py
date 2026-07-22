"""Tests for structured logging, request IDs, and secret redaction."""

import json
import logging

from app.services.logging_service import (
    JsonFormatter,
    RedactionFilter,
    RequestIdFilter,
    get_request_id,
    new_request_id,
    set_request_id,
)


def _record(msg, args=None):
    return logging.LogRecord("svc", logging.INFO, "path", 1, msg, args, None)


def test_json_formatter_emits_expected_fields():
    record = _record("hello")
    record.request_id = "r1"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "svc"
    assert payload["message"] == "hello"
    assert payload["request_id"] == "r1"
    assert "timestamp" in payload


def test_redaction_filter_masks_secret_values():
    redact = RedactionFilter(["supersecret", "AKIAEXAMPLE"])
    record = _record("connecting with key=%s token=%s", ("AKIAEXAMPLE", "supersecret"))

    redact.filter(record)

    message = record.getMessage()
    assert "supersecret" not in message
    assert "AKIAEXAMPLE" not in message
    assert message.count("***") == 2


def test_redaction_filter_ignores_empty_secrets():
    redact = RedactionFilter([None, "", "real"])
    record = _record("value=real")
    redact.filter(record)
    assert record.getMessage() == "value=***"


def test_request_id_filter_injects_current_id():
    set_request_id("abc-123")
    record = _record("anything")

    RequestIdFilter().filter(record)

    assert record.request_id == "abc-123"


def test_request_id_filter_defaults_when_unset():
    set_request_id(None)
    record = _record("anything")
    RequestIdFilter().filter(record)
    assert record.request_id == "-"


def test_new_request_id_is_unique():
    assert new_request_id() != new_request_id()


def test_get_request_id_round_trips():
    set_request_id("xyz")
    assert get_request_id() == "xyz"
