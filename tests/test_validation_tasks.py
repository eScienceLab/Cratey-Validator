"""Tests for the store-backed validation orchestration (run_validation_job)."""

import json
from unittest import mock

import pytest

from app.crates.layout import result_key
from app.storage.errors import StorageError
from app.storage.memory import InMemoryStorage
from app.tasks import validation_tasks
from app.tasks.validation_tasks import run_validation_job
from app.utils.config import Settings
from app.validation.results import ValidationOutcome, ValidationStatus

RUNNER = "app.tasks.validation_tasks.validate_crate_path"
WEBHOOK = "app.tasks.validation_tasks.send_webhook_notification"


def _settings() -> Settings:
    return Settings.from_env(
        {
            "STORAGE_ENABLED": "true",
            "S3_ENDPOINT": "minio:9000",
            "S3_ACCESS_KEY": "a",
            "S3_SECRET_KEY": "b",
            "S3_BUCKET": "ro-crates",
            "CELERY_BROKER_URL": "redis://r/0",
            "CELERY_RESULT_BACKEND": "redis://r/1",
        }
    )


def _stored_outcome(storage: InMemoryStorage, crate_id: str) -> dict:
    raw = storage.get_bytes(result_key("validation-results", crate_id))
    return json.loads(raw)


@pytest.fixture
def storage() -> InMemoryStorage:
    return InMemoryStorage()


def test_valid_zip_crate_is_validated_and_persisted(storage):
    storage.put_bytes("crates/foo.zip", b"PK\x03\x04")

    with mock.patch(RUNNER) as run, mock.patch(WEBHOOK) as hook:
        run.return_value = ValidationOutcome(
            status=ValidationStatus.VALID, profile=None, detail={"r": 1}, created_at="t"
        )
        outcome = run_validation_job(storage, "foo", _settings(), created_at="t")

    assert outcome.status is ValidationStatus.VALID
    assert _stored_outcome(storage, "foo")["status"] == "valid"
    # The validator was handed the downloaded zip path.
    assert run.call_args.args[0].endswith("foo.zip")
    hook.assert_not_called()


def test_directory_crate_is_downloaded_and_persisted(storage):
    storage.put_bytes("crates/foo/ro-crate-metadata.json", b"{}")
    storage.put_bytes("crates/foo/data.csv", b"x")

    with mock.patch(RUNNER) as run, mock.patch(WEBHOOK):
        run.return_value = ValidationOutcome(
            status=ValidationStatus.INVALID, detail={"issues": [1]}, created_at="t"
        )
        run_validation_job(storage, "foo", _settings(), created_at="t")

    assert _stored_outcome(storage, "foo")["status"] == "invalid"


def test_missing_crate_persists_error_outcome(storage):
    with mock.patch(RUNNER) as run, mock.patch(WEBHOOK):
        outcome = run_validation_job(storage, "absent", _settings(), created_at="t")

    assert outcome.status is ValidationStatus.ERROR
    assert _stored_outcome(storage, "absent")["status"] == "error"
    run.assert_not_called()  # never reached the validator


def test_webhook_is_sent_with_outcome_when_url_given(storage):
    storage.put_bytes("crates/foo.zip", b"PK")

    with mock.patch(RUNNER) as run, mock.patch(WEBHOOK) as hook:
        run.return_value = ValidationOutcome(status=ValidationStatus.VALID, created_at="t")
        run_validation_job(
            storage, "foo", _settings(), webhook_url="https://hook", created_at="t"
        )

    hook.assert_called_once()
    url, payload = hook.call_args.args
    assert url == "https://hook"
    assert payload["status"] == "valid"


def test_transient_storage_error_propagates_for_retry():
    class FlakyStorage(InMemoryStorage):
        def get_bytes(self, key):
            raise StorageError("temporary outage")

    storage = FlakyStorage()
    storage.put_bytes("crates/foo.zip", b"PK")  # so resolution finds the zip

    with mock.patch(RUNNER), mock.patch(WEBHOOK):
        with pytest.raises(StorageError):
            run_validation_job(storage, "foo", _settings(), created_at="t")


def test_created_at_is_persisted(storage):
    storage.put_bytes("crates/foo.zip", b"PK")
    with mock.patch(RUNNER) as run, mock.patch(WEBHOOK):
        run.return_value = ValidationOutcome(status=ValidationStatus.VALID, created_at="2026-06-16T00:00:00Z")
        run_validation_job(storage, "foo", _settings(), created_at="2026-06-16T00:00:00Z")

    assert _stored_outcome(storage, "foo")["created_at"] == "2026-06-16T00:00:00Z"
