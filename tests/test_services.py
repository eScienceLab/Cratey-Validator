"""Tests for the validation service layer."""

from unittest.mock import patch

import pytest
from flask import Flask

from app import create_app
from app.crates.ids import InvalidCrateId
from app.crates.layout import result_key
from app.crates.resolver import AmbiguousCrate, CrateNotFound
from app.services.validation_service import (
    get_ro_crate_validation_task,
    queue_ro_crate_validation_task,
    run_metadata_validation,
)
from app.storage.memory import InMemoryStorage
from app.utils.config import InvalidAPIUsage, Settings
from app.validation.results import ValidationOutcome, ValidationStatus


def _storage_env() -> dict:
    return {
        "STORAGE_ENABLED": "true",
        "S3_ENDPOINT": "minio:9000",
        "S3_ACCESS_KEY": "a",
        "S3_SECRET_KEY": "b",
        "S3_BUCKET": "ro-crates",
        "CELERY_BROKER_URL": "redis://r/0",
        "CELERY_RESULT_BACKEND": "redis://r/1",
    }


@pytest.fixture
def flask_app():
    """Bare app context for functions that only need jsonify."""
    app = Flask(__name__)
    with app.app_context():
        yield app


@pytest.fixture
def app_ctx():
    """Storage-enabled app context, so current_app.config['SETTINGS'] is set."""
    app = create_app(settings=Settings.from_env(_storage_env()))
    with app.app_context():
        yield app


# --- queue_ro_crate_validation_task --------------------------------------


@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.resolve_crate")
@patch("app.services.validation_service._build_storage")
def test_queue_resolves_then_delays(mock_storage, mock_resolve, mock_delay, app_ctx):
    response, status = queue_ro_crate_validation_task("crate123", "ro-crate", "https://hook")

    assert status == 202
    assert response.json == {"message": "Validation in progress"}
    mock_resolve.assert_called_once()
    mock_delay.assert_called_once_with("crate123", "ro-crate", "https://hook")


@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.resolve_crate", side_effect=CrateNotFound("nope"))
@patch("app.services.validation_service._build_storage")
def test_queue_not_found_propagates_without_queueing(
    mock_storage, mock_resolve, mock_delay, app_ctx
):
    with pytest.raises(CrateNotFound):
        queue_ro_crate_validation_task("missing")
    mock_delay.assert_not_called()


@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.resolve_crate", side_effect=AmbiguousCrate("both"))
@patch("app.services.validation_service._build_storage")
def test_queue_ambiguous_propagates_without_queueing(
    mock_storage, mock_resolve, mock_delay, app_ctx
):
    with pytest.raises(AmbiguousCrate):
        queue_ro_crate_validation_task("dup")
    mock_delay.assert_not_called()


# --- run_metadata_validation (synchronous) -------------------------------


@patch("app.services.validation_service.validate_metadata")
def test_run_metadata_validation_valid_is_200(mock_validate, flask_app):
    mock_validate.return_value = ValidationOutcome(
        status=ValidationStatus.VALID, profile="ro-crate", detail={"report": "ok"}
    )

    response, status = run_metadata_validation('{"@graph": []}', "ro-crate", "/app/profiles")

    assert status == 200
    assert response.json["status"] == "valid"
    mock_validate.assert_called_once_with(
        {"@graph": []}, profile_name="ro-crate", profiles_path="/app/profiles"
    )


@patch("app.services.validation_service.validate_metadata")
def test_run_metadata_validation_invalid_is_200(mock_validate, flask_app):
    mock_validate.return_value = ValidationOutcome(
        status=ValidationStatus.INVALID, detail={"issues": [1]}
    )
    response, status = run_metadata_validation('{"@graph": []}')
    assert status == 200
    assert response.json["status"] == "invalid"


@patch("app.services.validation_service.validate_metadata")
def test_run_metadata_validation_error_outcome_is_422(mock_validate, flask_app):
    mock_validate.return_value = ValidationOutcome.from_error("validator blew up")
    response, status = run_metadata_validation('{"@graph": []}')
    assert status == 422
    assert response.json["status"] == "error"
    assert "validator blew up" in response.json["error"]


@pytest.mark.parametrize(
    "crate_json, response_error",
    [
        (None, "Missing required parameter: crate_json"),
        ("", "Missing required parameter: crate_json"),
        ("{", "not valid JSON"),
        ("{}", "empty"),
    ],
    ids=["missing", "blank", "invalid_json", "empty_json"],
)
def test_run_metadata_validation_json_errors(flask_app, crate_json, response_error):
    response, status = run_metadata_validation(crate_json)
    assert status == 422
    assert response_error in response.json["error"]


# --- get_ro_crate_validation_task ----------------------------------------


@patch("app.services.validation_service._build_storage")
def test_get_returns_stored_result(mock_storage, app_ctx):
    storage = InMemoryStorage()
    storage.put_bytes(result_key("validation-results", "crate123"), b'{"status": "valid"}')
    mock_storage.return_value = storage

    response, status = get_ro_crate_validation_task("crate123")

    assert status == 200
    assert response.json["status"] == "valid"


@patch("app.services.validation_service._build_storage")
def test_get_missing_result_is_404(mock_storage, app_ctx):
    mock_storage.return_value = InMemoryStorage()

    with pytest.raises(InvalidAPIUsage) as exc_info:
        get_ro_crate_validation_task("crate123")
    assert exc_info.value.status_code == 404


@patch("app.services.validation_service._build_storage")
def test_get_invalid_id_raises(mock_storage, app_ctx):
    mock_storage.return_value = InMemoryStorage()
    with pytest.raises(InvalidCrateId):
        get_ro_crate_validation_task("../bad")
