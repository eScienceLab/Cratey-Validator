import pytest
from unittest.mock import patch, MagicMock
from flask import Flask

from app.services.validation_service import (
    queue_ro_crate_validation_task,
    queue_ro_crate_metadata_validation_task,
    get_ro_crate_validation_task
)

from app.utils.minio_utils import InvalidAPIUsage


@pytest.fixture
def flask_app():
    app = Flask(__name__)
    with app.app_context():
        yield app


# Test function: queue_ro_crate_validation_task

@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.check_ro_crate_exists", return_value=True)
def test_queue_task_success(
    mock_exists,
    mock_delay,
    flask_app
):
    response, status_code = queue_ro_crate_validation_task("crate123", "profileA", "http://webhook.com")

    mock_exists.assert_called_once_with("crate123")
    mock_delay.assert_called_once_with("crate123", "profileA", "http://webhook.com")
    assert status_code == 202
    assert response.json == {"message": "Validation in progress"}


@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.check_ro_crate_exists", return_value=False)
def test_queue_ro_crate_missing_exception(
    mock_exists,
    mock_delay,
    flask_app
):
    with pytest.raises(InvalidAPIUsage) as exc_info:
        queue_ro_crate_validation_task("crate12z", "profileA", "http://webhook.com")

    assert "No RO-Crate with prefix: crate12z" in str(exc_info.value.message)
    mock_exists.assert_called_once_with("crate12z")
    mock_delay.assert_not_called()


@patch("app.services.validation_service.process_validation_task_by_id.delay", side_effect=Exception("Celery down"))
@patch("app.services.validation_service.check_ro_crate_exists", return_value=True)
def test_queue_task_exception(
    mock_exists,
    mock_delay,
    flask_app
):
    response, status_code = queue_ro_crate_validation_task("crate123")

    mock_exists.assert_called_once_with("crate123")
    assert status_code == 500
    assert response.json == {"error": "Celery down"}


# Test function: queue_ro_crate_metadata_validation_task

def test_queue_metadata_with_webhook(flask_app):
    with patch("app.services.validation_service.process_validation_task_by_metadata.delay") as mock_delay:
        mock_result = MagicMock()
        mock_delay.return_value = mock_result

        crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context"}'
        response, status = queue_ro_crate_metadata_validation_task(crate_json, "profile", "http://webhook")

        mock_delay.assert_called_once_with(crate_json, "profile", "http://webhook")
        assert status == 202
        assert response.json == {"message": "Validation in progress"}


def test_queue_metadata_without_webhook(flask_app):
    with patch("app.services.validation_service.process_validation_task_by_metadata.delay") as mock_delay:
        mock_result = MagicMock()
        mock_result.get.return_value = {"status": "ok"}
        mock_delay.return_value = mock_result

        crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context"}'
        response, status = queue_ro_crate_metadata_validation_task(crate_json, "profile", None)

        mock_delay.assert_called_once_with(crate_json, "profile", None)
        assert status == 200
        assert response.json == {"result": {"status": "ok"}}


def test_queue_metadata_missing_json(flask_app):
    response, status = queue_ro_crate_metadata_validation_task(None)

    assert status == 422
    assert response.json == {"error": "Missing required parameter: crate_json"}


def test_queue_metadata_invalid_json(flask_app):

    crate_json = '{'
    response, status = queue_ro_crate_metadata_validation_task(crate_json)

    assert status == 422
    assert 'not valid JSON' in response.json['error']


def test_queue_metadata_empty_json(flask_app):

    crate_json = '{}'
    response, status = queue_ro_crate_metadata_validation_task(crate_json)

    assert status == 422
    assert response.json == {"error": "Required parameter crate_json is empty"}


def test_queue_metadata_exception(flask_app):
    with patch("app.services.validation_service.process_validation_task_by_metadata.delay",
               side_effect=Exception("Celery error")):
        crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context"}'
        response, status = queue_ro_crate_metadata_validation_task(crate_json)

        assert status == 500
        assert response.json == {"error": "Celery error"}


# Test function: get_ro_crate_validation_task

def test_get_validation_success(flask_app):
    with patch("app.services.validation_service.return_ro_crate_validation") as mock_return:
        mock_return.return_value = {"status": "valid"}

        response, status = get_ro_crate_validation_task("crate123")

        mock_return.assert_called_once_with("crate123")
        assert status == 200
        assert response == {"status": "valid"}


def test_get_validation_missing_id(flask_app):
    response, status = get_ro_crate_validation_task(None)

    assert status == 400
    assert response.json == {"error": "Missing required parameter: crate_id"}


def test_get_validation_exception(flask_app):
    with patch("app.services.validation_service.return_ro_crate_validation",
               side_effect=InvalidAPIUsage("MinIO S3 Error: empty", 500)):

        with pytest.raises(InvalidAPIUsage) as exc_info:
            get_ro_crate_validation_task("crate789")

        assert exc_info.value.status_code == 500
        assert "MinIO S3 Error" in str(exc_info.value.message)
