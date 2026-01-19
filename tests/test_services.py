import pytest
from unittest.mock import patch, MagicMock
from flask import Flask
from flask.testing import FlaskClient

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

@pytest.mark.parametrize(
        "crate_id, rocrate_exists, minio_client, delay_side_effects, payload, profiles_path, status_code, response_dict",
        [
            (
                "crate123", True, "minio_client", None,
                {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                    "root_path": "base_path",
                    "webhook_url": "https://webhook.example.com",
                    "profile_name": "default"
                },
                None,
                202, {"message": "Validation in progress"}
            ),
            (
                "crate123", True, "minio_client", Exception("Celery down"),
                {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                    "root_path": "base_path",
                    "webhook_url": "https://webhook.example.com",
                    "profile_name": "default"
                },
                None,
                500, {"error": "Celery down"}
            ),
        ],
        ids=["successful_queue", "celery_server_down"]
)
@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.check_ro_crate_exists")
@patch("app.services.validation_service.get_minio_client")
def test_queue_ro_crate_validation_task(
    mock_client,
    mock_exists,
    mock_delay,
    flask_app: FlaskClient, crate_id: str, rocrate_exists: bool, minio_client: str,
    delay_side_effects: Exception, payload: dict, profiles_path: str, status_code: int, response_dict: dict
):
    mock_delay.side_effect = delay_side_effects
    mock_exists.return_value = rocrate_exists
    mock_client.return_value = minio_client

    minio_config = payload["minio_config"] if "minio_config" in payload else None
    root_path = payload["root_path"] if "root_path" in payload else None
    profile_name = payload["profile_name"] if "profile_name" in payload else None
    webhook_url = payload["webhook_url"] if "webhook_url" in payload else None

    response, status_code = queue_ro_crate_validation_task(minio_config, crate_id, root_path,
                                                           profile_name, webhook_url, profiles_path)

    mock_client.assert_called_once_with(minio_config)
    mock_exists.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, root_path)
    mock_delay.assert_called_once_with(minio_config, crate_id, root_path, profile_name, webhook_url, profiles_path)
    assert status_code == status_code
    assert response.json == response_dict


@pytest.mark.parametrize(
        "crate_id, rocrate_exists, minio_client, payload, iau_message",
        [
            (
                "crate12z", False, "minio_client",
                {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                    "root_path": "base_path",
                    "webhook_url": "https://webhook.example.com",
                    "profile_name": "default"
                }, "No RO-Crate with prefix: crate12z"
            ),
        ],
        ids=["no_rocrate_exists"]
)
@patch("app.services.validation_service.process_validation_task_by_id.delay")
@patch("app.services.validation_service.check_ro_crate_exists")
@patch("app.services.validation_service.get_minio_client")
def test_queue_ro_crate_validation_task_failure(
    mock_client,
    mock_exists,
    mock_delay,
    flask_app: FlaskClient, crate_id: str, rocrate_exists: bool,
    minio_client: str, payload: dict, iau_message: str
):
    mock_exists.return_value = rocrate_exists
    mock_client.return_value = minio_client

    minio_config = payload["minio_config"] if "minio_config" in payload else None
    root_path = payload["root_path"] if "root_path" in payload else None
    profile_name = payload["profile_name"] if "profile_name" in payload else None
    webhook_url = payload["webhook_url"] if "webhook_url" in payload else None

    with pytest.raises(InvalidAPIUsage) as exc_info:
        queue_ro_crate_validation_task(minio_config, crate_id, root_path, profile_name, webhook_url)

    assert iau_message in str(exc_info.value.message)
    mock_client.assert_called_once_with(minio_config)
    mock_exists.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, root_path)
    mock_delay.assert_not_called()


# Test function: queue_ro_crate_metadata_validation_task

@pytest.mark.parametrize(
        "crate_json, profile, webhook, status_code, return_value, response_json, delay_side_effect",
        [
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
                "default", "http://webhook",
                202, None, {"message": "Validation in progress"},
                None
            ),
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
                "default", None,
                200, {"status": "ok"}, {"result": {"status": "ok"}},
                None
            ),
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
                "default", "http://webhook",
                500, None, {"error": "Celery error"},
                Exception("Celery error")
            ),
        ],
        ids=["success_with_webhook", "success_without_webhook", "failure_celery_error"]
)
def test_queue_metadata(flask_app, crate_json: dict, profile: str, webhook: str,
                        status_code: int, return_value: dict, response_json: dict,
                        delay_side_effect: Exception):
    with patch("app.services.validation_service.process_validation_task_by_metadata.delay",
               side_effect=delay_side_effect) as mock_delay:
        mock_result = MagicMock()
        if return_value is not None:
            mock_result.get.return_value = return_value
        if delay_side_effect is None:
            mock_delay.return_value = mock_result

        response, status = queue_ro_crate_metadata_validation_task(crate_json, profile, webhook)

        mock_delay.assert_called_once_with(crate_json, profile, webhook)
        assert status == status_code
        assert response.json == response_json


@pytest.mark.parametrize(
        "crate_json, status_code, response_error",
        [
            (
                None,
                422, "Missing required parameter: crate_json"
            ),
            (
                "{",
                422, "not valid JSON"
            ),
            (
                "{}",
                422, "Required parameter crate_json is empty"
            ),
        ]
)
def test_queue_metadata_json_errors(flask_app, crate_json: str, status_code: int, response_error: str):
    response, status = queue_ro_crate_metadata_validation_task(crate_json)
    assert status == status_code
    assert response_error in response.json["error"]


# Test function: get_ro_crate_validation_task

@pytest.mark.parametrize(
        "minio_config, crate_id, crate_exists, validation_exists, " +
        "validation_value, status_code, error_message, minio_client",
        [
            (
                {
                    "endpoint": "localhost:9000",
                    "accesskey": "admin",
                    "secret": "password123",
                    "ssl": False,
                    "bucket": "test_bucket"
                },
                "crate123", True, True, {"status": "valid"}, 200, None,
                "minio_client"
            ),
            (
                {
                    "endpoint": "localhost:9000",
                    "accesskey": "admin",
                    "secret": "password123",
                    "ssl": False,
                    "bucket": "test_bucket"
                },
                "crate123", False, False, None, 400, "No RO-Crate with prefix: crate123",
                "minio_client"
            ),
            (
                {
                    "endpoint": "localhost:9000",
                    "accesskey": "admin",
                    "secret": "password123",
                    "ssl": False,
                    "bucket": "test_bucket"
                },
                "crate123", True, False, None, 400, "No validation result yet for RO-Crate: crate123",
                "minio_client"
            ),
        ],
        ids=["validation_exists", "rocrate_missing", "validation_missing"]
)
@patch("app.services.validation_service.check_ro_crate_exists")
@patch("app.services.validation_service.check_validation_exists")
@patch("app.services.validation_service.return_ro_crate_validation")
@patch("app.services.validation_service.get_minio_client")
def test_get_validation(
    mock_client,
    mock_return,
    mock_validation,
    mock_rocrate,
    flask_app, minio_config: dict, crate_id: str, crate_exists: bool,
    validation_exists: bool, validation_value: dict,
    status_code: int, error_message: str, minio_client: str
):
    mock_client.return_value = minio_client
    mock_rocrate.return_value = crate_exists
    mock_validation.return_value = validation_exists
    mock_return.return_value = validation_value

    if crate_exists and validation_exists:
        response, status = get_ro_crate_validation_task(minio_config, crate_id, "base_path")

        mock_client.assert_called_once_with(minio_config)
        mock_return.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, "base_path")
        mock_rocrate.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, "base_path")
        mock_validation.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, "base_path")

        assert status == status_code
        assert response == validation_value

    else:
        with pytest.raises(InvalidAPIUsage) as exc_info:
            get_ro_crate_validation_task(minio_config, crate_id, "base_path")

            assert exc_info.value.status_code == status_code
            assert error_message in str(exc_info.value.message)

            mock_rocrate.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, "base_path")
            if crate_exists:
                mock_validation.assert_called_once_with(minio_client, minio_config["bucket"], crate_id, "base_path")
            else:
                mock_validation.assert_not_called()
            mock_return.assert_not_called()
