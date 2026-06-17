from unittest.mock import patch

import pytest
from flask.testing import FlaskClient

from app import create_app
from app.utils.config import Settings


def _storage_env() -> dict:
    """Returns complete storage-enabled environment for building a storage-backed app."""
    return {
        "STORAGE_ENABLED": "true",
        "S3_ENDPOINT": "localhost:9000",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin",
        "S3_BUCKET": "test_bucket",
        "CELERY_BROKER_URL": "redis://localhost:6379/0",
        "CELERY_RESULT_BACKEND": "redis://localhost:6379/1",
    }


@pytest.fixture
def client():
    """Client with storage disabled (the default): only metadata validation is exposed."""
    app = create_app(settings=Settings.from_env({}))
    return app.test_client()


@pytest.fixture
def storage_client():
    """Client with storage enabled, so the ID-based validation endpoints are registered."""
    app = create_app(settings=Settings.from_env(_storage_env()))
    return app.test_client()


# Test POST API: /v1/ro_crates/{crate_id}/validation


@pytest.mark.parametrize(
    "payload, expected_args",
    [
        (
            {"profile_name": "ro-crate", "webhook_url": "https://hook"},
            ("crate-123", "ro-crate", "https://hook"),
        ),
        ({"profile_name": "ro-crate"}, ("crate-123", "ro-crate", None)),
        ({"webhook_url": "https://hook"}, ("crate-123", None, "https://hook")),
        ({}, ("crate-123", None, None)),
    ],
    ids=["all_fields", "no_webhook", "no_profile", "empty_body"],
)
def test_validate_by_id_queues_and_returns_202(storage_client, payload, expected_args):
    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = storage_client.post("/v1/ro_crates/crate-123/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_queue.assert_called_once_with(*expected_args)


def test_validate_by_id_no_longer_accepts_credentials(storage_client):
    """The request body carries no storage credentials; only optional fields."""
    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = storage_client.post(
            "/v1/ro_crates/crate-123/validation",
            json={"profile_name": "ro-crate"},
        )

        assert response.status_code == 202
        # Only crate_id, profile, webhook are forwarded — no minio_config.
        mock_queue.assert_called_once_with("crate-123", "ro-crate", None)


# Test POST API: /v1/ro_crates/validate_metadata


@pytest.mark.parametrize(
    "payload, status_code, response_json, profiles_path",
    [
        (
            {
                "crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
                "profile_name": "default",
            },
            200,
            {"status": "valid"},
            None,
        ),
        (
            {"crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}'},
            200,
            {"status": "valid"},
            None,
        ),
    ],
    ids=["success_with_all_fields", "success_without_profile_name"],
)
def test_validate_metadata_success(
    client: FlaskClient, payload, status_code, response_json, profiles_path
):
    with patch("app.ro_crates.routes.post_routes.run_metadata_validation") as mock_run:
        mock_run.return_value = (response_json, status_code)

        response = client.post("/v1/ro_crates/validate_metadata", json=payload)

        crate_json = payload.get("crate_json")
        profile_name = payload.get("profile_name")
        mock_run.assert_called_once_with(crate_json, profile_name, profiles_path=profiles_path)
        assert response.status_code == status_code
        assert response.json == response_json


@pytest.mark.parametrize(
    "payload, status_code, response_text",
    [
        ({"profile_name": "default"}, 422, "Missing data for required field"),
        ({"crate_json": ""}, 422, "Missing required parameter"),
        ({"crate_json": "{"}, 422, "not valid JSON"),
        ({"crate_json": "{}"}, 422, "empty"),
    ],
    ids=["missing_crate", "blank_crate", "malformed_crate", "empty_crate"],
)
def test_validate_metadata_failure(client: FlaskClient, payload, status_code, response_text):
    response = client.post("/v1/ro_crates/validate_metadata", json=payload)
    assert response.status_code == status_code
    assert response_text in response.get_data(as_text=True)


# Test GET API: /v1/ro_crates/{crate_id}/validation


def test_get_validation_by_id_returns_result(storage_client):
    with patch("app.ro_crates.routes.get_routes.get_ro_crate_validation_task") as mock_get:
        mock_get.return_value = ({"status": "valid"}, 200)

        response = storage_client.get("/v1/ro_crates/crate-123/validation")

        assert response.status_code == 200
        assert response.json == {"status": "valid"}
        mock_get.assert_called_once_with("crate-123")


# Test store-backed endpoints are unavailable when storage is disabled (the default)


def test_post_route_not_registered_when_storage_disabled(client: FlaskClient):
    response = client.post("/v1/ro_crates/crate-123/validation", json={})
    assert response.status_code == 404


def test_get_route_not_registered_when_storage_disabled(client: FlaskClient):
    response = client.get("/v1/ro_crates/crate-123/validation")
    assert response.status_code == 404


def test_metadata_route_available_when_storage_disabled(client: FlaskClient):
    payload = {"crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}'}
    with patch("app.ro_crates.routes.post_routes.run_metadata_validation") as mock_run:
        mock_run.return_value = ({"status": "valid"}, 200)

        response = client.post("/v1/ro_crates/validate_metadata", json=payload)

        assert response.status_code == 200
        mock_run.assert_called_once()
