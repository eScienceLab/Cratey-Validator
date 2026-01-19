from flask.testing import FlaskClient
import pytest
from unittest.mock import patch
from app import create_app


@pytest.fixture
def client():
    app = create_app()
    return app.test_client()


# Test POST API: /v1/ro_crates/{crate_id}/validation

@pytest.mark.parametrize(
        "crate_id, payload, profiles_path, status_code, response_json",
        [
            (
                "crate-123", {
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
                "crate-123", {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                    "root_path": "base_path",
                    "webhook_url": "https://webhook.example.com",
                },
                None,
                202, {"message": "Validation in progress"}
            ),
            (
                "crate-123", {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                    "root_path": "base_path",
                    "profile_name": "default"
                },
                None,
                202, {"message": "Validation in progress"}
            ),
            (
                "crate-123", {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                    "webhook_url": "https://webhook.example.com",
                    "profile_name": "default"
                },
                None,
                202, {"message": "Validation in progress"}
            ),
            (
                "crate-123", {
                    "minio_config": {
                        "endpoint": "localhost:9000",
                        "accesskey": "admin",
                        "secret": "password123",
                        "ssl": False,
                        "bucket": "test_bucket"
                    },
                },
                None,
                202, {"message": "Validation in progress"}
            ),
        ],
        ids=["validate_by_id", "validate_with_missing_profile_name",
             "validate_with_missing_webhook_url", "validate_with_missing_root_path",
             "validate_with_missing_root_path_and_profile_name_and_webhook_url"]
)
def test_validate_by_id_success(client: FlaskClient, crate_id: str, payload: dict,
                                profiles_path: str, status_code: int, response_json: dict):
    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = (response_json, status_code)

        response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        minio_config = payload["minio_config"] if "minio_config" in payload else None
        root_path = payload["root_path"] if "root_path" in payload else None
        profile_name = payload["profile_name"] if "profile_name" in payload else None
        webhook_url = payload["webhook_url"] if "webhook_url" in payload else None
        assert response.status_code == status_code
        assert response.json == response_json
        mock_queue.assert_called_once_with(minio_config, crate_id, root_path, profile_name, webhook_url, profiles_path)


@pytest.mark.parametrize(
    "crate_id, payload, status_code",
    [
        (
            "", {
                "minio_bucket": "test_bucket",
                "root_path": "base_path",
                "webhook_url": "https://webhook.example.com",
                "profile_name": "default"
            }, 404
        ),
        (
            "crate-123", {
                "root_path": "base_path",
                "webhook_url": "https://webhook.example.com",
                "profile_name": "default"
            }, 422
        ),
    ],
    ids=[
        "missing_crate_id_returns_404",
        "missing_minio_bucket_returns_422"
    ]
)
def test_validate_fails_missing_elements(client: FlaskClient, crate_id: str, payload: dict, status_code: int):
    response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)
    assert response.status_code == status_code


# Test POST API: /v1/ro_crates/validate_metadata

@pytest.mark.parametrize(
    "payload, status_code, response_json",
    [
        (
            {
                "crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
                "profile_name": "default"
            }, 200, {"status": "success"}
        ),
        (
            {
                "crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
            }, 200, {"status": "success"}
        ),
    ],
    ids=["success_with_all_fields", "success_without_profile_name"]
)
def test_validate_metadata_success(client: FlaskClient, payload: dict, status_code: int, response_json: dict):
    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_metadata_validation_task") as mock_queue:
        mock_queue.return_value = (response_json, status_code)

        response = client.post("/v1/ro_crates/validate_metadata", json=payload)

        crate_json = payload["crate_json"] if "crate_json" in payload else None
        profile_name = payload["profile_name"] if "profile_name" in payload else None

        mock_queue.assert_called_once_with(crate_json, profile_name)
        assert response.status_code == status_code
        assert response.json == response_json


@pytest.mark.parametrize(
    "payload, status_code, response_text",
    [
        (
            {
                "profile_name": "default"
            }, 422, "Missing data for required field"
        ),
        (
            {
                "crate_json": '',
            }, 422, "Missing required parameter"
        ),
        (
            {
                "crate_json": '{',
            }, 422, "not valid JSON"
        ),
        (
            {
                "crate_json": '{}',
            }, 422, "Required parameter crate_json is empty"
        ),
    ],
    ids=["failure_missing_crate", "failure_empty_crate",
         "failure_malformed_crate", "failure_empty_crate"]
)
def test_validate_metadata_failure(client: FlaskClient, payload: dict, status_code: int, response_text: str):
    response = client.post("/v1/ro_crates/validate_metadata", json=payload)
    assert response.status_code == status_code
    assert response_text in response.get_data(as_text=True)


# Test GET API: /v1/ro_crates/{crate_id}/validation

@pytest.mark.parametrize(
    "crate_id, payload, status_code",
    [
        (
            "", {
                "minio_config": {
                    "endpoint": "localhost:9000",
                    "accesskey": "admin",
                    "secret": "password123",
                    "ssl": False,
                    "bucket": "test_bucket"
                },
                "root_path": "base_path"
            }, 404
        ),
        (
            "crate-123", {
                "minio_config": {
                    "endpoint": "localhost:9000",
                    "accesskey": "admin",
                    "secret": "password123",
                    "ssl": False,
                },
                "root_path": "base_path"
            }, 422
        ),
    ],
    ids=["failure_missing_crate_id", "failure_missing_minio_bucket"]
)
def test_get_validation_by_id_failures(client: FlaskClient, crate_id: str, payload: dict, status_code: int):
    response = client.get(f"/v1/ro_crates/{crate_id}/validation", json=payload)
    assert response.status_code == status_code


def test_get_validation_by_id_success(client):
    crate_id = "crate-123"
    payload = {
        "minio_config": {
            "endpoint": "localhost:9000",
            "accesskey": "admin",
            "secret": "password123",
            "ssl": False,
            "bucket": "test_bucket"
        },
        "root_path": "base_path"
    }

    with patch("app.ro_crates.routes.get_routes.get_ro_crate_validation_task") as mock_get:
        mock_get.return_value = ({"status": "valid"}, 200)

        response = client.get(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 200
        assert response.json == {"status": "valid"}
        mock_get.assert_called_once_with(payload["minio_config"], "crate-123", "base_path")


def test_get_validation_by_id_missing_root_path(client):
    crate_id = "crate-123"
    payload = {
        "minio_config": {
            "endpoint": "localhost:9000",
            "accesskey": "admin",
            "secret": "password123",
            "ssl": False,
            "bucket": "test_bucket"
        }
    }

    with patch("app.ro_crates.routes.get_routes.get_ro_crate_validation_task") as mock_get:
        mock_get.return_value = ({"status": "valid"}, 200)

        response = client.get(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 200
        assert response.json == {"status": "valid"}
        mock_get.assert_called_once_with(payload["minio_config"], "crate-123", None)
