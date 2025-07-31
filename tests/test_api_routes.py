from flask.testing import FlaskClient
import pytest
from unittest.mock import patch
from app import create_app


@pytest.fixture
def client():
    app = create_app()
    return app.test_client()


# Test POST API: /v1/ro_crates/{crate_id}/validation

def test_validate_by_id_success(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
        "root_path": "base_path",
        "webhook_url": "https://webhook.example.com",
        "profile_name": "default"
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_queue.assert_called_once_with("test_bucket", "crate-123", "base_path", "default", "https://webhook.example.com")


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
def test_validate_fails_missing_elements(client, crate_id, payload, status_code):
    response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)
    assert response.status_code == status_code


def test_validate_by_id_missing_root_path_and_profile_name_and_webhook_url(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_queue.assert_called_once_with("test_bucket", "crate-123", None, None, None)


def test_validate_by_id_missing_profile_name(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
        "root_path": "base_path",
        "webhook_url": "https://webhook.example.com"
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_queue.assert_called_once_with("test_bucket", "crate-123", "base_path", None, "https://webhook.example.com")


def test_validate_by_id_missing_webhook_url(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
        "root_path": "base_path",
        "profile_name": "default"
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_queue.assert_called_once_with("test_bucket", "crate-123", "base_path", "default", None)


def test_validate_by_id_missing_root_path(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
        "profile_name": "default",
        "webhook_url": "https://webhook.example.com"
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_validation_task") as mock_queue:
        mock_queue.return_value = ({"message": "Validation in progress"}, 202)

        response = client.post(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_queue.assert_called_once_with("test_bucket", "crate-123", None, "default", "https://webhook.example.com")


# Test POST API: /v1/ro_crates/validate_metadata

def test_validate_metadata_with_all_fields(client: FlaskClient):
    """
    If both profile name and crate json are provided then processing
    of the RO-Crate metadata should continue, and return:
        - 200 status code
        - JSON data object which includes "status" which is "success"
    """
    test_data = {
        "crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}',
        "profile_name": "default"
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_metadata_validation_task") as mock_queue:
        mock_queue.return_value = ({"status": "success"}, 200)

        response = client.post("/v1/ro_crates/validate_metadata", json=test_data)
        print(response.json)
        mock_queue.assert_called_once_with(
                test_data["crate_json"],
                test_data["profile_name"]
                )
        assert response.status_code == 200
        assert response.json == {"status": "success"}


def test_validate_metadata_without_profile_name(client: FlaskClient):
    """
    If the profile name is not specified then processing
    of the RO-Crate metadata should continue, and return:
        - 200 status code
        - JSON data object which includes "status" which is "success"
    """
    test_data = {
        "crate_json": '{"@context": "https://w3id.org/ro/crate/1.1/context"}'
    }

    with patch("app.ro_crates.routes.post_routes.queue_ro_crate_metadata_validation_task") as mock_queue:
        mock_queue.return_value = ({"status": "success"}, 200)

        response = client.post("/v1/ro_crates/validate_metadata", json=test_data)
        mock_queue.assert_called_once_with(test_data["crate_json"], None)
        assert response.status_code == 200
        assert response.json == {"status": "success"}


def test_validate_metadata_missing_crate_json(client: FlaskClient):
    """
    If the RO-Crate is missing APIFlask should return:
        - 422 status code
        - Error message which includes 'Missing data for required field'
    """
    test_data = {
        "profile_name": "default"
    }

    response = client.post("/v1/ro_crates/validate_metadata", json=test_data)
    assert response.status_code == 422
    assert "Missing data for required field" in response.get_data(as_text=True)


def test_validate_metadata_emptystring_crate_json(client: FlaskClient):
    """
    If the RO-Crate is missing APIFlask should return:
        - 422 status code
        - Error message which includes 'Missing required parameter'
    """
    test_data = {
        "crate_json": "",
        "profile_name": "default"
    }

    response = client.post("/v1/ro_crates/validate_metadata", json=test_data)
    assert response.status_code == 422
    assert "Missing required parameter" in response.get_data(as_text=True)


def test_validate_metadata_malformed_crate_json(client: FlaskClient):
    """
    If the RO-Crate is missing APIFlask should return:
        - 422 status code
        - Error message which includes 'not valid JSON'
    """
    test_data = {
        "crate_json": "{",
        "profile_name": "default"
    }

    response = client.post("/v1/ro_crates/validate_metadata", json=test_data)
    assert response.status_code == 422
    assert "not valid JSON" in response.get_data(as_text=True)


def test_validate_metadata_emptydict_crate_json(client: FlaskClient):
    """
    If the RO-Crate is missing APIFlask should return:
        - 422 status code
        - Error message which includes 'Required parameter crate_json is empty'
    """
    test_data = {
        "crate_json": "{}",
        "profile_name": "default"
    }

    response = client.post("/v1/ro_crates/validate_metadata", json=test_data)
    assert response.status_code == 422
    assert "Required parameter crate_json is empty" in response.get_data(as_text=True)


# Test GET API: /v1/ro_crates/{crate_id}/validation

def test_get_validation_by_id_success(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
        "root_path": "base_path"
    }

    with patch("app.ro_crates.routes.get_routes.get_ro_crate_validation_task") as mock_get:
        mock_get.return_value = ({"status": "valid"}, 200)

        response = client.get(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 200
        assert response.json == {"status": "valid"}
        mock_get.assert_called_once_with("test_bucket", "crate-123", "base_path")


def test_get_validation_by_id_fails_missing_crate_id(client):
    payload = {
        "minio_bucket": "test_bucket",
        "root_path": "base_path"
    }

    response = client.get("/v1/ro_crates//validation", json=payload)

    assert response.status_code == 404


def test_get_validation_by_id_fails_missing_minio_bucket(client):
    crate_id = "crate-123"
    payload = {
        "root_path": "base_path"
    }

    response = client.get(f"/v1/ro_crates/{crate_id}/validation", json=payload)

    assert response.status_code == 422


def test_get_validation_by_id_missing_root_path(client):
    crate_id = "crate-123"
    payload = {
        "minio_bucket": "test_bucket",
    }

    with patch("app.ro_crates.routes.get_routes.get_ro_crate_validation_task") as mock_get:
        mock_get.return_value = ({"message": "Validation in progress"}, 202)

        response = client.get(f"/v1/ro_crates/{crate_id}/validation", json=payload)

        assert response.status_code == 202
        assert response.json == {"message": "Validation in progress"}
        mock_get.assert_called_once_with("test_bucket", "crate-123", None)
