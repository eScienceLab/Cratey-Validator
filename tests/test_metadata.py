from flask.testing import FlaskClient
import pytest
from unittest.mock import patch
from app import create_app


@pytest.fixture
def client():
    app = create_app()
    return app.test_client()


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

        response = client.post("/v1/validate_metadata", json=test_data)
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

        response = client.post("/v1/validate_metadata", json=test_data)
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

    response = client.post("/v1/validate_metadata", json=test_data)
    assert response.status_code == 422
    assert "Missing data for required field" in response.get_data(as_text=True)
