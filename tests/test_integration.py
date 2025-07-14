import pytest
import subprocess
import time
import requests
import json
import os


@pytest.fixture(scope="session", autouse=True)
def docker_compose():
    """Start Docker Compose before tests, shut down after."""
    print("Starting Docker Compose...")
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose-develop.yml", "up", "-d"],
        check=True
    )
    time.sleep(10)  # Wait for services to start — adjust as needed

    yield  # Run the tests

    print("Stopping Docker Compose...")
    subprocess.run(["docker", "compose", "down"], check=True)


def test_validate_metadata():
    url = "http://localhost:5001/v1/ro_crates/validate_metadata"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # Load the JSON from file
    filepath = os.path.join("tests/data", "ro-crate-metadata.json")
    with open(filepath, "r", encoding="utf-8") as f:
        crate_json_data = json.load(f)

    # The API expects the JSON to be passed as a string
    payload = {
        "crate_json": json.dumps(crate_json_data)
    }

    response = requests.post(url, json=payload, headers=headers)

    response_result = json.loads(response.json()['result'])

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions — update based on expected API behavior
    assert response.status_code == 200
    assert response_result['passed'] is True
