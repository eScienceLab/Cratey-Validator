import pytest
import subprocess
import time
import requests
import json
import os
import docker
from minio import Minio


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="session", autouse=True)
def docker_compose(docker_client):
    """Start Docker Compose before tests, shut down after."""
    print("Starting Docker Compose...")
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose-develop.yml", "up", "-d"],
        check=True
    )
    time.sleep(10)  # Wait for services to start — adjust as needed

    load_test_data_into_minio()

    yield  # Run the tests

    for container in docker_client.containers.list():
        if "cratey-validator" in container.name:
            logs = container.logs().decode("utf-8")

            print(f"\n======= Logs from {container.name} container =======")
            print(logs)

    print("Stopping Docker Compose...")
    subprocess.run(["docker", "compose", "down"], check=True)


def load_test_data_into_minio():
    """Connect to MinIO and upload test files."""
    minio_client = Minio(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "ro-crates"
    test_data_dir = "tests/data/ro_crates"

    # Ensure bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Walk and upload files
    for root, _, files in os.walk(test_data_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            object_name = os.path.relpath(file_path, test_data_dir)

            print(f"Uploading {file_path} as {object_name} to bucket {bucket_name}")
            minio_client.fput_object(bucket_name, object_name, file_path)


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


def test_no_rocrate_for_validation():
    ro_crate = "ro_crate_10"
    url = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    response = requests.post(url, json=payload, headers=headers)

    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions — update based on expected API behavior
    assert response.status_code == 400
    assert response_result['message'] == f"No RO-Crate with prefix: {ro_crate}"


def test_no_validation_result_for_missing_crate():
    ro_crate = "ro_crate_10"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 400
    assert response_result['message'] == f"No RO-Crate with prefix: {ro_crate}"


def test_get_existing_validation_result():
    ro_crate = "ro_crate_3"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 200
    assert response_result["passed"] is False


def test_rocrate_not_validated_yet():
    ro_crate = "ro_crate_not_validated"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 400
    assert response_result['message'] == f"No validation result yet for RO-Crate: {ro_crate}"


def test_zipped_rocrate_validation():
    ro_crate = "ro_crate_1"
    url_post = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    # POST action and tests
    response = requests.post(url_post, json=payload, headers=headers)
    response_result = response.json()['message']

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 202
    assert response_result == "Validation in progress"

    # wait for ro-crate to be validated
    time.sleep(10)

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    start_time = time.time()
    while response.status_code == 400:
        time.sleep(10)
        # GET action and tests
        response = requests.get(url_get, json=payload, headers=headers)
        response_result = response.json()
        # Print response for debugging
        print("Status Code:", response.status_code)
        print("Response JSON:", response_result)

        elapsed = time.time() - start_time
        if elapsed > 60:
            print("60 seconds passed. Exiting loop")
            break

    # Assertions
    assert response.status_code == 200
    assert response_result["passed"] is False


def test_directory_rocrate_validation():
    ro_crate = "ro_crate_2"
    url_post = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    # POST action and tests
    response = requests.post(url_post, json=payload, headers=headers)
    response_result = response.json()['message']

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 202
    assert response_result == "Validation in progress"

    # wait for ro-crate to be validated
    time.sleep(10)

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    start_time = time.time()
    while response.status_code == 400:
        time.sleep(10)
        # GET action and tests
        response = requests.get(url_get, json=payload, headers=headers)
        response_result = response.json()
        # Print response for debugging
        print("Status Code:", response.status_code)
        print("Response JSON:", response_result)

        elapsed = time.time() - start_time
        if elapsed > 60:
            print("60 seconds passed. Exiting loop")
            break

    # Assertions
    assert response.status_code == 200
    assert response_result["passed"] is False


def test_ignore_rocrates_not_on_basepath():
    ro_crate = "ro_crate_4"
    url_post = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates"
    }

    # POST action and tests
    response = requests.post(url_post, json=payload, headers=headers)
    response_result = response.json()['message']

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 400
    assert response_result == "No RO-Crate with prefix: ro_crate_4"


def test_zipped_rocrate_in_subdirectory_validation():
    ro_crate = "ro_crate_4"
    subdir_path = "project_a"
    url_post = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates",
        "root_path" : subdir_path
    }

    # POST action and tests
    response = requests.post(url_post, json=payload, headers=headers)
    response_result = response.json()['message']

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 202
    assert response_result == "Validation in progress"

    # wait for ro-crate to be validated
    time.sleep(10)

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    start_time = time.time()
    while response.status_code == 400:
        time.sleep(10)
        # GET action and tests
        response = requests.get(url_get, json=payload, headers=headers)
        response_result = response.json()
        # Print response for debugging
        print("Status Code:", response.status_code)
        print("Response JSON:", response_result)

        elapsed = time.time() - start_time
        if elapsed > 60:
            print("60 seconds passed. Exiting loop")
            break

    # Assertions
    assert response.status_code == 200
    assert response_result["passed"] is False


def test_directory_rocrate_in_subdirectory_validation():
    ro_crate = "ro_crate_5"
    subdir_path = "project_a"
    url_post = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    url_get = f"http://localhost:5001/v1/ro_crates/{ro_crate}/validation"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # The API expects the JSON to be passed as a string
    payload = {
        "minio_bucket" : "ro-crates",
        "root_path" : subdir_path
    }

    # POST action and tests
    response = requests.post(url_post, json=payload, headers=headers)
    response_result = response.json()['message']

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    # Assertions
    assert response.status_code == 202
    assert response_result == "Validation in progress"

    # wait for ro-crate to be validated
    time.sleep(10)

    # GET action and tests
    response = requests.get(url_get, json=payload, headers=headers)
    response_result = response.json()

    # Print response for debugging
    print("Status Code:", response.status_code)
    print("Response JSON:", response_result)

    start_time = time.time()
    while response.status_code == 400:
        time.sleep(10)
        # GET action and tests
        response = requests.get(url_get, json=payload, headers=headers)
        response_result = response.json()
        # Print response for debugging
        print("Status Code:", response.status_code)
        print("Response JSON:", response_result)

        elapsed = time.time() - start_time
        if elapsed > 60:
            print("60 seconds passed. Exiting loop")
            break

    # Assertions
    assert response.status_code == 200
    assert response_result["passed"] is False
