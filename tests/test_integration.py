"""Integration tests against the full Docker stack. Brings up the dev compose stack (flask +
celery + redis + RustFS objectstore), seeds crates into the canonical layout via boto3, and
drives the HTTP API.

Run with Docker available:  pytest -s -v tests/test_integration.py
Excluded from the unit-test run (it needs Docker).
"""

import os
import subprocess
import time

import boto3
import pytest
import requests

BASE_URL = "http://localhost:5001"
S3_URL = "http://localhost:9000"
BUCKET = "ro-crates"
CRATE_PREFIX = "crates"
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"
COMPOSE_FILE = "docker-compose-develop.yml"
PROJECT = "cratey_integration"
TEST_DATA = "tests/data/ro_crates"


def _compose(*args, env=None):
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            COMPOSE_FILE,
            "-p",
            PROJECT,
            "--profile",
            "objectstore",
            *args,
        ],
        check=True,
        env=env,
    )


def _wait_for(url, timeout=90):
    """Poll a URL until it responds, or fail after timeout seconds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=2).status_code:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {url}")


def _seed_crates(s3):
    """Create the bucket and upload test crates under the crates/ prefix."""
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.ClientError:
        pass  # already exists

    for root, _, files in os.walk(TEST_DATA):
        for name in files:
            path = os.path.join(root, name)
            rel = os.path.relpath(path, TEST_DATA)
            s3.upload_file(path, BUCKET, f"{CRATE_PREFIX}/{rel}")


def _poll_result(crate_id, timeout=90):
    """Poll the GET endpoint until a stored result appears (past 404)."""
    url = f"{BASE_URL}/v1/ro_crates/{crate_id}/validation"
    deadline = time.time() + timeout
    response = requests.get(url)
    while response.status_code == 404 and time.time() < deadline:
        time.sleep(3)
        response = requests.get(url)
    return response


@pytest.fixture(scope="session", autouse=True)
def stack():
    env = {
        **os.environ,
        "STORAGE_ENABLED": "true",
        "S3_ENDPOINT": "objectstore:9000",
        "S3_ACCESS_KEY": ACCESS_KEY,
        "S3_SECRET_KEY": SECRET_KEY,
        "S3_BUCKET": BUCKET,
        "S3_USE_SSL": "false",
        "RUSTFS_ACCESS_KEY": ACCESS_KEY,
        "RUSTFS_SECRET_KEY": SECRET_KEY,
    }
    _compose("up", "-d", "--build", env=env)
    try:
        _wait_for(f"{BASE_URL}/healthz")
        s3 = boto3.client(
            "s3",
            endpoint_url=S3_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        _seed_crates(s3)
        yield
    finally:
        _compose("down", "-v", env=env)


def test_healthz_and_readyz():
    assert requests.get(f"{BASE_URL}/healthz").json()["status"] == "ok"
    ready = requests.get(f"{BASE_URL}/readyz")
    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ready"
    assert body["checks"] == {"storage": "ok", "broker": "ok"}


def test_validate_metadata_inline():
    with open("tests/data/ro-crate-metadata.json", encoding="utf-8") as f:
        crate_json = f.read()

    response = requests.post(
        f"{BASE_URL}/v1/ro_crates/validate_metadata",
        json={"crate_json": crate_json},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "valid"


def test_missing_crate_returns_404():
    crate = "does_not_exist"
    response = requests.post(f"{BASE_URL}/v1/ro_crates/{crate}/validation", json={})
    assert response.status_code == 404
    assert response.json()["error"] == f"No crate found for ID '{crate}'"


def test_get_missing_result_returns_404():
    crate = "does_not_exist"
    response = requests.get(f"{BASE_URL}/v1/ro_crates/{crate}/validation")
    assert response.status_code == 404
    assert response.json()["message"] == f"No validation result yet for RO-Crate: {crate}"


def test_get_result_for_unvalidated_crate_returns_404():
    """A crate that exists but has not been validated yet has no stored result."""
    crate = "ro_crate_not_validated"
    response = requests.get(f"{BASE_URL}/v1/ro_crates/{crate}/validation")
    assert response.status_code == 404
    assert response.json()["message"] == f"No validation result yet for RO-Crate: {crate}"


def test_zipped_crate_validation():
    response = requests.post(f"{BASE_URL}/v1/ro_crates/ro_crate_1/validation", json={})
    assert response.status_code == 202
    assert response.json()["message"] == "Validation in progress"

    result = _poll_result("ro_crate_1")
    assert result.status_code == 200
    assert result.json()["status"] == "invalid"


def test_directory_crate_validation():
    response = requests.post(f"{BASE_URL}/v1/ro_crates/ro_crate_2/validation", json={})
    assert response.status_code == 202

    result = _poll_result("ro_crate_2")
    assert result.status_code == 200
    assert result.json()["status"] == "invalid"


def test_validation_with_explicit_profile():
    """A request carrying an explicit profile_name is accepted and produces a result."""
    response = requests.post(
        f"{BASE_URL}/v1/ro_crates/ro_crate_3/validation",
        json={"profile_name": "alpha-crate-0.1"},
    )
    assert response.status_code == 202

    result = _poll_result("ro_crate_3")
    assert result.status_code == 200
    assert result.json()["status"] == "invalid"
