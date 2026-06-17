"""Tests for the health and readiness endpoints."""

from unittest import mock

import pytest

from app import create_app
from app.utils.config import Settings


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
def disabled_client():
    return create_app(settings=Settings.from_env({})).test_client()


@pytest.fixture
def storage_client():
    return create_app(settings=Settings.from_env(_storage_env())).test_client()


def test_healthz_is_always_ok(disabled_client):
    response = disabled_client.get("/healthz")
    assert response.status_code == 200
    assert response.json["status"] == "ok"


def test_readyz_ready_when_storage_disabled(disabled_client):
    response = disabled_client.get("/readyz")
    assert response.status_code == 200
    assert response.json["status"] == "ready"
    assert response.json["checks"]["storage"] == "disabled"


def test_readyz_ok_when_all_checks_pass(storage_client):
    with mock.patch("app.health.check_storage", return_value=(True, "ok")), \
         mock.patch("app.health.check_broker", return_value=(True, "ok")):
        response = storage_client.get("/readyz")

    assert response.status_code == 200
    assert response.json["status"] == "ready"


def test_readyz_503_when_storage_unreachable(storage_client):
    with mock.patch("app.health.check_storage", return_value=(False, "bucket down")), \
         mock.patch("app.health.check_broker", return_value=(True, "ok")):
        response = storage_client.get("/readyz")

    assert response.status_code == 503
    assert response.json["status"] == "not ready"
    assert response.json["checks"]["storage"] == "bucket down"


def test_readyz_503_when_broker_unreachable(storage_client):
    with mock.patch("app.health.check_storage", return_value=(True, "ok")), \
         mock.patch("app.health.check_broker", return_value=(False, "broker down")):
        response = storage_client.get("/readyz")

    assert response.status_code == 503
    assert response.json["checks"]["broker"] == "broker down"
