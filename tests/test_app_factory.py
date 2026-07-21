"""Tests for the application factory's configuration."""

import pytest

from app import create_app
from app.utils.config import ConfigError, Settings


def _storage_env() -> dict:
    return {
        "STORAGE_ENABLED": "true",
        "S3_ENDPOINT": "minio:9000",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin",
        "S3_BUCKET": "ro-crates",
        "CELERY_BROKER_URL": "redis://redis:6379/0",
        "CELERY_RESULT_BACKEND": "redis://redis:6379/1",
    }


def _route_paths(app) -> set:
    return {rule.rule for rule in app.url_map.iter_rules()}


def test_create_app_fails_fast_on_invalid_storage_config(monkeypatch):
    """The default startup path validates config and refuses to start when broken."""
    monkeypatch.setenv("STORAGE_ENABLED", "true")
    for var in (
        "S3_ENDPOINT",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "S3_BUCKET",
        "CELERY_BROKER_URL",
        "CELERY_RESULT_BACKEND",
    ):
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(ConfigError):
        create_app()


def test_storage_routes_absent_when_disabled():
    app = create_app(settings=Settings.from_env({}))

    paths = _route_paths(app)
    assert "/v1/ro_crates/validate_metadata" in paths
    assert not any("validation" in p for p in paths)
    assert app.config["STORAGE_ENABLED"] is False


def test_storage_routes_registered_when_enabled():
    app = create_app(settings=Settings.from_env(_storage_env()))

    paths = _route_paths(app)
    assert any(p.endswith("/validation") for p in paths)
    assert app.config["STORAGE_ENABLED"] is True


def test_profiles_path_exposed_to_app_config():
    app = create_app(settings=Settings.from_env({"PROFILES_PATH": "/custom/profiles"}))
    assert app.config["PROFILES_PATH"] == "/custom/profiles"


def test_response_includes_generated_request_id_header():
    app = create_app(settings=Settings.from_env({}))
    client = app.test_client()

    response = client.post("/v1/ro_crates/validate_metadata", json={"crate_json": "{}"})

    assert response.headers.get("X-Request-ID")


def test_incoming_request_id_is_echoed():
    app = create_app(settings=Settings.from_env({}))
    client = app.test_client()

    response = client.post(
        "/v1/ro_crates/validate_metadata",
        json={"crate_json": "{}"},
        headers={"X-Request-ID": "caller-supplied-id"},
    )

    assert response.headers["X-Request-ID"] == "caller-supplied-id"


def test_openapi_groups_posts_under_one_tag_with_unique_tags():
    """Both POST endpoints share one docs group and the spec has no duplicate tags."""
    app = create_app(settings=Settings.from_env(_storage_env()))
    spec = app.test_client().get("/openapi.json").json

    tag_names = [tag["name"] for tag in spec["tags"]]
    assert len(tag_names) == len(set(tag_names))

    paths = spec["paths"]
    assert paths["/v1/ro_crates/validate_metadata"]["post"]["tags"] == ["Post_Routes"]
    assert paths["/v1/ro_crates/{crate_id}/validation"]["post"]["tags"] == ["Post_Routes"]
    assert paths["/v1/ro_crates/{crate_id}/validation"]["get"]["tags"] == ["Get_Routes"]
