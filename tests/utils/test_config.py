"""Tests for the validated Settings configuration object."""

import pytest

from app.utils.config import ConfigError, Settings


def test_defaults_when_storage_disabled():
    """With storage off, S3/Celery vars are not required and sensible defaults apply."""
    settings = Settings.from_env({})

    assert settings.storage_enabled is False
    assert settings.flask_env == "development"
    assert settings.debug is True
    assert settings.profiles_path is None
    assert settings.extra_profiles_path is None
    assert settings.cache_path is None
    assert settings.validation_offline is False


def test_validation_tuning_vars_are_read():
    settings = Settings.from_env(
        {
            "EXTRA_PROFILES_PATH": "/app/extra-profiles",
            "CACHE_PATH": "/app/.rocrate-cache",
            "VALIDATION_OFFLINE": "true",
        }
    )
    assert settings.extra_profiles_path == "/app/extra-profiles"
    assert settings.cache_path == "/app/.rocrate-cache"
    assert settings.validation_offline is True


def test_storage_enabled_requires_s3_and_broker_config():
    """Enabling storage without the needed vars fails early and naming every missing var."""
    with pytest.raises(ConfigError) as exc_info:
        Settings.from_env({"STORAGE_ENABLED": "true"})

    message = str(exc_info.value)
    for var in (
        "S3_ENDPOINT",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "S3_BUCKET",
        "CELERY_BROKER_URL",
        "CELERY_RESULT_BACKEND",
    ):
        assert var in message


def test_blank_required_value_is_treated_as_missing():
    """A whitespace-only required var counts as missing, not as a valid value."""
    env = _storage_env()
    env["S3_BUCKET"] = "   "

    with pytest.raises(ConfigError) as exc_info:
        Settings.from_env(env)

    assert "S3_BUCKET" in str(exc_info.value)


def test_valid_storage_config_populates_fields():
    """A complete storage config loads cleanly and parses booleans."""
    env = _storage_env()
    env["S3_USE_SSL"] = "true"

    settings = Settings.from_env(env)

    assert settings.storage_enabled is True
    assert settings.s3_endpoint == "minio:9000"
    assert settings.s3_bucket == "ro-crates"
    assert settings.s3_use_ssl is True
    assert settings.celery_broker_url == "redis://redis:6379/0"


def test_flask_env_production_disables_debug():
    settings = Settings.from_env({"FLASK_ENV": "production"})
    assert settings.flask_env == "production"
    assert settings.debug is False


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("no", False),
        ("", False),
        ("anything", False),
    ],
)
def test_storage_enabled_boolean_parsing(raw, expected):
    # Truthy values require a complete storage config; falsy values need nothing.
    env = _storage_env() if expected else {}
    env["STORAGE_ENABLED"] = raw
    assert Settings.from_env(env).storage_enabled is expected


def _storage_env() -> dict:
    """Returns a complete, valid storage-enabled environment for tests to mutate."""
    return {
        "STORAGE_ENABLED": "true",
        "S3_ENDPOINT": "minio:9000",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin",
        "S3_BUCKET": "ro-crates",
        "CELERY_BROKER_URL": "redis://redis:6379/0",
        "CELERY_RESULT_BACKEND": "redis://redis:6379/1",
    }
