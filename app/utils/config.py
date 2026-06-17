"""Configuration module for the Flask application."""

import os

from dataclasses import dataclass
from typing import Mapping, Optional

from celery import Celery
from flask import Flask


class ConfigError(RuntimeError):
    """Raised at startup when required configuration is missing or invalid."""


_TRUE_VALUES = ("true", "1", "yes", "on")


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in _TRUE_VALUES


def _clean(value: Optional[str]) -> Optional[str]:
    """Return a stripped value. Blank strings are treated as absent."""
    if value is None:
        return None
    value = value.strip()
    return value or None


@dataclass(frozen=True)
class Settings:
    """Validated application configuration loaded once at startup."""

    flask_env: str
    debug: bool
    storage_enabled: bool
    profiles_path: Optional[str]
    celery_broker_url: Optional[str]
    celery_result_backend: Optional[str]
    s3_endpoint: Optional[str]
    s3_access_key: Optional[str]
    s3_secret_key: Optional[str]
    s3_region: Optional[str]
    s3_bucket: Optional[str]
    s3_use_ssl: bool
    s3_crate_prefix: str
    s3_results_prefix: str

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "Settings":
        """Build Settings from an environment mapping, failing fast on bad config."""
        if env is None:
            env = os.environ

        flask_env = _clean(env.get("FLASK_ENV")) or "development"
        storage_enabled = _parse_bool(env.get("STORAGE_ENABLED"))

        # S3 validation needs both: (1) an object store, and (2) a broker;
        # require them up front so misconfiguration fails when starting, not
        # at the first request.
        if storage_enabled:
            required = (
                "S3_ENDPOINT",
                "S3_ACCESS_KEY",
                "S3_SECRET_KEY",
                "S3_BUCKET",
                "CELERY_BROKER_URL",
                "CELERY_RESULT_BACKEND",
            )
            missing = [name for name in required if _clean(env.get(name)) is None]
            if missing:
                raise ConfigError(
                    "STORAGE_ENABLED is true but these required variables are "
                    f"missing or blank: {', '.join(missing)}"
                )

        return cls(
            flask_env=flask_env,
            debug=flask_env != "production",
            storage_enabled=storage_enabled,
            profiles_path=_clean(env.get("PROFILES_PATH")),
            celery_broker_url=_clean(env.get("CELERY_BROKER_URL")),
            celery_result_backend=_clean(env.get("CELERY_RESULT_BACKEND")),
            s3_endpoint=_clean(env.get("S3_ENDPOINT")),
            s3_access_key=_clean(env.get("S3_ACCESS_KEY")),
            s3_secret_key=_clean(env.get("S3_SECRET_KEY")),
            s3_region=_clean(env.get("S3_REGION")),
            s3_bucket=_clean(env.get("S3_BUCKET")),
            s3_use_ssl=_parse_bool(env.get("S3_USE_SSL")),
            s3_crate_prefix=_clean(env.get("S3_CRATE_PREFIX")) or "crates",
            s3_results_prefix=_clean(env.get("S3_RESULTS_PREFIX"))
            or "validation-results",
        )


class InvalidAPIUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        super().__init__()
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv["message"] = self.message
        return rv


def make_celery(app: Flask = None) -> Celery:
    """
    Initialises and configures a Celery instance with the Flask application.

    :param app: The Flask application to use.
    :return: The Celery instance.
    """
    settings: Optional[Settings] = app.config.get("SETTINGS") if app else None

    celery = Celery(
        app.import_name if app else __name__,
        broker=settings.celery_broker_url if settings else None,
        backend=settings.celery_result_backend if settings else None,
    )

    if app:
        celery.conf.update(app.config)

        TaskBase = celery.Task

        class ContextTask(TaskBase):
            """Task class to run tasks within the Flask app context."""

            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return TaskBase.__call__(self, *args, **kwargs)

        celery.Task = ContextTask

    return celery
