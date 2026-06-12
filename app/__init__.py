"""Initialises and configures Flask, integrates Celery, and registers application blueprints."""

import logging

from apiflask import APIFlask

from app.ro_crates.routes import v1_post_bp, v1_minio_post_bp, v1_minio_get_bp
from app.utils.config import (
    Settings,
    InvalidAPIUsage,
    make_celery,
)
from flask import jsonify

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> APIFlask:
    """
    Creates and configures the Flask application.

    Configuration is loaded and validated up front via :class:`Settings`, so a
    misconfigured deployment fails at startup with a clear error rather than at
    the first request. A ``settings`` object may be injected for testing.

    :param settings: Pre-built settings; if omitted, loaded from the environment.
    :return: A configured Flask application instance.
    :raises ConfigError: If required configuration is missing or invalid.
    """
    if settings is None:
        settings = Settings.from_env()

    app = APIFlask(__name__)

    app.debug = settings.debug
    app.config["SETTINGS"] = settings
    app.config["STORAGE_ENABLED"] = settings.storage_enabled
    app.config["PROFILES_PATH"] = settings.profiles_path

    # Always available:
    app.register_blueprint(v1_post_bp, url_prefix="/v1/ro_crates")

    # Object storage is optional and disabled by default. Only register the
    # ID-based, store-backed routes when storage is enabled.
    if settings.storage_enabled:
        app.register_blueprint(v1_minio_post_bp, url_prefix="/v1/ro_crates")
        app.register_blueprint(v1_minio_get_bp, url_prefix="/v1/ro_crates")
        logger.info("Storage enabled: ID-based validation endpoints registered.")
    else:
        logger.info("Storage disabled: only metadata validation is available.")

    if app.debug:
        print("URL Map:")
        for rule in app.url_map.iter_rules():
            print(rule)

    @app.errorhandler(InvalidAPIUsage)
    def invalid_api_usage(e):
        return jsonify(e.to_dict()), e.status_code

    # Integrate Celery
    make_celery(app)

    return app
