"""Initialises and configures Flask, integrates Celery, and registers application blueprints."""

import logging

from apiflask import APIFlask
from flask import jsonify, request

from app.crates.ids import InvalidCrateId
from app.crates.resolver import AmbiguousCrate, CrateNotFound
from app.health import health_bp
from app.ro_crates.routes import v1_minio_get_bp, v1_minio_post_bp, v1_post_bp
from app.services.logging_service import (
    get_request_id,
    new_request_id,
    set_request_id,
)
from app.storage.errors import StorageError
from app.utils.config import (
    InvalidAPIUsage,
    Settings,
    make_celery,
)

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"


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
    app.register_blueprint(health_bp)
    app.register_blueprint(v1_post_bp, url_prefix="/v1/ro_crates")

    # Object storage is optional and disabled by default. Only register the
    # ID-based, store-backed routes when storage is enabled.
    if settings.storage_enabled:
        app.register_blueprint(v1_minio_post_bp, url_prefix="/v1/ro_crates")
        app.register_blueprint(v1_minio_get_bp, url_prefix="/v1/ro_crates")
        logger.info("Storage enabled: ID-based validation endpoints registered.")
    else:
        logger.info("Storage disabled: only metadata validation is available.")

    @app.before_request
    def assign_request_id():
        set_request_id(request.headers.get(REQUEST_ID_HEADER) or new_request_id())

    @app.after_request
    def attach_request_id(response):
        response.headers[REQUEST_ID_HEADER] = get_request_id()
        return response

    @app.errorhandler(InvalidAPIUsage)
    def invalid_api_usage(e):
        return jsonify(e.to_dict()), e.status_code

    @app.errorhandler(InvalidCrateId)
    def invalid_crate_id(e):
        return jsonify({"error": str(e)}), 400

    @app.errorhandler(CrateNotFound)
    def crate_not_found(e):
        return jsonify({"error": str(e)}), 404

    @app.errorhandler(AmbiguousCrate)
    def ambiguous_crate(e):
        return jsonify({"error": str(e)}), 409

    @app.errorhandler(StorageError)
    def storage_error(e):
        logger.error("Storage error: %s", e)
        return jsonify({"error": "Storage backend unavailable"}), 503

    # Integrate Celery
    make_celery(app)

    return app
