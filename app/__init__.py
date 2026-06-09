"""Initialises and configures Flask, integrates Celery, and registers application blueprints."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging
import os

from apiflask import APIFlask

from app.ro_crates.routes import v1_post_bp, v1_minio_post_bp, v1_minio_get_bp
from app.utils.config import (
    DevelopmentConfig,
    ProductionConfig,
    InvalidAPIUsage,
    make_celery,
)
from flask import jsonify

logger = logging.getLogger(__name__)


def create_app() -> APIFlask:
    """
    Creates and configures Flask application.

    :return: Flask: A configured Flask application instance.
    """
    app = APIFlask(__name__)

    # Load config before registering blueprints, so MINIO_ENABLED can
    # decide whether the backed endpoints are exposed.
    if os.getenv("FLASK_ENV") == "production":
        app.config.from_object(ProductionConfig)
    else:
        # Development environment:
        app.debug = True
        app.config.from_object(DevelopmentConfig)

    # Always available:
    app.register_blueprint(v1_post_bp, url_prefix="/v1/ro_crates")

    # MinIO is optional and disabled by default. Only register
    # the MinIO ID routes when enabled:
    if app.config.get("MINIO_ENABLED"):
        app.register_blueprint(v1_minio_post_bp, url_prefix="/v1/ro_crates")
        app.register_blueprint(v1_minio_get_bp, url_prefix="/v1/ro_crates")
        logger.info("MinIO storage enabled: ID-based validation endpoints registered.")
    else:
        logger.info("MinIO storage disabled: only metadata validation is available.")

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
