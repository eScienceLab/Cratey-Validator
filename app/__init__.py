"""Initialises and configures Flask, integrates Celery, and registers application blueprints."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import os

from apiflask import APIFlask

from app.celery_worker import make_celery, celery
from app.ro_crates.routes import ro_crates_post_bp, ro_crates_get_bp
from app.utils.config import DevelopmentConfig, ProductionConfig, make_celery


def create_app() -> APIFlask:
    """
    Creates and configures Flask application.

    :return: Flask: A configured Flask application instance.
    """
    app = APIFlask(__name__)
    app.register_blueprint(ro_crates_post_bp, url_prefix="/ro_crates")
    app.register_blueprint(ro_crates_get_bp, url_prefix="/ro_crates")

    # Load configuration:
    if os.getenv("FLASK_ENV") == "production":
        app.config.from_object(ProductionConfig)
    else:
        # Development environment:
        app.debug = True
        print("URL Map:")
        for rule in app.url_map.iter_rules():
            print(rule)
        app.config.from_object(DevelopmentConfig)

    # Integrate Celery
    make_celery(app)

    return app
