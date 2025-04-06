"""Initialises and configures Flask, integrates Celery, and registers application blueprints."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import os

from flask import Flask
from flask_restx import Api

from app.celery_worker import make_celery, celery
from app.ro_crates.routes import api as ro_crate_api
from app.utils.config import DevelopmentConfig, ProductionConfig, make_celery


def gather_apis() -> Api:
    """
    Gather all the API's from all services together.

    :return: Api: A configured API instance.
    """
    api = Api(
        title='RO-Crate Validator',
        version='0.1',
        description='RO-Crate Validation Service'
    )
    api.add_namespace(ro_crate_api)
    
    return(api)


def create_app() -> Flask:
    """
    Creates and configures Flask application.

    :return: Flask: A configured Flask application instance.
    """
    app = Flask(__name__)
    #app.register_blueprint(ro_crates_bp, url_prefix="/ro_crates")
    #app = ro_crates_bp

    api = gather_apis()
    api.init_app(app)

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
