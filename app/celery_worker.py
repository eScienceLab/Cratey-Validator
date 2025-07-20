"""Configures and initialises a Celery instance for use with a Flask application."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from celery import Celery
from flask import Flask


def make_celery(app: Flask = None) -> Celery:
    """
    Create and configure a Celery instance using Flask configuration.

    :param app: The Flask application. Configuration will be used to initialise Celery.
    :return: Celery: A configured Celery instance.
    :raises ValueError: If the Flask configuration values are not provided.
    """
    if not app:
        raise ValueError(
            "A Flask application instance must be provided to configure Celery."
        )

    if "CELERY_RESULT_BACKEND" not in app.config:
        raise ValueError(
            "Missing configuration: 'CELERY_RESULT_BACKEND' is not defined in the Flask app config."
        )

    if "CELERY_BROKER_URL" not in app.config:
        raise ValueError(
            "Missing configuration: 'CELERY_BROKER_URL' is not defined in the Flask app config."
        )

    celery_instance = Celery(
        app.import_name,
        backend=app.config["CELERY_RESULT_BACKEND"],
        broker=app.config["CELERY_BROKER_URL"],
    )

    if app:
        celery_instance.conf.update(app.config)

    return celery_instance


celery = Celery()
