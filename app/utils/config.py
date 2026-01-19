"""Configuration module for the Flask application."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import os

from celery import Celery
from flask import Flask


def get_env(name: str, default=None, required=False):
    value = os.environ.get(name, default)
    if required and value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


class Config:
    """Base configuration class for the Flask application."""

    # Celery configuration:
    CELERY_BROKER_URL = get_env("CELERY_BROKER_URL", required=False)
    CELERY_RESULT_BACKEND = get_env("CELERY_RESULT_BACKEND", required=False)

    # rocrate validator configuration:
    PROFILES_PATH = get_env("PROFILES_PATH", required=False)


class DevelopmentConfig(Config):
    """Development configuration class."""
    DEBUG = True


class ProductionConfig(Config):
    """Production configuration class."""
    DEBUG = False


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
        rv['message'] = self.message
        return rv


def make_celery(app: Flask = None) -> Celery:
    """
    Initialises and configures a Celery instance with the Flask application.

    :param app: The Flask application to use.
    :return: The Celery instance.
    """
    env = os.environ.get("FLASK_ENV", "development")
    config_cls = ProductionConfig if env == "production" else DevelopmentConfig

    celery = Celery(
        app.import_name if app else __name__,
        broker=config_cls.CELERY_BROKER_URL,
        backend=config_cls.CELERY_RESULT_BACKEND,
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
