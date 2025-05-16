"""Configuration module for the Flask application."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import os

from celery import Celery
from flask import Flask


class Config:
    """Base configuration class for the Flask application."""

    SECRET_KEY = os.getenv("SECRET_KEY", "my_precious")

    # Celery configuration:
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

    # MinIO configuration:
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bucket-name")


class DevelopmentConfig(Config):
    """Development configuration class."""

    DEBUG = True
    ENV = "development"


class ProductionConfig(Config):
    """Production configuration class."""

    DEBUG = False
    ENV = "production"


def make_celery(app: Flask = None) -> Celery:
    """
    Initialises and configures a Celery instance with the Flask application.

    :param app: The Flask application to use.
    :return: The Celery instance.
    """
    celery = Celery(
        app.import_name if app else __name__,
        broker=os.getenv("CELERY_BROKER_URL"),
        backend=os.getenv("CELERY_RESULT_BACKEND"),
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
