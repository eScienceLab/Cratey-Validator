"""Configures and initialises a Celery instance for use with a Flask application."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from celery import Celery


celery = Celery()
