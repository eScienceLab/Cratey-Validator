"""Logging service for the application."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure the logging for the application.

    :param level: The logging level to set. Defaults to INFO.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
