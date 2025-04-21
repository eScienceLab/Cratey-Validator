"""Service methods to queue RO-Crates for validation using the CRS4 validator and Celery."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging

from flask import jsonify, Response

from app.tasks.validation_tasks import (
    process_validation_task_by_id,
    return_ro_crate_validation
    )

logger = logging.getLogger(__name__)


def queue_ro_crate_validation_task(
    crate_id, profile_name=None, webhook_url=None
) -> tuple[Response, int]:
    """
    Queues an RO-Crate for validation with Celery.

    :param crate_id: The ID of the RO-Crate to validate.
    :param profile_name: The profile to validate against.
    :param webhook_url: The URL to POST the validation results to.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises: Exception: If an error occurs whilst queueing the task.
    """

    logging.info(f"Processing: {crate_id}, {profile_name}, {webhook_url}")

    if not crate_id:
        return jsonify({"error": "Missing required parameter: crate_id"}), 400

    try:
        process_validation_task_by_id.delay(crate_id, profile_name, webhook_url)
        return jsonify({"message": "Validation in progress"}), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_ro_crate_validation_task(
    crate_id
) -> tuple[Response, int]:
    """
    Retrieves an RO-Crate validation result.

    :param crate_id: The ID of the RO-Crate to validate.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises Exception: If an error occurs whilst retrieving RO-Crate
    """
    logging.info(f"Retrieving validation for: {crate_id}")

    if not crate_id:
        return jsonify({"error": "Missing required parameter: crate_id"}), 400

    try:
        return return_ro_crate_validation(crate_id), 200
    except Exception as e:
        return jsonify({"service error": str(e)}), 500
