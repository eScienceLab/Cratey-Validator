"""Service methods to queue RO-Crates for validation using the CRS4 validator and Celery."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging
import json

from flask import jsonify, Response

from app.tasks.validation_tasks import (
    process_validation_task_by_id,
    process_validation_task_by_metadata,
    return_ro_crate_validation,
    check_ro_crate_exists,
    check_validation_exists
    )

from app.utils.config import InvalidAPIUsage


logger = logging.getLogger(__name__)


def queue_ro_crate_validation_task(
    minio_bucket, crate_id, root_path=None, profile_name=None, webhook_url=None
) -> tuple[Response, int]:
    """
    Queues an RO-Crate for validation with Celery.

    :param minio_bucket: The MinIO bucket containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate to validate.
    :param root_path: The root path containing the RO-Crate.
    :param profile_name: The profile to validate against.
    :param webhook_url: The URL to POST the validation results to.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises: Exception: If an error occurs whilst queueing the task.
    """

    logging.info(f"Processing: {crate_id}, {profile_name}, {webhook_url}")
    logging.info(f"Minio Bucket: {minio_bucket}; Root path: {root_path}")

    if check_ro_crate_exists(minio_bucket, crate_id, root_path):
        logging.info("RO-Crate exists")
    else:
        logging.info("RO-Crate does not exist")
        raise InvalidAPIUsage(f"No RO-Crate with prefix: {crate_id}", 400)

    try:
        process_validation_task_by_id.delay(minio_bucket, crate_id, root_path, profile_name, webhook_url)
        return jsonify({"message": "Validation in progress"}), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def queue_ro_crate_metadata_validation_task(
    crate_json: str, profile_name=None, webhook_url=None
) -> tuple[Response, int]:
    """
    Queues an RO-Crate for validation with Celery.

    :param crate_id: The ID of the RO-Crate to validate.
    :param profile_name: The profile to validate against.
    :param webhook_url: The URL to POST the validation results to.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises: Exception: If an error occurs whilst queueing the task.
    """

    logging.info(f"Processing: {crate_json}, {profile_name}, {webhook_url}")

    if not crate_json:
        return jsonify({"error": "Missing required parameter: crate_json"}), 422

    try:
        json_dict = json.loads(crate_json)
    except json.decoder.JSONDecodeError as err:
        return jsonify({"error": f"Required parameter crate_json is not valid JSON: {err}"}), 422
    else:
        if len(json_dict) == 0:
            return jsonify({"error": "Required parameter crate_json is empty"}), 422

    try:
        result = process_validation_task_by_metadata.delay(
                                                     crate_json,
                                                     profile_name,
                                                     webhook_url
                )
        if webhook_url:
            return jsonify({"message": "Validation in progress"}), 202
        else:
            return jsonify({"result": result.get()}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_ro_crate_validation_task(
    minio_bucket: str,
    crate_id: str,
    root_path: str,
) -> tuple[Response, int]:
    """
    Retrieves an RO-Crate validation result.

    :param crate_id: The ID of the RO-Crate to validate.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises Exception: If an error occurs whilst retreiving validation result
    """
    logging.info(f"Retrieving validation for: {crate_id}")

    if check_ro_crate_exists(minio_bucket, crate_id, root_path):
        logging.info("RO-Crate exists")
    else:
        logging.info("RO-Crate does not exist")
        raise InvalidAPIUsage(f"No RO-Crate with prefix: {crate_id}", 400)

    if check_validation_exists(minio_bucket, crate_id, root_path):
        logging.info("Validation result exists")
    else:
        logging.info("Validation does not exist")
        raise InvalidAPIUsage(f"No validation result yet for RO-Crate: {crate_id}", 400)

    return return_ro_crate_validation(minio_bucket, crate_id, root_path), 200
