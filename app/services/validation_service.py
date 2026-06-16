"""Service methods to queue RO-Crates for validation using the CRS4 validator and Celery."""

import logging
import json

from flask import jsonify, Response

from app.tasks.validation_tasks import (
    process_validation_task_by_id,
    return_ro_crate_validation,
    check_ro_crate_exists,
    check_validation_exists,
)

from app.utils.config import InvalidAPIUsage
from app.utils.minio_utils import get_minio_client
from app.validation.runner import validate_metadata
from app.validation.results import ValidationStatus

logger = logging.getLogger(__name__)


def queue_ro_crate_validation_task(
    minio_config,
    crate_id,
    root_path=None,
    profile_name=None,
    webhook_url=None,
    profiles_path=None,
) -> tuple[Response, int]:
    """
    Queues an RO-Crate for validation with Celery.

    :param minio_config: Access settings for Minio instance containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate to validate.
    :param root_path: The root path containing the RO-Crate.
    :param profile_name: The profile to validate against.
    :param webhook_url: The URL to POST the validation results to.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises: Exception: If an error occurs whilst queueing the task.
    """

    logging.info(f"Processing: {crate_id}, {profile_name}, {webhook_url}")
    logging.info(f"Minio Bucket: {minio_config['bucket']}; Root path: {root_path}")

    minio_client = get_minio_client(minio_config)

    if check_ro_crate_exists(minio_client, minio_config["bucket"], crate_id, root_path):
        logging.info("RO-Crate exists")
    else:
        logging.info("RO-Crate does not exist")
        raise InvalidAPIUsage(f"No RO-Crate with prefix: {crate_id}", 400)

    try:
        process_validation_task_by_id.delay(
            minio_config, crate_id, root_path, profile_name, webhook_url, profiles_path
        )
        return jsonify({"message": "Validation in progress"}), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def run_metadata_validation(
    crate_json: str, profile_name=None, profiles_path=None
) -> tuple[Response, int]:
    """
    Validates RO-Crate metadata synchronously and returns the result inline.

    Metadata-only validation is fast and stateless, so it runs in the request
    rather than via Celery. Returns 200 for a valid/invalid outcome and 422
    when the input cannot be validated (bad JSON, empty, or a validator error).

    :param crate_json: The RO-Crate JSON-LD metadata, as a string.
    :param profile_name: The profile to validate against.
    :param profiles_path: A path to the profile definition directory.
    :return: A tuple containing a JSON response and an HTTP status code.
    """

    if not crate_json:
        return jsonify({"error": "Missing required parameter: crate_json"}), 422

    try:
        metadata = json.loads(crate_json)
    except json.JSONDecodeError as err:
        return jsonify({"error": f"crate_json is not valid JSON: {err}"}), 422

    if not metadata:
        return jsonify({"error": "Required parameter crate_json is empty"}), 422

    outcome = validate_metadata(
        metadata, profile_name=profile_name, profiles_path=profiles_path
    )
    status_code = 422 if outcome.status is ValidationStatus.ERROR else 200
    return jsonify(outcome.to_dict()), status_code


def get_ro_crate_validation_task(
    minio_config: dict,
    crate_id: str,
    root_path: str,
) -> tuple[Response, int]:
    """
    Retrieves an RO-Crate validation result.

    :param minio_config: Access settings for Minio instance containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate to validate.
    :param root_path: The root path containing the RO-Crate.
    :return: A tuple containing a JSON response and an HTTP status code.
    :raises Exception: If an error occurs whilst retreiving validation result
    """
    logging.info(f"Retrieving validation for: {crate_id}")

    minio_client = get_minio_client(minio_config)

    if check_ro_crate_exists(minio_client, minio_config["bucket"], crate_id, root_path):
        logging.info("RO-Crate exists")
    else:
        logging.info("RO-Crate does not exist")
        raise InvalidAPIUsage(f"No RO-Crate with prefix: {crate_id}", 400)

    if check_validation_exists(
        minio_client, minio_config["bucket"], crate_id, root_path
    ):
        logging.info("Validation result exists")
    else:
        logging.info("Validation does not exist")
        raise InvalidAPIUsage(f"No validation result yet for RO-Crate: {crate_id}", 400)

    return (
        return_ro_crate_validation(
            minio_client, minio_config["bucket"], crate_id, root_path
        ),
        200,
    )
