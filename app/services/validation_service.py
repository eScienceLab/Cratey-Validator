"""Service layer for RO-Crate validation requests."""

import json
import logging

from flask import Response, current_app, jsonify

from app.crates.ids import validate_crate_id
from app.crates.layout import result_key
from app.crates.resolver import resolve_crate
from app.storage.errors import ObjectNotFound
from app.storage.s3 import S3Backend
from app.tasks.validation_tasks import process_validation_task_by_id
from app.utils.config import InvalidAPIUsage
from app.validation.results import ValidationStatus
from app.validation.runner import validate_metadata

logger = logging.getLogger(__name__)


def _build_storage() -> S3Backend:
    """Build the storage backend from server-side settings."""
    settings = current_app.config["SETTINGS"]
    return S3Backend.from_settings(settings)


def queue_ro_crate_validation_task(
    crate_id: str, profile_name=None, webhook_url=None
) -> tuple[Response, int]:
    """
    Resolve a crate by ID and queue it for asynchronous validation.

    Credentials and layout are server-side; the request carries only the ID and
    optional profile/webhook. Resolution happens before queueing so a bad or
    missing crate is reported immediately. ``InvalidCrateId`` / ``CrateNotFound``
    / ``AmbiguousCrate`` / ``StorageError`` propagate to the app error handlers.

    :param crate_id: The ID of the RO-Crate to validate.
    :param profile_name: The profile to validate against.
    :param webhook_url: The URL to POST the validation result to.
    :return: A JSON response and HTTP status code.
    """
    settings = current_app.config["SETTINGS"]
    storage = _build_storage()

    # Raises if the crate is missing/ambiguous/invalid -> handled as 4xx.
    resolve_crate(storage, crate_id, settings.s3_crate_prefix)

    process_validation_task_by_id.delay(crate_id, profile_name, webhook_url)
    return jsonify({"message": "Validation in progress"}), 202


def run_metadata_validation(
    crate_json: str, profile_name=None, profiles_path=None
) -> tuple[Response, int]:
    """
    Validate RO-Crate metadata synchronously and return the result inline.

    Metadata-only validation is fast and stateless, so it runs in the request
    rather than via Celery. Returns 200 for a valid/invalid outcome and 422
    when the input cannot be validated (bad JSON, empty, or a validator error).

    :param crate_json: The RO-Crate JSON-LD metadata, as a string.
    :param profile_name: The profile to validate against.
    :param profiles_path: A path to the profile definition directory.
    :return: A JSON response and HTTP status code.
    """
    if not crate_json:
        return jsonify({"error": "Missing required parameter: crate_json"}), 422

    try:
        metadata = json.loads(crate_json)
    except json.JSONDecodeError as err:
        return jsonify({"error": f"crate_json is not valid JSON: {err}"}), 422

    if not metadata:
        return jsonify({"error": "Required parameter crate_json is empty"}), 422

    outcome = validate_metadata(metadata, profile_name=profile_name, profiles_path=profiles_path)
    status_code = 422 if outcome.status is ValidationStatus.ERROR else 200
    return jsonify(outcome.to_dict()), status_code


def get_ro_crate_validation_task(crate_id: str) -> tuple[Response, int]:
    """
    Return a crate's stored validation result.

    Reads the result object from the results prefix. A missing result yields a
    404; the stored outcome (including a persisted ``error`` outcome) is returned
    as-is otherwise.

    :param crate_id: The ID of the RO-Crate whose result is requested.
    :return: A JSON response and HTTP status code.
    """
    settings = current_app.config["SETTINGS"]
    storage = _build_storage()

    validate_crate_id(crate_id)  # raises InvalidCrateId -> 400

    try:
        data = storage.get_bytes(result_key(settings.s3_results_prefix, crate_id))
    except ObjectNotFound:
        raise InvalidAPIUsage(f"No validation result yet for RO-Crate: {crate_id}", 404)

    return jsonify(json.loads(data)), 200
