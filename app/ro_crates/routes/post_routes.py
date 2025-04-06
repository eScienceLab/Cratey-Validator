"""Defines post API endpoints for validating RO-Crates using their IDs from MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint
from flask import request, Response

from app.services.validation_service import queue_ro_crate_validation_task

post_routes_bp = APIBlueprint("post_routes", __name__)


@post_routes_bp.route("/validate_by_id", methods=["POST"])
def validate_ro_crate_from_id() -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate using its ID from MinIO. Requires webhook_url.

    :param id: The ID of the RO-Crate to validate. Required.
    :param profile_name: The profile name for validation. Optional.
    :param webhook_url: The webhook URL where validation results will be sent. Required.
    :return: A tuple containing the validation task's response and an HTTP status code.
    :raises: KeyError: If required parameters (`id` or `webhook_url`) are missing.
    """

    crate_id = request.form.get("id")
    profile_name = request.form.get("profile_name")
    webhook_url = request.form.get("webhook_url")

    if not crate_id:
        raise KeyError("Missing required parameter: 'id'")
    if not webhook_url:
        raise KeyError("Missing required parameter: 'webhook_url'")

    return queue_ro_crate_validation_task(crate_id, profile_name, webhook_url)


@post_routes_bp.route("/validate_by_id_no_webhook", methods=["POST"])
def validate_ro_crate_from_id_no_webhook() -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate using its ID from MinIO. Does not require webhook_url.

    :param id: The ID of the RO-Crate to validate. Required.
    :param profile_name: The profile name for validation. Optional.
    :return: A tuple containing the validation task's response and an HTTP status code.
    :raises: KeyError: If required parameters (`id` or `webhook_url`) are missing.
    """
    crate_id = request.form.get("id")
    profile_name = request.form.get("profile_name")

    if not crate_id:
        raise KeyError("Missing required parameter: 'id'")

    return queue_ro_crate_validation_task(crate_id, profile_name)
