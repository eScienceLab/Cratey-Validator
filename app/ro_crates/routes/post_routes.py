"""Defines post API endpoints for validating RO-Crates using their IDs from MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint, Schema
from apiflask.fields import String
from flask import Response

from app.services.validation_service import (
    queue_ro_crate_validation_task,
    queue_ro_crate_metadata_validation_task
)
from app.utils.config import InvalidAPIUsage


post_routes_bp = APIBlueprint("post_routes", __name__)


class ValidateCrate(Schema):
    profile_name = String(required=False)
    webhook_url = String(required=False)


class ValidateJSON(Schema):
    crate_json = String(required=True)
    profile_name = String(required=False)


@post_routes_bp.post("<string:crate_id>/validation")
@post_routes_bp.input(ValidateCrate(partial=False), location='json')
def validate_ro_crate_via_id(json_data, crate_id) -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate using its ID from MinIO.

    Parameters:
    - **profile_name**: The profile name for validation. _Optional_.
    - **webhook_url**: The webhook URL where validation results will be sent. _Optional_.

    Returns:
    - A tuple containing the validation task's response and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_id` or `webhook_url`) are missing.
    """

    if "webhook_url" in json_data:
        webhook_url = json_data["webhook_url"]
    else:
        webhook_url = None

    if "profile_name" in json_data:
        profile_name = json_data["profile_name"]
    else:
        profile_name = None

    return queue_ro_crate_validation_task(crate_id, profile_name, webhook_url)


@post_routes_bp.post("/validate_metadata")
@post_routes_bp.input(ValidateJSON(partial=False), location='json')  # -> json_data
def validate_ro_crate_metadata(json_data) -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate JSON file uploaded to the Service.

    Parameters:
    - **crate_json**: The RO-Crate JSON-LD, as a string. _Required_
    - **profile_name**: The profile name for validation. _Optional_.

    Returns:
    - A tuple containing the validation task's response and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_json`) are missing.
    """

    crate_json = json_data["crate_json"]

    if "profile_name" in json_data:
        profile_name = json_data["profile_name"]
    else:
        profile_name = None

    return queue_ro_crate_metadata_validation_task(crate_json, profile_name)
