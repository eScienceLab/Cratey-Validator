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

post_routes_bp = APIBlueprint("post_routes", __name__)


class ValidateData(Schema):
    crate_id = String(required=True)
    profile_name = String(required=False)
    webhook_url = String(required=False)


class ValidateJSON(Schema):
    crate_json = String(required=True)
    profile_name = String(required=False)


@post_routes_bp.post("/validate_by_id")
@post_routes_bp.input(ValidateData(partial=True), location='json')
def validate_ro_crate_from_id(json_data) -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate using its ID from MinIO.

    Parameters:
    - **crate_id**: The ID of the RO-Crate to validate. _Required_.
    - **profile_name**: The profile name for validation. _Optional_.
    - **webhook_url**: The webhook URL where validation results will be sent. _Required_.

    Returns:
    - A tuple containing the validation task's response and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_id` or `webhook_url`) are missing.
    """

    try:
        crate_id = json_data["crate_id"]
    except:
        raise KeyError("Missing required parameter: 'crate_id'")
    try:
        webhook_url = json_data["webhook_url"]
    except:
        raise KeyError("Missing required parameter: 'webhook_url'")

    try:
        profile_name = json_data["profile_name"]
    except:
        profile_name = None

    return queue_ro_crate_validation_task(crate_id, profile_name, webhook_url)


@post_routes_bp.post("/validate_by_id_no_webhook")
@post_routes_bp.input(ValidateData(partial=True), location='json')  # -> json_data
def validate_ro_crate_from_id_no_webhook(json_data) -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate using its ID from MinIO.

    Parameters:
    - **crate_id**: The ID of the RO-Crate to validate. _Required_.
    - **profile_name**: The profile name for validation. _Optional_.

    Returns:
    - A tuple containing the validation task's response and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_id`) are missing.
    """

    try:
        crate_id = json_data['crate_id']
    except:
        raise KeyError("Missing required parameter: 'id'")

    try:
        profile_name = json_data['profile_name']
    except:
        profile_name = None

    return queue_ro_crate_validation_task(crate_id, profile_name)


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

    try:
        crate_json = json_data['crate_json']
    except:
        raise KeyError("Missing required parameter: 'crate_json'")

    try:
        profile_name = json_data['profile_name']
    except:
        profile_name = None

    return queue_ro_crate_metadata_validation_task(crate_json, profile_name)
