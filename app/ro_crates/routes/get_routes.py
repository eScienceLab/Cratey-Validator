"""Defines get API endpoints for validating RO-Crates using their IDs from MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint, Schema
from apiflask.fields import Integer, String
from flask import request, Response

from app.services.validation_service import get_ro_crate_validation_task

get_routes_bp = APIBlueprint("get_routes", __name__)

class validate_data(Schema):
    crate_id = String(required=True)


@get_routes_bp.get("/get_validation_by_id")
@get_routes_bp.input(validate_data(partial=True), location='json')
def get_ro_crate_validation_by_id(json_data) -> tuple[Response, int]:
    """
    Endpoint to obtain an RO-Crate validation result using its ID from MinIO.

    Args:
      crate_id:
        The ID of the RO-Crate to validate. _Required_.

    Returns:
      A tuple containing the validation result and an HTTP status code.

    Raises:
      KeyError: If required parameters (`crate_id`) are missing.
    """

    try:
        crate_id = json_data["crate_id"]
    except:
        raise KeyError("Missing required parameter: 'crate_id'")

    return get_ro_crate_validation_task(crate_id)
