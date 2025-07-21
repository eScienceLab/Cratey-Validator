"""Defines get API endpoints for validating RO-Crates using their IDs from MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint
from flask import Response

from app.services.validation_service import get_ro_crate_validation_task

get_routes_bp = APIBlueprint("get_routes", __name__)


@get_routes_bp.get("<string:crate_id>/validation")
def get_ro_crate_validation_by_id(crate_id) -> tuple[Response, int]:
    """
    Endpoint to obtain an RO-Crate validation result using its ID from MinIO.

    Returns:
    - A tuple containing the validation result and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_id`) are missing.
    """

    return get_ro_crate_validation_task(crate_id)
