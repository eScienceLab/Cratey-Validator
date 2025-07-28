"""Defines get API endpoints for validating RO-Crates using their IDs from MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint, Schema
from apiflask.fields import String
from flask import Response

from app.services.validation_service import get_ro_crate_validation_task

get_routes_bp = APIBlueprint("get_routes", __name__)


class ValidateResult(Schema):
    minio_bucket = String(required=True)
    root_path = String(required=False)


@get_routes_bp.get("<string:crate_id>/validation")
@get_routes_bp.input(ValidateResult(partial=False), location='json')
def get_ro_crate_validation_by_id(json_data, crate_id) -> tuple[Response, int]:
    """
    Endpoint to obtain an RO-Crate validation result using its ID from MinIO.

    Path Parameters:
    - **crate_id**: The RO-Crate ID. _Required_.

    Request Body Parameters:
    - **minio_bucket**: The MinIO bucket containing the RO-Crate. _Required_
    - **root_path**: The root path containing the RO-Crate. _Optional_

    Returns:
    - A tuple containing the validation result and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_id`) are missing.
    """

    minio_bucket = json_data["minio_bucket"]

    if "root_path" in json_data:
        root_path = json_data["root_path"]
    else:
        root_path = None

    return get_ro_crate_validation_task(minio_bucket, crate_id, root_path)
