"""Defines get API endpoints for validating RO-Crates using their IDs from MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint, Schema
from apiflask.fields import String, Boolean
from marshmallow.fields import Nested
from flask import Response

from app.services.validation_service import get_ro_crate_validation_task

get_routes_bp = APIBlueprint("get_routes", __name__)


class MinioConfig(Schema):
    endpoint = String(required=True)
    accesskey = String(required=True)
    secret = String(required=True)
    ssl = Boolean(required=True)
    bucket = String(required=True)


class ValidateResult(Schema):
    minio_config = Nested(MinioConfig, required=True)
    root_path = String(required=False)


@get_routes_bp.get("<string:crate_id>/validation")
@get_routes_bp.input(ValidateResult(partial=False), location='json')
def get_ro_crate_validation_by_id(json_data, crate_id) -> tuple[Response, int]:
    """
    Endpoint to obtain an RO-Crate validation result using its ID from MinIO.

    Path Parameters:
    - **crate_id**: The RO-Crate ID. _Required_.

    Request Body Parameters:
    - **minio_config**: The MinIO bucket containing the RO-Crate. _Required_
      - **endpoint**: Endpoint, e.g. 'localhost:9000'
      - **accesskey**: Access key / username
      - **secret**: Secret / password
      - **ssl**: Use SSL encryption? True/False
      - **bucket**: The MinIO bucket to access 
    - **root_path**: The root path containing the RO-Crate. _Optional_

    Returns:
    - A tuple containing the validation result and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_id`) are missing.
    """

    minio_config = json_data["minio_config"]

    if "root_path" in json_data:
        root_path = json_data["root_path"]
    else:
        root_path = None

    return get_ro_crate_validation_task(minio_config, crate_id, root_path)
