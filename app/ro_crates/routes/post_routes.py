"""POST endpoints for validating RO-Crates by stored ID or by inline metadata."""

from apiflask import APIBlueprint, Schema
from apiflask.fields import String
from flask import Response, current_app

from app.services.validation_service import (
    queue_ro_crate_validation_task,
    run_metadata_validation,
)

# Always-on blueprint:
post_routes_bp = APIBlueprint("post_routes", __name__)

# Store-backed blueprint. Only registered when storage is enabled
# (see app.create_app), so the ID-based routes are unreachable by default.
minio_post_routes_bp = APIBlueprint("minio_post_routes", __name__)


class ValidateCrate(Schema):
    profile_name = String(required=False)
    webhook_url = String(required=False)


class ValidateJSON(Schema):
    crate_json = String(required=True)
    profile_name = String(required=False)


@minio_post_routes_bp.post("<string:crate_id>/validation")
@minio_post_routes_bp.input(ValidateCrate(partial=False), location="json")
def validate_ro_crate_via_id(json_data, crate_id) -> tuple[Response, int]:
    """
    Validate a stored RO-Crate by its ID.

    Storage credentials and layout are configured server-side; the request body
    carries only optional fields.

    Path Parameters:
    - **crate_id**: The RO-Crate ID. _Required_.

    Request Body Parameters:
    - **profile_name**: The profile name for validation. _Optional_.
    - **webhook_url**: The webhook URL where the validation result will be sent. _Optional_.

    Returns:
    - A tuple containing the validation task's response and an HTTP status code.
    """

    profile_name = json_data.get("profile_name")
    webhook_url = json_data.get("webhook_url")

    return queue_ro_crate_validation_task(crate_id, profile_name, webhook_url)


@post_routes_bp.post("/validate_metadata")
@post_routes_bp.input(ValidateJSON(partial=False), location="json")  # -> json_data
def validate_ro_crate_metadata(json_data) -> tuple[Response, int]:
    """
    Endpoint to validate an RO-Crate JSON file uploaded to the Service.

    Request Body Parameters:
    - **crate_json**: The RO-Crate JSON-LD, as a string. _Required_
    - **profile_name**: The profile name for validation. _Optional_.

    Returns:
    - A tuple containing the validation task's response and an HTTP status code.

    Raises:
    - KeyError: If required parameters (`crate_json`) are missing.
    """

    crate_json = json_data["crate_json"]
    profile_name = json_data.get("profile_name")

    settings = current_app.config["SETTINGS"]

    return run_metadata_validation(
        crate_json,
        profile_name,
        profiles_path=settings.profiles_path,
        extra_profiles_path=settings.extra_profiles_path,
        cache_path=settings.cache_path,
        offline=settings.validation_offline,
    )
