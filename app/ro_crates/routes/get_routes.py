"""GET endpoint for retrieving a stored RO-Crate validation result by ID."""

from apiflask import APIBlueprint
from flask import Response

from app.services.validation_service import get_ro_crate_validation_task

get_routes_bp = APIBlueprint("get_routes", __name__)


@get_routes_bp.get("<string:crate_id>/validation")
def get_ro_crate_validation_by_id(crate_id) -> tuple[Response, int]:
    """
    Obtain a stored RO-Crate validation result by its ID.

    Path Parameters:
    - **crate_id**: The RO-Crate ID. _Required_.

    Returns:
    - A tuple containing the stored validation result and an HTTP status code.
      Returns 404 if no result has been stored for the crate yet.
    """

    return get_ro_crate_validation_task(crate_id)
