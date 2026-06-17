"""Liveness and readiness endpoints for orchestration.

``/healthz`` reports that the process is up. ``/readyz`` reports whether the
service can actually serve S3 requests, by checking the object store and the
Celery broker. When storage is disabled, those dependencies are not required
and report ``disabled``.
"""

import logging

from apiflask import APIBlueprint
from flask import current_app, jsonify

from app.storage.s3 import S3Backend

logger = logging.getLogger(__name__)

health_bp = APIBlueprint("health", __name__)

# Short connection timeout (seconds) so readiness checks fail quickly.
_BROKER_TIMEOUT = 3


def check_storage(settings) -> tuple[bool, str]:
    """Return whether the object store is reachable, with a detail string."""
    if not settings.storage_enabled:
        return True, "disabled"
    try:
        S3Backend.from_settings(settings).health_check()
        return True, "ok"
    except Exception as error:  # noqa: BLE001 - any failure means not ready
        logger.warning("Storage readiness check failed: %s", error)
        return False, str(error)


def check_broker(settings) -> tuple[bool, str]:
    """Return whether the Celery broker is reachable, with a detail string."""
    if not settings.storage_enabled:
        return True, "disabled"
    try:
        from kombu import Connection

        with Connection(settings.celery_broker_url) as connection:
            connection.ensure_connection(max_retries=1, timeout=_BROKER_TIMEOUT)
        return True, "ok"
    except Exception as error:  # noqa: BLE001 - any failure means not ready
        logger.warning("Broker readiness check failed: %s", error)
        return False, str(error)


@health_bp.get("/healthz")
def healthz():
    """Liveness: the process is running."""
    return jsonify({"status": "ok"}), 200


@health_bp.get("/readyz")
def readyz():
    """Readiness: dependencies needed to serve requests are reachable."""
    settings = current_app.config["SETTINGS"]

    storage_ok, storage_detail = check_storage(settings)
    broker_ok, broker_detail = check_broker(settings)
    ready = storage_ok and broker_ok

    body = {
        "status": "ready" if ready else "not ready",
        "checks": {"storage": storage_detail, "broker": broker_detail},
    }
    return jsonify(body), (200 if ready else 503)
