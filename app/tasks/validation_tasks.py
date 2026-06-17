"""Store-backed RO-Crate validation: orchestration and the Celery task.

The orchestration lives in :func:`run_validation_job`, a plain function that is
fully testable with an in-memory storage backend. The Celery task is a thin
wrapper that builds the backend from server-side settings and adds retries for
transient storage failures.

Stages are ordered fetch -> validate -> persist -> webhook, so a failed store
write can never trigger a "success" webhook. The outcome (including ``error``
outcomes) is always persisted, so a later GET reflects what happened.
"""

import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Optional

from app.celery_worker import celery
from app.crates.ids import InvalidCrateId
from app.crates.layout import result_key
from app.crates.resolver import (
    AmbiguousCrate,
    CrateNotFound,
    ResolvedCrate,
    resolve_crate,
)
from app.storage.base import StorageBackend
from app.storage.errors import StorageError
from app.storage.s3 import S3Backend
from app.utils.config import Settings
from app.utils.webhook_utils import send_webhook_notification
from app.validation.results import ValidationOutcome
from app.validation.runner import validate_crate_path

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _download_crate(storage: StorageBackend, resolved: ResolvedCrate, temp_dir: str) -> str:
    """Download a resolved crate into ``temp_dir`` and return its local path."""
    if resolved.is_zip:
        local_path = os.path.join(temp_dir, f"{resolved.crate_id}.zip")
        with open(local_path, "wb") as handle:
            handle.write(storage.get_bytes(resolved.key))
        return local_path

    dest = os.path.join(temp_dir, resolved.crate_id)
    os.makedirs(dest, exist_ok=True)
    storage.download_tree(resolved.key, dest)
    return dest


def run_validation_job(
    storage: StorageBackend,
    crate_id: str,
    settings: Settings,
    profile_name: Optional[str] = None,
    webhook_url: Optional[str] = None,
    created_at: Optional[str] = None,
) -> ValidationOutcome:
    """Fetch, validate, persist, and (optionally) notify, for one crate.

    Resolution/validation problems become ``error`` outcomes that are persisted
    like any other. A :class:`StorageError` (transient infrastructure failure)
    propagates so the caller can retry.
    """
    created_at = created_at or _utcnow_iso()
    temp_dir = tempfile.mkdtemp()
    try:
        try:
            resolved = resolve_crate(storage, crate_id, settings.s3_crate_prefix)
            local_path = _download_crate(storage, resolved, temp_dir)
        except (CrateNotFound, AmbiguousCrate, InvalidCrateId) as error:
            logger.error("Cannot validate crate %s: %s", crate_id, error)
            outcome = ValidationOutcome.from_error(
                str(error), profile=profile_name, created_at=created_at
            )
        else:
            outcome = validate_crate_path(
                local_path,
                profile_name=profile_name,
                profiles_path=settings.profiles_path,
                created_at=created_at,
            )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    # Persist before notifying, so a write failure cannot precede a webhook.
    storage.put_bytes(
        result_key(settings.s3_results_prefix, crate_id),
        outcome.to_json().encode("utf-8"),
        content_type="application/json",
    )

    if webhook_url:
        send_webhook_notification(webhook_url, outcome.to_dict())

    return outcome


@celery.task(
    autoretry_for=(StorageError,),
    max_retries=3,
    retry_backoff=True,
    retry_backoff_max=60,
)
def process_validation_task_by_id(
    crate_id: str,
    profile_name: Optional[str] = None,
    webhook_url: Optional[str] = None,
) -> None:
    """Celery entry point: validate a stored crate by ID.

    Credentials and layout come from server-side settings (never the request),
    so no secrets travel through the broker. Transient storage failures are
    retried with exponential backoff.
    """
    settings = Settings.from_env()
    storage = S3Backend.from_settings(settings)
    run_validation_job(
        storage,
        crate_id,
        settings,
        profile_name=profile_name,
        webhook_url=webhook_url,
    )
