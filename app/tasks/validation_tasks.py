"""Tasks and helper methods for processing RO-Crate validation."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging
import os
from tempfile import TemporaryDirectory

from rocrate_validator import services
from rocrate_validator.models import ValidationResult
from rocrate_validator.requirements.shacl import models

from app.celery_worker import celery
from app.utils.minio_utils import (
    fetch_ro_crate_from_minio,
    update_validation_status_in_minio,
    get_validation_status_from_minio,
    unzip_ro_crate,
)
from app.utils.webhook_utils import send_webhook_notification

logger = logging.getLogger(__name__)


@celery.task
def process_validation_task_by_id(
    crate_id: str, profile_name: str | None, webhook_url: str | None
) -> None:
    """
    Background task to process the RO-Crate validation by ID.

    :param crate_id: The ID of the RO-Crate to validate.
    :param profile_name: The name of the validation profile to use. Defaults to None.
    :param webhook_url: The webhook URL to send notifications to. Defaults to None.
    :raises Exception: If an error occurs during the validation process.

    :todo: Replace the Crate ID with a more comprehensive system, and replace profile name with URI.
    """

    file_path = None

    try:
        # Fetch the RO-Crate from MinIO using the provided ID:
        file_path = fetch_ro_crate_from_minio(crate_id)

        logging.info(f"Processing validation task for {file_path}")

        # Perform validation:
        validation_result = perform_ro_crate_validation(file_path, profile_name)

        if isinstance(validation_result, str):
            logging.error(f"Validation failed: {validation_result}")
            # TODO: Send webhook with failure notification
            raise Exception(f"Validation failed: {validation_result}")

        if not validation_result.has_issues():
            logging.info(f"RO Crate {file_path} is valid.")
        else:
            logging.info(f"RO Crate {file_path} is invalid.")

        # Update the validation status in MinIO:
        update_validation_status_in_minio(crate_id, validation_result.to_json())

        # TODO: Prepare the data to send to the webhook, and send the webhook notification.

        if webhook_url:
            send_webhook_notification(webhook_url, validation_result.to_json())

    except Exception as e:
        logging.error(f"Error processing validation task: {e}")

        # Send failure notification via webhook
        error_data = {"profile_name": profile_name, "error": str(e)}
        send_webhook_notification(webhook_url, error_data)

    finally:
        # Clean up the temporary file if it was created:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


def perform_ro_crate_validation(
    file_path: str, profile_name: str | None
) -> ValidationResult | str:
    """
    Validates an RO-Crate using the provided .zip file path and profile name.
    The .zip file is extracted to a temporary directory for validation.

    :param file_path: Path to the RO-Crate .zip file to validate.
    :param profile_name: Name of the validation profile to use. If None, the validator will auto-detect.
    :return: The validation result, or an error message string if validation fails.
    """

    try:
        logging.info(
            f"Unzipping and validating {file_path} with profile {profile_name}"
        )

        with TemporaryDirectory() as temp_dir:
            unzip_ro_crate(file_path, temp_dir)

            # Search for directory with ro-crate-metadata.json:
            crate_root = None
            for root, dirs, files in os.walk(temp_dir):
                if "ro-crate-metadata.json" in files:
                    crate_root = root
                    break

            if not crate_root:
                raise FileNotFoundError(
                    "Could not locate ro-crate-metadata.json after extraction"
                )

            logging.info(f"Detected RO-Crate root directory: {crate_root}")

            # Print directory contents:
            for root, dirs, files in os.walk(crate_root):
                for f in files:
                    logging.info(f"Validator sees file: {os.path.join(root, f)}")

            settings = services.ValidationSettings(
                rocrate_uri=crate_root,
                requirement_severity=models.Severity.REQUIRED,
                **({"profile_identifier": profile_name} if profile_name else {}),
            )

            return services.validate(settings)

    except Exception as e:
        logging.error(f"Unexpected error during validation: {e}")
        return str(e)


def return_ro_crate_validation(
    crate_id: str,
) -> dict | str:
    """
    Retrieves the validation result for an RO-Crate using the provided Crate ID.

    :param crate_id: The ID of the RO-Crate that has been validated
    :return: The validation result
    :raises Exception: If an error occurs in the retrieving the validation result
    """

    try:
        logging.info(f"Fetching validation result for RO-Crate {crate_id}")

        return get_validation_status_from_minio(crate_id)

    except Exception as e:
        logging.error(f"Unexpected error when retrieving validation: {e}")
        return str(e)
