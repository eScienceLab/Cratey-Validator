"""Tasks and helper methods for processing RO-Crate validation."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging
import os
import shutil
from typing import Optional

from rocrate_validator import services
from rocrate_validator.models import ValidationResult

from app.celery_worker import celery
from app.utils.minio_utils import (
    fetch_ro_crate_from_minio,
    update_validation_status_in_minio,
    get_validation_status_from_minio
)
from app.utils.webhook_utils import send_webhook_notification
from app.utils.file_utils import build_metadata_only_rocrate

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


@celery.task
def process_validation_task_by_metadata(
    crate_json: str, profile_name: str | None, webhook_url: str | None
) -> ValidationResult | str:
    """
    Background task to process the RO-Crate validation for a given json metadata string.

    :param crate_json: A string containing the RO-Crate JSON metadata to validate.
    :param profile_name: The name of the validation profile to use. Defaults to None.
    :param webhook_url: The webhook URL to send notifications to. Defaults to None.
    :raises Exception: If an error occurs during the validation process.

    :todo: Replace the Crate ID with a more comprehensive system, and replace profile name with URI.
    """

    skip_checks_list = ['ro-crate-1.1_12.1']
    file_path = None

    try:
        # Fetch the RO-Crate from MinIO using the provided ID:
        file_path = build_metadata_only_rocrate(crate_json)

        logging.info(f"Processing validation task for {file_path}")

        # Perform validation:
        validation_result = perform_ro_crate_validation(file_path,
                                                        profile_name,
                                                        skip_checks_list
                                                        )

        if isinstance(validation_result, str):
            logging.error(f"Validation failed: {validation_result}")
            # TODO: Send webhook with failure notification
            raise Exception(f"Validation failed: {validation_result}")

        if not validation_result.has_issues():
            logging.info(f"RO Crate {file_path} is valid.")
        else:
            logging.info(f"RO Crate {file_path} is invalid.")

        if webhook_url:
            send_webhook_notification(webhook_url, validation_result.to_json())

    except Exception as e:
        logging.error(f"Error processing validation task: {e}")

        # Send failure notification via webhook
        error_data = {"profile_name": profile_name, "error": str(e)}
        if webhook_url:
            send_webhook_notification(webhook_url, error_data)

    finally:
        # Clean up the temporary file if it was created:
        if file_path and os.path.exists(file_path):
            shutil.rmtree(file_path)

        if isinstance(validation_result, str):
            return validation_result
        else:
            return validation_result.to_json()


def perform_ro_crate_validation(
    file_path: str, profile_name: str | None, skip_checks_list: Optional[list] = None
) -> ValidationResult | str:
    """
    Validates an RO-Crate using the provided file path and profile name.

    :param file_path: The path to the RO-Crate file to validate
    :param profile_name: The name of the validation profile to use. Defaults to None. If None, the CRS4 validator will
        attempt to determine the profile.
    :param skip_checks_list: A list of checks to skip, if needed
    :return: The validation result.
    :raises Exception: If an error occurs during the validation process.
    """

    try:
        logging.info(f"Validating {file_path} with profile {profile_name}")

        full_file_path = os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ),
            file_path,
        )
        settings = services.ValidationSettings(
            rocrate_uri=full_file_path,
            **({"profile_identifier": profile_name} if profile_name else {}),
            **({"skip_checks": skip_checks_list} if skip_checks_list else {})
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

    logging.info(f"Fetching validation result for RO-Crate {crate_id}")

    return get_validation_status_from_minio(crate_id)
