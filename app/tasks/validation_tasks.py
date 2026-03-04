"""Tasks and helper methods for processing RO-Crate validation."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging
import os
import shutil
import json
from typing import Optional

from rocrate_validator import services
from rocrate_validator.models import ValidationResult

from app.celery_worker import celery
from app.utils.minio_utils import (
    fetch_ro_crate_from_minio,
    update_validation_status_in_minio,
    get_validation_status_from_minio,
    get_minio_client,
    find_rocrate_object_on_minio,
    find_validation_object_on_minio,
)
from app.utils.webhook_utils import send_webhook_notification

logger = logging.getLogger(__name__)


@celery.task
def process_validation_task_by_id(
    minio_config: dict,
    crate_id: str,
    root_path: str,
    profile_name: str | None,
    webhook_url: str | None,
    profiles_path: str | None,
) -> None:
    """
    Background task to process the RO-Crate validation by ID.

    :param minio_config: The MinIO configuration.
    :param crate_id: The ID of the RO-Crate to validate.
    :param root_path: The root path containing the RO-Crate.
    :param profile_name: The name of the validation profile to use. Defaults to None.
    :param webhook_url: The webhook URL to send notifications to. Defaults to None.
    :raises Exception: If an error occurs during the validation process.

    """

    # TODO: Split try statements: (1) fetch and validate; (2) write to minio; (3) webhook

    minio_client = get_minio_client(minio_config)

    file_path = None

    try:
        # Fetch the RO-Crate from MinIO using the provided ID:
        file_path = fetch_ro_crate_from_minio(
            minio_client, minio_config["bucket"], crate_id, root_path
        )

        logging.info(f"Processing validation task for {file_path}")

        # Perform validation:
        validation_result = perform_ro_crate_validation(
            file_path, profile_name, profiles_path=profiles_path
        )

        if isinstance(validation_result, str):
            logging.error(f"Validation failed: {validation_result}")
            # TODO: Send webhook with failure notification
            raise Exception(f"Validation failed: {validation_result}")

        if not validation_result.has_issues():
            logging.info(f"RO Crate {crate_id} is valid.")
        else:
            logging.info(f"RO Crate {crate_id} is invalid.")

        # Update the validation status in MinIO:
        update_validation_status_in_minio(
            minio_client,
            minio_config["bucket"],
            crate_id,
            root_path,
            validation_result.to_json(),
        )

        # TODO: Prepare the data to send to the webhook, and send the webhook notification.

        if webhook_url:
            send_webhook_notification(webhook_url, validation_result.to_json())

    except Exception as e:
        logging.error(f"Error processing validation task: {e}")

        # TODO: Should we write error messages to the minio instance too?

        # Send failure notification via webhook
        if webhook_url:
            error_data = {"profile_name": profile_name, "error": str(e)}
            send_webhook_notification(webhook_url, error_data)

    finally:
        # Clean up the temporary file if it was created:
        if file_path and os.path.exists(file_path):
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)


@celery.task
def process_validation_task_by_metadata(
    crate_json: str,
    profile_name: str | None,
    webhook_url: str | None,
    profiles_path: Optional[str] = None,
) -> ValidationResult | str:
    """
    Background task to process the RO-Crate validation for a given json metadata string.

    :param crate_json: A string containing the RO-Crate JSON metadata to validate.
    :param profile_name: The name of the validation profile to use. Defaults to None.
    :param webhook_url: The webhook URL to send notifications to. Defaults to None.
    :param profiles_path: The path to the profiles definition directory. Defaults to None.
    :raises Exception: If an error occurs during the validation process.

    :todo: Replace the Crate ID with a more comprehensive system, and replace profile name with URI.
    """

    try:
        logging.info("Processing validation task for provided metadata string")

        # Perform validation:
        validation_result = perform_metadata_validation(
            crate_json, profile_name, profiles_path=profiles_path
        )

        if isinstance(validation_result, str):
            logging.error(f"Validation failed: {validation_result}")
            # TODO: Send webhook with failure notification
            raise Exception(f"Validation failed: {validation_result}")

        if not validation_result.has_issues():
            logging.info("RO Crate metadata is valid.")
        else:
            logging.info("RO Crate metadata is invalid.")

        if webhook_url:
            send_webhook_notification(webhook_url, validation_result.to_json())

    except Exception as e:
        logging.error(f"Error processing validation task: {e}")

        # Send failure notification via webhook
        error_data = {"profile_name": profile_name, "error": str(e)}
        if webhook_url:
            send_webhook_notification(webhook_url, error_data)

    finally:
        if isinstance(validation_result, str):
            return validation_result
        else:
            return validation_result.to_json()


def perform_ro_crate_validation(
    file_path: str,
    profile_name: str | None,
    skip_checks_list: Optional[list] = None,
    profiles_path: Optional[str] = None,
) -> ValidationResult | str:
    """
    Validates an RO-Crate using the provided file path and profile name.

    :param file_path: The path to the RO-Crate file to validate
    :param profile_name: The name of the validation profile to use. Defaults to None. If None, the CRS4 validator will
        attempt to determine the profile.
    :param profiles_path: The path to the profiles definition directory
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
            **({"skip_checks": skip_checks_list} if skip_checks_list else {}),
            **({"profiles_path": profiles_path} if profiles_path else {}),
        )

        return services.validate(settings)

    except Exception as e:
        logging.error(f"Unexpected error during validation: {e}")
        return str(e)


def perform_metadata_validation(
    crate_json: str,
    profile_name: str | None,
    skip_checks_list: Optional[list] = None,
    profiles_path: Optional[str] = None,
) -> ValidationResult | str:
    """
    Validates only RO-Crate metadata provided as a json string.

    :param crate_json: The JSON string containing the metadata
    :param profile_name: The name of the validation profile to use. Defaults to None. If None, the CRS4 validator will
        attempt to determine the profile.
    :param profiles_path: The path to the profiles definition directory
    :param skip_checks_list: A list of checks to skip, if needed
    :return: The validation result.
    :raises Exception: If an error occurs during the validation process.
    """

    try:
        logging.info(f"Validating ro-crate metadata with profile {profile_name}")

        settings = services.ValidationSettings(
            **({"metadata_only": True}),
            **({"metadata_dict": json.loads(crate_json)}),
            **({"profile_identifier": profile_name} if profile_name else {}),
            **({"skip_checks": skip_checks_list} if skip_checks_list else {}),
            **({"profiles_path": profiles_path} if profiles_path else {}),
        )

        return services.validate(settings)

    except Exception as e:
        logging.error(f"Unexpected error during validation: {e}")
        return str(e)


def check_ro_crate_exists(
    minio_client: object,
    bucket_name: str,
    crate_id: str,
    root_path: str,
) -> bool:
    """
    Checks for the existence of an RO-Crate using the provided Crate ID.

    :param minio_client: The MinIO client
    :param bucket_name: The MinIO bucket containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate to validate.
    :param root_path: The root path containing the RO-Crate.
    :return: Boolean indicating existence
    """

    logging.info(f"Checking for existence of RO-Crate {crate_id}")

    if find_rocrate_object_on_minio(crate_id, minio_client, bucket_name, root_path):
        return True
    else:
        return False


def check_validation_exists(
    minio_client: object,
    bucket_name: str,
    crate_id: str,
    root_path: str,
) -> bool:
    """
    Checks for the existence of a validation result using the provided Crate ID.

    :param minio_client: The MinIO client
    :param minio_bucket: The MinIO bucket containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate to validate.
    :param root_path: The root path containing the RO-Crate.
    :return: Boolean indicating existence
    """

    logging.info(f"Checking for existence of RO-Crate {crate_id}")

    if find_validation_object_on_minio(crate_id, minio_client, bucket_name, root_path):
        return True
    else:
        return False


def return_ro_crate_validation(
    minio_client: object,
    bucket_name: str,
    crate_id: str,
    root_path: str,
) -> dict | str:
    """
    Retrieves the validation result for an RO-Crate using the provided Crate ID.

    :param minio_client: The MinIO client
    :param crate_id: The ID of the RO-Crate that has been validated
    :return: The validation result
    """

    logging.info(f"Fetching validation result for RO-Crate {crate_id}")

    return get_validation_status_from_minio(
        minio_client, bucket_name, crate_id, root_path
    )
