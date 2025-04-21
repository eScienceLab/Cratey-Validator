"""Utility methods for interacting with MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import json
import logging
import os
import tempfile

from dotenv import load_dotenv
from io import BytesIO
from minio import Minio, S3Error


logger = logging.getLogger(__name__)


def fetch_ro_crate_from_minio(crate_id: str) -> str:
    """
    Fetches an RO-Crate from MinIO based on the crate ID. Downloads the crate as a file and returns local file path.

    :param crate_id: The ID of the RO-Crate to fetch from MinIO.
    :return: The local file path where the RO-Crate is saved.
    :raises S3Error: If an error occurs during the MinIO operation.
    :raises ValueError: If the required environment variables are not set.
    :raises Exception: If an unexpected error occurs during the operation.
    """

    load_dotenv()

    try:
        minio_client, bucket_name = get_minio_client_and_bucket()

        object_name = f"{crate_id}.zip"

        # Prepare temporary file path to store RO Crate for validation:
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, object_name)

        logging.info(
            f"Fetching RO-Crate {object_name} from MinIO bucket {bucket_name}. File path: {file_path}"
        )
        minio_client.fget_object(bucket_name, object_name, file_path)
        logging.info(
            f"RO-Crate {object_name} fetched successfully and saved to {file_path}."
        )

        return file_path

    except S3Error as s3_error:
        logging.error(f"MinIO S3 Error: {s3_error}")
        raise

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error fetching RO-Crate from MinIO: {e}")
        raise


def update_validation_status_in_minio(crate_id: str, validation_status: str) -> None:
    """
    Uploads the validation status to the MinIO bucket.

    :param crate_id: The ID of the RO-Crate in MinIO
    :param validation_status: The validation result to upload
    :raises S3Error: If an error occurs during the MinIO operation
    :raises ValueError: If the required environment variables are not set
    :raises Exception: If an unexpected error occurs
    """

    load_dotenv()

    try:
        minio_client, bucket_name = get_minio_client_and_bucket()

        # The object in MinIO is <crate_id>/validation_status.txt
        object_name = f"{crate_id}/validation_status.txt"
        
        # convert to utf-8 encoded string
        validation_string = json.dumps(validation_status).encode("utf-8")

        minio_client.put_object(
            bucket_name,
            object_name,
            data=BytesIO(validation_string),
            length=len(validation_string),
            content_type="application/json",
        )

        logging.info(
            f"Validation status file uploaded to {bucket_name}/{object_name} successfully."
        )

    except S3Error as s3_error:
        logging.error(f"MinIO S3 Error: {s3_error}")
        raise

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error updating validation status in MinIO: {e}")
        raise


def get_minio_client_and_bucket() -> [Minio, str]:
    """
    Initialises the MinIO client and retrieves the bucket name from environment variables.

    :return: A tuple containing the MinIO client and the bucket name.
    :raises ValueError: If required environment variables are not set.
    """
    load_dotenv()

    minio_client = Minio(
        endpoint=os.environ.get("MINIO_ENDPOINT"),
        access_key=os.environ.get("MINIO_ROOT_USER"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD"),
        secure=False,
    )

    bucket_name = os.environ.get("MINIO_BUCKET_NAME")
    if not bucket_name:
        raise ValueError(
            "RO Crate MINIO_BUCKET_NAME is not set in the environment variables."
        )

    return minio_client, bucket_name
