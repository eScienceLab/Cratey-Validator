"""Utility methods for interacting with MinIO."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import json
import logging
import os
import tempfile

from io import BytesIO
from minio import Minio, S3Error
from app.utils.config import InvalidAPIUsage


logger = logging.getLogger(__name__)


def fetch_ro_crate_from_minio(minio_client: object, minio_bucket: str, crate_id: str, root_path: str) -> str:
    """
    Fetches an RO-Crate from MinIO based on the crate ID. Downloads the crate as a file and returns local file path.

    :param minio_client: The MinIO client
    :param minio_bucket: The MinIO bucket containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate to fetch from MinIO.
    :param root_path: The root path containing the RO-Crate.
    :return: The local file path where the RO-Crate is saved.
    """

    rocrate_object = find_rocrate_object_on_minio(crate_id, minio_client, minio_bucket, root_path)

    rocrate_minio_path = rocrate_object.object_name
    rocrate_name = rocrate_minio_path.split('/')[-1]

    temp_dir = tempfile.mkdtemp()
    local_root_path = os.path.join(temp_dir, rocrate_name)

    logging.info(
        f"Fetching RO-Crate {rocrate_name} from MinIO bucket {minio_bucket}. File path {local_root_path}"
    )

    if rocrate_object.is_dir:
        os.makedirs(os.path.dirname(local_root_path), exist_ok=True)

        objects_list = get_minio_object_list(rocrate_minio_path, minio_client, minio_bucket, recursive=True)
        for obj in objects_list:
            relative_path = obj.object_name[len(rocrate_minio_path):].lstrip("/")
            local_file_path = os.path.join(local_root_path, relative_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            download_file_from_minio(minio_client, minio_bucket, obj.object_name, local_file_path)

    else:
        file_path = local_root_path
        download_file_from_minio(minio_client, minio_bucket, rocrate_minio_path, file_path)

    logging.info(
        f"RO-Crate {rocrate_name} fetched successfully and saved to {local_root_path}."
    )

    return local_root_path


def update_validation_status_in_minio(minio_client: object, minio_bucket: str, crate_id: str, root_path: str, validation_status: str) -> None:
    """
    Uploads the validation status to the MinIO bucket.

    :param minio_client: The MinIO client
    :param minio_bucket: The MinIO bucket containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate in MinIO
    :param validation_status: The validation result to upload
    :raises S3Error: If an error occurs during the MinIO operation
    :raises ValueError: If the required environment variables are not set
    :raises Exception: If an unexpected error occurs
    """

    # The object in MinIO is <crate_id>_validation/validation_status.txt
    if root_path:
        object_name = f"{root_path}/{crate_id}_validation/validation_status.txt"
    else:
        object_name = f"{crate_id}_validation/validation_status.txt"

    # convert pretty string to dictionary, then back to plain utf-8 encoded string
    validation_string = json.dumps(json.loads(validation_status), indent=None).encode("utf-8")

    try:
        minio_client.put_object(
            minio_bucket,
            object_name,
            data=BytesIO(validation_string),
            length=len(validation_string),
            content_type="application/json",
        )

    except S3Error as s3_error:
        logging.error(f"MinIO S3 Error: {s3_error}")
        raise InvalidAPIUsage(f"MinIO S3 Error: {s3_error}", 500)

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise InvalidAPIUsage(f"Configuration Error: {value_error}", 500)

    except Exception as e:
        logging.error(f"Unexpected error updating validation status in MinIO: {e}")
        raise InvalidAPIUsage(f"Unknown Error: {e}", 500)

    logging.info(
        f"Validation status file uploaded to {minio_bucket}/{object_name} successfully."
    )


def get_validation_status_from_minio(minio_client: object, minio_bucket: str, crate_id: str, root_path: str) -> dict:
    """
    Checks for the existence of a validation report for the given RO-Crate in the MinIO bucket.
    Returns validation message if it exists, or notification that it is missing if not.

    :param minio_client: The MinIO client
    :param minio_bucket: The MinIO bucket containing the RO-Crate.
    :param crate_id: The ID of the RO-Crate in MinIO
    :return validation_status: Either the validation status, or note that this does not exist

    """

    # The object in MinIO is <crate_id>_validation/validation_status.txt
    if root_path:
        object_name = f"{root_path}/{crate_id}_validation/validation_status.txt"
    else:
        object_name = f"{crate_id}_validation/validation_status.txt"

    logging.info(f"Getting object {object_name}")

    try:
        response = minio_client.get_object(
            minio_bucket,
            object_name,
        )

        validation_message = json.loads(response.data.decode())
        response.close()
        response.release_conn()

    except S3Error as s3_error:
        logging.error(f"MinIO S3 Error: {s3_error}")
        raise InvalidAPIUsage(f"MinIO S3 Error: {s3_error}", 500)

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise InvalidAPIUsage(f"Configuration Error: {value_error}", 500)

    except Exception as e:
        logging.error(f"Unexpected error retrieving validation status from MinIO: {e}")
        raise InvalidAPIUsage(f"Unknown Error: {e}", 500)

    else:
        return validation_message


def download_file_from_minio(minio_client: object, minio_bucket: str, object_path: str, file_path: str) -> None:
    """
    Downloads a file from MinIO

    :param minio_client: MinIO object
    :param minio_bucket: name of MinIO bucket, string
    :param object_path: path to object on MinIO, string
    :param file_path: local path, string
    :raises S3Error: If an error occurs during the MinIO operation
    :raises ValueError: If the required environment variables are not set
    :raises Exception: If an unexpected error occurs
    """

    try:
        minio_client.fget_object(minio_bucket, object_path, file_path)

    except S3Error as s3_error:
        logging.error(f"MinIO S3 Error: {s3_error}")
        raise InvalidAPIUsage(f"MinIO S3 Error: {s3_error}", 500)

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise InvalidAPIUsage(f"Configuration Error: {value_error}", 500)

    except Exception as e:
        logging.error(f"Unexpected error retrieving file from MinIO: {e}")
        raise InvalidAPIUsage(f"Unknown Error: {e}", 500)


def find_validation_object_on_minio(rocrate_id: str, minio_client, minio_bucket: str, root_path: str) -> object:
    """
    Checks that the requested object exists on the MinIO instance.

    If it does not exist then a False value is returned.
    If it does exist then the minio.datatypes.Object is returned.

    :param rocrate_id: string containing the name of ro-crate
    :param root_path: string containing the path within which the ro-crate should be
    :param minio_client: minio object
    :param minio_bucket: string containing bucket on minio
    :return return_object: rocrate object we require
    :raise Exception: If validation result can't be found, 400
    """

    logging.info(f"Finding Validation result: {rocrate_id}_validation/validation_status.txt")

    if root_path:
        file_path = f"{root_path}/{rocrate_id}_validation/validation_status.txt"
    else:
        file_path = f"{rocrate_id}_validation/validation_status.txt"

    file_list = get_minio_object_list(file_path, minio_client, minio_bucket)

    return_object = False
    for obj in file_list:
        if obj.object_name == file_path:
            return_object = obj
            break

    if not return_object:
        logging.error(f"No validation result yet for RO-Crate: {rocrate_id}")
        return False
    else:
        return return_object


def find_rocrate_object_on_minio(rocrate_id: str, minio_client, minio_bucket: str, root_path: str) -> object | bool:
    """
    Checks that the requested object exists on the MinIO instance.

    If it does not exist then a False value is returned.
    If it does exist then the minio.datatypes.Object is returned.

    :param rocrate_id: string containing the name of ro-crate
    :param root_path: string containing the path within which the ro-crate should be
    :param minio_client: minio object
    :param minio_bucket: string containing bucket on minio
    :return return_object or False: rocrate object we require, or False result
    :raise Exception: If RO-Crate can't be found, 400
    """

    logging.info(f"Finding RO-Crate: {rocrate_id}")

    if root_path:
        rocrate_path = f"{root_path}/{rocrate_id}"
    else:
        rocrate_path = rocrate_id

    rocrate_list = get_minio_object_list(rocrate_path, minio_client, minio_bucket)

    return_object = False
    for obj in rocrate_list:
        # TODO: We should be checking here for the existence of the ro-crate metadata file within this object too
        if (obj.object_name == f"{rocrate_path}/" and obj.is_dir) or obj.object_name == f"{rocrate_path}.zip":
            return_object = obj
            break

    if not return_object:
        logging.error(f"No RO-Crate with prefix: {rocrate_path}")
        return False
    else:
        return return_object


def get_minio_object_list(object_path: str, minio_client, minio_bucket: str, recursive: bool = False) -> list:
    """
    Creates a list of objects which match the object_id and path_prefix

    :param object_path: The object ID, string
    :param path_prefix: Path prefix, string, optional
    :param minio_client: MinIO client object
    :param minio_bucket: string
    :param recursive: boolean, default = False
    :return object_list: List containing objects of type minio.datatypes.Object
    :raises S3Error: If an error occurs during the MinIO operation, 500
    :raises ValueError: If the required environment variables are not set, 500
    :raises Exception: If an unexpected error occurs, 500
    """

    try:
        response = minio_client.list_objects(
            minio_bucket,
            object_path,
            recursive=recursive
        )
        object_list = [obj for obj in response]

        response.close()

    except S3Error as s3_error:
        logging.error(f"MinIO S3 Error: {s3_error}")
        raise InvalidAPIUsage(f"MinIO S3 Error: {s3_error}", 500)

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise InvalidAPIUsage(f"Configuration Error: {value_error}", 500)

    except Exception as e:
        logging.error(f"Unexpected error getting object list from MinIO: {e}")
        raise InvalidAPIUsage(f"Unknown Error: {e}", 500)

    else:
        return object_list


def get_minio_client(minio_config: dict) -> Minio:
    """
    Initialises the MinIO client from provided settings.

    :param minio_config: A dictionary containing the below parameters
    :param endpoint: A string containing host and port. E.g. 'localhost:9000'
    :param access_key: A string containing the access key / username
    :param secret_key: A string containing the secret key / password
    :param use_ssl: Boolean defining if SSL connection should be used or not
    :return: The MinIO client.
    :raises ValueError: If required environment variables are not set.
    """

    minio_client = Minio(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["accesskey"],
        secret_key=minio_config["secret"],
        secure=minio_config["ssl"],
    )

    return minio_client
