"""Utility methods for interacting with the File System."""

# Author: Douglas Lowe, Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import json
import logging
import os
import tempfile

from dotenv import load_dotenv


logger = logging.getLogger(__name__)


def build_metadata_only_rocrate(crate_json: str) -> str:
    """
    Creates a temporary directory for an empty RO-Crate,
    and saves the JSON string as a metadata file.

    :param crate_json: The metadata string.
    :return: The local file path where the RO-Crate is saved.
    :raises ValueError: If the required environment variables are not set.
    :raises Exception: If an unexpected error occurs during the operation.
    """

    load_dotenv()

    try:
        # Prepare temporary file path to store RO Crate for validation:
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, 'ro-crate-metadata.json')

        logging.info(
            f"Creating RO-Crate Metadata file. File path: {file_path}"
        )
        with open(file_path, 'w') as f:
            f.write(crate_json)
        logging.info(
            f"RO-Crate metadata successfully saved to {file_path}."
        )

        return temp_dir

    except ValueError as value_error:
        logging.error(f"Configuration Error: {value_error}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error creating RO-Crate metadata: {e}")
        raise
