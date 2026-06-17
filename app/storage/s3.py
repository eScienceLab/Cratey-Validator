"""A boto3-backed StorageBackend for any S3-compatible object store.

Works against AWS S3, MinIO, RustFS, Ceph, and similar via an explicit
``endpoint_url``. Backend-specific failures are translated into the storage
error vocabulary so callers never see botocore exceptions.
"""

import os

from typing import List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from app.storage.base import ObjectStat
from app.storage.errors import ObjectNotFound, StorageError

# botocore error codes that mean "this key isn't here", as opposed to a
# transport/auth/bucket failure.
_NOT_FOUND_CODES = {"404", "NoSuchKey"}


class S3Backend:
    """StorageBackend implementation over a boto3 S3 client."""

    def __init__(self, client, bucket: str) -> None:
        self._client = client
        self.bucket = bucket

    @classmethod
    def from_settings(cls, settings) -> "S3Backend":
        """Build a backend from validated :class:`Settings`.

        The endpoint is taken verbatim and prefixed with the scheme implied by
        ``s3_use_ssl`` so the same config drives AWS or a self-hosted store.
        """
        scheme = "https" if settings.s3_use_ssl else "http"
        client = boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{settings.s3_endpoint}",
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region or "us-east-1",
            use_ssl=settings.s3_use_ssl,
        )
        return cls(client, settings.s3_bucket)

    def stat(self, key: str) -> ObjectStat:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            raise self._translate(error, key)
        except BotoCoreError as error:
            raise StorageError(f"Storage error for {key}: {error}") from error
        return ObjectStat(key=key, size=response["ContentLength"])

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        except ClientError as error:
            raise self._translate(error, key)
        except BotoCoreError as error:
            raise StorageError(f"Storage error for {key}: {error}") from error

    def put_bytes(
        self, key: str, data: bytes, content_type: Optional[str] = None
    ) -> None:
        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        try:
            self._client.put_object(**kwargs)
        except (ClientError, BotoCoreError) as error:
            raise StorageError(f"Failed to store {key}: {error}") from error

    def list(self, prefix: str) -> List[str]:
        keys: List[str] = []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        except (ClientError, BotoCoreError) as error:
            raise StorageError(f"Failed to list {prefix}: {error}") from error
        return sorted(keys)

    def download_tree(self, prefix: str, dest_dir: str) -> None:
        for key in self.list(prefix):
            relative_path = key[len(prefix) :]
            local_path = os.path.join(dest_dir, *relative_path.split("/"))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as handle:
                handle.write(self.get_bytes(key))

    def health_check(self) -> None:
        """Verify the bucket is reachable; raise ``StorageError`` if not."""
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except (ClientError, BotoCoreError) as error:
            raise StorageError(
                f"Bucket {self.bucket} not reachable: {error}"
            ) from error

    @staticmethod
    def _translate(error: ClientError, key: str) -> StorageError:
        """Map a botocore ClientError to the storage error vocabulary.

        Only a missing *object* becomes ``ObjectNotFound``; a missing bucket or
        an auth failure is an infrastructure problem and stays a ``StorageError``.
        ``head_object`` reports a missing key with code ``"404"`` (no error body);
        ``get_object`` reports ``"NoSuchKey"``.
        """
        code = error.response.get("Error", {}).get("Code", "")
        if code in _NOT_FOUND_CODES:
            return ObjectNotFound(key)
        return StorageError(f"Storage error for {key}: {error}")
