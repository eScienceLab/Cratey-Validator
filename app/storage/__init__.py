"""Object-storage abstraction.

The rest of the application depends only on the :class:`StorageBackend`
protocol, rather than a specific client, so backends (S3/MinIO/RustFS) and tests
are interchangeable.
"""

from app.storage.base import ObjectStat, StorageBackend
from app.storage.errors import ObjectNotFound, StorageError

__all__ = ["StorageBackend", "ObjectStat", "StorageError", "ObjectNotFound"]
