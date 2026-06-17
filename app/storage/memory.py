"""An in-memory storage backend for tests and local use."""

import os
from typing import Dict, List, Optional

from app.storage.base import ObjectStat
from app.storage.errors import ObjectNotFound


class InMemoryStorage:
    """A dict-backed :class:`StorageBackend` implementation.

    Works as a dependency-free test double and for local development without full S3 object store.
    """

    def __init__(self) -> None:
        self._objects: Dict[str, bytes] = {}

    def stat(self, key: str) -> ObjectStat:
        if key not in self._objects:
            raise ObjectNotFound(key)
        return ObjectStat(key=key, size=len(self._objects[key]))

    def get_bytes(self, key: str) -> bytes:
        try:
            return self._objects[key]
        except KeyError:
            raise ObjectNotFound(key)

    def put_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> None:
        self._objects[key] = data

    def list(self, prefix: str) -> List[str]:
        return sorted(key for key in self._objects if key.startswith(prefix))

    def download_tree(self, prefix: str, dest_dir: str) -> None:
        for key in self.list(prefix):
            relative_path = key[len(prefix) :]
            local_path = os.path.join(dest_dir, *relative_path.split("/"))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as handle:
                handle.write(self._objects[key])
