"""The storage backend protocol and shared value types."""

from dataclasses import dataclass
from typing import List, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class ObjectStat:
    """Lightweight metadata for a stored object."""

    key: str
    size: int


@runtime_checkable
class StorageBackend(Protocol):
    """Minimal object-storage interface the application depends on.

    Implementations translate backend-specific failures into
    :class:`~app.storage.errors.StorageError` (and ``ObjectNotFound`` for a
    missing key), so the caller never handles specific exceptions.
    """

    def stat(self, key: str) -> ObjectStat:
        """Return metadata for ``key`` or raise ``ObjectNotFound``."""
        ...

    def get_bytes(self, key: str) -> bytes:
        """Return the object's bytes or raise ``ObjectNotFound``."""
        ...

    def put_bytes(
        self, key: str, data: bytes, content_type: Optional[str] = None
    ) -> None:
        """Store ``data`` at ``key``, overwriting any existing object."""
        ...

    def list(self, prefix: str) -> List[str]:
        """Return the keys whose names start with ``prefix``, sorted."""
        ...

    def download_tree(self, prefix: str, dest_dir: str) -> None:
        """Download every object under ``prefix`` into ``dest_dir``.

        Keys are written relative to ``prefix``, recreating their directory
        structure beneath ``dest_dir``.
        """
        ...
