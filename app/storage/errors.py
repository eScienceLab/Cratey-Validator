"""Storage-layer exceptions, decoupled from any specific client."""


class StorageError(Exception):
    """Base class for object-storage failures."""


class ObjectNotFound(StorageError):
    """Raised when a requested object key does not exist."""

    def __init__(self, key: str):
        super().__init__(f"Object not found: {key}")
        self.key = key
