"""Structured JSON logging with request IDs and secret redaction."""

import json
import logging
import uuid
from contextvars import ContextVar
from typing import Iterable, Optional

# correlation ID readable from any logging call in the same context. per request basis.
# (request handler or Celery task). Defaults to "-" when unset.
_request_id: ContextVar[str] = ContextVar("request_id", default="-")


def new_request_id() -> str:
    """Return a fresh, unique request ID."""
    return str(uuid.uuid4())


def set_request_id(request_id: Optional[str]) -> None:
    """Set the current request ID (``None`` resets to the default)."""
    _request_id.set(request_id or "-")


def get_request_id() -> str:
    """Return the current request ID, or ``"-"`` if unset."""
    return _request_id.get()


class RequestIdFilter(logging.Filter):
    """Attaches the current request ID to every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        return True


class RedactionFilter(logging.Filter):
    """Masks known secret values wherever they appear in a log message."""

    def __init__(self, secrets: Iterable[Optional[str]]):
        super().__init__()
        self._secrets = [s for s in secrets if s]

    def filter(self, record: logging.LogRecord) -> bool:
        if self._secrets:
            message = record.getMessage()
            for secret in self._secrets:
                message = message.replace(secret, "***")
            record.msg = message
            record.args = None
        return True


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def setup_logging(settings=None, level: int = logging.INFO) -> None:
    """
    Configure root logging: JSON output, request IDs, and secret redaction.

    :param settings: Optional Settings; its credentials are redacted from logs.
    :param level: The logging level to set.
    """
    secrets = []
    if settings is not None:
        secrets = [settings.s3_secret_key, settings.s3_access_key]

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdFilter())
    handler.addFilter(RedactionFilter(secrets))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
