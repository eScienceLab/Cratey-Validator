"""Webhook delivery with bounded retries and backoff."""

import logging
import time

from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10


class WebhookDeliveryError(Exception):
    """Raised when a webhook could not be delivered after all retries."""


def send_webhook_notification(
    url: str,
    data: Any,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """
    POST ``data`` to ``url`` as JSON, retrying transient failures.

    Retries up to ``max_attempts`` times with exponential backoff. On final
    failure it raises :class:`WebhookDeliveryError` rather than swallowing the
    error, so the caller can surface it.

    :param url: The webhook URL to POST to.
    :param data: JSON-serialisable payload.
    :param max_attempts: Total number of attempts before giving up.
    :param base_delay: Base backoff delay in seconds (doubled each retry).
    :param sleep: Sleep function (injectable for testing).
    :raises WebhookDeliveryError: If delivery fails after ``max_attempts``.
    """
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(url, json=data, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            logger.info("Webhook delivered to %s (attempt %d)", url, attempt)
            return
        except requests.RequestException as error:
            last_error = error
            logger.warning(
                "Webhook attempt %d/%d to %s failed: %s",
                attempt,
                max_attempts,
                url,
                error,
            )
            if attempt < max_attempts:
                sleep(base_delay * (2 ** (attempt - 1)))

    raise WebhookDeliveryError(
        f"Failed to deliver webhook to {url} after {max_attempts} attempts: {last_error}"
    )
