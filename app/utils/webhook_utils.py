"""Utility methods for sending webhook notifications."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

import logging
import requests

from typing import Any

logger = logging.getLogger(__name__)


def send_webhook_notification(url: str, data: Any) -> None:
    """
    Sends a POST request to the specified webhook URL with the given data.

    :param url: The URL to send the webhook notification to.
    :param data: The data to send in the POST request.
    :raises requests.RequestException: If an error occurs when sending the notification.
    """

    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        logging.info(f"Webhook notification sent successfully to {url}")
    except requests.RequestException as e:
        logging.error(f"Failed to send webhook notification: {e}")
