"""Tests for webhook delivery with retry/backoff."""

from unittest import mock

import pytest
import requests

from app.utils import webhook_utils
from app.utils.webhook_utils import WebhookDeliveryError, send_webhook_notification


def _ok_response():
    response = mock.Mock()
    response.raise_for_status.return_value = None
    return response


def test_successful_delivery_posts_once():
    with mock.patch.object(webhook_utils.requests, "post", return_value=_ok_response()) as post:
        send_webhook_notification("https://hook", {"status": "valid"}, sleep=lambda _: None)
    post.assert_called_once()
    # The payload is sent as JSON and a timeout is set (no unbounded hang).
    assert post.call_args.kwargs["json"] == {"status": "valid"}
    assert "timeout" in post.call_args.kwargs


def test_retries_then_succeeds():
    flaky = [requests.ConnectionError("boom"), requests.ConnectionError("boom"), _ok_response()]
    sleeps = []
    with mock.patch.object(webhook_utils.requests, "post", side_effect=flaky) as post:
        send_webhook_notification("https://hook", {"x": 1}, max_attempts=3, sleep=sleeps.append)
    assert post.call_count == 3
    assert len(sleeps) == 2  # slept between the three attempts


def test_terminal_failure_raises_after_exhausting_attempts():
    with mock.patch.object(
        webhook_utils.requests, "post", side_effect=requests.ConnectionError("down")
    ) as post:
        with pytest.raises(WebhookDeliveryError) as exc_info:
            send_webhook_notification(
                "https://hook", {"x": 1}, max_attempts=3, sleep=lambda _: None
            )
    assert post.call_count == 3
    assert "https://hook" in str(exc_info.value)


def test_http_error_status_is_retried():
    bad = mock.Mock()
    bad.raise_for_status.side_effect = requests.HTTPError("500")
    with mock.patch.object(webhook_utils.requests, "post", return_value=bad) as post:
        with pytest.raises(WebhookDeliveryError):
            send_webhook_notification(
                "https://hook", {"x": 1}, max_attempts=2, sleep=lambda _: None
            )
    assert post.call_count == 2
