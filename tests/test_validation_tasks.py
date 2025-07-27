from unittest import mock
import pytest

from app.tasks.validation_tasks import (
    process_validation_task_by_id,
    perform_ro_crate_validation,
    return_ro_crate_validation,
    process_validation_task_by_metadata,
    check_ro_crate_exists
)

from app.utils.minio_utils import InvalidAPIUsage


# Test function: process_validation_task_by_id

@mock.patch("app.tasks.validation_tasks.os.remove")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.update_validation_status_in_minio")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.fetch_ro_crate_from_minio")
def test_process_validation_success(
    mock_fetch,
    mock_validate,
    mock_update,
    mock_webhook,
    mock_exists,
    mock_remove
):
    mock_fetch.return_value = "/tmp/crate.zip"

    mock_validation_result = mock.Mock()
    mock_validation_result.has_issues.return_value = False
    mock_validation_result.to_json.return_value = '{"status": "valid"}'
    mock_validate.return_value = mock_validation_result

    process_validation_task_by_id("crate123", "profileA", "https://example.com/hook")

    mock_fetch.assert_called_once_with("crate123")
    mock_validate.assert_called_once_with("/tmp/crate.zip", "profileA")
    mock_update.assert_called_once_with("crate123", '{"status": "valid"}')
    mock_webhook.assert_called_once_with("https://example.com/hook", '{"status": "valid"}')
    mock_remove.assert_called_once_with("/tmp/crate.zip")


@mock.patch("app.tasks.validation_tasks.os.remove")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.update_validation_status_in_minio")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.fetch_ro_crate_from_minio")
def test_process_validation_fails_with_message(
    mock_fetch,
    mock_validate,
    mock_update,
    mock_webhook,
    mock_exists,
    mock_remove
):
    mock_fetch.return_value = "/tmp/crate.zip"
    mock_validate.return_value = "Validation failed"

    process_validation_task_by_id("crate123", "profileA", "https://example.com/hook")

    mock_update.assert_not_called()
    mock_webhook.assert_called_once()
    args, kwargs = mock_webhook.call_args
    assert args[0] == "https://example.com/hook"
    assert "Validation failed" in args[1]["error"]
    mock_remove.assert_called_once_with("/tmp/crate.zip")


@mock.patch("app.tasks.validation_tasks.os.remove")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.update_validation_status_in_minio")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation", side_effect=Exception("Unexpected error"))
@mock.patch("app.tasks.validation_tasks.fetch_ro_crate_from_minio")
def test_process_validation_exception(
    mock_fetch,
    mock_validate,
    mock_update,
    mock_webhook,
    mock_exists,
    mock_remove
):
    mock_fetch.return_value = "/tmp/crate.zip"

    process_validation_task_by_id("crate123", "profileA", "https://example.com/hook")

    mock_update.assert_not_called()
    mock_webhook.assert_called_once()
    args, kwargs = mock_webhook.call_args
    assert args[0] == "https://example.com/hook"
    assert "Unexpected error" in args[1]["error"]
    mock_remove.assert_called_once_with("/tmp/crate.zip")


@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=False)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.update_validation_status_in_minio")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.fetch_ro_crate_from_minio", side_effect=Exception("MinIO fetch failed"))
def test_process_validation_fetch_error(
    mock_fetch,
    mock_validate,
    mock_update,
    mock_webhook,
    mock_exists
):
    process_validation_task_by_id("crate123", "profileA", "https://example.com/hook")

    mock_validate.assert_not_called()
    mock_update.assert_not_called()
    mock_webhook.assert_called_once()
    args, kwargs = mock_webhook.call_args
    assert args[0] == "https://example.com/hook"
    assert "MinIO fetch failed" in args[1]["error"]


# Test function: process_validation_task_by_metadata

@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.build_metadata_only_rocrate")
def test_validation_success_no_issues(
    mock_build, mock_validate, mock_webhook, mock_exists, mock_rmtree
):
    crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}'
    profile_name = "test-profile"
    webhook_url = "https://example.com/webhook"

    mock_path = "/tmp/crate"
    mock_build.return_value = mock_path

    mock_result = mock.Mock()
    mock_result.has_issues.return_value = False
    mock_result.to_json.return_value = '{"status": "valid"}'
    mock_validate.return_value = mock_result

    result = process_validation_task_by_metadata(crate_json, profile_name, webhook_url)

    assert result == '{"status": "valid"}'
    mock_build.assert_called_once_with(crate_json)
    mock_validate.assert_called_once()
    mock_webhook.assert_called_once_with(webhook_url, '{"status": "valid"}')
    mock_rmtree.assert_called_once_with(mock_path)


@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.build_metadata_only_rocrate")
def test_validation_success_with_issues(
    mock_build, mock_validate, mock_webhook, mock_exists, mock_rmtree
):
    crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}'
    mock_path = "/tmp/crate"
    mock_build.return_value = mock_path

    mock_result = mock.Mock()
    mock_result.has_issues.return_value = True
    mock_result.to_json.return_value = '{"status": "invalid"}'
    mock_validate.return_value = mock_result

    result = process_validation_task_by_metadata(crate_json, None, None)

    assert result == '{"status": "invalid"}'
    mock_webhook.assert_not_called()
    mock_rmtree.assert_called_once_with(mock_path)


@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation", return_value="Validation error")
@mock.patch("app.tasks.validation_tasks.build_metadata_only_rocrate")
def test_validation_fails_and_sends_error_notification_to_webhook(
    mock_build, mock_validate, mock_webhook, mock_exists, mock_rmtree
):
    crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}'
    mock_path = "/tmp/crate"
    mock_build.return_value = mock_path

    result = process_validation_task_by_metadata(crate_json, "profileX", "https://webhook.test")

    assert isinstance(result, str)
    assert "Validation error" in result

    # Error webhook should be sent
    mock_webhook.assert_called_once()
    args, kwargs = mock_webhook.call_args
    assert kwargs is None or "error" in args[1]

    mock_rmtree.assert_called_once_with(mock_path)


@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation", return_value="Validation error")
@mock.patch("app.tasks.validation_tasks.build_metadata_only_rocrate")
def test_validation_fails_with_no_webhook(
    mock_build, mock_validate, mock_webhook, mock_exists, mock_rmtree
):
    crate_json = '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}'
    mock_path = "/tmp/crate"
    mock_build.return_value = mock_path

    result = process_validation_task_by_metadata(crate_json, "profileX", None)

    assert isinstance(result, str)
    assert "Validation error" in result

    # Make sure webhook not sent
    mock_webhook.assert_not_called()
    mock_rmtree.assert_called_once_with(mock_path)


# Test function: perform_ro_crate_validation

@mock.patch("app.tasks.validation_tasks.services.validate")
@mock.patch("app.tasks.validation_tasks.services.ValidationSettings")
def test_validation_success_with_all_args(mock_validation_settings, mock_validate):
    mock_result = mock.Mock()
    mock_validate.return_value = mock_result

    file_path = "crates/test_crate"
    profile_name = "ro_profile"
    skip_checks = ["check1", "check2"]

    result = perform_ro_crate_validation(file_path, profile_name, skip_checks)

    # Assert that result was returned
    assert result == mock_result

    # Validate proper construction of ValidationSettings
    mock_validation_settings.assert_called_once()
    args, kwargs = mock_validation_settings.call_args
    assert kwargs["rocrate_uri"].endswith(file_path)
    assert kwargs["profile_identifier"] == profile_name
    assert kwargs["skip_checks"] == skip_checks

    mock_validate.assert_called_once_with(mock_validation_settings.return_value)


@mock.patch("app.tasks.validation_tasks.services.validate")
@mock.patch("app.tasks.validation_tasks.services.ValidationSettings")
def test_validation_success_with_minimal_args(mock_validation_settings, mock_validate):
    mock_result = mock.Mock()
    mock_validate.return_value = mock_result

    file_path = "crates/test_crate"
    result = perform_ro_crate_validation(file_path, None)

    assert result == mock_result

    args, kwargs = mock_validation_settings.call_args
    assert "profile_identifier" not in kwargs
    assert "skip_checks" not in kwargs


@mock.patch("app.tasks.validation_tasks.services.validate", side_effect=RuntimeError("Validation error"))
@mock.patch("app.tasks.validation_tasks.services.ValidationSettings")
def test_validation_raises_exception_and_returns_string(mock_validation_settings, mock_validate):
    file_path = "crates/test_crate"
    result = perform_ro_crate_validation(file_path, "profile", skip_checks_list=None)

    assert isinstance(result, str)
    assert "Validation error" in result
    mock_validate.assert_called_once()


@mock.patch("app.tasks.validation_tasks.services.validate")
@mock.patch("app.tasks.validation_tasks.services.ValidationSettings", side_effect=ValueError("Bad config"))
def test_validation_settings_error(mock_validation_settings, mock_validate):
    file_path = "crates/test_crate"
    result = perform_ro_crate_validation(file_path, None)

    assert isinstance(result, str)
    assert "Bad config" in result
    mock_validate.assert_not_called()


# Test function: return_ro_crate_validation

@mock.patch("app.tasks.validation_tasks.get_validation_status_from_minio")
def test_return_validation_returns_dict(mock_get_status):
    # Simulate dict result
    mock_get_status.return_value = {"status": "passed", "errors": []}

    result = return_ro_crate_validation("crate123")
    assert isinstance(result, dict)
    assert result["status"] == "passed"
    mock_get_status.assert_called_once_with("crate123")


@mock.patch("app.tasks.validation_tasks.get_validation_status_from_minio")
def test_return_validation_returns_string(mock_get_status):
    # Simulate string result
    mock_get_status.return_value = "Validation result: OK"

    result = return_ro_crate_validation("crate456")
    assert isinstance(result, str)
    assert "OK" in result
    mock_get_status.assert_called_once_with("crate456")


@mock.patch("app.tasks.validation_tasks.get_validation_status_from_minio")
def test_return_validation_raises_error(mock_get_status):
    # Simulate exception
    mock_get_status.side_effect = InvalidAPIUsage("MinIO S3 Error: empty", 500)

    with pytest.raises(InvalidAPIUsage) as exc_info:
        return_ro_crate_validation("crate789")

    assert "MinIO S3 Error" in str(exc_info.value.message)
    mock_get_status.assert_called_once_with("crate789")


# Test function: check_ro_crate_exists

@mock.patch("app.tasks.validation_tasks.get_minio_client_and_bucket", return_value=("mock_client", "mock_bucket"))
@mock.patch("app.tasks.validation_tasks.find_rocrate_object_on_minio", return_value="crate123")
def test_ro_crate_exists(
    mock_find_rocrate,
    mock_get_client
):
    result = check_ro_crate_exists("crate123")

    mock_get_client.assert_called_once()
    mock_find_rocrate.assert_called_once_with("crate123", "mock_client", "mock_bucket", storage_path='')
    assert result is True


@mock.patch("app.tasks.validation_tasks.get_minio_client_and_bucket", return_value=("mock_client", "mock_bucket"))
@mock.patch("app.tasks.validation_tasks.find_rocrate_object_on_minio", return_value=False)
def test_ro_crate_does_not_exist(
    mock_find_rocrate,
    mock_get_client
):
    result = check_ro_crate_exists("crate12z")

    mock_get_client.assert_called_once()
    mock_find_rocrate.assert_called_once_with("crate12z", "mock_client", "mock_bucket", storage_path='')
    assert result is False
