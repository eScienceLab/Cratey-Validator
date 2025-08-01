from unittest import mock
import pytest

from app.tasks.validation_tasks import (
    process_validation_task_by_id,
    perform_ro_crate_validation,
    return_ro_crate_validation,
    process_validation_task_by_metadata,
    check_ro_crate_exists,
    check_validation_exists
)

from app.utils.minio_utils import InvalidAPIUsage


# Test function: process_validation_task_by_id

@pytest.mark.parametrize(
        "crate_id, os_path_exists, os_path_isfile, os_path_isdir, return_value, webhook, profile, val_success, val_result",
        [
            ("crate123", True, True, False, "/tmp/crate.zip",
                "https://example.com/hook", "profileA", True, '{"status": "valid"}'),
            ("crate123", True, False, True, "/tmp/crate123",
                "https://example.com/hook", "profileA", True, '{"status": "valid"}'),
            ("crate123", True, False, True, "/tmp/crate123",
                None, "profileA", True, '{"status": "valid"}'),
        ],
        ids=["successful_validation_zip", "successful_validation_dir", "successful_validation_nowebhook"]
)
@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.remove")
@mock.patch("app.tasks.validation_tasks.os.path.exists")
@mock.patch("app.tasks.validation_tasks.os.path.isfile")
@mock.patch("app.tasks.validation_tasks.os.path.isdir")
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.update_validation_status_in_minio")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.fetch_ro_crate_from_minio")
def test_process_validation(
    mock_fetch,
    mock_validate,
    mock_update,
    mock_webhook,
    mock_isdir,
    mock_isfile,
    mock_exists,
    mock_remove,
    mock_rmtree,
    crate_id: str, os_path_exists: bool, os_path_isfile: bool, os_path_isdir: bool,
    return_value: str, webhook: str, profile: str, val_success: bool, val_result: str
):
    mock_exists.return_value = os_path_exists
    mock_isfile.return_value = os_path_isfile
    mock_isdir.return_value = os_path_isdir
    mock_fetch.return_value = return_value

    mock_validation_result = mock.Mock()
    mock_validation_result.has_issues.return_value = val_success
    mock_validation_result.to_json.return_value = val_result
    mock_validate.return_value = mock_validation_result

    process_validation_task_by_id("test_bucket", crate_id, "", profile, webhook)

    mock_fetch.assert_called_once_with("test_bucket", crate_id, "")
    mock_validate.assert_called_once_with(return_value, profile)
    mock_update.assert_called_once_with("test_bucket", crate_id, "", val_result)
    if webhook is not None:
        mock_webhook.assert_called_once_with(webhook, val_result)
    else:
        mock_webhook.assert_not_called()
    if os_path_exists and os_path_isfile:
        mock_remove.assert_called_once_with(return_value)
        mock_rmtree.assert_not_called()
    elif os_path_exists and os_path_isdir:
        mock_rmtree.assert_called_once_with(return_value)
        mock_remove.assert_not_called()


@pytest.mark.parametrize(
        "crate_id, os_path_exists, os_path_isfile, os_path_isdir, return_fetch, "
        + "webhook, profile, return_validate, validate_side_effect, fetch_side_effect",
        [
            ("crate123", True, True, False, "/tmp/crate.zip",
                "https://example.com/hook", "profileA", "Validation failed", None, None),
            ("crate123", True, True, False, "/tmp/crate.zip",
                "https://example.com/hook", "profileA", None, Exception("Unexpected error"), None),
            ("crate123", False, False, False, None,
                "https://example.com/hook", "profileA", None, None, Exception("MinIO fetch failed")),
        ],
        ids=["validation_fails_with_message", "validation_fails_with_validation_exception",
             "validation_fails_with_fetch_exception"]
)
@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.remove")
@mock.patch("app.tasks.validation_tasks.os.path.exists")
@mock.patch("app.tasks.validation_tasks.os.path.isfile")
@mock.patch("app.tasks.validation_tasks.os.path.isdir")
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.update_validation_status_in_minio")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.fetch_ro_crate_from_minio")
def test_process_validation_failure(
    mock_fetch,
    mock_validate,
    mock_update,
    mock_webhook,
    mock_isdir,
    mock_isfile,
    mock_exists,
    mock_remove,
    mock_rmtree,
    crate_id: str, os_path_exists: bool, os_path_isfile: bool, os_path_isdir: bool,
    return_fetch: str, webhook: str, profile: str, return_validate: str,
    validate_side_effect: Exception, fetch_side_effect: Exception
):
    mock_exists.return_value = os_path_exists
    mock_isfile.return_value = os_path_isfile
    mock_isdir.return_value = os_path_isdir

    if fetch_side_effect is None:
        mock_fetch.return_value = return_fetch
    else:
        mock_fetch.side_effect = fetch_side_effect

    if validate_side_effect is None:
        mock_validate.return_value = return_validate
    else:
        mock_validate.side_effect = validate_side_effect

    process_validation_task_by_id("test_bucket", crate_id, "", profile, webhook)

    if fetch_side_effect is None:
        mock_validate.assert_called_once_with(return_fetch, profile)
    else:
        mock_validate.assert_not_called()

    mock_update.assert_not_called()
    mock_webhook.assert_called_once()
    args, kwargs = mock_webhook.call_args
    assert args[0] == webhook
    if fetch_side_effect is not None:
        assert fetch_side_effect.args[0] in args[1]["error"]
    elif validate_side_effect is not None:
        assert validate_side_effect.args[0] in args[1]["error"]
    else:
        assert return_validate in args[1]["error"]

    if not os_path_exists:
        mock_remove.assert_not_called()
        mock_rmtree.assert_not_called()
    elif os_path_exists and os_path_isfile:
        mock_remove.assert_called_once_with(return_fetch)
        mock_rmtree.assert_not_called()
    elif os_path_exists and os_path_isdir:
        mock_rmtree.assert_called_once_with(return_fetch)
        mock_remove.assert_not_called()


# Test function: process_validation_task_by_metadata

@pytest.mark.parametrize(
        "crate_json, profile_name, webhook_url, mock_path, validation_json, validation_value, os_path_exists",
        [
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}',
                "test-profile", "https://example.com/webhook", "/tmp/crate",
                '{"status": "valid"}', False, True
            ),
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}',
                "test-profile", "https://example.com/webhook", "/tmp/crate",
                '{"status": "invalid"}', True, True
            )
        ],
        ids=["success_no_issues", "success_with_issues"]
)
@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.path.exists")
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.build_metadata_only_rocrate")
def test_metadata_validation(
    mock_build, mock_validate, mock_webhook, mock_exists, mock_rmtree,
    crate_json: str, profile_name: str, webhook_url: str, mock_path: str,
    validation_json: str, validation_value: bool, os_path_exists: bool
):
    mock_exists.return_value = os_path_exists
    mock_build.return_value = mock_path

    mock_result = mock.Mock()
    mock_result.has_issues.return_value = validation_value
    mock_result.to_json.return_value = validation_json
    mock_validate.return_value = mock_result

    result = process_validation_task_by_metadata(crate_json, profile_name, webhook_url)

    assert result == validation_json
    mock_build.assert_called_once_with(crate_json)
    mock_validate.assert_called_once()
    mock_webhook.assert_called_once_with(webhook_url, validation_json)
    mock_rmtree.assert_called_once_with(mock_path)


@pytest.mark.parametrize(
        "crate_json, profile_name, webhook_url, mock_path, validation_message, os_path_exists",
        [
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}',
                "test-profile", "https://example.com/webhook", "/tmp/crate",
                "Validation error", True
            ),
            (
                '{"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}',
                "test-profile", None, "/tmp/crate",
                "Validation error", True
            )
        ],
        ids=["validation_fails", "validation_fails_no_webhook"]
)
@mock.patch("app.tasks.validation_tasks.shutil.rmtree")
@mock.patch("app.tasks.validation_tasks.os.path.exists", return_value=True)
@mock.patch("app.tasks.validation_tasks.send_webhook_notification")
@mock.patch("app.tasks.validation_tasks.perform_ro_crate_validation")
@mock.patch("app.tasks.validation_tasks.build_metadata_only_rocrate")
def test_validation_fails_and_sends_error_notification_to_webhook(
    mock_build, mock_validate, mock_webhook, mock_exists, mock_rmtree,
    crate_json: str, profile_name: str, webhook_url: str, mock_path: str,
    validation_message: str, os_path_exists: bool
):
    mock_build.return_value = mock_path

    mock_validate.return_value = validation_message

    result = process_validation_task_by_metadata(crate_json, profile_name, webhook_url)

    assert isinstance(result, str)
    assert validation_message in result

    if webhook_url is not None:
        # Error webhook should be sent
        mock_webhook.assert_called_once()
        args, kwargs = mock_webhook.call_args
        assert kwargs is None or "error" in args[1]
    else:
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

    result = return_ro_crate_validation("test_bucket", "crate123", None)
    assert isinstance(result, dict)
    assert result["status"] == "passed"
    mock_get_status.assert_called_once_with("test_bucket", "crate123", None)


@mock.patch("app.tasks.validation_tasks.get_validation_status_from_minio")
def test_return_validation_returns_string(mock_get_status):
    # Simulate string result
    mock_get_status.return_value = "Validation result: OK"

    result = return_ro_crate_validation("test_bucket", "crate456", None)
    assert isinstance(result, str)
    assert "OK" in result
    mock_get_status.assert_called_once_with("test_bucket", "crate456", None)


@mock.patch("app.tasks.validation_tasks.get_validation_status_from_minio")
def test_return_validation_raises_error(mock_get_status):
    # Simulate exception
    mock_get_status.side_effect = InvalidAPIUsage("MinIO S3 Error: empty", 500)

    with pytest.raises(InvalidAPIUsage) as exc_info:
        return_ro_crate_validation("test_bucket", "crate789", None)

    assert "MinIO S3 Error" in str(exc_info.value.message)
    mock_get_status.assert_called_once_with("test_bucket", "crate789", None)


# Test function: check_ro_crate_exists

@mock.patch("app.tasks.validation_tasks.get_minio_client", return_value="mock_client")
@mock.patch("app.tasks.validation_tasks.find_rocrate_object_on_minio", return_value="crate123")
def test_ro_crate_exists(
    mock_find_rocrate,
    mock_get_client
):
    result = check_ro_crate_exists("test_bucket", "crate123", "base_path")

    mock_get_client.assert_called_once()
    mock_find_rocrate.assert_called_once_with("crate123", "mock_client", "test_bucket", "base_path")
    assert result is True


@mock.patch("app.tasks.validation_tasks.get_minio_client", return_value="mock_client")
@mock.patch("app.tasks.validation_tasks.find_rocrate_object_on_minio", return_value=False)
def test_ro_crate_does_not_exist(
    mock_find_rocrate,
    mock_get_client
):
    result = check_ro_crate_exists("test_bucket", "crate12z", "base_path")

    mock_get_client.assert_called_once()
    mock_find_rocrate.assert_called_once_with("crate12z", "mock_client", "test_bucket", "base_path")
    assert result is False


# Test function: check_validation_exists

@mock.patch("app.tasks.validation_tasks.get_minio_client", return_value="mock_client")
@mock.patch("app.tasks.validation_tasks.find_validation_object_on_minio", return_value="crate123")
def test_validation_exists(
    mock_find_validation,
    mock_get_client
):
    result = check_validation_exists("test_bucket", "crate123", "base_path")

    mock_get_client.assert_called_once()
    mock_find_validation.assert_called_once_with("crate123", "mock_client", "test_bucket", "base_path")
    assert result is True


@mock.patch("app.tasks.validation_tasks.get_minio_client", return_value="mock_client")
@mock.patch("app.tasks.validation_tasks.find_validation_object_on_minio", return_value=False)
def test_validation_does_not_exist(
    mock_find_validation,
    mock_get_client
):
    result = check_validation_exists("test_bucket", "crate12z", "base_path")

    mock_get_client.assert_called_once()
    mock_find_validation.assert_called_once_with("crate12z", "mock_client", "test_bucket", "base_path")
    assert result is False
