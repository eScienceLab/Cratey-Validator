import os
import tempfile
import json
import pytest
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from unittest.mock import MagicMock
from unittest import mock


@pytest.fixture
def mock_minio_response():
    response = MagicMock()
    response.data.decode.return_value = json.dumps({"status": "valid"})
    return response

# Testing function: get_minio_client_and_bucket

def test_get_minio_client_success(monkeypatch):
    # Set required env vars
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "admin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "password123")
    monkeypatch.setenv("MINIO_BUCKET_NAME", "test-bucket")

    from app.utils.minio_utils import get_minio_client_and_bucket
    client, bucket = get_minio_client_and_bucket()

    assert isinstance(client, Minio)
    assert bucket == "test-bucket"
    assert client._base_url.host == "localhost:9000"


def test_get_minio_client_missing_bucket_name(monkeypatch):
    # Set all except MINIO_BUCKET_NAME
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "admin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "password123")
    monkeypatch.setenv("MINIO_BUCKET_NAME", "")

    from app.utils.minio_utils import get_minio_client_and_bucket
    with pytest.raises(ValueError, match="MINIO_BUCKET_NAME is not set"):
        get_minio_client_and_bucket()


# Testing function: get_validation_status_from_minio

def test_successful_retrieval(mocker, mock_minio_response):
    mock_client = MagicMock()
    mock_bucket = "test-bucket"
    mock_client.get_object.return_value = mock_minio_response
    mocker.patch("app.utils.minio_utils.get_minio_client_and_bucket", return_value=(mock_client, mock_bucket))

    from app.utils.minio_utils import get_validation_status_from_minio
    result = get_validation_status_from_minio("crate123")

    assert result == {"status": "valid"}
    mock_minio_response.close.assert_called_once()
    mock_minio_response.release_conn.assert_called_once()


def test_s3_error_raised(mocker):
    mock_client = MagicMock()
    mock_bucket = "test-bucket"
    mock_client.get_object.side_effect = S3Error(
                    code="S3 error",
                    message=None,
                    resource=None,
                    request_id=None,
                    host_id=None,
                    response=None
                )
    mocker.patch("app.utils.minio_utils.get_minio_client_and_bucket", return_value=(mock_client, mock_bucket))

    from app.utils.minio_utils import get_validation_status_from_minio
    with pytest.raises(S3Error):
        get_validation_status_from_minio("crate123")


def test_value_error_raised(mocker):
    mocker.patch("app.utils.minio_utils.get_minio_client_and_bucket", side_effect=ValueError("Missing env var"))

    from app.utils.minio_utils import get_validation_status_from_minio
    with pytest.raises(ValueError):
        get_validation_status_from_minio("crate123")


def test_generic_exception_raised(mocker):
    mock_client = MagicMock()
    mock_bucket = "test-bucket"
    mock_client.get_object.side_effect = Exception("Unexpected failure")
    mocker.patch("app.utils.minio_utils.get_minio_client_and_bucket", return_value=(mock_client, mock_bucket))

    from app.utils.minio_utils import get_validation_status_from_minio
    with pytest.raises(Exception):
        get_validation_status_from_minio("crate123")


# Testing function: fetch_ro_crate_from_minio

@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
@mock.patch("app.utils.minio_utils.tempfile.mkdtemp")
@mock.patch("app.utils.minio_utils.os.path.join", side_effect=os.path.join)
def test_fetch_ro_crate_success(mock_join, mock_mkdtemp, mock_get_client, mock_load_dotenv):
    # Setup mocks
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")
    mock_mkdtemp.return_value = "/tmp/testdir"

    crate_id = "test_crate"
    expected_path = os.path.join("/tmp/testdir", f"{crate_id}.zip")

    from app.utils.minio_utils import fetch_ro_crate_from_minio

    result = fetch_ro_crate_from_minio(crate_id)

    mock_load_dotenv.assert_called_once()
    mock_get_client.assert_called_once()
    mock_minio_client.fget_object.assert_called_once_with(
        "test-bucket", f"{crate_id}.zip", expected_path
    )

    assert result == expected_path


@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
@mock.patch("app.utils.minio_utils.tempfile.mkdtemp", return_value="/tmp/testdir")
def test_fetch_ro_crate_s3_error(mock_mkdtemp, mock_get_client, mock_load_dotenv):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")

    crate_id = "bad_crate"
    mock_minio_client.fget_object.side_effect = S3Error(
                                code="S3 error",
                                message=None,
                                resource=None,
                                request_id=None,
                                host_id=None,
                                response=None
                        )

    from app.utils.minio_utils import fetch_ro_crate_from_minio
    with pytest.raises(S3Error):
        fetch_ro_crate_from_minio(crate_id)


@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket", side_effect=ValueError("Missing config"))
def test_fetch_ro_crate_value_error(mock_get_client, mock_load_dotenv):
    from app.utils.minio_utils import fetch_ro_crate_from_minio
    with pytest.raises(ValueError):
        fetch_ro_crate_from_minio("any_crate")


@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
@mock.patch("app.utils.minio_utils.tempfile.mkdtemp", return_value="/tmp/testdir")
def test_fetch_ro_crate_unexpected_error(mock_mkdtemp, mock_get_client, mock_load_dotenv):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")
    mock_minio_client.fget_object.side_effect = RuntimeError("Unexpected failure")

    from app.utils.minio_utils import fetch_ro_crate_from_minio
    with pytest.raises(RuntimeError):
        fetch_ro_crate_from_minio("any_crate")


# Testing function: update_validation_status_in_minio

@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_update_validation_status_success(mock_get_client, mock_load_dotenv):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")

    crate_id = "crate123"
    validation_status = json.dumps({"status": "valid", "errors": []})

    from app.utils.minio_utils import update_validation_status_in_minio
    update_validation_status_in_minio(crate_id, validation_status)

    expected_object_name = f"{crate_id}/validation_status.txt"
    expected_data = json.dumps(json.loads(validation_status), indent=None).encode("utf-8")

    mock_minio_client.put_object.assert_called_once()
    args, kwargs = mock_minio_client.put_object.call_args

    # FIXME: Original suggested test expected 4 values in args, but returned only 2.
    #        Solution was to check both args and kwargs for the 'data' and 'length' objects.
    #        Do we need to chose one format of call_args for our tests, or is this ambiguity okay? 
    bucket_name = args[0] if args else kwargs["bucket_name"]
    object_name = args[1] if len(args) > 1 else kwargs["object_name"]
    actual_data_stream = args[2] if len(args) > 2 else kwargs["data"]
    length = args[3] if len(args) > 3 else kwargs["length"]

    assert bucket_name == "test-bucket"
    assert object_name == expected_object_name
    assert isinstance(actual_data_stream, BytesIO)
    actual_data_stream.seek(0)
    assert actual_data_stream.read() == expected_data
    assert length == len(expected_data)
    assert kwargs["content_type"] == "application/json"


@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_update_validation_status_s3_error(mock_get_client, mock_load_dotenv):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")
    mock_minio_client.put_object.side_effect = S3Error(
                                code="S3 error",
                                message=None,
                                resource=None,
                                request_id=None,
                                host_id=None,
                                response=None
                        )

    from app.utils.minio_utils import update_validation_status_in_minio
    with pytest.raises(S3Error):
        update_validation_status_in_minio("crate123", json.dumps({"status": "valid"}))


@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket", side_effect=ValueError("Missing env vars"))
def test_update_validation_status_value_error(mock_get_client, mock_load_dotenv):
    from app.utils.minio_utils import update_validation_status_in_minio
    with pytest.raises(ValueError):
        update_validation_status_in_minio("crate123", json.dumps({"status": "valid"}))


@mock.patch("app.utils.minio_utils.load_dotenv")
@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_update_validation_status_unexpected_error(mock_get_client, mock_load_dotenv):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")
    mock_minio_client.put_object.side_effect = RuntimeError("Unexpected failure")

    from app.utils.minio_utils import update_validation_status_in_minio
    with pytest.raises(RuntimeError):
        update_validation_status_in_minio("crate123", json.dumps({"status": "valid"}))


# Testing function: get_validation_status_from_minio

@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_get_validation_status_success(mock_get_client, mock_minio_response):
    mock_minio_client = mock.Mock()
    mock_minio_client.get_object.return_value = mock_minio_response
    mock_get_client.return_value = (mock_minio_client, "test-bucket")

    crate_id = "crate123"
    from app.utils.minio_utils import get_validation_status_from_minio
    result = get_validation_status_from_minio(crate_id)

    assert result == {"status": "valid"}
    mock_minio_client.get_object.assert_called_once_with("test-bucket", f"{crate_id}/validation_status.txt")
    mock_minio_response.close.assert_called_once()
    mock_minio_response.release_conn.assert_called_once()


@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_get_validation_status_s3_error(mock_get_client):
    mock_minio_client = mock.Mock()
    mock_minio_client.get_object.side_effect = S3Error(
                                code="S3 error",
                                message=None,
                                resource=None,
                                request_id=None,
                                host_id=None,
                                response=None
                        )
    mock_get_client.return_value = (mock_minio_client, "test-bucket")

    from app.utils.minio_utils import get_validation_status_from_minio
    with pytest.raises(S3Error):
        get_validation_status_from_minio("crate123")


@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket", side_effect=ValueError("Missing config"))
def test_get_validation_status_value_error(mock_get_client):
    from app.utils.minio_utils import get_validation_status_from_minio
    with pytest.raises(ValueError):
        get_validation_status_from_minio("crate123")


@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_get_validation_status_generic_exception(mock_get_client):
    mock_minio_client = mock.Mock()
    mock_minio_client.get_object.side_effect = RuntimeError("Unexpected failure")
    mock_get_client.return_value = (mock_minio_client, "test-bucket")

    from app.utils.minio_utils import get_validation_status_from_minio
    with pytest.raises(RuntimeError):
        get_validation_status_from_minio("crate123")
