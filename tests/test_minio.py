import json
import pytest
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from unittest.mock import MagicMock, patch
from unittest import mock


@pytest.fixture
def mock_minio_response():
    response = MagicMock()
    response.data.decode.return_value = json.dumps({"status": "valid"})
    return response


class DummyObject:
    def __init__(self, name, is_dir=False):
        self.object_name = name
        self.is_dir = is_dir


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


# Testing function: get_minio_object_list

def test_get_minio_object_list_success():
    # Setup mock response
    mock_response = MagicMock()
    mock_objects = [DummyObject("file1.txt"), DummyObject("file2.txt")]
    mock_response.__iter__.return_value = iter(mock_objects)

    # Patch minio_client
    mock_minio_client = MagicMock()
    mock_minio_client.list_objects.return_value = mock_response

    # Call function
    from app.utils.minio_utils import get_minio_object_list
    result = get_minio_object_list("path/", mock_minio_client, "my-bucket", recursive=True)

    # Assert
    assert result == mock_objects
    mock_minio_client.list_objects.assert_called_once_with("my-bucket", "path/", recursive=True)
    mock_response.close.assert_called_once()


def test_get_minio_object_list_s3error():
    mock_minio_client = MagicMock()
    mock_minio_client.list_objects.side_effect = S3Error(
                                code="S3 error",
                                message=None,
                                resource=None,
                                request_id=None,
                                host_id=None,
                                response=None
                                )

    from app.utils.minio_utils import get_minio_object_list, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_minio_object_list("path/", mock_minio_client, "my-bucket")

    assert exc.value.status_code == 500
    assert "MinIO S3 Error" in str(exc.value.message)


def test_get_minio_object_list_value_error():
    mock_minio_client = MagicMock()
    mock_minio_client.list_objects.side_effect = ValueError("Missing config")

    from app.utils.minio_utils import get_minio_object_list, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_minio_object_list("path/", mock_minio_client, "my-bucket")

    assert exc.value.status_code == 500
    assert "Configuration Error" in str(exc.value.message)


def test_get_minio_object_list_unexpected_error():
    mock_minio_client = MagicMock()
    mock_minio_client.list_objects.side_effect = RuntimeError("Something went wrong")

    from app.utils.minio_utils import get_minio_object_list, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_minio_object_list("path/", mock_minio_client, "my-bucket")

    assert exc.value.status_code == 500
    assert "Unknown Error" in str(exc.value.message)


# Testing function: find_rocrate_object_on_minio

@patch("app.utils.minio_utils.get_minio_object_list")
def test_rocrate_found_as_directory(mock_get_list):
    # Simulate a directory object match
    obj = DummyObject("my/path/rocrate123/", is_dir=True)
    mock_get_list.return_value = [obj]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio("rocrate123", minio_client, "bucket", storage_path="my/path")
    assert result == obj


@patch("app.utils.minio_utils.get_minio_object_list")
def test_rocrate_found_as_zip(mock_get_list):
    # Simulate a zip object match
    obj = DummyObject("rocrate123.zip")
    mock_get_list.return_value = [obj]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio("rocrate123", minio_client, "bucket")
    assert result == obj


@patch("app.utils.minio_utils.get_minio_object_list")
def test_rocrate_not_found(mock_get_list):
    # Simulate no matching object
    mock_get_list.return_value = [
        DummyObject("something_else"),
        DummyObject("another_dir", is_dir=True)
    ]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio("rocrate123", minio_client, "bucket")

    mock_get_list.assert_called_once()
    assert not result


@patch("app.utils.minio_utils.get_minio_object_list")
def test_storage_path_none(mock_get_list):
    # Ensures correct rocrate_path is used when storage_path is None
    obj = DummyObject("rocrate456.zip")
    mock_get_list.return_value = [obj]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio("rocrate456", minio_client, "bucket")
    assert result == obj


@patch("app.utils.minio_utils.get_minio_object_list")
def test_storage_path_provided(mock_get_list):
    # Ensures correct rocrate_path is used when storage_path is provided
    obj = DummyObject("data/rocrate789/", is_dir=True)
    mock_get_list.return_value = [obj]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio("rocrate789", minio_client, "bucket", storage_path="data")
    assert result == obj


# Testing function: find_validation_object_on_minio

@patch("app.utils.minio_utils.get_minio_object_list")
def test_validation_object_found_with_storage_path(mock_get_list):
    # Setup
    expected_path = "my/storage/rocrate123_validation/validation_status.txt"
    obj = DummyObject(expected_path)
    mock_get_list.return_value = [obj]

    from app.utils.minio_utils import find_validation_object_on_minio
    # Execute
    result = find_validation_object_on_minio("rocrate123", MagicMock(), "bucket", storage_path="my/storage")

    # Assert
    assert result == obj
    mock_get_list.assert_called_once_with(expected_path, mock.ANY, "bucket")


@patch("app.utils.minio_utils.get_minio_object_list")
def test_validation_object_found_without_storage_path(mock_get_list):
    # Setup
    expected_path = "rocrate123_validation/validation_status.txt"
    obj = DummyObject(expected_path)
    mock_get_list.return_value = [obj]

    from app.utils.minio_utils import find_validation_object_on_minio
    # Execute
    result = find_validation_object_on_minio("rocrate123", MagicMock(), "bucket")

    # Assert
    assert result == obj
    mock_get_list.assert_called_once_with(expected_path, mock.ANY, "bucket")


@patch("app.utils.minio_utils.get_minio_object_list")
def test_validation_object_not_found(mock_get_list):
    # Setup: object name does not match exactly
    mock_get_list.return_value = [DummyObject("some/other/object.txt")]

    from app.utils.minio_utils import find_validation_object_on_minio
    result = find_validation_object_on_minio("rocrate999", MagicMock(), "bucket")

    assert result is False


@patch("app.utils.minio_utils.get_minio_object_list")
def test_validation_object_empty_list(mock_get_list):
    # Setup: no objects returned
    mock_get_list.return_value = []

    from app.utils.minio_utils import find_validation_object_on_minio
    result = find_validation_object_on_minio("rocrate999", MagicMock(), "bucket")

    assert result is False


# Testing function: download_file_from_minio

@patch("app.utils.minio_utils.logging")
def test_download_success(mock_logging):
    mock_minio = MagicMock()

    from app.utils.minio_utils import download_file_from_minio
    # No exceptions raised
    download_file_from_minio(mock_minio, "bucket", "remote/path.txt", "local/path.txt")

    mock_minio.fget_object.assert_called_once_with("bucket", "remote/path.txt", "local/path.txt")
    mock_logging.error.assert_not_called()


@patch("app.utils.minio_utils.logging")
def test_download_s3error(mock_logging):
    mock_minio = MagicMock()
    mock_minio.fget_object.side_effect = S3Error("S3 error", "", "", "", "", "")

    from app.utils.minio_utils import download_file_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        download_file_from_minio(mock_minio, "bucket", "remote/path.txt", "local/path.txt")

    assert exc.value.status_code == 500
    assert "MinIO S3 Error" in str(exc.value.message)
    mock_logging.error.assert_called_once()


@patch("app.utils.minio_utils.logging")
def test_download_value_error(mock_logging):
    mock_minio = MagicMock()
    mock_minio.fget_object.side_effect = ValueError("Missing config")

    from app.utils.minio_utils import download_file_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        download_file_from_minio(mock_minio, "bucket", "remote/path.txt", "local/path.txt")

    assert exc.value.status_code == 500
    assert "Configuration Error" in str(exc.value.message)
    mock_logging.error.assert_called_once()


@patch("app.utils.minio_utils.logging")
def test_download_unexpected_error(mock_logging):
    mock_minio = MagicMock()
    mock_minio.fget_object.side_effect = RuntimeError("Something bad happened")

    from app.utils.minio_utils import download_file_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        download_file_from_minio(mock_minio, "bucket", "remote/path.txt", "local/path.txt")

    assert exc.value.status_code == 500
    assert "Unknown Error" in str(exc.value.message)
    mock_logging.error.assert_called_once()


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

    from app.utils.minio_utils import get_validation_status_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_validation_status_from_minio("crate123")

    assert exc.value.status_code == 500
    assert "S3 Error" in str(exc.value.message)


def test_value_error_raised(mocker):
    mocker.patch("app.utils.minio_utils.get_minio_client_and_bucket", side_effect=ValueError("Missing env var"))

    from app.utils.minio_utils import get_validation_status_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_validation_status_from_minio("crate123")

    assert exc.value.status_code == 500
    assert "Configuration Error" in str(exc.value.message)


def test_generic_exception_raised(mocker):
    mock_client = MagicMock()
    mock_bucket = "test-bucket"
    mock_client.get_object.side_effect = Exception("Unexpected failure")
    mocker.patch("app.utils.minio_utils.get_minio_client_and_bucket", return_value=(mock_client, mock_bucket))

    from app.utils.minio_utils import get_validation_status_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_validation_status_from_minio("crate123")

    assert exc.value.status_code == 500
    assert "Unknown Error" in str(exc.value.message)


# Testing function: update_validation_status_in_minio

@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_update_validation_status_success(mock_get_client):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")

    crate_id = "crate123"
    validation_status = json.dumps({"status": "valid", "errors": []})

    from app.utils.minio_utils import update_validation_status_in_minio
    update_validation_status_in_minio(crate_id, validation_status)

    expected_object_name = f"{crate_id}_validation/validation_status.txt"
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


@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_update_validation_status_s3_error(mock_get_client):
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

    from app.utils.minio_utils import update_validation_status_in_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        update_validation_status_in_minio("crate123", json.dumps({"status": "valid"}))

    assert exc.value.status_code == 500
    assert "S3 Error" in str(exc.value.message)


@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket", side_effect=ValueError("Missing env vars"))
def test_update_validation_status_value_error(mock_get_client):
    from app.utils.minio_utils import update_validation_status_in_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        update_validation_status_in_minio("crate123", json.dumps({"status": "valid"}))

    assert exc.value.status_code == 500
    assert "Configuration Error" in str(exc.value.message)


@mock.patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_update_validation_status_unexpected_error(mock_get_client):
    mock_minio_client = mock.Mock()
    mock_get_client.return_value = (mock_minio_client, "test-bucket")
    mock_minio_client.put_object.side_effect = RuntimeError("Unexpected failure")

    from app.utils.minio_utils import update_validation_status_in_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        update_validation_status_in_minio("crate123", json.dumps({"status": "valid"}))

    assert exc.value.status_code == 500
    assert "Unknown Error" in str(exc.value.message)


# Testing function: fetch_ro_crate_from_minio

@patch("app.utils.minio_utils.download_file_from_minio")
@patch("app.utils.minio_utils.get_minio_object_list")
@patch("app.utils.minio_utils.find_rocrate_object_on_minio")
@patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_fetch_rocrate_zip(
    mock_get_client_and_bucket,
    mock_find_object,
    mock_get_list,
    mock_download,
    tmp_path,
):
    # Setup mocks
    mock_get_client_and_bucket.return_value = ("minio_client", "bucket")
    rocrate_obj = DummyObject("some/path/rocrate123.zip", is_dir=False)
    mock_find_object.return_value = rocrate_obj

    from app.utils.minio_utils import fetch_ro_crate_from_minio

    with patch("app.utils.minio_utils.tempfile.mkdtemp", return_value=str(tmp_path)):
        # Execute
        result = fetch_ro_crate_from_minio("rocrate123")

        # Assert
        expected_path = tmp_path / "rocrate123.zip"
        assert result == str(expected_path)
        mock_download.assert_called_once_with(
            "minio_client", "bucket",
            "some/path/rocrate123.zip", str(expected_path))


@patch("app.utils.minio_utils.download_file_from_minio")
@patch("app.utils.minio_utils.get_minio_object_list")
@patch("app.utils.minio_utils.find_rocrate_object_on_minio")
@patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_fetch_rocrate_directory(
    mock_get_client_and_bucket,
    mock_find_object,
    mock_get_list,
    mock_download,
    tmp_path,
):
    # Setup mocks
    mock_get_client_and_bucket.return_value = ("minio_client", "bucket")
    rocrate_obj = DummyObject("rocrates/rocrate124", is_dir=True)
    mock_find_object.return_value = rocrate_obj

    from app.utils.minio_utils import fetch_ro_crate_from_minio

    with patch("app.utils.minio_utils.tempfile.mkdtemp", return_value=str(tmp_path)):
        # Objects inside the RO-Crate
        mock_get_list.return_value = [
            DummyObject("rocrates/rocrate124/metadata.json"),
            DummyObject("rocrates/rocrate124/data/file1.txt"),
        ]

        # Execute
        result = fetch_ro_crate_from_minio("rocrate124")

        # Assert
        expected_root = tmp_path / "rocrate124"
        assert result == str(expected_root)
        mock_download.assert_any_call(
            "minio_client", "bucket",
            "rocrates/rocrate124/metadata.json",
            str(expected_root / "metadata.json")
        )
        mock_download.assert_any_call(
            "minio_client", "bucket",
            "rocrates/rocrate124/data/file1.txt",
            str(expected_root / "data/file1.txt")
        )


@patch("app.utils.minio_utils.download_file_from_minio")
@patch("app.utils.minio_utils.get_minio_object_list")
@patch("app.utils.minio_utils.find_rocrate_object_on_minio")
@patch("app.utils.minio_utils.get_minio_client_and_bucket")
def test_fetch_rocrate_handles_empty_dir(
    mock_get_client_and_bucket,
    mock_find_object,
    mock_get_list,
    mock_download,
    tmp_path,
):
    mock_get_client_and_bucket.return_value = ("minio_client", "bucket")
    rocrate_obj = DummyObject("rocrate456", is_dir=True)
    mock_find_object.return_value = rocrate_obj
    mock_get_list.return_value = []

    from app.utils.minio_utils import fetch_ro_crate_from_minio

    with patch("app.utils.minio_utils.tempfile.mkdtemp", return_value=str(tmp_path)):
        result = fetch_ro_crate_from_minio("rocrate456")

        expected_root = tmp_path / "rocrate456"
        assert result == str(expected_root)
        mock_download.assert_not_called()
