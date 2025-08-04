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


# Testing function: get_minio_client

@pytest.mark.parametrize(
        "minio_config",
        [
            {
                "endpoint": "localhost:9000",
                "accesskey": "admin",
                "secret": "password123",
                "ssl": False
            },
            {
                "endpoint": "localhost:9000",
                "accesskey": "admin",
                "secret": "password123",
                "ssl": False,
                "bucket": "ignore_this"
            }
        ],
        ids=["base_case", "ignore_extra_items"]
)
def test_get_minio_client_success(minio_config: dict):

    from app.utils.minio_utils import get_minio_client
    client = get_minio_client(minio_config)

    assert isinstance(client, Minio)
    assert client._base_url.host == "localhost:9000"


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


@pytest.mark.parametrize(
        "bucket, path, status_code, list_side_effect, error_check",
        [
            (
                "my-bucket", "path/rocrate.zip", 500,
                S3Error(code="S3 error",
                        message=None,
                        resource=None,
                        request_id=None,
                        host_id=None,
                        response=None),
                "MinIO S3 Error"
            ),
            (
                "my-bucket", "path/rocrate.zip", 500,
                ValueError("Missing config"),
                "Configuration Error"
            ),
            (
                "my-bucket", "path/rocrate.zip", 500,
                RuntimeError("Something went wrong"),
                "Unknown Error"
            ),
        ],
        ids=["s3error", "value_error", "unexpected_error"]
)
def test_get_minio_object_list_errors(bucket: str, path: str, status_code: int, list_side_effect, error_check: str):
    mock_minio_client = MagicMock()
    mock_minio_client.list_objects.side_effect = list_side_effect

    from app.utils.minio_utils import get_minio_object_list, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_minio_object_list(path, mock_minio_client, bucket)

    assert exc.value.status_code == status_code
    assert error_check in str(exc.value.message)


# Testing function: find_rocrate_object_on_minio


@pytest.mark.parametrize(
    "rocrate_object, crateid, bucket, root_path",
    [
        (
            DummyObject("my/path/rocrate123/", is_dir=True),
            "rocrate123", "bucket", "my/path"
        ),
        (
            DummyObject("my/path/rocrate123.zip"),
            "rocrate123", "bucket", "my/path"
        ),
        (
            DummyObject("rocrate123.zip"),
            "rocrate123", "bucket", None
        ),
    ],
    ids=["rocrate_directory", "rocrate_zip", "rootpath_none"]
)
@patch("app.utils.minio_utils.get_minio_object_list")
def test_finding_rocrate_on_minio(
        mock_get_list,
        rocrate_object: DummyObject, crateid: str, bucket: str, root_path: str):
    # Simulate a directory object match
    mock_get_list.return_value = [rocrate_object]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio(crateid, minio_client, bucket, root_path)
    assert result == rocrate_object


@patch("app.utils.minio_utils.get_minio_object_list")
def test_rocrate_not_found(mock_get_list):
    # Simulate no matching object
    mock_get_list.return_value = [
        DummyObject("something_else"),
        DummyObject("another_dir", is_dir=True)
    ]
    minio_client = MagicMock()

    from app.utils.minio_utils import find_rocrate_object_on_minio
    result = find_rocrate_object_on_minio("rocrate123", minio_client, "bucket", None)

    mock_get_list.assert_called_once()
    assert not result


# Testing function: find_validation_object_on_minio

@pytest.mark.parametrize(
    "object_path, crateid, bucket, root_path",
    [
        (
            "my/storage/rocrate123_validation/validation_status.txt",
            "rocrate123", "bucket", "my/storage"
        ),
        (
            "rocrate123_validation/validation_status.txt",
            "rocrate123", "bucket", None
        ),
    ],
    ids=["with_storage_path", "without_storage_path"]
)
@patch("app.utils.minio_utils.get_minio_object_list")
def test_validation_object_found_with_storage_path(
        mock_get_list,
        object_path: str, crateid: str, bucket: str, root_path: str):
    # Setup
    obj = DummyObject(object_path)
    mock_get_list.return_value = [obj]

    from app.utils.minio_utils import find_validation_object_on_minio
    # Execute
    result = find_validation_object_on_minio(crateid, MagicMock(), bucket, root_path)

    # Assert
    assert result == obj
    mock_get_list.assert_called_once_with(object_path, mock.ANY, bucket)


@pytest.mark.parametrize(
    "object_list, crateid, bucket, root_path",
    [
        (
            [DummyObject("some/other/object.txt")],
            "rocrate999", "bucket", None
        ),
        (
            [],
            "rocrate999", "bucket", None
        ),
    ],
    ids=["other_objects", "empty_list"]
)
@patch("app.utils.minio_utils.get_minio_object_list")
def test_validation_object_not_found(
        mock_get_list,
        object_list: list, crateid: str, bucket: str, root_path: str):
    # Setup: no objects returned
    mock_get_list.return_value = object_list

    from app.utils.minio_utils import find_validation_object_on_minio
    result = find_validation_object_on_minio(crateid, MagicMock(), bucket, root_path)

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


@pytest.mark.parametrize(
        "bucket, remotepath, localpath, status_code, get_side_effect, error_check",
        [
            (
                "my-bucket", "remote/path.txt", "local/path.txt", 500,
                S3Error(code="S3 error",
                        message=None,
                        resource=None,
                        request_id=None,
                        host_id=None,
                        response=None),
                "MinIO S3 Error"
            ),
            (
                "my-bucket", "remote/path.txt", "local/path.txt", 500,
                ValueError("Missing config"),
                "Configuration Error"
            ),
            (
                "my-bucket", "remote/path.txt", "local/path.txt", 500,
                RuntimeError("Something went wrong"),
                "Unknown Error"
            ),
        ],
        ids=["s3error", "value_error", "unexpected_error"]
)
@patch("app.utils.minio_utils.logging")
def test_download_s3error(
        mock_logging,
        bucket: str, remotepath: str, localpath: str, status_code: int,
        get_side_effect, error_check: str
):
    mock_minio = MagicMock()
    mock_minio.fget_object.side_effect = get_side_effect

    from app.utils.minio_utils import download_file_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        download_file_from_minio(mock_minio, bucket, remotepath, localpath)

    assert exc.value.status_code == status_code
    assert error_check in str(exc.value.message)
    mock_logging.error.assert_called_once()


# Testing function: get_validation_status_from_minio

def test_successful_retrieval(mocker, mock_minio_response):
    mock_client = MagicMock()
    mock_client.get_object.return_value = mock_minio_response
    mocker.patch("app.utils.minio_utils.get_minio_client", return_value=mock_client)

    from app.utils.minio_utils import get_validation_status_from_minio
    result = get_validation_status_from_minio("test_bucket", "crate123", None)

    assert result == {"status": "valid"}
    mock_minio_response.close.assert_called_once()
    mock_minio_response.release_conn.assert_called_once()


@pytest.mark.parametrize(
        "bucket, crateid, root_path, status_code, get_side_effect, error_check",
        [
            (
                "my-bucket", "crate123", None, 500,
                S3Error(code="S3 error",
                        message=None,
                        resource=None,
                        request_id=None,
                        host_id=None,
                        response=None),
                "MinIO S3 Error"
            ),
            (
                "my-bucket", "crate123", None, 500,
                ValueError("Missing env var"),
                "Configuration Error"
            ),
            (
                "my-bucket", "crate123", None, 500,
                RuntimeError("Unexpected failure"),
                "Unknown Error"
            ),
        ],
        ids=["s3error", "value_error", "unexpected_error"]
)
def test_get_validation_error_raised(
        mocker,
        bucket: str, crateid: str, root_path: str, status_code: int, get_side_effect, error_check: str
):
    mock_client = MagicMock()
    mock_client.get_object.side_effect = get_side_effect
    mocker.patch("app.utils.minio_utils.get_minio_client", return_value=mock_client)

    from app.utils.minio_utils import get_validation_status_from_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        get_validation_status_from_minio(bucket, crateid, root_path)

    assert exc.value.status_code == status_code
    assert error_check in str(exc.value.message)


# Testing function: update_validation_status_in_minio

def test_update_validation_status_success():
    mock_minio_client = mock.Mock()

    crate_id = "crate123"
    validation_status = json.dumps({"status": "valid", "errors": []})

    from app.utils.minio_utils import update_validation_status_in_minio
    update_validation_status_in_minio(mock_minio_client, "test_bucket", crate_id, "", validation_status)

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

    assert bucket_name == "test_bucket"
    assert object_name == expected_object_name
    assert isinstance(actual_data_stream, BytesIO)
    actual_data_stream.seek(0)
    assert actual_data_stream.read() == expected_data
    assert length == len(expected_data)
    assert kwargs["content_type"] == "application/json"


@pytest.mark.parametrize(
        "bucket, crateid, root_path, validation_result, put_side_effect, error_check, status_code",
        [
            (
                "my-bucket", "crate123", None,
                {"status": "valid"},
                S3Error(code="S3 error",
                        message=None,
                        resource=None,
                        request_id=None,
                        host_id=None,
                        response=None),
                "MinIO S3 Error", 500
            ),
            (
                "my-bucket", "crate123", None,
                {"status": "valid"},
                ValueError("Missing env vars"),
                "Configuration Error", 500
            ),
            (
                "my-bucket", "crate123", None,
                {"status": "valid"},
                RuntimeError("Unexpected failure"),
                "Unknown Error", 500
            ),
        ],
        ids=["s3error", "value_error", "unexpected_error"]
)
def test_update_validation_status_erro(
        bucket: str, crateid: str, root_path: str, validation_result: dict,
        put_side_effect, error_check: str, status_code: int
):
    mock_minio_client = mock.Mock()
    mock_minio_client.put_object.side_effect = put_side_effect

    from app.utils.minio_utils import update_validation_status_in_minio, InvalidAPIUsage
    with pytest.raises(InvalidAPIUsage) as exc:
        update_validation_status_in_minio(mock_minio_client, bucket, crateid, root_path, json.dumps(validation_result))

    assert exc.value.status_code == status_code
    assert error_check in str(exc.value.message)


# Testing function: fetch_ro_crate_from_minio

@patch("app.utils.minio_utils.download_file_from_minio")
@patch("app.utils.minio_utils.get_minio_object_list")
@patch("app.utils.minio_utils.find_rocrate_object_on_minio")
def test_fetch_rocrate_zip(
    mock_find_object,
    mock_get_list,
    mock_download,
    tmp_path,
):
    # Setup mocks
    minio_client = "minio_client"
    rocrate_obj = DummyObject("some/path/rocrate123.zip", is_dir=False)
    mock_find_object.return_value = rocrate_obj

    from app.utils.minio_utils import fetch_ro_crate_from_minio

    with patch("app.utils.minio_utils.tempfile.mkdtemp", return_value=str(tmp_path)):
        # Execute
        result = fetch_ro_crate_from_minio(minio_client, "test_bucket", "rocrate123", "some/path")

    # Assert
    expected_path = tmp_path / "rocrate123.zip"
    assert result == str(expected_path)
    mock_download.assert_called_once_with(
        "minio_client", "test_bucket",
        "some/path/rocrate123.zip", str(expected_path))


@patch("app.utils.minio_utils.download_file_from_minio")
@patch("app.utils.minio_utils.get_minio_object_list")
@patch("app.utils.minio_utils.find_rocrate_object_on_minio")
def test_fetch_rocrate_directory(
    mock_find_object,
    mock_get_list,
    mock_download,
    tmp_path,
):
    # Setup mocks
    minio_client = "minio_client"
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
        result = fetch_ro_crate_from_minio(minio_client, "test_bucket", "rocrate124", "rocrates")

        # Assert
        expected_root = tmp_path / "rocrate124"
        assert result == str(expected_root)
        mock_download.assert_any_call(
            "minio_client", "test_bucket",
            "rocrates/rocrate124/metadata.json",
            str(expected_root / "metadata.json")
        )
        mock_download.assert_any_call(
            "minio_client", "test_bucket",
            "rocrates/rocrate124/data/file1.txt",
            str(expected_root / "data/file1.txt")
        )


@patch("app.utils.minio_utils.download_file_from_minio")
@patch("app.utils.minio_utils.get_minio_object_list")
@patch("app.utils.minio_utils.find_rocrate_object_on_minio")
def test_fetch_rocrate_handles_empty_dir(
    mock_find_object,
    mock_get_list,
    mock_download,
    tmp_path,
):
    minio_client = "minio_client"
    rocrate_obj = DummyObject("rocrate456", is_dir=True)
    mock_find_object.return_value = rocrate_obj
    mock_get_list.return_value = []

    from app.utils.minio_utils import fetch_ro_crate_from_minio

    with patch("app.utils.minio_utils.tempfile.mkdtemp", return_value=str(tmp_path)):
        result = fetch_ro_crate_from_minio(minio_client, "test_bucket", "rocrate456", "")

        expected_root = tmp_path / "rocrate456"
        assert result == str(expected_root)
        mock_download.assert_not_called()
