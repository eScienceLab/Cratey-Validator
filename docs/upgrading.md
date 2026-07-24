# Upgrading from 1.x

The RO-Crate Validation Service 2.0 release replaced the MinIO-specific storage layer with a general S3-compatible one.

!!! note
    If you only use metadata validation (`POST /v1/ro_crates/validate_metadata`), nothing changes and the endpoint, request fields and responses are the same as in 1.\*. The rest of this page concerns storage-backed validation.

## Server settings

The old `MINIO_*` variables are replaced by equivalent `S3_*` variables, and storage is now switched on explicitly:

| 1.\* | Now |
|------|-----|
| `MINIO_ENDPOINT` | `S3_ENDPOINT` |
| `MINIO_ROOT_USER` | `S3_ACCESS_KEY` |
| `MINIO_ROOT_PASSWORD` | `S3_SECRET_KEY` |
| `MINIO_BUCKET_NAME` | `S3_BUCKET` |
| `ssl` field in each request | `S3_USE_SSL` |
| — | `STORAGE_ENABLED` (new; must be `true` for the stored-crate endpoints to exist) |
| — | `S3_CRATE_PREFIX`, `S3_RESULTS_PREFIX` (new; default `crates` and `validation-results`) |
| `FLASK_APP=cratey.py` | `FLASK_APP=wsgi.py` |

The published image is now `ghcr.io/esciencelab/ro-crate-validation-service` (with a `-fivesafes-profile` variant that has the Five Safes profile included). The [configuration reference](installation.md#configuration-reference) lists all the settings.

## Keeping your existing MinIO

You do not need to change object store as MinIO is S3-compatible. Set `S3_ENDPOINT` to your existing MinIO endpoint, `S3_ACCESS_KEY` and `S3_SECRET_KEY` to your MinIO credentials, and `S3_BUCKET` to your bucket.

## API changes

The service connects to the object store defined in the server-side configuration. Requests carry only the crate ID and validation options. The body of `POST /v1/ro_crates/{crate_id}/validation` contains the optional `profile_name` and `webhook_url`. `GET /v1/ro_crates/{crate_id}/validation` takes no body. 

!!! warning
    Ensure that you update any existing request bodies before sending requests to the new RO-Crate Validation Service API. Incorrect request bodies will receive `422` validation errors.

### Crate IDs

A crate ID is the short label that addresses an RO-Crate in the API path, for example `my-dataset-2026` in `POST /v1/ro_crates/my-dataset-2026/validation`. It is chosen by whoever uploads the RO-Crate, and the service composes the object keys from it: `{S3_CRATE_PREFIX}/<id>.zip` for a zipped RO-Crate, or `{S3_CRATE_PREFIX}/<id>/` for a directory. The ID itself is not a filename, path or URL.

Crate IDs are now validated strictly: they must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. This does not allow slashes or path segments. Paths inside the bucket are handled by the prefix settings.

Response codes are more specific than the 1.\* `400`/`500` pattern:

| Situation | 1.\* | Now |
|-----------|------|-----|
| Crate not found in the store | `400` | `404` |
| No validation result stored yet | `400` | `404` |
| Invalid crate ID | — | `400` |
| Both zip and directory exist for one ID | — | `409` |
| Request body invalid (e.g., contains removed 1.\* fields) | — | `422` |
| Object store unreachable | `500` | `503` |

Validation results are saved to `{S3_RESULTS_PREFIX}/{id}.json` (by default `validation-results/<id>.json`) instead of `{crate_id}_validation/validation_status.txt`. The [API reference](api.md) documents the current endpoints in full.

## Existing RO-Crates and results

The service now finds an RO-Crate at a fixed key rather than by prefix search: a zipped RO-Crate must be at `{S3_CRATE_PREFIX}/<id>.zip` and a directory RO-Crate under `{S3_CRATE_PREFIX}/<id>/`, so an existing RO-Crate may need moving into the RO-Crate prefix. Results stored by 1.\* are not read by the new service, so you will need to re-validate an RO-Crate whose result you still need.
