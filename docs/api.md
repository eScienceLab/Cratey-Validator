# Crate Validator REST API

## Overview

The Crate Validator REST API exposes operations for validating stored 
RO-Crates,  validating submitted RO-Crate metadata, and retrieving saved 
validation results.

Stored RO-Crate validation uses MinIO-compatible object storage. Requests that 
operate on stored crates include `minio_config`, which tells the service where 
the crate is stored and which bucket to use.


## Endpoints

### `POST /v1/ro_crates/{crate_id}/validation`

Queues validation for an RO-Crate stored in MinIO-compatible object storage. 
The `crate_id` path parameter is the name used to find the crate object in the 
configured bucket.

Request body:

```jsonc
{
  "minio_config": {
    "endpoint": "string",   // required, e.g. "localhost:9000" or "minio:9000"
    "accesskey": "string",  // required, MinIO access key or username
    "secret": "string",     // required, MinIO secret key or password
    "ssl": false,           // required, true when the MinIO endpoint uses HTTPS
    "bucket": "string"      // required, bucket containing the RO-Crate
  },
  "root_path": "string",     // optional folder/path inside the bucket
  "profile_name": "string"   // optional validation profile name
}
```

Expected responses:

- `202`: validation queued.
- `400`: RO-Crate does not exist or validation request cannot be satisfied.
- `500`: internal service, MinIO, Celery, or validation error.


### `GET /v1/ro_crates/{crate_id}/validation`

Fetches the latest validation result from MinIO-compatible object storage.

Request body:

```jsonc
{
  "minio_config": {
    "endpoint": "string",   // required, e.g. "localhost:9000" or "minio:9000"
    "accesskey": "string",  // required, MinIO access key or username
    "secret": "string",     // required, MinIO secret key or password
    "ssl": false,           // required, true when the MinIO endpoint uses HTTPS
    "bucket": "string"      // required, bucket containing the RO-Crate
  },
  "root_path": "string"     // optional folder/path inside the bucket
}
```

Expected responses:

- `200`: validation result JSON returned.
- `400`: RO-Crate or validation result is missing.
- `500`: MinIO or internal retrieval error.


### `POST /v1/ro_crates/validate_metadata`

Validates a submitted RO-Crate metadata JSON string.

Request body:

```jsonc
{
  "crate_json": "string",   // required, stringified content of ro-crate-metadata.json
  "profile_name": "string"  // optional validation profile name
}
```

Expected responses:

- `200`: validation result returned.
- `422`: missing, malformed, or empty metadata JSON.
- `500`: internal validation error.


## Validation Profiles

The optional `profile_name` field selects a specific RO-Crate validation 
profile. If omitted, the service uses the validator default.

Custom profile definitions can be made available to the service through the 
`PROFILES_PATH` environment variable.


## Result Storage

Validation results for stored RO-Crates are saved back to MinIO-compatible 
object storage.

Without `root_path`, results are stored at:

```text
{crate_id}_validation/validation_status.txt
```

With `root_path`, results are stored at:

```text
{root_path}/{crate_id}_validation/validation_status.txt
```

