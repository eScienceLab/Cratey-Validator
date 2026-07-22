# Installation

## Service Snapshot

Cratey Validator is a web service that checks whether RO-Crates follow the 
expected structure and metadata rules. It can validate a complete RO-Crate 
stored in MinIO-compatible object storage, or it can validate the contents 
of an `ro-crate-metadata.json` file directly.

At a high level, a client sends a validation request, the service runs the 
RO-Crate validation checks, and the result is either returned directly or 
saved so it can be retrieved later.

## What You Can Validate

### Stored RO-Crates

Use this option when the RO-Crate already exists in MinIO-compatible object 
storage. The service reads the crate from the configured bucket, runs the 
validation checks, and saves the validation result back to object storage.

This is useful for validating complete crates as part of an upload, review, 
or publication workflow.


### Metadata Files

Use this option when you only need to check the contents of 
`ro-crate-metadata.json`. Instead of asking the service to download a full 
crate, you submit the metadata JSON directly and receive the validation result 
in the response.

This is useful for quick checks while editing metadata or before a complete 
crate has been assembled.


### Saved Validation Results

After a stored RO-Crate has been validated, the saved validation result can be 
retrieved later. This lets another application or user interface show the most 
recent validation status without rerunning the checks.


## Before You Start

For stored RO-Crate validation, you need:

- A MinIO-compatible object store.
- A bucket containing the RO-Crate.
- Access credentials for that bucket.
- The crate identifier used by the object store.

For metadata-only validation, you only need the contents of 
`ro-crate-metadata.json`.


## Running The Service

The service can be started with Docker Compose:

```bash
docker compose up --build
```

For local container development, use:

```bash
docker compose --file docker-compose-develop.yml up --build
```

Expected local services:

- Flask API: `http://localhost:5001`
- MinIO API: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- Redis: `localhost:6379`

MinIO needs a bucket for RO-Crates, normally `ro-crates`. Bucket versioning 
should be enabled so uploaded crate objects can be tracked reliably.


## Configuration

The main environment variables are:

- `FLASK_APP`: Flask entrypoint, normally `cratey.py`.
- `FLASK_ENV`: selects development or production config.
- `CELERY_BROKER_URL`: Redis broker URL.
- `CELERY_RESULT_BACKEND`: Redis result backend URL.
- `PROFILES_PATH`: optional path to custom RO-Crate validator profile 
definitions.
- `MINIO_ENDPOINT`: default MinIO endpoint used by Docker examples.
- `MINIO_ROOT_USER`: MinIO root username for local development.
- `MINIO_ROOT_PASSWORD`: MinIO root password.
- `MINIO_BUCKET_NAME`: default bucket name used by local setup.

API calls also pass MinIO access details in `minio_config`, so the service can validate crates in a specified object store and bucket.

## More Information

- For endpoint paths, request bodies, response codes, validation profiles, and 
result storage paths, see the [REST API documentation](./rest_api).
- For implementation details, service components, runtime flow, and test 
coverage, see the [Architecture documentation](./architecture).
- For deployment context and the architecture diagram, see 
[Deployment](./deployment).