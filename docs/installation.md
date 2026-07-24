# Installation & Setup

The RO-Crate Validation Service works in two ways: a metadata-only mode, in which the contents of an `ro-crate-metadata.json` file are assessed, and storage-backed validation, where complete RO-Crates (zip or directory) are evaluated. 

## Quick start

You will need Docker with Docker Compose. 

To start, clone the repository, copy the example environment file, and start the stack:

```bash
git clone https://github.com/eScienceLab/RO-Crate-Validation-Service.git
cd RO-Crate-Validation-Service
cp example.env .env
docker compose up --build
```

!!! warning
    Remember to update the default `.env` values when running the object store in production.

The API is served at `http://localhost:5001`. Redis and a Celery worker are also started, but these are only used once storage is enabled. 

To check the service is up, run:

```bash
curl http://localhost:5001/healthz
```

This returns `{"status": "ok"}`.

To validate the contents of an `ro-crate-metadata.json` file, post to the metadata endpoint. The [running example](https://www.researchobject.org/ro-crate/specification/1.2/introduction.html#running-example) from the RO-Crate specification is a good test document. 

The file needs to be sent as an escaped JSON string, which `jq` can do:

```bash
jq -Rs '{crate_json: .}' ro-crate-metadata.json | curl -X POST http://localhost:5001/v1/ro_crates/validate_metadata -H 'Content-Type: application/json' -d @-
```

The response contains a `status` of `valid`, `invalid` or `error`, along with the detailed validation. For more information, the [API reference](api.md) describes the endpoints and result format in full.

## Enabling object storage

To validate a complete RO-Crate (zip or directory) held in an object store, set `STORAGE_ENABLED=true` in `.env`. 

The storage-backed validation mode requires six settings, [described below](#configuration-reference): `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`. 

The service will fail at startup if any are missing. The Compose stack already sets the Celery variables to the bundled Redis, so in practice only the S3 settings in `.env` matter here.

Then start the stack with the bundled development object store (RustFS), run:

```bash
docker compose --profile objectstore up --build
```

RustFS serves the S3 API on port 9000 and a web console at `http://localhost:9001`. Development credentials are set in `example.env`. 

!!! warning
    The service does not create the bucket itself. Create the bucket in the console or with an S3 client. The bucket name needs to match `S3_BUCKET` (`ro-crates` by default). 

Upload an RO-Crate under the prefix: `crates/<id>.zip` for a zipped RO-Crate, or `crates/<id>/` for a directory. Note that for a zipped RO-Crate, `ro-crate-metadata.json` must be at the root of the archive.

The readiness endpoint checks the object store and broker connections:

```bash
curl http://localhost:5001/readyz
```

## Using your own object store

Any S3-compatible store can be used in place of RustFS, including AWS S3, MinIO and Ceph: set `S3_ENDPOINT`, the credentials and `S3_BUCKET` for your store. In this case, do not run the `objectstore` profile. 

If you already use a 1.x release against MinIO, the [upgrade guide](upgrading.md) maps the old settings to the new ones.

## Configuration reference

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_ENABLED` | `false` | Enable the stored-crate endpoints and storage checks |
| `S3_ENDPOINT` | — | Object store endpoint, e.g. `objectstore:9000` (required in storage mode) |
| `S3_ACCESS_KEY` | — | Object store access key (required in storage mode) |
| `S3_SECRET_KEY` | — | Object store secret key (required in storage mode) |
| `S3_BUCKET` | — | Bucket holding RO-Crates and results (required in storage mode) |
| `S3_USE_SSL` | `false` | Use HTTPS to the object store |
| `S3_REGION` | — | Region; needed for AWS S3 |
| `S3_CRATE_PREFIX` | `crates` | Key prefix RO-Crates are read from |
| `S3_RESULTS_PREFIX` | `validation-results` | Key prefix results are written to |
| `CELERY_BROKER_URL` | — | Redis broker URL (required in storage mode; preset in the Compose stack) |
| `CELERY_RESULT_BACKEND` | — | Celery result backend URL (required in storage mode; preset in the Compose stack) |
| `PROFILES_PATH` | — | Profiles directory that replaces the bundled profiles |
| `EXTRA_PROFILES_PATH` | — | Profiles directory added to the bundled profiles |
| `CACHE_PATH` | `/app/.rocrate-cache` | Validator HTTP cache location |
| `VALIDATION_OFFLINE` | `false` | Validate using only the cache, with no network access |
| `FLASK_ENV` | `development` | Set to `production` to disable debug behaviour |

## Custom profiles

The validator comes with several RO-Crate profiles, and for the Five Safes RO-Crate, the prebuilt `ghcr.io/esciencelab/ro-crate-validation-service-fivesafes-profile` image has the `five-safes-crate` profile already included; see [Five Safes validation](five-safes.md).

To add other profiles, mount a directory into both the `flask` and `celery_worker` containers, and set `EXTRA_PROFILES_PATH` to the mounted path. Note that both containers need the mount as metadata-only validation runs in the API process and stored-crate validation runs in the worker. There is a working example in `docker-compose-develop.yml`. 

!!! note
    `EXTRA_PROFILES_PATH` adds the directory to the bundled profiles, whereas `PROFILES_PATH` replaces them entirely.

## Offline validation

The validator fetches profile and context resources over HTTP and caches them. The published images pre-populate this cache at build time, so setting `VALIDATION_OFFLINE=true` runs validation entirely from the cache, with no network access at runtime. This is useful inside TREs and other restricted networks. 

Online validation (the default) also uses and refreshes the same cache. Offline validation requires `rocrate-validator` at 0.10.0 or later, which the published images include.
