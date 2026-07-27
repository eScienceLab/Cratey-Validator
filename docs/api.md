# API Reference

The examples below use the Compose stack's local address, `http://localhost:5001`. Note that the service serves its own OpenAPI specification at `/docs`.

!!! note
    `POST /v1/ro_crates/validate_metadata` is always available, but the **storage-backed endpoints** are only available when the service runs with `STORAGE_ENABLED=true` (see [Installation & Setup](installation.md#enabling-object-storage)); without this set, requests return `404`. 

## Validate metadata

`POST /v1/ro_crates/validate_metadata`

This validates the contents of an `ro-crate-metadata.json` document and returns the result in the response.

| Field | Required | Description |
|-------|----------|-------------|
| `crate_json` | yes | The metadata document, as a JSON string |
| `profile_name` | no | Profile to validate against, e.g. `ro-crate-1.2`; defaults to `ro-crate-1.1` when omitted |

!!! warning
    Currently, the validation profile is not detected from the RO-Crate. In other words, a `conformsTo` declaration in the metadata does not influence which validation profile is used by the validator, and the validation always runs against `profile_name`, or `ro-crate-1.1` when it is omitted.

To validate a file:

```bash
jq -Rs '{crate_json: .}' ro-crate-metadata.json | curl -X POST http://localhost:5001/v1/ro_crates/validate_metadata -H 'Content-Type: application/json' -d @-
```

To choose a profile, add it to the `jq` object: `jq -Rs '{crate_json: ., profile_name: "ro-crate-1.2"}' ro-crate-metadata.json`.

| Code | Meaning |
|------|---------|
| `200` | Validated; the result has a `status` of `valid` or `invalid` |
| `422` | `crate_json` is either missing, empty or invalid, or the validation could not run (an `error` result) |

## Validate a stored RO-Crate

`POST /v1/ro_crates/{crate_id}/validation`

This queues validation of an RO-Crate held in the object store. The RO-Crate is resolved first, so a missing or ambiguous crate ID may be reported; the validation itself runs on a worker. 

!!! note
    See [Crate IDs](#crate-ids) for how `{crate_id}` maps to objects in the bucket.

| Field | Required | Description |
|-------|----------|-------------|
| `profile_name` | no | Profile to validate against; defaults to `ro-crate-1.1` when omitted |
| `webhook_url` | no | URL that receives the result when validation finishes |

```bash
curl -X POST http://localhost:5001/v1/ro_crates/my-dataset-2026/validation -H 'Content-Type: application/json' -d '{"profile_name": "ro-crate-1.2"}'
```

| Code | Meaning |
|------|---------|
| `202` | Queued; the body is `{"message": "Validation in progress"}` |
| `400` | Invalid Crate ID |
| `404` | No RO-Crate at the expected keys, or storage mode is not enabled |
| `409` | Both a zip and a directory exist for this Crate ID |
| `422` | Request body invalid |
| `503` | Object store unreachable |

## Fetch a validation result

`GET /v1/ro_crates/{crate_id}/validation`

This returns the stored result for an RO-Crate.

```bash
curl http://localhost:5001/v1/ro_crates/my-dataset-2026/validation
```

| Code | Meaning |
|------|---------|
| `200` | The stored result, including persisted `error` results |
| `400` | Invalid Crate ID |
| `404` | No result stored for this Crate ID yet |

## Validation results

Every validation produces a result object:

```json
{
  "status": "invalid",
  "profile": "ro-crate-1.2",
  "created_at": "2026-07-22T10:30:00+00:00",
  "detail": {}
}
```

An RO-Crate's `status` can be:

| `status` | Meaning |
|----------|---------|
| `valid` | The RO-Crate conforms to the profile |
| `invalid` | Validated, but with conformance issues listed in `detail` |
| `error` | The validation could not run; the reason is in an `error` field instead of `detail` |

!!! note
    `detail` contains the complete validation report. `created_at` is the UTC time of a stored-crate validation, and `null` for metadata-only validation, which does not set it. `profile` is the requested profile name, or `null` when the default (`ro-crate-1.1`) was used.

For stored RO-Crates the same object is saved to `{S3_RESULTS_PREFIX}/<id>.json` and returned by the GET endpoint.

## Webhooks

If `webhook_url` was given, the worker POSTs the result object to it as JSON once validation finishes. The result is saved to the store first and the webhook sent after, so a notification is never sent for a result that was not stored. 

Note that delivery is attempted three times, waiting `0.5s` then `1s` between attempts, with a `10s` timeout per attempt.

## Crate IDs

A Crate ID is the label in the URL path that identifies an RO-Crate in the object store: the service looks for `{S3_CRATE_PREFIX}/<id>.zip` (zip) or `{S3_CRATE_PREFIX}/<id>/` (directory). Crate IDs must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`: they start with a letter or digit, may contain letters, digits, `.`, `_` and `-`, and are at most 128 characters long. Anything else is rejected with `400`.

## Health

`GET /healthz` reports that the process is up, and always returns `200 {"status": "ok"}`. `GET /readyz` checks the object store and Celery broker, returning `200` when ready and `503` otherwise, with the individual checks in the body. When storage is off, both checks report `disabled`.
