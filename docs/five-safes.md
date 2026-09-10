# Five Safes RO-Crate validation

The [Five Safes RO-Crate 0.4 profile](https://trefx.uk/5s-crate/) describes an RO-Crate used to request and record workflow runs on sensitive data in Trusted Research Environments (TREs), supporting the Five Safes framework. The RO-Crate Validation Service validates against this profile when `profile_name` is set to `five-safes-crate`.

!!! warning
    Note that the Five Safes RO-Crate 0.4 profile is not bundled with the base validator, so the service needs the profile to be made available. There are two ways to do this, described below.

## Getting a service with the profile

The prebuilt `ghcr.io/esciencelab/ro-crate-validation-service-fivesafes-profile` image packages the `five-safes-crate` profile with the standard RO-Crate Validation Service. The image also carries a pre-warmed validation cache, so it supports offline validation (`VALIDATION_OFFLINE=true`) inside restricted networks. The profile version is fixed when the image is built, and is recorded in the image label `org.ro-crate-validation-service.five-safes-profile-version`.

Alternatively, you may run the standard service image with the profile directory mounted and `EXTRA_PROFILES_PATH` set, [as described in custom profiles](installation.md#custom-profiles). 

The `five-safes-crate` profile itself is defined in the [eScienceLab rocrate-validator fork](https://github.com/eScienceLab/rocrate-validator).

## Validating a Five Safes RO-Crate

A [complete example crate](https://github.com/eScienceLab/rocrate-validator/blob/five-safes-0.7.4-beta/tests/data/crates/valid/five-safes-crate-result/ro-crate-metadata.json) is available in the fork's test data.

!!! note
    The current `-fivesafes-profile` image pairs the profile with a base profile for RO-Crate 1.1, whilst the profile itself expects RO-Crate 1.2. The walkthrough below mounts the matched profile set instead.

For this walkthrough, run the service from a checkout of this repository, with the repository's profile set mounted in place of the bundled profiles:

```bash
docker run --rm -p 5001:5000 \
  -e FLASK_APP=wsgi.py \
  -e PROFILES_PATH=/app/profiles \
  -v "$PWD/tests/data/rocrate_validator_profiles:/app/profiles:ro" \
  ghcr.io/esciencelab/ro-crate-validation-service-fivesafes-profile:latest
```

Download the example `ro-crate-metadata.json`:

```bash
curl -sO https://raw.githubusercontent.com/eScienceLab/rocrate-validator/five-safes-0.7.4-beta/tests/data/crates/valid/five-safes-crate-result/ro-crate-metadata.json
```

and validate it with `profile_name` set to `five-safes-crate`:

```bash
jq -Rs '{crate_json: ., profile_name: "five-safes-crate"}' ro-crate-metadata.json | curl -X POST http://localhost:5001/v1/ro_crates/validate_metadata -H 'Content-Type: application/json' -d @-
```

The crate conforms, so the response (abridged) is:

```json
{
  "status": "valid",
  "profile": "five-safes-crate",
  "created_at": null,
  "detail": {
    "issues": [],
    "passed": true
  }
}
```

To see how conformance issues are reported, remove something the profile requires, such as the `CreateAction` recording the requested workflow run. Validate again:

```bash
jq '."@graph" |= map(select(."@type" != "CreateAction"))' ro-crate-metadata.json > broken.json
```

```bash
jq -Rs '{crate_json: ., profile_name: "five-safes-crate"}' broken.json | curl -X POST http://localhost:5001/v1/ro_crates/validate_metadata -H 'Content-Type: application/json' -d @-
```

The result will now show `invalid`, and each entry in `detail.issues` identifies the failed check, its severity, and the entity at fault (abridged):

```json
{
  "status": "invalid",
  "profile": "five-safes-crate",
  "detail": {
    "issues": [
      {
        "check": {
          "identifier": "five-safes-crate-0.4_25.1",
          "name": "mentions"
        },
        "severity": "REQUIRED",
        "message": "`RootDataEntity` MUST reference at least one `CreateAction` through `mentions`",
        "violatingEntity": "./"
      }
    ],
    "passed": false
  }
}
```

Complete RO-Crates work the same way through the storage-backed endpoints: upload the crate as `crates/<id>.zip` (or a directory under `crates/<id>/`), then queue validation with the profile:

```bash
curl -X POST http://localhost:5001/v1/ro_crates/my-5s-crate/validation -H 'Content-Type: application/json' -d '{"profile_name": "five-safes-crate"}'
```

The [API reference](api.md) covers the endpoints, results and webhooks in full.
