"""Runs rocrate_validator and adapts its output to a ValidationOutcome.

This is the boundary to the external validator. Both entry points always
return a :class:`ValidationOutcome` - a validator exception becomes an ``error``
outcome rather than a string, so callers never have to type-check the result.
"""

import logging
from typing import Optional

from rocrate_validator import services

from app.validation.results import ValidationOutcome

logger = logging.getLogger(__name__)


def validate_crate_path(
    rocrate_uri: str,
    profile_name: Optional[str] = None,
    profiles_path: Optional[str] = None,
    extra_profiles_path: Optional[str] = None,
    skip_checks: Optional[list] = None,
    cache_path: Optional[str] = None,
    offline: bool = False,
    created_at: Optional[str] = None,
) -> ValidationOutcome:
    """Validate a crate on disk (a directory or zip) at ``rocrate_uri``."""
    return _run(
        {"rocrate_uri": rocrate_uri},
        profile_name=profile_name,
        profiles_path=profiles_path,
        extra_profiles_path=extra_profiles_path,
        skip_checks=skip_checks,
        cache_path=cache_path,
        offline=offline,
        created_at=created_at,
    )


def validate_metadata(
    metadata: dict,
    profile_name: Optional[str] = None,
    profiles_path: Optional[str] = None,
    extra_profiles_path: Optional[str] = None,
    skip_checks: Optional[list] = None,
    cache_path: Optional[str] = None,
    offline: bool = False,
    created_at: Optional[str] = None,
) -> ValidationOutcome:
    """Validate an in-memory RO-Crate metadata graph."""
    return _run(
        {"metadata_only": True, "metadata_dict": metadata},
        profile_name=profile_name,
        profiles_path=profiles_path,
        extra_profiles_path=extra_profiles_path,
        skip_checks=skip_checks,
        cache_path=cache_path,
        offline=offline,
        created_at=created_at,
    )


def _run(
    base_settings: dict,
    profile_name: Optional[str],
    profiles_path: Optional[str],
    extra_profiles_path: Optional[str],
    skip_checks: Optional[list],
    cache_path: Optional[str],
    offline: bool,
    created_at: Optional[str],
) -> ValidationOutcome:
    options = dict(base_settings)
    if profile_name:
        options["profile_identifier"] = profile_name
    if profiles_path:
        options["profiles_path"] = profiles_path
    if extra_profiles_path:
        options["extra_profiles_path"] = extra_profiles_path
    if skip_checks:
        options["skip_checks"] = skip_checks
    if cache_path:
        options["cache_path"] = cache_path
    if offline:
        options["offline"] = offline

    try:
        settings = services.ValidationSettings(**options)
        result = services.validate(settings)
    except Exception as error:  # noqa: BLE001 - adapt any validator failure to an outcome
        logger.error("Validation failed: %s", error)
        return ValidationOutcome.from_error(str(error), profile=profile_name, created_at=created_at)

    return ValidationOutcome.from_validator_result(
        result, profile=profile_name, created_at=created_at
    )
