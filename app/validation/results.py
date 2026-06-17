"""Defines an explicit result type for validation."""

import json
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ValidationStatus(str, Enum):
    """The outcome of a validation run."""

    VALID = "valid"
    INVALID = "invalid"
    ERROR = "error"


@dataclass(frozen=True)
class ValidationOutcome:
    """The result of validating a crate or its metadata.

    ``detail`` holds the validator's report for ``valid``/``invalid`` outcomes;
    ``error`` holds the message for an ``error`` outcome. The two are mutually
    exclusive.
    """

    status: ValidationStatus
    profile: Optional[str] = None
    detail: Optional[dict] = None
    error: Optional[str] = None
    created_at: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.status is ValidationStatus.VALID

    def to_dict(self) -> dict:
        data = {
            "status": self.status.value,
            "profile": self.profile,
            "created_at": self.created_at,
        }
        if self.detail is not None:
            data["detail"] = self.detail
        if self.error is not None:
            data["error"] = self.error
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_validator_result(
        cls, result, profile: Optional[str] = None, created_at: Optional[str] = None
    ) -> "ValidationOutcome":
        """Build an outcome from a rocrate_validator ``ValidationResult``."""
        status = ValidationStatus.INVALID if result.has_issues() else ValidationStatus.VALID
        return cls(
            status=status,
            profile=profile,
            detail=json.loads(result.to_json()),
            created_at=created_at,
        )

    @classmethod
    def from_error(
        cls,
        message: str,
        profile: Optional[str] = None,
        created_at: Optional[str] = None,
    ) -> "ValidationOutcome":
        """Build an error outcome from a failure message."""
        return cls(
            status=ValidationStatus.ERROR,
            profile=profile,
            error=message,
            created_at=created_at,
        )
