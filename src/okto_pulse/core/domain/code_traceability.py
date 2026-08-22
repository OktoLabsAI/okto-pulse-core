"""Pure public contracts for agent-attested code traceability.

Pulse never opens a repository, invokes Git, resolves a symbol, or searches a
workspace.  The immutable values in this module describe observations made by
an authenticated external agent and the server-owned ledger derived from those
submissions.  Validation is intentionally limited to shape, scope, bounded
values, canonical digests, and internally coherent claims.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Generic, TypeVar
import unicodedata


CODE_TRACEABILITY_CONTRACT_VERSION = "pulse-code-traceability/v1"
CODE_INVESTIGATION_CANONICALIZATION_PROFILE = "pulse-code-receipt-c14n-v1"
CODE_INVESTIGATION_LIMITS_PROFILE = "pulse-code-receipt-limits-v1"
CODE_EVIDENCE_EXCERPT_OMITTED_NOT_SUBMITTED = "not_submitted"
CODE_EVIDENCE_LEGACY_CLASSIFICATION_BATCH_LIMIT = 100
CODE_EVIDENCE_CLASSIFICATION_ACTOR_ID_MAX_BYTES = 255

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


class CodeTraceabilityEnforcement(str, Enum):
    """Closed board-level enforcement for agent-mediated traceability.

    Code Traceability is always evaluated.  ``ADVISORY`` records actionable
    findings without rejecting a lifecycle transition, while ``BLOCKING``
    turns the same findings into gate blockers.  Historical ``off`` values
    are a persistence compatibility concern and are deliberately not part of
    this authored domain contract.
    """

    ADVISORY = "advisory"
    BLOCKING = "blocking"


@dataclass(frozen=True, slots=True)
class CodeTraceabilityLimits:
    """Closed limits frozen by ``pulse-code-receipt-limits-v1``."""

    challenge_ttl_seconds: int = 600
    observed_at_clock_skew_seconds: int = 300
    open_requests_per_actor_board: int = 8
    receipt_envelope_bytes: int = 256 * 1024
    evidence_envelope_bytes: int = 128 * 1024
    resolution_envelope_bytes: int = 256 * 1024
    execution_envelope_bytes: int = 64 * 1024
    omission_entries: int = 100
    resolution_candidates: int = 20
    mcp_batch_items: int = 32
    evidence_excerpt_bytes: int = 8 * 1024
    evidence_detail_excerpt_bytes: int = 2 * 1024
    batch_excerpt_bytes: int = 64 * 1024
    relative_path_bytes: int = 1024
    symbol_text_bytes: int = 2048
    context_heads: int = 64
    context_receipts: int = 200
    context_receipt_revocations: int = 200
    context_evidence: int = 200
    context_evidence_links: int = 500
    context_evidence_dispositions: int = 500
    context_targets: int = 200
    context_target_spec_links: int = 500
    context_target_evidence_links: int = 500
    context_resolutions: int = 500
    context_executions: int = 500
    context_overlaps: int = 500
    context_waivers: int = 200

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is not int or value < 1:
                raise CodeTraceabilityContractError(
                    "code_traceability_limits_invalid",
                    details={"field": field.name},
                )


DEFAULT_CODE_TRACEABILITY_LIMITS = CodeTraceabilityLimits()

CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS: Mapping[str, int] = MappingProxyType(
    {
        "heads": DEFAULT_CODE_TRACEABILITY_LIMITS.context_heads,
        "receipts": DEFAULT_CODE_TRACEABILITY_LIMITS.context_receipts,
        "receipt_revocations": (
            DEFAULT_CODE_TRACEABILITY_LIMITS.context_receipt_revocations
        ),
        "evidence": DEFAULT_CODE_TRACEABILITY_LIMITS.context_evidence,
        "evidence_links": DEFAULT_CODE_TRACEABILITY_LIMITS.context_evidence_links,
        "evidence_dispositions": (
            DEFAULT_CODE_TRACEABILITY_LIMITS.context_evidence_dispositions
        ),
        "targets": DEFAULT_CODE_TRACEABILITY_LIMITS.context_targets,
        "target_spec_links": (
            DEFAULT_CODE_TRACEABILITY_LIMITS.context_target_spec_links
        ),
        "target_evidence_links": (
            DEFAULT_CODE_TRACEABILITY_LIMITS.context_target_evidence_links
        ),
        "resolutions": DEFAULT_CODE_TRACEABILITY_LIMITS.context_resolutions,
        "executions": DEFAULT_CODE_TRACEABILITY_LIMITS.context_executions,
        "overlaps": DEFAULT_CODE_TRACEABILITY_LIMITS.context_overlaps,
        "waivers": DEFAULT_CODE_TRACEABILITY_LIMITS.context_waivers,
    }
)


@dataclass(frozen=True, slots=True)
class CodeTraceabilityRemediation:
    action: str
    tool: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "action",
            _required_text(self.action, "code_traceability_remediation_invalid"),
        )
        object.__setattr__(
            self,
            "tool",
            _optional_text(self.tool, "code_traceability_remediation_invalid"),
        )

    def as_dict(self) -> dict[str, str | None]:
        return {"action": self.action, "tool": self.tool}


class CodeTraceabilityContractError(ValueError):
    """Stable, transport-neutral failure envelope for closed domain checks."""

    default_code = "code_traceability_contract_invalid"

    def __init__(
        self,
        code: str | None = None,
        message: str | None = None,
        *,
        details: Mapping[str, object] | None = None,
        remediation: Sequence[CodeTraceabilityRemediation] = (),
    ) -> None:
        resolved_code = code or self.default_code
        if not isinstance(resolved_code, str) or not resolved_code.strip():
            raise TypeError("code_traceability_error_code_invalid")
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("code_traceability_error_details_invalid")
        if isinstance(remediation, str | bytes) or not isinstance(
            remediation, Sequence
        ):
            raise TypeError("code_traceability_error_remediation_invalid")
        resolved_remediation = tuple(remediation)
        if any(
            not isinstance(item, CodeTraceabilityRemediation)
            for item in resolved_remediation
        ):
            raise TypeError("code_traceability_error_remediation_invalid")
        self.code = resolved_code.strip()
        self.message = message or self.code
        self.details = MappingProxyType(dict(details or {}))
        self.remediation = resolved_remediation
        super().__init__(self.message)

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
            "remediation": [item.as_dict() for item in self.remediation],
        }

    def to_error_dict(self) -> dict[str, object]:
        return self.as_dict()


def _typed_error(name: str, code: str) -> type[CodeTraceabilityContractError]:
    def __init__(
        self: CodeTraceabilityContractError,
        message: str | None = None,
        *,
        details: Mapping[str, object] | None = None,
        remediation: Sequence[CodeTraceabilityRemediation] = (),
    ) -> None:
        CodeTraceabilityContractError.__init__(
            self,
            code=code,
            message=message,
            details=details,
            remediation=remediation,
        )

    return type(
        name,
        (CodeTraceabilityContractError,),
        {
            "default_code": code,
            "__init__": __init__,
            "__module__": __name__,
        },
    )


# Stable errors required by the public contract.  Explicit globals keep them
# importable and allow adapters/transports to catch a precise failure class.
CodeInvestigationRequestNotFound = _typed_error(
    "CodeInvestigationRequestNotFound",
    "code_investigation_request_not_found",
)
CodeInvestigationRequestNotOpen = _typed_error(
    "CodeInvestigationRequestNotOpen",
    "code_investigation_request_not_open",
)
CodeInvestigationChallengeInvalid = _typed_error(
    "CodeInvestigationChallengeInvalid",
    "code_investigation_challenge_invalid",
)
CodeInvestigationReceiptExpired = _typed_error(
    "CodeInvestigationReceiptExpired",
    "code_investigation_receipt_expired",
)
CodeInvestigationReceiptRevoked = _typed_error(
    "CodeInvestigationReceiptRevoked",
    "code_investigation_receipt_revoked",
)
CodeInvestigationReceiptConflicted = _typed_error(
    "CodeInvestigationReceiptConflicted",
    "code_investigation_receipt_conflicted",
)
CodeInvestigationChallengeConsumed = _typed_error(
    "CodeInvestigationChallengeConsumed",
    "code_investigation_challenge_consumed",
)
CodeInvestigationActorKindRequired = _typed_error(
    "CodeInvestigationActorKindRequired",
    "code_investigation_actor_kind_required",
)
CodeInvestigationCapabilityMissing = _typed_error(
    "CodeInvestigationCapabilityMissing",
    "code_investigation_capability_missing",
)
CodeInvestigationAttestorMismatch = _typed_error(
    "CodeInvestigationAttestorMismatch",
    "code_investigation_attestor_mismatch",
)
CodeInvestigationSourceScopeMismatch = _typed_error(
    "CodeInvestigationSourceScopeMismatch",
    "code_investigation_source_scope_mismatch",
)
CodeInvestigationSubjectVersionConflict = _typed_error(
    "CodeInvestigationSubjectVersionConflict",
    "code_investigation_subject_version_conflict",
)
CodeInvestigationSelectorScopeMismatch = _typed_error(
    "CodeInvestigationSelectorScopeMismatch",
    "code_investigation_selector_scope_mismatch",
)
CodeInvestigationUnavailable = _typed_error(
    "CodeInvestigationUnavailable",
    "code_investigation_unavailable",
)
CodeInvestigationHeadConflict = _typed_error(
    "CodeInvestigationHeadConflict",
    "code_investigation_head_conflict",
)
CodeInvestigationIdempotencyConflict = _typed_error(
    "CodeInvestigationIdempotencyConflict",
    "code_investigation_idempotency_conflict",
)
CodeInvestigationProfileMismatch = _typed_error(
    "CodeInvestigationProfileMismatch",
    "code_investigation_profile_mismatch",
)
CodeInvestigationTrustInsufficient = _typed_error(
    "CodeInvestigationTrustInsufficient",
    "code_investigation_trust_insufficient",
)
CodeInvestigationCurrentnessUnknown = _typed_error(
    "CodeInvestigationCurrentnessUnknown",
    "code_investigation_currentness_unknown",
)
CodeInvestigationPayloadDigestMismatch = _typed_error(
    "CodeInvestigationPayloadDigestMismatch",
    "code_investigation_payload_digest_mismatch",
)
CodeInvestigationSubmissionLimitExceeded = _typed_error(
    "CodeInvestigationSubmissionLimitExceeded",
    "code_investigation_submission_limit_exceeded",
)
CodeInvestigationNoRelevantExistingImplementationInvalid = _typed_error(
    "CodeInvestigationNoRelevantExistingImplementationInvalid",
    "code_investigation_no_relevant_existing_implementation_invalid",
)
CodePathInvalid = _typed_error("CodePathInvalid", "code_path_invalid")
CodePathDenied = _typed_error("CodePathDenied", "code_path_denied")
CodeEvidenceSubmissionFailed = _typed_error(
    "CodeEvidenceSubmissionFailed",
    "code_evidence_submission_failed",
)
CodeEvidenceReceiptMismatch = _typed_error(
    "CodeEvidenceReceiptMismatch",
    "code_evidence_receipt_mismatch",
)
CodeEvidenceImmutable = _typed_error(
    "CodeEvidenceImmutable",
    "code_evidence_immutable",
)
CodeEvidenceAttestationRequired = _typed_error(
    "CodeEvidenceAttestationRequired",
    "code_evidence_attestation_required",
)
CodeEvidenceDispositionRequired = _typed_error(
    "CodeEvidenceDispositionRequired",
    "code_evidence_disposition_required",
)
CodeEvidenceLinkInvalid = _typed_error(
    "CodeEvidenceLinkInvalid",
    "code_evidence_link_invalid",
)
CodeDeliveryContextRequired = _typed_error(
    "CodeDeliveryContextRequired",
    "code_delivery_context_required",
)
CodeDeliveryContextOverrideReasonRequired = _typed_error(
    "CodeDeliveryContextOverrideReasonRequired",
    "code_delivery_context_override_reason_required",
)
CodeEvidenceSourceRoleRequired = _typed_error(
    "CodeEvidenceSourceRoleRequired",
    "code_evidence_source_role_required",
)
CodeEvidenceLegacyRoleWriteForbidden = _typed_error(
    "CodeEvidenceLegacyRoleWriteForbidden",
    "code_evidence_legacy_role_write_forbidden",
)
CodeEvidenceInterpretationLimitRequired = _typed_error(
    "CodeEvidenceInterpretationLimitRequired",
    "code_evidence_interpretation_limit_required",
)
CodeEvidenceBaselineProvenanceInvalid = _typed_error(
    "CodeEvidenceBaselineProvenanceInvalid",
    "code_evidence_baseline_provenance_invalid",
)
CodeEvidencePostBaselineSourceForbidden = _typed_error(
    "CodeEvidencePostBaselineSourceForbidden",
    "code_evidence_post_baseline_source_forbidden",
)
CodeEvidenceMaterialityLinkRequired = _typed_error(
    "CodeEvidenceMaterialityLinkRequired",
    "code_evidence_materiality_link_required",
)
CodeEvidenceLegacyClassificationHumanRequired = _typed_error(
    "CodeEvidenceLegacyClassificationHumanRequired",
    "code_evidence_legacy_classification_human_required",
)
CodeEvidenceLegacyClassificationItemsRequired = _typed_error(
    "CodeEvidenceLegacyClassificationItemsRequired",
    "code_evidence_legacy_classification_items_required",
)
CodeEvidenceLegacyClassificationItemsDuplicate = _typed_error(
    "CodeEvidenceLegacyClassificationItemsDuplicate",
    "code_evidence_legacy_classification_items_duplicate",
)
CodeEvidenceLegacyClassificationLimitExceeded = _typed_error(
    "CodeEvidenceLegacyClassificationLimitExceeded",
    "code_evidence_legacy_classification_limit_exceeded",
)
CodeEvidenceLegacyClassificationEvidenceNotFound = _typed_error(
    "CodeEvidenceLegacyClassificationEvidenceNotFound",
    "code_evidence_legacy_classification_evidence_not_found",
)
CodeEvidenceLegacyClassificationLegacyRequired = _typed_error(
    "CodeEvidenceLegacyClassificationLegacyRequired",
    "code_evidence_legacy_classification_legacy_required",
)
CodeEvidenceLegacyClassificationPayloadConflict = _typed_error(
    "CodeEvidenceLegacyClassificationPayloadConflict",
    "code_evidence_legacy_classification_payload_conflict",
)
CodeEvidenceLegacyClassificationRevisionConflict = _typed_error(
    "CodeEvidenceLegacyClassificationRevisionConflict",
    "code_evidence_legacy_classification_revision_conflict",
)
CodeEvidenceLegacyClassificationIdempotencyConflict = _typed_error(
    "CodeEvidenceLegacyClassificationIdempotencyConflict",
    "code_evidence_legacy_classification_idempotency_conflict",
)
CodeEvidenceLegacyClassificationPersistenceConflict = _typed_error(
    "CodeEvidenceLegacyClassificationPersistenceConflict",
    "code_evidence_legacy_classification_persistence_conflict",
)
ImplementationTargetInvalid = _typed_error(
    "ImplementationTargetInvalid",
    "implementation_target_invalid",
)
ImplementationTargetResolutionRequired = _typed_error(
    "ImplementationTargetResolutionRequired",
    "implementation_target_resolution_required",
)
ImplementationTargetResolutionOutdated = _typed_error(
    "ImplementationTargetResolutionOutdated",
    "implementation_target_resolution_outdated",
)
ImplementationTargetStale = _typed_error(
    "ImplementationTargetStale",
    "implementation_target_stale",
)
ImplementationTargetAmbiguous = _typed_error(
    "ImplementationTargetAmbiguous",
    "implementation_target_ambiguous",
)
ImplementationTargetMissing = _typed_error(
    "ImplementationTargetMissing",
    "implementation_target_missing",
)
ImplementationOverlapBlocking = _typed_error(
    "ImplementationOverlapBlocking",
    "implementation_overlap_blocking",
)
ImplementationOverlapAcknowledgementStale = _typed_error(
    "ImplementationOverlapAcknowledgementStale",
    "implementation_overlap_ack_stale",
)
TargetExecutionDispositionRequired = _typed_error(
    "TargetExecutionDispositionRequired",
    "target_execution_disposition_required",
)
CodeTraceabilityWaiverRequired = _typed_error(
    "CodeTraceabilityWaiverRequired",
    "code_traceability_waiver_required",
)
CodeTraceabilityLocked = _typed_error(
    "CodeTraceabilityLocked",
    "code_traceability_locked",
)


def _required_text(value: object, code: str, *, max_bytes: int | None = None) -> str:
    if not isinstance(value, str):
        raise CodeTraceabilityContractError(code)
    normalized = unicodedata.normalize("NFC", value).strip()
    if not normalized or _CONTROL_RE.search(normalized):
        raise CodeTraceabilityContractError(code)
    if max_bytes is not None and len(normalized.encode("utf-8")) > max_bytes:
        raise CodeInvestigationSubmissionLimitExceeded(
            details={"field": code, "max_bytes": max_bytes}
        )
    return normalized


def _optional_text(
    value: object,
    code: str,
    *,
    max_bytes: int | None = None,
) -> str | None:
    if value is None:
        return None
    return _required_text(value, code, max_bytes=max_bytes)


def _strict_bool(value: object, code: str) -> bool:
    if not isinstance(value, bool):
        raise CodeTraceabilityContractError(code)
    return value


def _positive_int(value: object, code: str) -> int:
    if type(value) is not int or value < 1:
        raise CodeTraceabilityContractError(code)
    return value


def _non_negative_int(value: object, code: str) -> int:
    if type(value) is not int or value < 0:
        raise CodeTraceabilityContractError(code)
    return value


def _confidence(value: object, code: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise CodeTraceabilityContractError(code)
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise CodeTraceabilityContractError(code)
    return 0.0 if result == 0.0 else result


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise CodeTraceabilityContractError(code)
    return value.astimezone(timezone.utc)


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise CodeTraceabilityContractError(code)
    return normalized


def _optional_sha256(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, code)


EnumT = TypeVar("EnumT", bound=Enum)


def _enum(value: object, enum_type: type[EnumT], code: str) -> EnumT:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        try:
            return enum_type(unicodedata.normalize("NFC", value).strip())
        except ValueError:
            pass
    raise CodeTraceabilityContractError(code)


def _typed_tuple(
    value: object,
    expected_type: type[Any],
    code: str,
    *,
    max_items: int | None = None,
) -> tuple[Any, ...]:
    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        raise CodeTraceabilityContractError(code)
    result = tuple(value)
    if any(not isinstance(item, expected_type) for item in result):
        raise CodeTraceabilityContractError(code)
    if max_items is not None and len(result) > max_items:
        raise CodeInvestigationSubmissionLimitExceeded(
            details={"field": code, "max_items": max_items}
        )
    return result


def _enum_tuple(
    value: object,
    enum_type: type[EnumT],
    code: str,
) -> tuple[EnumT, ...]:
    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        raise CodeTraceabilityContractError(code)
    resolved = tuple(_enum(item, enum_type, code) for item in value)
    if len(set(resolved)) != len(resolved):
        raise CodeTraceabilityContractError(code)
    return tuple(sorted(resolved, key=lambda item: str(item.value)))


def _line_pair(
    start: object,
    end: object,
    code: str,
) -> tuple[int | None, int | None]:
    if start is None and end is None:
        return None, None
    if start is None or end is None:
        raise CodeTraceabilityContractError(code)
    resolved_start = _positive_int(start, code)
    resolved_end = _positive_int(end, code)
    if resolved_end < resolved_start:
        raise CodeTraceabilityContractError(code)
    return resolved_start, resolved_end


def normalize_code_relative_path(value: object) -> str:
    """Validate a claimed relative path without touching any filesystem."""

    path = _required_text(
        value,
        "code_path_invalid",
        max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.relative_path_bytes,
    ).replace("\\", "/")
    if path.startswith("/") or path.startswith("//") or _WINDOWS_DRIVE_RE.match(path):
        raise CodePathInvalid(details={"reason": "absolute_path"})
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise CodePathInvalid(details={"reason": "unsafe_segment"})
    lowered = [part.casefold() for part in parts]
    if ".git" in lowered:
        raise CodePathDenied(details={"reason": "repository_metadata"})
    basename = lowered[-1]
    sensitive = (
        basename == ".env"
        or basename.startswith(".env.")
        or basename.endswith(".pem")
        or basename.endswith(".key")
        or basename in {"id_rsa", "id_ed25519"}
        or basename.startswith("credentials")
        or basename.startswith("secrets")
    )
    if sensitive:
        raise CodePathDenied(details={"reason": "sensitive_path"})
    return "/".join(parts)


def _canonicalize(value: object) -> object:
    if isinstance(value, Enum):
        return _canonicalize(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonicalize(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, datetime):
        normalized = _aware_utc(value, "code_traceability_datetime_invalid")
        rendered = normalized.isoformat(timespec="microseconds")
        return rendered.replace("+00:00", "Z")
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CodeTraceabilityContractError(
                    "code_traceability_canonical_key_invalid"
                )
            normalized_key = unicodedata.normalize("NFC", key)
            if normalized_key in result:
                raise CodeTraceabilityContractError(
                    "code_traceability_canonical_duplicate_key"
                )
            result[normalized_key] = _canonicalize(item)
        return result
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_canonicalize(item) for item in value]
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CodeTraceabilityContractError(
                "code_traceability_canonical_number_invalid"
            )
        if value == 0.0:
            return 0
        if value.is_integer():
            return int(value)
        return value
    raise CodeTraceabilityContractError(
        "code_traceability_canonical_type_invalid",
        details={"type": type(value).__name__},
    )


def canonical_code_traceability_json_bytes(value: object) -> bytes:
    """Encode a value using the v1 deterministic NFC JSON profile."""

    try:
        return json.dumps(
            _canonicalize(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        if isinstance(exc, CodeTraceabilityContractError):
            raise
        raise CodeTraceabilityContractError(
            "code_traceability_canonicalization_invalid"
        ) from exc


def canonical_code_traceability_sha256(value: object) -> str:
    return hashlib.sha256(canonical_code_traceability_json_bytes(value)).hexdigest()


def _enforce_envelope_size(
    value: object,
    *,
    max_bytes: int,
    envelope: str,
) -> None:
    actual_bytes = len(canonical_code_traceability_json_bytes(value))
    if actual_bytes > max_bytes:
        raise CodeInvestigationSubmissionLimitExceeded(
            details={
                "envelope": envelope,
                "actual_bytes": actual_bytes,
                "max_bytes": max_bytes,
            }
        )


def parse_code_traceability_json(raw: str | bytes) -> object:
    """Parse canonical input while rejecting duplicate keys and non-finite numbers."""

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            normalized = unicodedata.normalize("NFC", key)
            if normalized in result:
                raise CodeTraceabilityContractError(
                    "code_traceability_canonical_duplicate_key"
                )
            result[normalized] = value
        return result

    def reject_constant(value: str) -> object:
        raise CodeTraceabilityContractError(
            "code_traceability_canonical_number_invalid",
            details={"value": value},
        )

    try:
        return json.loads(
            raw,
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
        )
    except CodeTraceabilityContractError:
        raise
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CodeTraceabilityContractError("code_traceability_json_invalid") from exc


class CodeTraceabilitySubjectType(str, Enum):
    REFINEMENT = "refinement"
    SPEC = "spec"
    CARD = "card"


class DeliveryContext(str, Enum):
    """Implementation context inherited by a Spec from its Refinement."""

    BROWNFIELD = "brownfield"
    GREENFIELD = "greenfield"
    HYBRID = "hybrid"


class CodeInvestigationRequestStatus(str, Enum):
    OPEN = "open"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    REVOKED = "revoked"


class CodeInvestigationOutcome(str, Enum):
    ACCESSIBLE = "accessible"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


class ContextualInvestigationOutcomeV2(str, Enum):
    """Meaning of an investigation after applying the delivery context."""

    EVIDENCE_APPLICABLE = "evidence_applicable"
    NO_RELEVANT_EXISTING_IMPLEMENTATION = "no_relevant_existing_implementation"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


_CONTEXTUAL_TO_LEGACY_INVESTIGATION_OUTCOME = MappingProxyType(
    {
        ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE: (
            CodeInvestigationOutcome.ACCESSIBLE
        ),
        ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION: (
            CodeInvestigationOutcome.ACCESSIBLE
        ),
        ContextualInvestigationOutcomeV2.PARTIAL: CodeInvestigationOutcome.PARTIAL,
        ContextualInvestigationOutcomeV2.UNAVAILABLE: (
            CodeInvestigationOutcome.UNAVAILABLE
        ),
    }
)


def legacy_code_investigation_outcome(
    outcome: ContextualInvestigationOutcomeV2,
) -> CodeInvestigationOutcome:
    """Project one authored V2 outcome into the readable legacy field."""

    resolved = _enum(
        outcome,
        ContextualInvestigationOutcomeV2,
        "code_investigation_contextual_outcome_invalid",
    )
    return _CONTEXTUAL_TO_LEGACY_INVESTIGATION_OUTCOME[resolved]


class CodeInvestigationTrustLevel(str, Enum):
    SINGLE_ATTESTATION = "single_attestation"
    CORROBORATED = "corroborated"
    CONFLICTED = "conflicted"


class CodeInvestigationAcceptanceStatus(str, Enum):
    ACCEPTED = "accepted"


class CodeInvestigationHeadState(str, Enum):
    CURRENT = "current"
    CONFLICTED = "conflicted"


class CodeInvestigationReceiptCurrentness(str, Enum):
    CURRENT = "current"
    OUTDATED = "outdated"
    CONFLICTED = "conflicted"
    EXPIRED = "expired"
    REVOKED = "revoked"
    UNKNOWN = "unknown"


class CodeInvestigationCapability(str, Enum):
    SOURCE_IDENTITY = "source_identity"
    REVISION_IDENTITY = "revision_identity"
    WORKSPACE_FINGERPRINT = "workspace_fingerprint"
    FILE_READ = "file_read"
    SYMBOL_RESOLUTION = "symbol_resolution"
    RENAME_OBSERVATION = "rename_observation"
    SAFE_EXCERPT = "safe_excerpt"
    PATH_CONTAINMENT = "path_containment"
    SYMLINK_CONTAINMENT = "symlink_containment"
    SECRET_SCAN = "secret_scan"
    BINARY_DETECTION = "binary_detection"


class CodeInvestigationOmissionReason(str, Enum):
    SIZE_CAP = "size_cap"
    SECRET_REDACTION = "secret_redaction"
    BINARY_CONTENT = "binary_content"
    PERMISSION_DENIED = "permission_denied"
    PATH_POLICY = "path_policy"
    UNSUPPORTED_LANGUAGE = "unsupported_language"
    TIMEOUT = "timeout"
    SUBMODULE_SKIPPED = "submodule_skipped"
    OTHER_BOUNDED = "other_bounded"


class WorkspaceReproducibilityClaim(str, Enum):
    COMMITTED = "committed"
    WORKTREE_SNAPSHOT = "worktree_snapshot"
    METADATA_ONLY = "metadata_only"


class CodeEvidenceType(str, Enum):
    BEHAVIOR = "behavior"
    STRUCTURE = "structure"
    CONTRACT = "contract"
    TEST = "test"
    CONFIGURATION = "configuration"
    DATA_MODEL = "data_model"
    MIGRATION = "migration"
    DEPENDENCY = "dependency"
    RUNTIME_OBSERVATION = "runtime_observation"


class CodeEvidenceSourceRole(str, Enum):
    """How an AS-IS source may be interpreted by a clean-context consumer.

    ``UNCATEGORIZED_LEGACY`` exists only so old Evidence can be projected
    truthfully. New contextual Evidence must use one of the authored roles.
    """

    CURRENT_IMPLEMENTATION = "current_implementation"
    EXISTING_SCAFFOLD = "existing_scaffold"
    EXISTING_CONSTRAINT = "existing_constraint"
    REFERENCE_PATTERN = "reference_pattern"
    UNCATEGORIZED_LEGACY = "uncategorized_legacy"


class CodeEvidenceContextOrigin(str, Enum):
    """Truthful origin of the contextual meaning shown to a consumer."""

    AUTHORED = "authored"
    HUMAN_LEGACY_CLASSIFICATION = "human_legacy_classification"
    UNCLASSIFIED_LEGACY = "unclassified_legacy"


class CodeEvidenceBaselinePresence(str, Enum):
    """Where the evidenced source existed at the frozen investigation baseline."""

    COMMITTED_SNAPSHOT = "committed_snapshot"
    PREEXISTING_WORKTREE = "preexisting_worktree"


class CodeEvidenceSelectorKind(str, Enum):
    SYMBOL = "symbol"
    FILE = "file"
    SPAN = "span"
    CONFIGURATION_KEY = "configuration_key"
    SCHEMA_OBJECT = "schema_object"
    ENDPOINT = "endpoint"
    TEST_CASE = "test_case"


class CodeEvidenceAttestationState(str, Enum):
    AGENT_ATTESTED = "agent_attested"
    AGENT_ATTESTED_WORKTREE = "agent_attested_worktree"


class CodeEvidenceAttestationBasis(str, Enum):
    AUTHENTICATED_AGENT_RECEIPT = "authenticated_agent_receipt"


class CodeTraceabilityLifecycleStatus(str, Enum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    REVOKED = "revoked"


class SpecEntityType(str, Enum):
    SPEC = "spec"
    FUNCTIONAL_REQUIREMENT = "functional_requirement"
    TECHNICAL_REQUIREMENT = "technical_requirement"
    ACCEPTANCE_CRITERION = "acceptance_criterion"
    BUSINESS_RULE = "business_rule"
    API_CONTRACT = "api_contract"
    INTEGRATION_REQUIREMENT = "integration_requirement"
    OBSERVABILITY_REQUIREMENT = "observability_requirement"
    DECISION = "decision"
    TEST_SCENARIO = "test_scenario"


class CodeEvidenceSpecRelationType(str, Enum):
    SUPPORTS = "supports"
    CONSTRAINS = "constrains"
    MOTIVATES = "motivates"
    IMPLEMENTS = "implements"
    TESTS = "tests"
    CONTRADICTS = "contradicts"


class CodeEvidenceDispositionKind(str, Enum):
    NOT_RELEVANT = "not_relevant"
    SUPERSEDED = "superseded"
    DEFERRED = "deferred"


class ImplementationTargetSelectorKind(str, Enum):
    SYMBOL = "symbol"
    FILE = "file"
    GLOB = "glob"
    SEMANTIC = "semantic"
    NEW_FILE = "new_file"


class ImplementationTargetRole(str, Enum):
    READ = "read"
    MODIFY = "modify"
    EXTEND = "extend"
    CREATE = "create"
    DELETE = "delete"
    TEST = "test"
    VALIDATE = "validate"


MUTATING_IMPLEMENTATION_TARGET_ROLES = frozenset(
    {
        ImplementationTargetRole.MODIFY,
        ImplementationTargetRole.EXTEND,
        ImplementationTargetRole.CREATE,
        ImplementationTargetRole.DELETE,
    }
)


class ImplementationTargetEvidenceRelationType(str, Enum):
    DERIVED_FROM = "derived_from"
    VALIDATES = "validates"
    REPLACES = "replaces"


class ImplementationTargetResolutionState(str, Enum):
    RESOLVED = "resolved"
    MOVED = "moved"
    STALE = "stale"
    AMBIGUOUS = "ambiguous"
    MISSING = "missing"
    UNAVAILABLE = "unavailable"


class ImplementationTargetExecutionDisposition(str, Enum):
    TOUCHED = "touched"
    NOT_TOUCHED = "not_touched"
    REPLACED = "replaced"
    CREATED = "created"
    DELETED = "deleted"
    SUPERSEDED = "superseded"


class TargetOverlapDisposition(str, Enum):
    ORDERED_BY_DEPENDENCY = "ordered_by_dependency"
    ACCEPTED_PARALLEL = "accepted_parallel"
    MERGED_TARGETS = "merged_targets"
    FALSE_POSITIVE = "false_positive"


class TargetOverlapSeverity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    INFORMATIONAL = "informational"
    NONE = "none"


class CodeTraceabilityWaiverEntityType(str, Enum):
    REFINEMENT = "refinement"
    SPEC = "spec"
    CARD = "card"
    SPEC_ENTITY = "spec_entity"


class CodeTraceabilityWaiverScope(str, Enum):
    CODE_EVIDENCE = "code_evidence"
    EVIDENCE_LINKAGE = "evidence_linkage"
    IMPLEMENTATION_TARGET = "implementation_target"
    TARGET_RESOLUTION = "target_resolution"
    TARGET_OVERLAP = "target_overlap"


class CodeTraceabilityWaiverReason(str, Enum):
    NO_CODE_CHANGE = "no_code_change"
    DOCUMENTATION_ONLY = "documentation_only"
    MANUAL_PROCESS = "manual_process"
    EXTERNAL_SOURCE_UNAVAILABLE = "external_source_unavailable"
    CONCEPTUAL_BOARD = "conceptual_board"
    RUNTIME_ONLY = "runtime_only"
    OTHER = "other"


class CodeTraceabilityProjectionProfile(str, Enum):
    SUMMARY = "summary"
    DETAIL = "detail"
    FULL = "full"


class CodeTraceabilityContextScope(str, Enum):
    DEFAULT = "default"
    GATE = "gate"


def authored_code_evidence_source_role(
    value: object,
) -> CodeEvidenceSourceRole:
    """Resolve a V2 write role while keeping the legacy value projection-only."""

    try:
        role = _enum(
            value,
            CodeEvidenceSourceRole,
            "code_evidence_source_role_required",
        )
    except CodeTraceabilityContractError as exc:
        raise CodeEvidenceSourceRoleRequired() from exc
    if role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY:
        raise CodeEvidenceLegacyRoleWriteForbidden()
    return role


@dataclass(frozen=True, slots=True)
class RefinementDeliveryContextProvenance:
    """Versioned provenance for context authored on one Refinement version."""

    value: DeliveryContext
    source_refinement_id: str
    source_refinement_version: int

    def __post_init__(self) -> None:
        try:
            value = _enum(
                self.value,
                DeliveryContext,
                "code_delivery_context_required",
            )
        except CodeTraceabilityContractError as exc:
            raise CodeDeliveryContextRequired() from exc
        object.__setattr__(self, "value", value)
        object.__setattr__(
            self,
            "source_refinement_id",
            _required_text(
                self.source_refinement_id,
                "code_delivery_context_source_refinement_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "source_refinement_version",
            _positive_int(
                self.source_refinement_version,
                "code_delivery_context_source_refinement_version_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class DirectSpecDeliveryContextProvenance:
    """Context explicitly authored on a Spec with no Refinement source."""

    value: DeliveryContext
    source_spec_id: str
    source_spec_version: int

    def __post_init__(self) -> None:
        try:
            value = _enum(
                self.value,
                DeliveryContext,
                "code_delivery_context_required",
            )
        except CodeTraceabilityContractError as exc:
            raise CodeDeliveryContextRequired() from exc
        object.__setattr__(self, "value", value)
        object.__setattr__(
            self,
            "source_spec_id",
            _required_text(
                self.source_spec_id,
                "code_delivery_context_source_spec_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "source_spec_version",
            _positive_int(
                self.source_spec_version,
                "code_delivery_context_source_spec_version_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class SpecDeliveryContextProvenance:
    """Versioned provenance for the delivery context materialized on a Spec."""

    value: DeliveryContext
    inherited_value: DeliveryContext
    source_refinement_id: str
    source_refinement_version: int
    override_reason: str | None = None

    def __post_init__(self) -> None:
        try:
            value = _enum(
                self.value,
                DeliveryContext,
                "code_delivery_context_required",
            )
            inherited_value = _enum(
                self.inherited_value,
                DeliveryContext,
                "code_delivery_context_required",
            )
        except CodeTraceabilityContractError as exc:
            raise CodeDeliveryContextRequired() from exc
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "inherited_value", inherited_value)
        object.__setattr__(
            self,
            "source_refinement_id",
            _required_text(
                self.source_refinement_id,
                "code_delivery_context_source_refinement_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "source_refinement_version",
            _positive_int(
                self.source_refinement_version,
                "code_delivery_context_source_refinement_version_invalid",
            ),
        )
        override_reason = _optional_text(
            self.override_reason,
            "code_delivery_context_override_reason_invalid",
        )
        if value is not inherited_value and override_reason is None:
            raise CodeDeliveryContextOverrideReasonRequired()
        if value is inherited_value and override_reason is not None:
            raise CodeTraceabilityContractError(
                "code_delivery_context_override_reason_invalid",
                details={"reason": "override_absent"},
            )
        object.__setattr__(self, "override_reason", override_reason)

    @property
    def overridden(self) -> bool:
        return self.value is not self.inherited_value


@dataclass(frozen=True, slots=True)
class CodeEvidenceBaselineProvenance:
    """Frozen proof that Evidence points to source present before delivery work."""

    presence: CodeEvidenceBaselinePresence
    workspace_state_id: str
    provenance_note: str | None = None

    def __post_init__(self) -> None:
        try:
            presence = _enum(
                self.presence,
                CodeEvidenceBaselinePresence,
                "code_evidence_baseline_provenance_invalid",
            )
            workspace_state_id = _required_text(
                self.workspace_state_id,
                "code_evidence_baseline_provenance_invalid",
            )
            provenance_note = _optional_text(
                self.provenance_note,
                "code_evidence_baseline_provenance_invalid",
            )
        except CodeTraceabilityContractError as exc:
            raise CodeEvidenceBaselineProvenanceInvalid() from exc
        if (
            presence is CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            and provenance_note is None
        ):
            raise CodeEvidenceBaselineProvenanceInvalid(
                details={"field": "provenance_note"}
            )
        object.__setattr__(self, "presence", presence)
        object.__setattr__(self, "workspace_state_id", workspace_state_id)
        object.__setattr__(self, "provenance_note", provenance_note)


@dataclass(frozen=True, slots=True)
class CodeEvidenceLegacyClassification:
    """One immutable actor-authored overlay for a legacy Evidence item.

    The record never edits the Evidence payload.  ``revision`` and
    ``predecessor_classification_id`` form an append-only per-Evidence chain;
    batch metadata lets an adapter provide atomic replay without requiring a
    mutable batch row.
    """

    id: str
    batch_id: str
    board_id: str
    evidence_id: str
    evidence_payload_sha256: str
    revision: int
    predecessor_classification_id: str | None
    source_role: CodeEvidenceSourceRole
    relevance_summary: str
    scope_relation: str
    source_origin: str
    interpretation_limit: str | None
    baseline_provenance: CodeEvidenceBaselineProvenance
    classified_by: str
    classified_at: datetime
    justification: str
    idempotency_key: str
    request_sha256: str
    batch_item_count: int
    batch_item_index: int
    context_contract_version: int = 2
    classification_sha256: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "id",
            "batch_id",
            "board_id",
            "evidence_id",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_evidence_legacy_classification_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "classified_by",
            _required_text(
                self.classified_by,
                "code_evidence_legacy_classification_classified_by_invalid",
                max_bytes=CODE_EVIDENCE_CLASSIFICATION_ACTOR_ID_MAX_BYTES,
            ),
        )
        if self.context_contract_version != 2:
            raise CodeTraceabilityContractError(
                "code_evidence_legacy_classification_contract_version_invalid"
            )
        object.__setattr__(
            self,
            "evidence_payload_sha256",
            _sha256(
                self.evidence_payload_sha256,
                "code_evidence_legacy_classification_payload_sha256_invalid",
            ),
        )
        revision = _positive_int(
            self.revision,
            "code_evidence_legacy_classification_revision_invalid",
        )
        object.__setattr__(self, "revision", revision)
        predecessor = _optional_text(
            self.predecessor_classification_id,
            "code_evidence_legacy_classification_predecessor_invalid",
        )
        if (revision == 1) != (predecessor is None):
            raise CodeTraceabilityContractError(
                "code_evidence_legacy_classification_predecessor_invalid"
            )
        object.__setattr__(self, "predecessor_classification_id", predecessor)
        try:
            source_role = authored_code_evidence_source_role(self.source_role)
        except CodeTraceabilityContractError as exc:
            raise CodeEvidenceLegacyClassificationLegacyRequired() from exc
        object.__setattr__(self, "source_role", source_role)
        for name in ("relevance_summary", "scope_relation", "source_origin"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_evidence_legacy_classification_{name}_required",
                    max_bytes=20_000,
                ),
            )
        interpretation_limit = _optional_text(
            self.interpretation_limit,
            "code_evidence_legacy_classification_interpretation_limit_invalid",
            max_bytes=20_000,
        )
        if (
            source_role
            in {
                CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
                CodeEvidenceSourceRole.REFERENCE_PATTERN,
            }
            and interpretation_limit is None
        ):
            raise CodeEvidenceInterpretationLimitRequired(
                details={"source_role": source_role.value}
            )
        object.__setattr__(self, "interpretation_limit", interpretation_limit)
        if not isinstance(self.baseline_provenance, CodeEvidenceBaselineProvenance):
            raise CodeEvidenceBaselineProvenanceInvalid()
        object.__setattr__(
            self,
            "classified_at",
            _aware_utc(
                self.classified_at,
                "code_evidence_legacy_classification_classified_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "justification",
            _required_text(
                self.justification,
                "code_evidence_legacy_classification_justification_required",
                max_bytes=20_000,
            ),
        )
        object.__setattr__(
            self,
            "request_sha256",
            _sha256(
                self.request_sha256,
                "code_evidence_legacy_classification_request_sha256_invalid",
            ),
        )
        item_count = _positive_int(
            self.batch_item_count,
            "code_evidence_legacy_classification_batch_count_invalid",
        )
        if item_count > CODE_EVIDENCE_LEGACY_CLASSIFICATION_BATCH_LIMIT:
            raise CodeEvidenceLegacyClassificationLimitExceeded(
                details={
                    "max_items": CODE_EVIDENCE_LEGACY_CLASSIFICATION_BATCH_LIMIT
                }
            )
        item_index = _positive_int(
            self.batch_item_index,
            "code_evidence_legacy_classification_batch_index_invalid",
        )
        if item_index > item_count:
            raise CodeTraceabilityContractError(
                "code_evidence_legacy_classification_batch_index_invalid"
            )
        object.__setattr__(self, "batch_item_count", item_count)
        object.__setattr__(self, "batch_item_index", item_index)
        expected_digest = canonical_code_traceability_sha256(
            self.digest_payload()
        )
        if self.classification_sha256 is not None:
            provided_digest = _sha256(
                self.classification_sha256,
                "code_evidence_legacy_classification_sha256_invalid",
            )
            if provided_digest != expected_digest:
                raise CodeEvidenceLegacyClassificationPayloadConflict(
                    details={"field": "classification_sha256"}
                )
        object.__setattr__(self, "classification_sha256", expected_digest)

    def digest_payload(self) -> dict[str, object]:
        return {
            "contract_version": 1,
            "operation": "classify_legacy_code_evidence",
            "id": self.id,
            "batch_id": self.batch_id,
            "board_id": self.board_id,
            "evidence_id": self.evidence_id,
            "evidence_payload_sha256": self.evidence_payload_sha256,
            "revision": self.revision,
            "predecessor_classification_id": (
                self.predecessor_classification_id
            ),
            "source_role": self.source_role,
            "relevance_summary": self.relevance_summary,
            "scope_relation": self.scope_relation,
            "source_origin": self.source_origin,
            "interpretation_limit": self.interpretation_limit,
            "baseline_provenance": self.baseline_provenance,
            "classified_by": self.classified_by,
            "classified_at": self.classified_at,
            "justification": self.justification,
            "idempotency_key": self.idempotency_key,
            "request_sha256": self.request_sha256,
            "batch_item_count": self.batch_item_count,
            "batch_item_index": self.batch_item_index,
            "context_contract_version": self.context_contract_version,
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceLegacyClassificationBatchReceipt:
    """Logical receipt for one atomic append across multiple Evidence heads."""

    batch_id: str
    board_id: str
    classified_by: str
    classified_at: datetime
    idempotency_key: str
    request_sha256: str
    classifications: tuple[CodeEvidenceLegacyClassification, ...]
    replayed: bool = False

    def __post_init__(self) -> None:
        for name in ("batch_id", "board_id", "classified_by", "idempotency_key"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_evidence_legacy_classification_batch_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "classified_at",
            _aware_utc(
                self.classified_at,
                "code_evidence_legacy_classification_batch_time_invalid",
            ),
        )
        object.__setattr__(
            self,
            "request_sha256",
            _sha256(
                self.request_sha256,
                "code_evidence_legacy_classification_request_sha256_invalid",
            ),
        )
        items = _typed_tuple(
            self.classifications,
            CodeEvidenceLegacyClassification,
            "code_evidence_legacy_classification_batch_items_invalid",
            max_items=CODE_EVIDENCE_LEGACY_CLASSIFICATION_BATCH_LIMIT,
        )
        if not items:
            raise CodeEvidenceLegacyClassificationItemsRequired()
        if items != tuple(sorted(items, key=lambda item: item.evidence_id)):
            raise CodeTraceabilityContractError(
                "code_evidence_legacy_classification_batch_order_invalid"
            )
        if len({item.evidence_id for item in items}) != len(items):
            raise CodeEvidenceLegacyClassificationItemsDuplicate()
        for index, item in enumerate(items, start=1):
            if (
                item.batch_id != self.batch_id
                or item.board_id != self.board_id
                or item.classified_by != self.classified_by
                or item.classified_at != self.classified_at
                or item.idempotency_key != self.idempotency_key
                or item.request_sha256 != self.request_sha256
                or item.batch_item_count != len(items)
                or item.batch_item_index != index
            ):
                raise CodeTraceabilityContractError(
                    "code_evidence_legacy_classification_batch_incoherent"
                )
        if not isinstance(self.replayed, bool):
            raise CodeTraceabilityContractError(
                "code_evidence_legacy_classification_batch_replayed_invalid"
            )
        object.__setattr__(self, "classifications", items)


@dataclass(frozen=True, slots=True)
class SourceContextEvidenceItemV2:
    """Effective contextual meaning without modifying the Evidence record."""

    evidence_id: str
    source_role: CodeEvidenceSourceRole
    relevance_summary: str | None
    scope_relation: str | None
    source_origin: str | None
    interpretation_limit: str | None
    baseline_provenance: CodeEvidenceBaselineProvenance | None
    context_origin: CodeEvidenceContextOrigin
    context_contract_version: int | None = None
    classification_revision: int | None = None
    classification_sha256: str | None = None
    classification_id: str | None = None
    classified_by: str | None = None
    classified_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evidence_id",
            _required_text(
                self.evidence_id,
                "source_context_evidence_id_invalid",
            ),
        )
        role = _enum(
            self.source_role,
            CodeEvidenceSourceRole,
            "source_context_evidence_role_invalid",
        )
        origin = _enum(
            self.context_origin,
            CodeEvidenceContextOrigin,
            "source_context_evidence_origin_invalid",
        )
        object.__setattr__(self, "source_role", role)
        object.__setattr__(self, "context_origin", origin)
        contextual = (
            self.relevance_summary,
            self.scope_relation,
            self.source_origin,
            self.interpretation_limit,
            self.baseline_provenance,
        )
        classification_values = (
            self.classification_revision,
            self.classification_sha256,
            self.classification_id,
        )
        actor_values = (self.classified_by, self.classified_at)
        if origin is CodeEvidenceContextOrigin.UNCLASSIFIED_LEGACY:
            if (
                role is not CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY
                or self.context_contract_version is not None
                or any(item is not None for item in contextual)
                or any(item is not None for item in classification_values)
                or any(item is not None for item in actor_values)
            ):
                raise CodeTraceabilityContractError(
                    "source_context_evidence_origin_invalid"
                )
            return
        if self.context_contract_version != 2:
            raise CodeTraceabilityContractError(
                "source_context_evidence_contract_version_invalid"
            )
        if role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY:
            raise CodeTraceabilityContractError(
                "source_context_evidence_role_invalid"
            )
        for name in ("relevance_summary", "scope_relation", "source_origin"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"source_context_evidence_{name}_invalid",
                    max_bytes=20_000,
                ),
            )
        object.__setattr__(
            self,
            "interpretation_limit",
            _optional_text(
                self.interpretation_limit,
                "source_context_evidence_interpretation_limit_invalid",
                max_bytes=20_000,
            ),
        )
        if not isinstance(self.baseline_provenance, CodeEvidenceBaselineProvenance):
            raise CodeEvidenceBaselineProvenanceInvalid()
        if origin is CodeEvidenceContextOrigin.AUTHORED:
            if any(item is not None for item in classification_values) or any(
                item is not None for item in actor_values
            ):
                raise CodeTraceabilityContractError(
                    "source_context_evidence_origin_invalid"
                )
            return
        if any(item is None for item in classification_values):
            raise CodeTraceabilityContractError(
                "source_context_evidence_classification_invalid"
            )
        object.__setattr__(
            self,
            "classification_revision",
            _positive_int(
                self.classification_revision,
                "source_context_classification_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "classification_sha256",
            _sha256(
                self.classification_sha256,
                "source_context_classification_sha256_invalid",
            ),
        )
        object.__setattr__(
            self,
            "classification_id",
            _required_text(
                self.classification_id,
                "source_context_classification_id_invalid",
            ),
        )
        if (self.classified_by is None) != (self.classified_at is None):
            raise CodeTraceabilityContractError(
                "source_context_classification_actor_invalid"
            )
        if self.classified_by is not None:
            object.__setattr__(
                self,
                "classified_by",
                _required_text(
                    self.classified_by,
                    "source_context_classification_actor_invalid",
                ),
            )
            object.__setattr__(
                self,
                "classified_at",
                _aware_utc(
                    self.classified_at,
                    "source_context_classification_actor_invalid",
                ),
            )


@dataclass(frozen=True, slots=True)
class SourceContextClassificationBaselineInputV2:
    """Server-authored baseline defaults for one legacy classification form."""

    presence: CodeEvidenceBaselinePresence
    workspace_state_id: str
    provenance_note: str | None
    provenance_note_required: bool

    def __post_init__(self) -> None:
        presence = _enum(
            self.presence,
            CodeEvidenceBaselinePresence,
            "source_context_classification_baseline_presence_invalid",
        )
        object.__setattr__(self, "presence", presence)
        object.__setattr__(
            self,
            "workspace_state_id",
            _required_text(
                self.workspace_state_id,
                "source_context_classification_baseline_workspace_state_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "provenance_note",
            _optional_text(
                self.provenance_note,
                "source_context_classification_baseline_provenance_note_invalid",
                max_bytes=20_000,
            ),
        )
        required = _strict_bool(
            self.provenance_note_required,
            "source_context_classification_baseline_note_required_invalid",
        )
        if required is not (
            presence is CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
        ):
            raise CodeTraceabilityContractError(
                "source_context_classification_baseline_note_required_invalid"
            )
        object.__setattr__(self, "provenance_note_required", required)


@dataclass(frozen=True, slots=True)
class SourceContextClassificationInputV2:
    """Server-authoritative optimistic fence and baseline for one legacy item."""

    evidence_id: str
    expected_evidence_payload_sha256: str
    expected_classification_revision: int
    baseline_provenance: SourceContextClassificationBaselineInputV2

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evidence_id",
            _required_text(
                self.evidence_id,
                "source_context_classification_input_evidence_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_evidence_payload_sha256",
            _sha256(
                self.expected_evidence_payload_sha256,
                "source_context_classification_input_payload_sha256_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_classification_revision",
            _non_negative_int(
                self.expected_classification_revision,
                "source_context_classification_input_revision_invalid",
            ),
        )
        if not isinstance(
            self.baseline_provenance,
            SourceContextClassificationBaselineInputV2,
        ):
            raise CodeTraceabilityContractError(
                "source_context_classification_input_baseline_invalid"
            )


@dataclass(frozen=True, slots=True)
class SourceContextRoleCountsV2:
    """Closed counts for every Code Evidence source-role classification."""

    current_implementation_count: int = 0
    existing_scaffold_count: int = 0
    existing_constraint_count: int = 0
    reference_pattern_count: int = 0
    uncategorized_legacy_count: int = 0

    def __post_init__(self) -> None:
        for name in (
            "current_implementation_count",
            "existing_scaffold_count",
            "existing_constraint_count",
            "reference_pattern_count",
            "uncategorized_legacy_count",
        ):
            object.__setattr__(
                self,
                name,
                _non_negative_int(
                    getattr(self, name),
                    "source_context_role_count_invalid",
                ),
            )

    @property
    def total_count(self) -> int:
        return sum(
            (
                self.current_implementation_count,
                self.existing_scaffold_count,
                self.existing_constraint_count,
                self.reference_pattern_count,
                self.uncategorized_legacy_count,
            )
        )

    def count_for(self, role: CodeEvidenceSourceRole) -> int:
        resolved = _enum(
            role,
            CodeEvidenceSourceRole,
            "source_context_role_invalid",
        )
        return {
            CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION: (
                self.current_implementation_count
            ),
            CodeEvidenceSourceRole.EXISTING_SCAFFOLD: self.existing_scaffold_count,
            CodeEvidenceSourceRole.EXISTING_CONSTRAINT: (
                self.existing_constraint_count
            ),
            CodeEvidenceSourceRole.REFERENCE_PATTERN: self.reference_pattern_count,
            CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY: (
                self.uncategorized_legacy_count
            ),
        }[resolved]


@dataclass(frozen=True, slots=True)
class SourceContextClassificationStateV2:
    """Structural classification state without an invented status vocabulary."""

    classified_count: int
    uncategorized_legacy_count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "classified_count",
            _non_negative_int(
                self.classified_count,
                "source_context_classified_count_invalid",
            ),
        )
        object.__setattr__(
            self,
            "uncategorized_legacy_count",
            _non_negative_int(
                self.uncategorized_legacy_count,
                "source_context_uncategorized_legacy_count_invalid",
            ),
        )

    @property
    def fully_classified(self) -> bool:
        return self.uncategorized_legacy_count == 0

    @property
    def has_unclassified_legacy(self) -> bool:
        return self.uncategorized_legacy_count > 0


@dataclass(frozen=True, slots=True)
class SourceContextSummaryV2:
    """Small transport-neutral summary consumed by later human projections."""

    delivery_context: DeliveryContext | None
    delivery_context_provenance: (
        RefinementDeliveryContextProvenance
        | SpecDeliveryContextProvenance
        | DirectSpecDeliveryContextProvenance
        | None
    )
    investigation_outcome: ContextualInvestigationOutcomeV2 | None
    role_counts: SourceContextRoleCountsV2
    classification_state: SourceContextClassificationStateV2
    evidence_applicable: bool | None
    interpretation_rule: str
    items_not_current_implementation_count: int
    technical_details_available: bool

    def __post_init__(self) -> None:
        delivery_context = self.delivery_context
        provenance = self.delivery_context_provenance
        if delivery_context is not None:
            delivery_context = _enum(
                delivery_context,
                DeliveryContext,
                "code_delivery_context_required",
            )
        if (delivery_context is None) != (provenance is None):
            raise CodeTraceabilityContractError(
                "source_context_delivery_context_provenance_invalid",
                details={"reason": "legacy_pair_incoherent"},
            )
        if provenance is not None and not isinstance(
            provenance,
            RefinementDeliveryContextProvenance
            | SpecDeliveryContextProvenance
            | DirectSpecDeliveryContextProvenance,
        ):
            raise CodeTraceabilityContractError(
                "source_context_delivery_context_provenance_invalid"
            )
        if provenance is not None and delivery_context is not provenance.value:
            raise CodeTraceabilityContractError(
                "source_context_delivery_context_provenance_invalid",
                details={"reason": "effective_value_mismatch"},
            )
        outcome = self.investigation_outcome
        if outcome is not None:
            outcome = _enum(
                outcome,
                ContextualInvestigationOutcomeV2,
                "source_context_investigation_outcome_invalid",
            )
            if delivery_context is None:
                raise CodeTraceabilityContractError(
                    "source_context_investigation_outcome_invalid",
                    details={"reason": "delivery_context_required"},
                )
        if not isinstance(self.role_counts, SourceContextRoleCountsV2):
            raise CodeTraceabilityContractError("source_context_role_counts_invalid")
        if not isinstance(
            self.classification_state,
            SourceContextClassificationStateV2,
        ):
            raise CodeTraceabilityContractError(
                "source_context_classification_state_invalid"
            )
        classified_count = (
            self.role_counts.total_count - self.role_counts.uncategorized_legacy_count
        )
        if (
            self.classification_state.classified_count != classified_count
            or self.classification_state.uncategorized_legacy_count
            != self.role_counts.uncategorized_legacy_count
        ):
            raise CodeTraceabilityContractError(
                "source_context_classification_state_invalid",
                details={"reason": "role_count_mismatch"},
            )
        if (
            outcome
            is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
            and (
                delivery_context is not DeliveryContext.GREENFIELD
                or self.role_counts.current_implementation_count != 0
            )
        ):
            raise CodeTraceabilityContractError(
                "source_context_investigation_outcome_invalid",
                details={
                    "reason": "no_relevant_existing_implementation_incoherent"
                },
            )
        evidence_applicable = self.evidence_applicable
        if evidence_applicable is not None:
            evidence_applicable = _strict_bool(
                evidence_applicable,
                "source_context_evidence_applicable_invalid",
            )
        expected_applicability = {
            ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE: True,
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION: (
                False
            ),
            ContextualInvestigationOutcomeV2.PARTIAL: None,
            ContextualInvestigationOutcomeV2.UNAVAILABLE: None,
            None: None,
        }[outcome]
        if evidence_applicable is not expected_applicability:
            raise CodeTraceabilityContractError(
                "source_context_evidence_applicable_invalid",
                details={"reason": "investigation_outcome_mismatch"},
            )
        interpretation_rule = _required_text(
            self.interpretation_rule,
            "source_context_interpretation_rule_invalid",
        )
        not_current_count = _non_negative_int(
            self.items_not_current_implementation_count,
            "source_context_items_not_current_implementation_count_invalid",
        )
        expected_not_current_count = (
            self.role_counts.total_count - self.role_counts.current_implementation_count
        )
        if not_current_count != expected_not_current_count:
            raise CodeTraceabilityContractError(
                "source_context_items_not_current_implementation_count_invalid",
                details={"expected": expected_not_current_count},
            )
        technical_details_available = _strict_bool(
            self.technical_details_available,
            "source_context_technical_details_available_invalid",
        )
        object.__setattr__(self, "delivery_context", delivery_context)
        object.__setattr__(self, "delivery_context_provenance", provenance)
        object.__setattr__(self, "investigation_outcome", outcome)
        object.__setattr__(self, "evidence_applicable", evidence_applicable)
        object.__setattr__(self, "interpretation_rule", interpretation_rule)
        object.__setattr__(
            self,
            "items_not_current_implementation_count",
            not_current_count,
        )
        object.__setattr__(
            self,
            "technical_details_available",
            technical_details_available,
        )


@dataclass(frozen=True, slots=True)
class SourceContextCurrentReceiptV2:
    """Current contextual receipt identity sealed into one Refinement snapshot."""

    receipt_id: str
    source_ref: str
    generation: int
    head_revision: int
    payload_sha256: str
    delivery_context: DeliveryContext | None
    contextual_outcome: ContextualInvestigationOutcomeV2 | None
    context_contract_version: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "receipt_id", _required_text(
            self.receipt_id,
            "source_context_receipt_id_invalid",
        ))
        object.__setattr__(
            self,
            "source_ref",
            normalize_code_source_ref(self.source_ref),
        )
        object.__setattr__(self, "generation", _positive_int(
            self.generation,
            "source_context_receipt_generation_invalid",
        ))
        object.__setattr__(self, "head_revision", _positive_int(
            self.head_revision,
            "source_context_receipt_head_revision_invalid",
        ))
        object.__setattr__(self, "payload_sha256", _sha256(
            self.payload_sha256,
            "source_context_receipt_payload_sha256_invalid",
        ))
        delivery_context = self.delivery_context
        contextual_outcome = self.contextual_outcome
        if (delivery_context is None) != (contextual_outcome is None):
            raise CodeTraceabilityContractError(
                "source_context_receipt_context_invalid"
            )
        if delivery_context is None:
            if self.context_contract_version is not None:
                raise CodeTraceabilityContractError(
                    "source_context_receipt_contract_version_invalid"
                )
        elif self.context_contract_version != 2:
            raise CodeTraceabilityContractError(
                "source_context_receipt_contract_version_invalid"
            )
        if delivery_context is not None:
            delivery_context = _enum(
                delivery_context,
                DeliveryContext,
                "code_delivery_context_required",
            )
            contextual_outcome = _enum(
                contextual_outcome,
                ContextualInvestigationOutcomeV2,
                "source_context_investigation_outcome_invalid",
            )
        object.__setattr__(self, "delivery_context", delivery_context)
        object.__setattr__(self, "contextual_outcome", contextual_outcome)

    def as_dict(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            "source_ref": self.source_ref,
            "generation": self.generation,
            "head_revision": self.head_revision,
            "payload_sha256": self.payload_sha256,
            "delivery_context": (
                None
                if self.delivery_context is None
                else self.delivery_context.value
            ),
            "contextual_outcome": (
                None
                if self.contextual_outcome is None
                else self.contextual_outcome.value
            ),
            "context_contract_version": self.context_contract_version,
        }


@dataclass(frozen=True, slots=True)
class SourceContextClassificationFenceV2:
    """Extensible append-only classification fence; I3 seals explicit absence."""

    revision: int | None = None
    payload_sha256: str | None = None

    def __post_init__(self) -> None:
        if (self.revision is None) != (self.payload_sha256 is None):
            raise CodeTraceabilityContractError(
                "source_context_classification_fence_invalid"
            )
        if self.revision is not None:
            object.__setattr__(self, "revision", _positive_int(
                self.revision,
                "source_context_classification_revision_invalid",
            ))
            object.__setattr__(self, "payload_sha256", _sha256(
                self.payload_sha256,
                "source_context_classification_sha256_invalid",
            ))

    def as_dict(self) -> dict[str, object]:
        return {
            "revision": self.revision,
            "payload_sha256": self.payload_sha256,
        }


@dataclass(frozen=True, slots=True)
class RefinementSourceContextManifestV2:
    """Immutable contextual input sealed at one exact Refinement version."""

    refinement_id: str
    refinement_version: int
    summary: SourceContextSummaryV2
    current_receipts: tuple[SourceContextCurrentReceiptV2, ...]
    classification_fence: SourceContextClassificationFenceV2 = (
        SourceContextClassificationFenceV2()
    )
    contract_version: int = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise CodeTraceabilityContractError(
                "source_context_manifest_contract_version_invalid"
            )
        refinement_id = _required_text(
            self.refinement_id,
            "source_context_manifest_refinement_id_invalid",
        )
        refinement_version = _positive_int(
            self.refinement_version,
            "source_context_manifest_refinement_version_invalid",
        )
        if not isinstance(self.summary, SourceContextSummaryV2):
            raise CodeTraceabilityContractError(
                "source_context_manifest_summary_invalid"
            )
        provenance = self.summary.delivery_context_provenance
        if provenance is not None and (
            not isinstance(provenance, RefinementDeliveryContextProvenance)
            or provenance.source_refinement_id != refinement_id
            or provenance.source_refinement_version != refinement_version
        ):
            raise CodeTraceabilityContractError(
                "source_context_manifest_provenance_invalid"
            )
        receipts = tuple(self.current_receipts)
        if any(not isinstance(item, SourceContextCurrentReceiptV2) for item in receipts):
            raise CodeTraceabilityContractError(
                "source_context_manifest_receipts_invalid"
            )
        sorted_receipts = tuple(
            sorted(receipts, key=lambda item: (item.source_ref, item.receipt_id))
        )
        if receipts != sorted_receipts or len({item.source_ref for item in receipts}) != len(receipts):
            raise CodeTraceabilityContractError(
                "source_context_manifest_receipts_invalid"
            )
        if not isinstance(
            self.classification_fence,
            SourceContextClassificationFenceV2,
        ):
            raise CodeTraceabilityContractError(
                "source_context_classification_fence_invalid"
            )
        object.__setattr__(self, "refinement_id", refinement_id)
        object.__setattr__(self, "refinement_version", refinement_version)
        object.__setattr__(self, "current_receipts", receipts)

    def as_dict(self) -> dict[str, object]:
        summary = self.summary
        provenance = summary.delivery_context_provenance
        provenance_payload = None
        if isinstance(provenance, RefinementDeliveryContextProvenance):
            provenance_payload = {
                "value": provenance.value.value,
                "source_refinement_id": provenance.source_refinement_id,
                "source_refinement_version": provenance.source_refinement_version,
            }
        return {
            "contract_version": self.contract_version,
            "subject_type": CodeTraceabilitySubjectType.REFINEMENT.value,
            "subject_id": self.refinement_id,
            "subject_version": self.refinement_version,
            "delivery_context": (
                None
                if summary.delivery_context is None
                else summary.delivery_context.value
            ),
            "delivery_context_provenance": provenance_payload,
            "current_receipts": [item.as_dict() for item in self.current_receipts],
            "investigation_outcome": (
                None
                if summary.investigation_outcome is None
                else summary.investigation_outcome.value
            ),
            "evidence_applicable": summary.evidence_applicable,
            "role_counts": {
                "current_implementation_count": (
                    summary.role_counts.current_implementation_count
                ),
                "existing_scaffold_count": summary.role_counts.existing_scaffold_count,
                "existing_constraint_count": (
                    summary.role_counts.existing_constraint_count
                ),
                "reference_pattern_count": summary.role_counts.reference_pattern_count,
                "uncategorized_legacy_count": (
                    summary.role_counts.uncategorized_legacy_count
                ),
            },
            "classification_state": {
                "classified_count": summary.classification_state.classified_count,
                "uncategorized_legacy_count": (
                    summary.classification_state.uncategorized_legacy_count
                ),
            },
            "classification_fence": self.classification_fence.as_dict(),
            "interpretation_rule": summary.interpretation_rule,
            "items_not_current_implementation_count": (
                summary.items_not_current_implementation_count
            ),
            "technical_details_available": summary.technical_details_available,
        }

    @property
    def payload_sha256(self) -> str:
        return canonical_code_traceability_sha256(self.as_dict())


def normalize_code_source_ref(value: object) -> str:
    """Validate that a server-issued source identity is opaque, not a locator."""

    source_ref = _required_text(value, "code_investigation_source_ref_invalid")
    if (
        "/" in source_ref
        or "\\" in source_ref
        or "://" in source_ref
        or _WINDOWS_DRIVE_RE.match(source_ref)
    ):
        raise CodeInvestigationSourceScopeMismatch(
            details={"reason": "source_ref_must_be_opaque"}
        )
    return source_ref


@dataclass(frozen=True, slots=True)
class CodeInvestigationOmission:
    reason_code: CodeInvestigationOmissionReason
    affected_scope_digest: str
    count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_code",
            _enum(
                self.reason_code,
                CodeInvestigationOmissionReason,
                "code_investigation_omission_reason_invalid",
            ),
        )
        object.__setattr__(
            self,
            "affected_scope_digest",
            _sha256(
                self.affected_scope_digest,
                "code_investigation_omission_scope_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "count",
            _positive_int(self.count, "code_investigation_omission_count_invalid"),
        )


def code_investigation_omission_digest(
    omissions: Sequence[CodeInvestigationOmission],
) -> str:
    return canonical_code_traceability_sha256(tuple(omissions))


@dataclass(frozen=True, slots=True)
class CodeInvestigationTooling:
    tool_id: str
    tool_version: str
    method_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tool_id",
            _required_text(self.tool_id, "code_investigation_tool_id_invalid"),
        )
        object.__setattr__(
            self,
            "tool_version",
            _required_text(
                self.tool_version,
                "code_investigation_tool_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "method_id",
            _required_text(self.method_id, "code_investigation_method_id_invalid"),
        )


@dataclass(frozen=True, slots=True)
class ObservedWorkspaceStateRef:
    """Agent-declared source state; never an independently verified snapshot."""

    declared_revision: str | None
    workspace_state_id: str
    declared_dirty: bool
    observed_at: datetime
    reproducibility_claim: WorkspaceReproducibilityClaim
    fingerprint_algorithm: str
    manifest_digest: str
    manifest_entry_count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "declared_revision",
            _optional_text(
                self.declared_revision,
                "code_investigation_declared_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "workspace_state_id",
            _required_text(
                self.workspace_state_id,
                "code_investigation_workspace_state_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "declared_dirty",
            _strict_bool(
                self.declared_dirty,
                "code_investigation_declared_dirty_invalid",
            ),
        )
        object.__setattr__(
            self,
            "observed_at",
            _aware_utc(
                self.observed_at,
                "code_investigation_observed_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "reproducibility_claim",
            _enum(
                self.reproducibility_claim,
                WorkspaceReproducibilityClaim,
                "code_investigation_reproducibility_claim_invalid",
            ),
        )
        object.__setattr__(
            self,
            "fingerprint_algorithm",
            _required_text(
                self.fingerprint_algorithm,
                "code_investigation_fingerprint_algorithm_invalid",
            ),
        )
        object.__setattr__(
            self,
            "manifest_digest",
            _sha256(
                self.manifest_digest,
                "code_investigation_manifest_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "manifest_entry_count",
            _non_negative_int(
                self.manifest_entry_count,
                "code_investigation_manifest_entry_count_invalid",
            ),
        )
        if (
            self.reproducibility_claim is WorkspaceReproducibilityClaim.COMMITTED
            and self.declared_dirty
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_workspace_claim_incoherent"
            )


def code_investigation_observation_sha256(
    *,
    source_ref: str,
    selector_scope_digest: str,
    outcome: CodeInvestigationOutcome,
    capabilities: Sequence[CodeInvestigationCapability],
    source_identity_digest: str | None,
    declared_revision: str | None,
    workspace_state: ObservedWorkspaceStateRef | None,
    omission_manifest: Sequence[CodeInvestigationOmission],
) -> str:
    """Hash the corroboration claim, excluding actor, tooling, and timestamps."""

    state_claim: Mapping[str, object] | None = None
    if workspace_state is not None:
        if not isinstance(workspace_state, ObservedWorkspaceStateRef):
            raise CodeTraceabilityContractError(
                "code_investigation_workspace_state_invalid"
            )
        state_claim = {
            "workspace_state_id": workspace_state.workspace_state_id,
            "declared_dirty": workspace_state.declared_dirty,
            "reproducibility_claim": workspace_state.reproducibility_claim,
            "fingerprint_algorithm": workspace_state.fingerprint_algorithm,
            "manifest_digest": workspace_state.manifest_digest,
            "manifest_entry_count": workspace_state.manifest_entry_count,
        }
    return canonical_code_traceability_sha256(
        {
            "source_ref": normalize_code_source_ref(source_ref),
            "selector_scope_digest": _sha256(
                selector_scope_digest,
                "code_investigation_selector_scope_digest_invalid",
            ),
            "outcome": _enum(
                outcome,
                CodeInvestigationOutcome,
                "code_investigation_receipt_outcome_invalid",
            ),
            "capabilities": _enum_tuple(
                capabilities,
                CodeInvestigationCapability,
                "code_investigation_receipt_capabilities_invalid",
            ),
            "source_identity_digest": _optional_sha256(
                source_identity_digest,
                "code_investigation_source_identity_digest_invalid",
            ),
            "declared_revision": _optional_text(
                declared_revision,
                "code_investigation_declared_revision_invalid",
            ),
            "workspace_state": state_claim,
            "omission_manifest": _typed_tuple(
                omission_manifest,
                CodeInvestigationOmission,
                "code_investigation_omission_manifest_invalid",
                max_items=DEFAULT_CODE_TRACEABILITY_LIMITS.omission_entries,
            ),
        }
    )


def code_investigation_observation_sha256_v2(
    *,
    source_ref: str,
    selector_scope_digest: str,
    delivery_context: DeliveryContext,
    outcome: ContextualInvestigationOutcomeV2,
    capabilities: Sequence[CodeInvestigationCapability],
    source_identity_digest: str | None,
    declared_revision: str | None,
    workspace_state: ObservedWorkspaceStateRef | None,
    omission_manifest: Sequence[CodeInvestigationOmission],
) -> str:
    """Hash a contextual investigation without changing the V1 digest contract."""

    state_claim: Mapping[str, object] | None = None
    if workspace_state is not None:
        if not isinstance(workspace_state, ObservedWorkspaceStateRef):
            raise CodeTraceabilityContractError(
                "code_investigation_workspace_state_invalid"
            )
        state_claim = {
            "workspace_state_id": workspace_state.workspace_state_id,
            "declared_dirty": workspace_state.declared_dirty,
            "reproducibility_claim": workspace_state.reproducibility_claim,
            "fingerprint_algorithm": workspace_state.fingerprint_algorithm,
            "manifest_digest": workspace_state.manifest_digest,
            "manifest_entry_count": workspace_state.manifest_entry_count,
        }
    return canonical_code_traceability_sha256(
        {
            "contract_version": 2,
            "source_ref": normalize_code_source_ref(source_ref),
            "selector_scope_digest": _sha256(
                selector_scope_digest,
                "code_investigation_selector_scope_digest_invalid",
            ),
            "delivery_context": _enum(
                delivery_context,
                DeliveryContext,
                "code_delivery_context_required",
            ),
            "outcome": _enum(
                outcome,
                ContextualInvestigationOutcomeV2,
                "code_investigation_contextual_outcome_invalid",
            ),
            "capabilities": _enum_tuple(
                capabilities,
                CodeInvestigationCapability,
                "code_investigation_receipt_capabilities_invalid",
            ),
            "source_identity_digest": _optional_sha256(
                source_identity_digest,
                "code_investigation_source_identity_digest_invalid",
            ),
            "declared_revision": _optional_text(
                declared_revision,
                "code_investigation_declared_revision_invalid",
            ),
            "workspace_state": state_claim,
            "omission_manifest": _typed_tuple(
                omission_manifest,
                CodeInvestigationOmission,
                "code_investigation_omission_manifest_invalid",
                max_items=DEFAULT_CODE_TRACEABILITY_LIMITS.omission_entries,
            ),
        }
    )


@dataclass(frozen=True, slots=True)
class CodeInvestigationRequest:
    id: str
    board_id: str
    subject_type: CodeTraceabilitySubjectType
    subject_id: str
    subject_version: int
    issued_to_actor_id: str
    source_ref: str
    required_capabilities: tuple[CodeInvestigationCapability, ...]
    selector_scope_digest: str
    expected_head_generation: int
    expected_predecessor_receipt_id: str | None
    canonicalization_profile: str
    limits_profile: str
    challenge_key_id: str
    challenge_token_hash: str
    status: CodeInvestigationRequestStatus
    single_use: bool
    expires_at: datetime
    requested_by: str
    created_at: datetime
    consumed_at: datetime | None
    request_payload_sha256: str
    idempotency_key: str

    def __post_init__(self) -> None:
        for name in ("id", "board_id", "subject_id", "issued_to_actor_id"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_investigation_request_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "subject_type",
            _enum(
                self.subject_type,
                CodeTraceabilitySubjectType,
                "code_investigation_request_subject_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "subject_version",
            _positive_int(
                self.subject_version,
                "code_investigation_request_subject_version_invalid",
            ),
        )
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        capabilities = _enum_tuple(
            self.required_capabilities,
            CodeInvestigationCapability,
            "code_investigation_request_capabilities_invalid",
        )
        object.__setattr__(self, "required_capabilities", capabilities)
        object.__setattr__(
            self,
            "selector_scope_digest",
            _sha256(
                self.selector_scope_digest,
                "code_investigation_selector_scope_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_head_generation",
            _non_negative_int(
                self.expected_head_generation,
                "code_investigation_expected_head_generation_invalid",
            ),
        )
        predecessor = _optional_text(
            self.expected_predecessor_receipt_id,
            "code_investigation_expected_predecessor_invalid",
        )
        if (self.expected_head_generation == 0) != (predecessor is None):
            raise CodeTraceabilityContractError(
                "code_investigation_expected_head_incoherent"
            )
        object.__setattr__(
            self,
            "expected_predecessor_receipt_id",
            predecessor,
        )
        canonicalization_profile = _required_text(
            self.canonicalization_profile,
            "code_investigation_canonicalization_profile_invalid",
        )
        limits_profile = _required_text(
            self.limits_profile,
            "code_investigation_limits_profile_invalid",
        )
        if (
            canonicalization_profile != CODE_INVESTIGATION_CANONICALIZATION_PROFILE
            or limits_profile != CODE_INVESTIGATION_LIMITS_PROFILE
        ):
            raise CodeInvestigationProfileMismatch()
        object.__setattr__(
            self,
            "canonicalization_profile",
            canonicalization_profile,
        )
        object.__setattr__(self, "limits_profile", limits_profile)
        object.__setattr__(
            self,
            "challenge_key_id",
            _required_text(
                self.challenge_key_id,
                "code_investigation_challenge_key_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "challenge_token_hash",
            _sha256(
                self.challenge_token_hash,
                "code_investigation_challenge_token_hash_invalid",
            ),
        )
        status = _enum(
            self.status,
            CodeInvestigationRequestStatus,
            "code_investigation_request_status_invalid",
        )
        object.__setattr__(self, "status", status)
        if not _strict_bool(
            self.single_use,
            "code_investigation_request_single_use_invalid",
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_request_single_use_required"
            )
        created_at = _aware_utc(
            self.created_at,
            "code_investigation_request_created_at_invalid",
        )
        expires_at = _aware_utc(
            self.expires_at,
            "code_investigation_request_expires_at_invalid",
        )
        ttl = (expires_at - created_at).total_seconds()
        if not 0 < ttl <= DEFAULT_CODE_TRACEABILITY_LIMITS.challenge_ttl_seconds:
            raise CodeTraceabilityContractError(
                "code_investigation_request_ttl_invalid"
            )
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "expires_at", expires_at)
        consumed_at = (
            None
            if self.consumed_at is None
            else _aware_utc(
                self.consumed_at,
                "code_investigation_request_consumed_at_invalid",
            )
        )
        if (status is CodeInvestigationRequestStatus.CONSUMED) != (
            consumed_at is not None
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_request_consumption_incoherent"
            )
        if consumed_at is not None and consumed_at < created_at:
            raise CodeTraceabilityContractError(
                "code_investigation_request_consumed_at_invalid"
            )
        object.__setattr__(self, "consumed_at", consumed_at)
        object.__setattr__(
            self,
            "requested_by",
            _required_text(
                self.requested_by,
                "code_investigation_requested_by_invalid",
            ),
        )
        object.__setattr__(
            self,
            "request_payload_sha256",
            _sha256(
                self.request_payload_sha256,
                "code_investigation_request_payload_sha256_invalid",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "code_investigation_idempotency_key_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class CodeInvestigationReceipt:
    id: str
    request_id: str
    board_id: str
    subject_type: CodeTraceabilitySubjectType
    subject_id: str
    subject_version: int
    attestor_actor_id: str
    generation: int
    predecessor_receipt_id: str | None
    trust_level: CodeInvestigationTrustLevel
    acceptance_status: CodeInvestigationAcceptanceStatus
    outcome: CodeInvestigationOutcome
    capabilities: tuple[CodeInvestigationCapability, ...]
    source_ref: str
    source_identity_digest: str | None
    canonicalization_profile: str
    limits_profile: str
    selector_scope_digest: str
    declared_revision: str | None
    workspace_state: ObservedWorkspaceStateRef | None
    omission_manifest: tuple[CodeInvestigationOmission, ...]
    omission_digest: str
    omission_count: int
    tooling: CodeInvestigationTooling
    observed_at: datetime
    received_at: datetime
    expires_at: datetime
    observation_sha256: str
    payload_sha256: str
    idempotency_key: str
    delivery_context: DeliveryContext | None = None
    contextual_outcome: ContextualInvestigationOutcomeV2 | None = None
    context_contract_version: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "id",
            "request_id",
            "board_id",
            "subject_id",
            "attestor_actor_id",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_investigation_receipt_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "subject_type",
            _enum(
                self.subject_type,
                CodeTraceabilitySubjectType,
                "code_investigation_receipt_subject_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "subject_version",
            _positive_int(
                self.subject_version,
                "code_investigation_receipt_subject_version_invalid",
            ),
        )
        generation = _positive_int(
            self.generation,
            "code_investigation_receipt_generation_invalid",
        )
        object.__setattr__(self, "generation", generation)
        predecessor = _optional_text(
            self.predecessor_receipt_id,
            "code_investigation_receipt_predecessor_invalid",
        )
        if (generation == 1) != (predecessor is None):
            raise CodeTraceabilityContractError(
                "code_investigation_receipt_lineage_incoherent"
            )
        object.__setattr__(self, "predecessor_receipt_id", predecessor)
        object.__setattr__(
            self,
            "trust_level",
            _enum(
                self.trust_level,
                CodeInvestigationTrustLevel,
                "code_investigation_receipt_trust_invalid",
            ),
        )
        acceptance = _enum(
            self.acceptance_status,
            CodeInvestigationAcceptanceStatus,
            "code_investigation_receipt_acceptance_invalid",
        )
        if acceptance is not CodeInvestigationAcceptanceStatus.ACCEPTED:
            raise CodeTraceabilityContractError(
                "code_investigation_receipt_acceptance_invalid"
            )
        object.__setattr__(self, "acceptance_status", acceptance)
        outcome = _enum(
            self.outcome,
            CodeInvestigationOutcome,
            "code_investigation_receipt_outcome_invalid",
        )
        object.__setattr__(self, "outcome", outcome)
        delivery_context = self.delivery_context
        contextual_outcome = self.contextual_outcome
        if (delivery_context is None) != (contextual_outcome is None):
            raise CodeDeliveryContextRequired()
        if contextual_outcome is None:
            if self.context_contract_version is not None:
                raise CodeTraceabilityContractError(
                    "code_investigation_context_contract_version_invalid"
                )
        elif self.context_contract_version != 2:
            raise CodeTraceabilityContractError(
                "code_investigation_context_contract_version_invalid"
            )
        if contextual_outcome is not None:
            delivery_context = _enum(
                delivery_context,
                DeliveryContext,
                "code_delivery_context_required",
            )
            contextual_outcome = _enum(
                contextual_outcome,
                ContextualInvestigationOutcomeV2,
                "code_investigation_contextual_outcome_invalid",
            )
            if outcome is not legacy_code_investigation_outcome(contextual_outcome):
                raise CodeTraceabilityContractError(
                    "code_investigation_contextual_outcome_mapping_invalid"
                )
            if (
                contextual_outcome
                is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
                and delivery_context is not DeliveryContext.GREENFIELD
            ):
                raise CodeInvestigationNoRelevantExistingImplementationInvalid(
                    details={"delivery_context": delivery_context.value}
                )
        object.__setattr__(self, "delivery_context", delivery_context)
        object.__setattr__(self, "contextual_outcome", contextual_outcome)
        object.__setattr__(
            self,
            "capabilities",
            _enum_tuple(
                self.capabilities,
                CodeInvestigationCapability,
                "code_investigation_receipt_capabilities_invalid",
            ),
        )
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        object.__setattr__(
            self,
            "source_identity_digest",
            _optional_sha256(
                self.source_identity_digest,
                "code_investigation_source_identity_digest_invalid",
            ),
        )
        canonicalization_profile = _required_text(
            self.canonicalization_profile,
            "code_investigation_canonicalization_profile_invalid",
        )
        limits_profile = _required_text(
            self.limits_profile,
            "code_investigation_limits_profile_invalid",
        )
        if (
            canonicalization_profile != CODE_INVESTIGATION_CANONICALIZATION_PROFILE
            or limits_profile != CODE_INVESTIGATION_LIMITS_PROFILE
        ):
            raise CodeInvestigationProfileMismatch()
        object.__setattr__(
            self,
            "canonicalization_profile",
            canonicalization_profile,
        )
        object.__setattr__(self, "limits_profile", limits_profile)
        object.__setattr__(
            self,
            "selector_scope_digest",
            _sha256(
                self.selector_scope_digest,
                "code_investigation_selector_scope_digest_invalid",
            ),
        )
        declared_revision = _optional_text(
            self.declared_revision,
            "code_investigation_declared_revision_invalid",
        )
        object.__setattr__(self, "declared_revision", declared_revision)
        workspace_state = self.workspace_state
        if workspace_state is not None and not isinstance(
            workspace_state, ObservedWorkspaceStateRef
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_workspace_state_invalid"
            )
        omissions = _typed_tuple(
            self.omission_manifest,
            CodeInvestigationOmission,
            "code_investigation_omission_manifest_invalid",
            max_items=DEFAULT_CODE_TRACEABILITY_LIMITS.omission_entries,
        )
        if contextual_outcome is None:
            if outcome is CodeInvestigationOutcome.ACCESSIBLE and omissions:
                raise CodeTraceabilityContractError(
                    "code_investigation_outcome_omissions_incoherent"
                )
            if outcome is not CodeInvestigationOutcome.ACCESSIBLE and not omissions:
                raise CodeTraceabilityContractError(
                    "code_investigation_omission_reason_required"
                )
        elif contextual_outcome in {
            ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
        }:
            if omissions:
                raise CodeTraceabilityContractError(
                    "code_investigation_outcome_omissions_incoherent"
                )
        elif not omissions:
            raise CodeTraceabilityContractError(
                "code_investigation_omission_reason_required"
            )
        object.__setattr__(self, "omission_manifest", omissions)
        expected_omission_digest = code_investigation_omission_digest(omissions)
        omission_digest = _sha256(
            self.omission_digest,
            "code_investigation_omission_digest_invalid",
        )
        if omission_digest != expected_omission_digest:
            raise CodeInvestigationPayloadDigestMismatch(
                details={"field": "omission_digest"}
            )
        object.__setattr__(self, "omission_digest", omission_digest)
        omission_count = _non_negative_int(
            self.omission_count,
            "code_investigation_omission_count_invalid",
        )
        expected_count = sum(item.count for item in omissions)
        if omission_count != expected_count:
            raise CodeInvestigationPayloadDigestMismatch(
                details={"field": "omission_count"}
            )
        object.__setattr__(self, "omission_count", omission_count)
        if not isinstance(self.tooling, CodeInvestigationTooling):
            raise CodeTraceabilityContractError("code_investigation_tooling_invalid")
        observed_at = _aware_utc(
            self.observed_at,
            "code_investigation_observed_at_invalid",
        )
        received_at = _aware_utc(
            self.received_at,
            "code_investigation_received_at_invalid",
        )
        expires_at = _aware_utc(
            self.expires_at,
            "code_investigation_receipt_expires_at_invalid",
        )
        if expires_at <= received_at:
            raise CodeTraceabilityContractError(
                "code_investigation_receipt_expiry_invalid"
            )
        if workspace_state is not None and (
            workspace_state.declared_revision != declared_revision
            or workspace_state.observed_at != observed_at
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_workspace_state_mismatch"
            )
        capabilities = set(self.capabilities)
        capability_claims = (
            (
                self.source_identity_digest is not None,
                CodeInvestigationCapability.SOURCE_IDENTITY,
            ),
            (
                declared_revision is not None,
                CodeInvestigationCapability.REVISION_IDENTITY,
            ),
            (
                workspace_state is not None,
                CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
            ),
        )
        for claim_present, capability in capability_claims:
            if claim_present and capability not in capabilities:
                raise CodeInvestigationCapabilityMissing(
                    details={"capability": capability.value}
                )
        if (
            contextual_outcome
            is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
        ):
            missing_claims = tuple(
                name
                for name, value in (
                    ("source_identity_digest", self.source_identity_digest),
                    ("declared_revision", declared_revision),
                    ("workspace_state", workspace_state),
                )
                if value is None
            )
            identity_capabilities = {
                CodeInvestigationCapability.SOURCE_IDENTITY,
                CodeInvestigationCapability.REVISION_IDENTITY,
                CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
            }
            missing_identity_capabilities = tuple(
                sorted(item.value for item in identity_capabilities - capabilities)
            )
            if missing_claims or missing_identity_capabilities:
                raise CodeInvestigationNoRelevantExistingImplementationInvalid(
                    details={
                        "missing_claims": missing_claims,
                        "missing_capabilities": missing_identity_capabilities,
                    }
                )
        if (
            contextual_outcome is ContextualInvestigationOutcomeV2.UNAVAILABLE
            and any(
                value is not None
                for value in (
                    self.source_identity_digest,
                    declared_revision,
                    workspace_state,
                )
            )
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_unavailable_claims_incoherent"
            )
        clock_skew = abs((received_at - observed_at).total_seconds())
        if clock_skew > DEFAULT_CODE_TRACEABILITY_LIMITS.observed_at_clock_skew_seconds:
            raise CodeTraceabilityContractError(
                "code_investigation_observed_at_clock_skew"
            )
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "expires_at", expires_at)
        for name in ("observation_sha256", "payload_sha256"):
            object.__setattr__(
                self,
                name,
                _sha256(
                    getattr(self, name),
                    f"code_investigation_{name}_invalid",
                ),
            )
        if contextual_outcome is None:
            expected_observation_sha256 = code_investigation_observation_sha256(
                source_ref=self.source_ref,
                selector_scope_digest=self.selector_scope_digest,
                outcome=outcome,
                capabilities=self.capabilities,
                source_identity_digest=self.source_identity_digest,
                declared_revision=declared_revision,
                workspace_state=workspace_state,
                omission_manifest=omissions,
            )
        else:
            expected_observation_sha256 = code_investigation_observation_sha256_v2(
                source_ref=self.source_ref,
                selector_scope_digest=self.selector_scope_digest,
                delivery_context=delivery_context,
                outcome=contextual_outcome,
                capabilities=self.capabilities,
                source_identity_digest=self.source_identity_digest,
                declared_revision=declared_revision,
                workspace_state=workspace_state,
                omission_manifest=omissions,
            )
        if self.observation_sha256 != expected_observation_sha256:
            raise CodeInvestigationPayloadDigestMismatch(
                details={"field": "observation_sha256"}
            )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "code_investigation_idempotency_key_invalid",
            ),
        )
        envelope: object = self
        if contextual_outcome is None:
            # Preserve the byte budget of the historical V1 aggregate. The
            # additive V2 fields do not retroactively enlarge legacy receipts.
            envelope = {
                field.name: getattr(self, field.name)
                for field in fields(self)
                if field.name
                not in {
                    "delivery_context",
                    "contextual_outcome",
                    "context_contract_version",
                }
            }
        _enforce_envelope_size(
            envelope,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.receipt_envelope_bytes,
            envelope="receipt",
        )

    @property
    def effective_outcome(
        self,
    ) -> CodeInvestigationOutcome | ContextualInvestigationOutcomeV2:
        """Return the authored outcome while keeping legacy receipts readable."""

        return self.contextual_outcome or self.outcome


@dataclass(frozen=True, slots=True)
class CodeInvestigationReceiptRevocation:
    id: str
    receipt_id: str
    board_id: str
    reason_code: str
    justification: str
    revoked_by: str
    revoked_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "receipt_id",
            "board_id",
            "reason_code",
            "justification",
            "revoked_by",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_investigation_revocation_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "revoked_at",
            _aware_utc(
                self.revoked_at,
                "code_investigation_revocation_revoked_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class CodeInvestigationHead:
    board_id: str
    source_ref: str
    generation: int
    latest_receipt_id: str
    current_receipt_id: str | None
    state: CodeInvestigationHeadState
    revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "code_investigation_head_board_id_invalid"),
        )
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        object.__setattr__(
            self,
            "generation",
            _positive_int(
                self.generation,
                "code_investigation_head_generation_invalid",
            ),
        )
        object.__setattr__(
            self,
            "latest_receipt_id",
            _required_text(
                self.latest_receipt_id,
                "code_investigation_head_latest_receipt_id_invalid",
            ),
        )
        current_receipt_id = _optional_text(
            self.current_receipt_id,
            "code_investigation_head_current_receipt_id_invalid",
        )
        state = _enum(
            self.state,
            CodeInvestigationHeadState,
            "code_investigation_head_state_invalid",
        )
        if state is CodeInvestigationHeadState.CURRENT and current_receipt_id is None:
            raise CodeTraceabilityContractError(
                "code_investigation_head_current_receipt_required"
            )
        object.__setattr__(self, "current_receipt_id", current_receipt_id)
        object.__setattr__(self, "state", state)
        object.__setattr__(
            self,
            "revision",
            _positive_int(self.revision, "code_investigation_head_revision_invalid"),
        )
        object.__setattr__(
            self,
            "updated_at",
            _aware_utc(
                self.updated_at,
                "code_investigation_head_updated_at_invalid",
            ),
        )


def code_investigation_receipt_currentness(
    receipt: CodeInvestigationReceipt,
    *,
    head: CodeInvestigationHead | None,
    at: datetime,
    revocation: CodeInvestigationReceiptRevocation | None = None,
    expected_delivery_context: DeliveryContext | None = None,
) -> CodeInvestigationReceiptCurrentness:
    """Classify ledger currentness without probing the underlying source."""

    if not isinstance(receipt, CodeInvestigationReceipt):
        raise CodeTraceabilityContractError("code_investigation_receipt_invalid")
    evaluated_at = _aware_utc(at, "code_investigation_currentness_at_invalid")
    if expected_delivery_context is not None:
        expected_delivery_context = _enum(
            expected_delivery_context,
            DeliveryContext,
            "code_delivery_context_required",
        )
    if revocation is not None:
        if (
            not isinstance(revocation, CodeInvestigationReceiptRevocation)
            or revocation.board_id != receipt.board_id
            or revocation.receipt_id != receipt.id
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_revocation_receipt_mismatch"
            )
        return CodeInvestigationReceiptCurrentness.REVOKED
    if evaluated_at >= receipt.expires_at:
        return CodeInvestigationReceiptCurrentness.EXPIRED
    if head is None:
        return CodeInvestigationReceiptCurrentness.UNKNOWN
    if (
        not isinstance(head, CodeInvestigationHead)
        or head.board_id != receipt.board_id
        or head.source_ref != receipt.source_ref
    ):
        raise CodeInvestigationSourceScopeMismatch()
    if head.state is CodeInvestigationHeadState.CONFLICTED:
        return CodeInvestigationReceiptCurrentness.CONFLICTED
    if (
        expected_delivery_context is not None
        and receipt.delivery_context is not None
        and receipt.delivery_context is not expected_delivery_context
    ):
        return CodeInvestigationReceiptCurrentness.OUTDATED
    if head.current_receipt_id == receipt.id:
        return CodeInvestigationReceiptCurrentness.CURRENT
    return CodeInvestigationReceiptCurrentness.OUTDATED


@dataclass(frozen=True, slots=True)
class CodeInvestigationReceiptCommitResult:
    request: CodeInvestigationRequest
    receipt: CodeInvestigationReceipt
    head: CodeInvestigationHead
    replayed: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request, CodeInvestigationRequest)
            or not isinstance(self.receipt, CodeInvestigationReceipt)
            or not isinstance(self.head, CodeInvestigationHead)
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_commit_result_invalid"
            )
        if (
            self.request.id != self.receipt.request_id
            or self.request.board_id != self.receipt.board_id
            or self.request.subject_type != self.receipt.subject_type
            or self.request.subject_id != self.receipt.subject_id
            or self.request.subject_version != self.receipt.subject_version
            or self.request.source_ref != self.receipt.source_ref
            or self.head.board_id != self.receipt.board_id
            or self.head.source_ref != self.receipt.source_ref
            or self.head.latest_receipt_id != self.receipt.id
            or self.head.generation != self.receipt.generation
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_commit_result_mismatch"
            )
        object.__setattr__(
            self,
            "replayed",
            _strict_bool(self.replayed, "code_investigation_replayed_invalid"),
        )


def _optional_excerpt(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CodeTraceabilityContractError("code_evidence_excerpt_invalid")
    normalized = unicodedata.normalize("NFC", value).replace("\r\n", "\n")
    normalized = normalized.replace("\r", "\n")
    if not normalized or any(
        ord(character) < 32 and character not in {"\t", "\n"}
        for character in normalized
    ):
        raise CodeTraceabilityContractError("code_evidence_excerpt_invalid")
    if (
        len(normalized.encode("utf-8"))
        > DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_excerpt_bytes
    ):
        raise CodeInvestigationSubmissionLimitExceeded(
            details={
                "field": "excerpt",
                "max_bytes": (DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_excerpt_bytes),
            }
        )
    return normalized


@dataclass(frozen=True, slots=True)
class CodeEvidence:
    id: str
    board_id: str
    investigation_receipt_id: str
    source_ref: str
    parent_type: CodeTraceabilitySubjectType
    parent_id: str
    parent_version: int | None
    evidence_type: CodeEvidenceType
    claim: str
    workspace_state: ObservedWorkspaceStateRef
    selector_kind: CodeEvidenceSelectorKind
    relative_path: str | None
    language: str | None
    symbol_kind: str | None
    qualified_symbol: str | None
    symbol_signature: str | None
    snapshot_line_start: int | None
    snapshot_line_end: int | None
    excerpt: str | None
    excerpt_sha256: str | None
    declared_file_blob_sha256: str | None
    declared_source_content_sha256: str
    excerpt_omitted_reason: str | None
    attestation_state: CodeEvidenceAttestationState
    attestation_basis: CodeEvidenceAttestationBasis
    lifecycle_status: CodeTraceabilityLifecycleStatus
    supersedes_evidence_id: str | None
    revocation_reason: str | None
    submitted_by: str
    received_at: datetime
    payload_sha256: str
    idempotency_key: str
    source_role: CodeEvidenceSourceRole = CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY
    relevance_summary: str | None = None
    scope_relation: str | None = None
    source_origin: str | None = None
    interpretation_limit: str | None = None
    baseline_provenance: CodeEvidenceBaselineProvenance | None = None
    context_contract_version: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "id",
            "board_id",
            "investigation_receipt_id",
            "parent_id",
            "claim",
            "submitted_by",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_evidence_{name}_invalid",
                ),
            )
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        parent_type = _enum(
            self.parent_type,
            CodeTraceabilitySubjectType,
            "code_evidence_parent_type_invalid",
        )
        object.__setattr__(self, "parent_type", parent_type)
        parent_version = (
            None
            if self.parent_version is None
            else _positive_int(
                self.parent_version,
                "code_evidence_parent_version_invalid",
            )
        )
        if (
            parent_type is CodeTraceabilitySubjectType.REFINEMENT
            and parent_version is None
        ):
            raise CodeTraceabilityContractError(
                "code_evidence_refinement_version_required"
            )
        object.__setattr__(self, "parent_version", parent_version)
        object.__setattr__(
            self,
            "evidence_type",
            _enum(
                self.evidence_type,
                CodeEvidenceType,
                "code_evidence_type_invalid",
            ),
        )
        if not isinstance(self.workspace_state, ObservedWorkspaceStateRef):
            raise CodeEvidenceAttestationRequired(details={"field": "workspace_state"})
        selector_kind = _enum(
            self.selector_kind,
            CodeEvidenceSelectorKind,
            "code_evidence_selector_kind_invalid",
        )
        object.__setattr__(self, "selector_kind", selector_kind)
        relative_path = (
            None
            if self.relative_path is None
            else normalize_code_relative_path(self.relative_path)
        )
        object.__setattr__(self, "relative_path", relative_path)
        for name in ("language", "symbol_kind"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"code_evidence_{name}_invalid",
                ),
            )
        for name in ("qualified_symbol", "symbol_signature"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"code_evidence_{name}_invalid",
                    max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
                ),
            )
        if (
            selector_kind is CodeEvidenceSelectorKind.SYMBOL
            and self.qualified_symbol is None
        ):
            raise CodeTraceabilityContractError(
                "code_evidence_qualified_symbol_required"
            )
        if selector_kind is CodeEvidenceSelectorKind.FILE and relative_path is None:
            raise CodeTraceabilityContractError("code_evidence_relative_path_required")
        line_start, line_end = _line_pair(
            self.snapshot_line_start,
            self.snapshot_line_end,
            "code_evidence_snapshot_lines_invalid",
        )
        if line_start is not None and relative_path is None:
            raise CodeTraceabilityContractError("code_evidence_snapshot_path_required")
        object.__setattr__(self, "snapshot_line_start", line_start)
        object.__setattr__(self, "snapshot_line_end", line_end)
        excerpt = _optional_excerpt(self.excerpt)
        excerpt_sha256 = _optional_sha256(
            self.excerpt_sha256,
            "code_evidence_excerpt_sha256_invalid",
        )
        if excerpt is None:
            if excerpt_sha256 is not None:
                raise CodeTraceabilityContractError(
                    "code_evidence_excerpt_hash_incoherent"
                )
        else:
            actual_excerpt_sha256 = hashlib.sha256(excerpt.encode("utf-8")).hexdigest()
            if excerpt_sha256 != actual_excerpt_sha256:
                raise CodeInvestigationPayloadDigestMismatch(
                    details={"field": "excerpt_sha256"}
                )
        object.__setattr__(self, "excerpt", excerpt)
        object.__setattr__(self, "excerpt_sha256", excerpt_sha256)
        object.__setattr__(
            self,
            "declared_file_blob_sha256",
            _optional_sha256(
                self.declared_file_blob_sha256,
                "code_evidence_declared_file_blob_sha256_invalid",
            ),
        )
        object.__setattr__(
            self,
            "declared_source_content_sha256",
            _sha256(
                self.declared_source_content_sha256,
                "code_evidence_declared_source_content_sha256_invalid",
            ),
        )
        omitted_reason = _optional_text(
            self.excerpt_omitted_reason,
            "code_evidence_excerpt_omitted_reason_invalid",
        )
        if excerpt is not None and omitted_reason is not None:
            raise CodeTraceabilityContractError(
                "code_evidence_excerpt_omission_incoherent"
            )
        object.__setattr__(self, "excerpt_omitted_reason", omitted_reason)
        attestation_state = _enum(
            self.attestation_state,
            CodeEvidenceAttestationState,
            "code_evidence_attestation_state_invalid",
        )
        expected_attestation = (
            CodeEvidenceAttestationState.AGENT_ATTESTED_WORKTREE
            if self.workspace_state.declared_dirty
            else CodeEvidenceAttestationState.AGENT_ATTESTED
        )
        if attestation_state is not expected_attestation:
            raise CodeEvidenceAttestationRequired(
                details={"reason": "workspace_attestation_mismatch"}
            )
        object.__setattr__(self, "attestation_state", attestation_state)
        basis = _enum(
            self.attestation_basis,
            CodeEvidenceAttestationBasis,
            "code_evidence_attestation_basis_invalid",
        )
        if basis is not CodeEvidenceAttestationBasis.AUTHENTICATED_AGENT_RECEIPT:
            raise CodeEvidenceAttestationRequired()
        object.__setattr__(self, "attestation_basis", basis)
        lifecycle = _enum(
            self.lifecycle_status,
            CodeTraceabilityLifecycleStatus,
            "code_evidence_lifecycle_status_invalid",
        )
        object.__setattr__(self, "lifecycle_status", lifecycle)
        supersedes = _optional_text(
            self.supersedes_evidence_id,
            "code_evidence_supersedes_evidence_id_invalid",
        )
        if supersedes == self.id:
            raise CodeTraceabilityContractError("code_evidence_cannot_supersede_self")
        object.__setattr__(self, "supersedes_evidence_id", supersedes)
        revocation_reason = _optional_text(
            self.revocation_reason,
            "code_evidence_revocation_reason_invalid",
        )
        if (lifecycle is CodeTraceabilityLifecycleStatus.REVOKED) != (
            revocation_reason is not None
        ):
            raise CodeTraceabilityContractError("code_evidence_revocation_incoherent")
        object.__setattr__(self, "revocation_reason", revocation_reason)
        object.__setattr__(
            self,
            "received_at",
            _aware_utc(self.received_at, "code_evidence_received_at_invalid"),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "code_evidence_payload_sha256_invalid"),
        )
        source_role = _enum(
            self.source_role,
            CodeEvidenceSourceRole,
            "code_evidence_source_role_required",
        )
        contextual_values = {
            "relevance_summary": self.relevance_summary,
            "scope_relation": self.scope_relation,
            "source_origin": self.source_origin,
            "interpretation_limit": self.interpretation_limit,
            "baseline_provenance": self.baseline_provenance,
        }
        if source_role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY:
            if (
                self.context_contract_version is not None
                or any(value is not None for value in contextual_values.values())
            ):
                raise CodeEvidenceLegacyRoleWriteForbidden()
            object.__setattr__(self, "source_role", source_role)
        else:
            if self.context_contract_version != 2:
                raise CodeTraceabilityContractError(
                    "code_evidence_context_contract_version_invalid"
                )
            source_role = authored_code_evidence_source_role(source_role)
            object.__setattr__(self, "source_role", source_role)
            for name in ("relevance_summary", "scope_relation", "source_origin"):
                value = _required_text(
                    getattr(self, name),
                    f"code_evidence_{name}_required",
                )
                object.__setattr__(self, name, value)
            interpretation_limit = _optional_text(
                self.interpretation_limit,
                "code_evidence_interpretation_limit_invalid",
            )
            if (
                source_role
                in {
                    CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
                    CodeEvidenceSourceRole.REFERENCE_PATTERN,
                }
                and interpretation_limit is None
            ):
                raise CodeEvidenceInterpretationLimitRequired(
                    details={"source_role": source_role.value}
                )
            object.__setattr__(
                self,
                "interpretation_limit",
                interpretation_limit,
            )
            if not isinstance(
                self.baseline_provenance,
                CodeEvidenceBaselineProvenance,
            ):
                raise CodeEvidenceBaselineProvenanceInvalid()
            if (
                self.baseline_provenance.workspace_state_id
                != self.workspace_state.workspace_state_id
            ):
                raise CodeEvidencePostBaselineSourceForbidden(
                    details={"reason": "workspace_state_mismatch"}
                )
            baseline_is_worktree = (
                self.baseline_provenance.presence
                is CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            )
            if baseline_is_worktree is not self.workspace_state.declared_dirty:
                raise CodeEvidenceBaselineProvenanceInvalid(
                    details={"reason": "workspace_presence_mismatch"}
                )
        _enforce_envelope_size(
            self,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_envelope_bytes,
            envelope="evidence",
        )

    @property
    def content_sha256(self) -> str:
        """Immutable Evidence content digest used by snapshots and Spec links."""

        return self.payload_sha256


def source_context_evidence_item_v2(
    evidence: CodeEvidence,
    classification: CodeEvidenceLegacyClassification | None = None,
    *,
    include_classification_actor: bool = False,
) -> SourceContextEvidenceItemV2:
    """Resolve authored/legacy context without guessing from code coordinates."""

    if not isinstance(evidence, CodeEvidence):
        raise CodeTraceabilityContractError("source_context_evidence_invalid")
    if classification is not None:
        if (
            not isinstance(classification, CodeEvidenceLegacyClassification)
            or classification.board_id != evidence.board_id
            or classification.evidence_id != evidence.id
            or classification.evidence_payload_sha256 != evidence.payload_sha256
            or evidence.source_role
            is not CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY
        ):
            raise CodeEvidenceLegacyClassificationPayloadConflict(
                details={"evidence_id": evidence.id}
            )
        return SourceContextEvidenceItemV2(
            evidence_id=evidence.id,
            source_role=classification.source_role,
            relevance_summary=classification.relevance_summary,
            scope_relation=classification.scope_relation,
            source_origin=classification.source_origin,
            interpretation_limit=classification.interpretation_limit,
            baseline_provenance=classification.baseline_provenance,
            context_origin=(
                CodeEvidenceContextOrigin.HUMAN_LEGACY_CLASSIFICATION
            ),
            context_contract_version=2,
            classification_revision=classification.revision,
            classification_sha256=classification.classification_sha256,
            classification_id=classification.id,
            classified_by=(
                classification.classified_by
                if include_classification_actor
                else None
            ),
            classified_at=(
                classification.classified_at
                if include_classification_actor
                else None
            ),
        )
    if evidence.source_role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY:
        return SourceContextEvidenceItemV2(
            evidence_id=evidence.id,
            source_role=evidence.source_role,
            relevance_summary=None,
            scope_relation=None,
            source_origin=None,
            interpretation_limit=None,
            baseline_provenance=None,
            context_origin=CodeEvidenceContextOrigin.UNCLASSIFIED_LEGACY,
            context_contract_version=None,
        )
    return SourceContextEvidenceItemV2(
        evidence_id=evidence.id,
        source_role=evidence.source_role,
        relevance_summary=evidence.relevance_summary,
        scope_relation=evidence.scope_relation,
        source_origin=evidence.source_origin,
        interpretation_limit=evidence.interpretation_limit,
        baseline_provenance=evidence.baseline_provenance,
        context_origin=CodeEvidenceContextOrigin.AUTHORED,
        context_contract_version=2,
    )


def source_context_classification_input_v2(
    evidence: CodeEvidence,
    classification: CodeEvidenceLegacyClassification | None = None,
) -> SourceContextClassificationInputV2:
    """Build the only client-safe command defaults for legacy Evidence."""

    if (
        not isinstance(evidence, CodeEvidence)
        or evidence.source_role is not CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY
    ):
        raise CodeTraceabilityContractError(
            "source_context_classification_input_legacy_evidence_required"
        )
    if classification is not None:
        # Reuse the effective-context resolver as the single payload/scope guard.
        source_context_evidence_item_v2(evidence, classification)
        presence = classification.baseline_provenance.presence
        workspace_state_id = (
            classification.baseline_provenance.workspace_state_id
        )
        provenance_note = classification.baseline_provenance.provenance_note
        revision = classification.revision
    else:
        presence = (
            CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            if evidence.workspace_state.declared_dirty
            else CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT
        )
        workspace_state_id = evidence.workspace_state.workspace_state_id
        provenance_note = None
        revision = 0
    return SourceContextClassificationInputV2(
        evidence_id=evidence.id,
        expected_evidence_payload_sha256=evidence.payload_sha256,
        expected_classification_revision=revision,
        baseline_provenance=SourceContextClassificationBaselineInputV2(
            presence=presence,
            workspace_state_id=workspace_state_id,
            provenance_note=provenance_note,
            provenance_note_required=(
                presence is CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            ),
        ),
    )


def source_context_evidence_payload_v2(
    item: SourceContextEvidenceItemV2,
) -> dict[str, object]:
    """Canonical, actor-free contextual payload sealed by a snapshot."""

    if not isinstance(item, SourceContextEvidenceItemV2):
        raise CodeTraceabilityContractError("source_context_evidence_invalid")
    return {
        "context_contract_version": item.context_contract_version,
        "context_origin": item.context_origin.value,
        "source_role": item.source_role.value,
        "relevance_summary": item.relevance_summary,
        "scope_relation": item.scope_relation,
        "source_origin": item.source_origin,
        "interpretation_limit": item.interpretation_limit,
        "baseline_provenance": item.baseline_provenance,
    }


def source_context_classification_fence_v2(
    classifications: Sequence[CodeEvidenceLegacyClassification],
) -> SourceContextClassificationFenceV2:
    """Build a deterministic aggregate fence over current per-Evidence heads."""

    items = tuple(classifications)
    if any(not isinstance(item, CodeEvidenceLegacyClassification) for item in items):
        raise CodeTraceabilityContractError(
            "source_context_classifications_invalid"
        )
    if not items:
        return SourceContextClassificationFenceV2()
    ordered = tuple(sorted(items, key=lambda item: item.evidence_id))
    if len({item.evidence_id for item in ordered}) != len(ordered):
        raise CodeTraceabilityContractError(
            "source_context_classifications_invalid"
        )
    return SourceContextClassificationFenceV2(
        revision=sum(item.revision for item in ordered),
        payload_sha256=canonical_code_traceability_sha256(
            [
                {
                    "evidence_id": item.evidence_id,
                    "revision": item.revision,
                    "classification_sha256": item.classification_sha256,
                }
                for item in ordered
            ]
        ),
    )


SOURCE_CONTEXT_INTERPRETATION_RULE_V2 = (
    "Only current implementation Evidence represents existing delivered behavior; "
    "all other source roles provide context only."
)


def parse_refinement_source_context_manifest_v2(
    value: object,
) -> RefinementSourceContextManifestV2:
    """Parse one canonical frozen Refinement manifest without source access.

    The parser deliberately accepts only the public persisted payload. It does
    not consult Evidence, receipt heads, a repository, or any other mutable
    authority, so callers can verify an immutable snapshot before deciding how
    to project or rebase it.
    """

    if not isinstance(value, Mapping):
        raise CodeTraceabilityContractError(
            "source_context_manifest_structure_invalid"
        )

    provenance_raw = value.get("delivery_context_provenance")
    provenance = None
    if provenance_raw is not None:
        if not isinstance(provenance_raw, Mapping):
            raise CodeTraceabilityContractError(
                "source_context_manifest_provenance_invalid"
            )
        provenance = RefinementDeliveryContextProvenance(
            value=provenance_raw.get("value"),
            source_refinement_id=provenance_raw.get("source_refinement_id"),
            source_refinement_version=provenance_raw.get(
                "source_refinement_version"
            ),
        )

    role_counts_raw = value.get("role_counts")
    classification_state_raw = value.get("classification_state")
    classification_fence_raw = value.get("classification_fence")
    receipts_raw = value.get("current_receipts")
    if (
        not isinstance(role_counts_raw, Mapping)
        or not isinstance(classification_state_raw, Mapping)
        or not isinstance(classification_fence_raw, Mapping)
        or isinstance(receipts_raw, str | bytes)
        or not isinstance(receipts_raw, Sequence)
    ):
        raise CodeTraceabilityContractError(
            "source_context_manifest_structure_invalid"
        )

    current_receipts: list[SourceContextCurrentReceiptV2] = []
    for item in receipts_raw:
        if not isinstance(item, Mapping):
            raise CodeTraceabilityContractError(
                "source_context_manifest_receipts_invalid"
            )
        current_receipts.append(
            SourceContextCurrentReceiptV2(
                receipt_id=item.get("receipt_id"),
                source_ref=item.get("source_ref"),
                generation=item.get("generation"),
                head_revision=item.get("head_revision"),
                payload_sha256=item.get("payload_sha256"),
                delivery_context=item.get("delivery_context"),
                contextual_outcome=item.get("contextual_outcome"),
                context_contract_version=item.get("context_contract_version"),
            )
        )

    manifest = RefinementSourceContextManifestV2(
        refinement_id=value.get("subject_id"),
        refinement_version=value.get("subject_version"),
        summary=SourceContextSummaryV2(
            delivery_context=value.get("delivery_context"),
            delivery_context_provenance=provenance,
            investigation_outcome=value.get("investigation_outcome"),
            role_counts=SourceContextRoleCountsV2(
                current_implementation_count=role_counts_raw.get(
                    "current_implementation_count"
                ),
                existing_scaffold_count=role_counts_raw.get(
                    "existing_scaffold_count"
                ),
                existing_constraint_count=role_counts_raw.get(
                    "existing_constraint_count"
                ),
                reference_pattern_count=role_counts_raw.get(
                    "reference_pattern_count"
                ),
                uncategorized_legacy_count=role_counts_raw.get(
                    "uncategorized_legacy_count"
                ),
            ),
            classification_state=SourceContextClassificationStateV2(
                classified_count=classification_state_raw.get("classified_count"),
                uncategorized_legacy_count=classification_state_raw.get(
                    "uncategorized_legacy_count"
                ),
            ),
            evidence_applicable=value.get("evidence_applicable"),
            interpretation_rule=value.get("interpretation_rule"),
            items_not_current_implementation_count=value.get(
                "items_not_current_implementation_count"
            ),
            technical_details_available=value.get("technical_details_available"),
        ),
        current_receipts=tuple(current_receipts),
        classification_fence=SourceContextClassificationFenceV2(
            revision=classification_fence_raw.get("revision"),
            payload_sha256=classification_fence_raw.get("payload_sha256"),
        ),
        contract_version=value.get("contract_version"),
    )
    if (
        value.get("subject_type")
        != CodeTraceabilitySubjectType.REFINEMENT.value
        or manifest.summary.interpretation_rule
        != SOURCE_CONTEXT_INTERPRETATION_RULE_V2
        or dict(value) != manifest.as_dict()
    ):
        raise CodeTraceabilityContractError(
            "source_context_manifest_structure_invalid"
        )
    return manifest

_SOURCE_CONTEXT_OUTCOME_PRECEDENCE: Mapping[ContextualInvestigationOutcomeV2, int] = (
    MappingProxyType(
        {
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION: 0,
            ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE: 1,
            ContextualInvestigationOutcomeV2.PARTIAL: 2,
            ContextualInvestigationOutcomeV2.UNAVAILABLE: 3,
        }
    )
)


def aggregate_current_contextual_investigation_outcome_v2(
    outcomes: Sequence[ContextualInvestigationOutcomeV2 | None],
) -> ContextualInvestigationOutcomeV2 | None:
    """Aggregate already-current authored outcomes with closed precedence.

    The caller owns current-head selection. ``None`` denotes a readable legacy
    receipt and never causes Core to infer a contextual outcome from V1 fields.
    """

    if isinstance(outcomes, str | bytes) or not isinstance(outcomes, Sequence):
        raise CodeTraceabilityContractError(
            "source_context_investigation_outcomes_invalid"
        )
    contextual: list[ContextualInvestigationOutcomeV2] = []
    for value in outcomes:
        if value is None:
            continue
        contextual.append(
            _enum(
                value,
                ContextualInvestigationOutcomeV2,
                "source_context_investigation_outcome_invalid",
            )
        )
    if not contextual:
        return None
    return max(
        contextual,
        key=_SOURCE_CONTEXT_OUTCOME_PRECEDENCE.__getitem__,
    )


def active_source_context_role_counts_v2(
    evidence: Sequence[CodeEvidence | SourceContextEvidenceItemV2],
) -> SourceContextRoleCountsV2:
    """Count factual source roles from active, already-scoped Evidence only."""

    if isinstance(evidence, str | bytes) or not isinstance(evidence, Sequence):
        raise CodeTraceabilityContractError("source_context_evidence_invalid")
    counts = {role: 0 for role in CodeEvidenceSourceRole}
    for item in evidence:
        if not isinstance(item, CodeEvidence | SourceContextEvidenceItemV2):
            raise CodeTraceabilityContractError("source_context_evidence_invalid")
        if isinstance(item, SourceContextEvidenceItemV2) or (
            item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
        ):
            counts[item.source_role] += 1
    return SourceContextRoleCountsV2(
        current_implementation_count=counts[
            CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION
        ],
        existing_scaffold_count=counts[CodeEvidenceSourceRole.EXISTING_SCAFFOLD],
        existing_constraint_count=counts[CodeEvidenceSourceRole.EXISTING_CONSTRAINT],
        reference_pattern_count=counts[CodeEvidenceSourceRole.REFERENCE_PATTERN],
        uncategorized_legacy_count=counts[CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY],
    )


def build_source_context_summary_v2(
    *,
    delivery_context: DeliveryContext | None,
    delivery_context_provenance: (
        RefinementDeliveryContextProvenance
        | SpecDeliveryContextProvenance
        | DirectSpecDeliveryContextProvenance
        | None
    ),
    current_investigation_outcomes: Sequence[ContextualInvestigationOutcomeV2 | None],
    evidence: Sequence[CodeEvidence],
    classifications: Sequence[CodeEvidenceLegacyClassification] = (),
) -> SourceContextSummaryV2:
    """Build the source-blind canonical summary from server-selected facts."""

    outcome = aggregate_current_contextual_investigation_outcome_v2(
        current_investigation_outcomes
    )
    if isinstance(classifications, str | bytes) or not isinstance(
        classifications, Sequence
    ):
        raise CodeTraceabilityContractError("source_context_classifications_invalid")
    classifications_by_evidence: dict[str, CodeEvidenceLegacyClassification] = {}
    for item in classifications:
        if (
            not isinstance(item, CodeEvidenceLegacyClassification)
            or item.evidence_id in classifications_by_evidence
        ):
            raise CodeTraceabilityContractError(
                "source_context_classifications_invalid"
            )
        classifications_by_evidence[item.evidence_id] = item
    effective_items = tuple(
        source_context_evidence_item_v2(
            item,
            classifications_by_evidence.get(item.id),
        )
        for item in evidence
        if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
    )
    if set(classifications_by_evidence) - {
        item.evidence_id for item in effective_items
    }:
        raise CodeTraceabilityContractError("source_context_classifications_invalid")
    role_counts = active_source_context_role_counts_v2(effective_items)
    classification_state = SourceContextClassificationStateV2(
        classified_count=(
            role_counts.total_count - role_counts.uncategorized_legacy_count
        ),
        uncategorized_legacy_count=role_counts.uncategorized_legacy_count,
    )
    evidence_applicable = {
        ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE: True,
        ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION: False,
        ContextualInvestigationOutcomeV2.PARTIAL: None,
        ContextualInvestigationOutcomeV2.UNAVAILABLE: None,
        None: None,
    }[outcome]
    return SourceContextSummaryV2(
        delivery_context=delivery_context,
        delivery_context_provenance=delivery_context_provenance,
        investigation_outcome=outcome,
        role_counts=role_counts,
        classification_state=classification_state,
        evidence_applicable=evidence_applicable,
        interpretation_rule=SOURCE_CONTEXT_INTERPRETATION_RULE_V2,
        items_not_current_implementation_count=(
            role_counts.total_count - role_counts.current_implementation_count
        ),
        technical_details_available=role_counts.total_count > 0,
    )


@dataclass(frozen=True, slots=True)
class CodeEvidenceSpecLink:
    id: str
    board_id: str
    spec_id: str
    evidence_id: str
    entity_type: SpecEntityType
    entity_id: str
    relation_type: CodeEvidenceSpecRelationType
    rationale: str
    evidence_content_sha256: str
    source_refinement_version: int | None
    spec_version: int
    created_by: str
    created_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "board_id",
            "spec_id",
            "evidence_id",
            "entity_id",
            "rationale",
            "created_by",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_evidence_spec_link_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "entity_type",
            _enum(
                self.entity_type,
                SpecEntityType,
                "code_evidence_spec_link_entity_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "relation_type",
            _enum(
                self.relation_type,
                CodeEvidenceSpecRelationType,
                "code_evidence_spec_link_relation_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "evidence_content_sha256",
            _sha256(
                self.evidence_content_sha256,
                "code_evidence_spec_link_content_sha256_invalid",
            ),
        )
        object.__setattr__(
            self,
            "source_refinement_version",
            (
                None
                if self.source_refinement_version is None
                else _positive_int(
                    self.source_refinement_version,
                    "code_evidence_spec_link_refinement_version_invalid",
                )
            ),
        )
        object.__setattr__(
            self,
            "spec_version",
            _positive_int(
                self.spec_version,
                "code_evidence_spec_link_spec_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "code_evidence_spec_link_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class CodeEvidenceDisposition:
    id: str
    board_id: str
    spec_id: str
    evidence_id: str
    disposition: CodeEvidenceDispositionKind
    justification: str
    spec_version: int
    active: bool
    created_by: str
    created_at: datetime
    cleared_by: str | None
    cleared_at: datetime | None

    def __post_init__(self) -> None:
        for name in (
            "id",
            "board_id",
            "spec_id",
            "evidence_id",
            "justification",
            "created_by",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_evidence_disposition_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "disposition",
            _enum(
                self.disposition,
                CodeEvidenceDispositionKind,
                "code_evidence_disposition_kind_invalid",
            ),
        )
        object.__setattr__(
            self,
            "spec_version",
            _positive_int(
                self.spec_version,
                "code_evidence_disposition_spec_version_invalid",
            ),
        )
        active = _strict_bool(
            self.active,
            "code_evidence_disposition_active_invalid",
        )
        object.__setattr__(self, "active", active)
        created_at = _aware_utc(
            self.created_at,
            "code_evidence_disposition_created_at_invalid",
        )
        object.__setattr__(self, "created_at", created_at)
        cleared_by = _optional_text(
            self.cleared_by,
            "code_evidence_disposition_cleared_by_invalid",
        )
        cleared_at = (
            None
            if self.cleared_at is None
            else _aware_utc(
                self.cleared_at,
                "code_evidence_disposition_cleared_at_invalid",
            )
        )
        if active == (cleared_by is not None or cleared_at is not None):
            raise CodeTraceabilityContractError(
                "code_evidence_disposition_clearance_incoherent"
            )
        if not active and (cleared_by is None or cleared_at is None):
            raise CodeTraceabilityContractError(
                "code_evidence_disposition_clearance_incoherent"
            )
        if cleared_at is not None and cleared_at < created_at:
            raise CodeTraceabilityContractError(
                "code_evidence_disposition_cleared_at_invalid"
            )
        object.__setattr__(self, "cleared_by", cleared_by)
        object.__setattr__(self, "cleared_at", cleared_at)


@dataclass(frozen=True, slots=True)
class ImplementationTarget:
    id: str
    board_id: str
    card_id: str
    source_ref: str
    selector_kind: ImplementationTargetSelectorKind
    relative_path_hint: str | None
    language: str | None
    symbol_kind: str | None
    qualified_symbol: str | None
    symbol_signature: str | None
    role: ImplementationTargetRole
    intent: str
    required: bool
    source_spec_version: int
    baseline_evidence_id: str | None
    lifecycle_status: CodeTraceabilityLifecycleStatus
    revision: int
    current_resolution_id: str | None
    last_change_reason_sha256: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "board_id", "card_id", "intent", "created_by"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"implementation_target_{name}_invalid",
                ),
            )
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        selector_kind = _enum(
            self.selector_kind,
            ImplementationTargetSelectorKind,
            "implementation_target_selector_kind_invalid",
        )
        object.__setattr__(self, "selector_kind", selector_kind)
        relative_path_hint = (
            None
            if self.relative_path_hint is None
            else normalize_code_relative_path(self.relative_path_hint)
        )
        object.__setattr__(self, "relative_path_hint", relative_path_hint)
        for name in ("language", "symbol_kind"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"implementation_target_{name}_invalid",
                ),
            )
        for name in ("qualified_symbol", "symbol_signature"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"implementation_target_{name}_invalid",
                    max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
                ),
            )
        if (
            selector_kind is ImplementationTargetSelectorKind.SYMBOL
            and self.qualified_symbol is None
        ):
            raise ImplementationTargetInvalid(details={"field": "qualified_symbol"})
        if (
            selector_kind
            in {
                ImplementationTargetSelectorKind.FILE,
                ImplementationTargetSelectorKind.NEW_FILE,
            }
            and relative_path_hint is None
        ):
            raise ImplementationTargetInvalid(details={"field": "relative_path_hint"})
        object.__setattr__(
            self,
            "role",
            _enum(
                self.role,
                ImplementationTargetRole,
                "implementation_target_role_invalid",
            ),
        )
        object.__setattr__(
            self,
            "required",
            _strict_bool(self.required, "implementation_target_required_invalid"),
        )
        object.__setattr__(
            self,
            "source_spec_version",
            _positive_int(
                self.source_spec_version,
                "implementation_target_source_spec_version_invalid",
            ),
        )
        for name in ("baseline_evidence_id", "current_resolution_id"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"implementation_target_{name}_invalid",
                ),
            )
        lifecycle = _enum(
            self.lifecycle_status,
            CodeTraceabilityLifecycleStatus,
            "implementation_target_lifecycle_status_invalid",
        )
        object.__setattr__(self, "lifecycle_status", lifecycle)
        object.__setattr__(
            self,
            "revision",
            _positive_int(self.revision, "implementation_target_revision_invalid"),
        )
        object.__setattr__(
            self,
            "last_change_reason_sha256",
            _optional_sha256(
                self.last_change_reason_sha256,
                "implementation_target_change_reason_sha256_invalid",
            ),
        )
        created_at = _aware_utc(
            self.created_at,
            "implementation_target_created_at_invalid",
        )
        updated_at = _aware_utc(
            self.updated_at,
            "implementation_target_updated_at_invalid",
        )
        if updated_at < created_at:
            raise ImplementationTargetInvalid(details={"field": "updated_at"})
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)


@dataclass(frozen=True, slots=True)
class ImplementationTargetSpecLink:
    id: str
    target_id: str
    spec_id: str
    entity_type: SpecEntityType
    entity_id: str
    created_by: str
    created_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "target_id",
            "spec_id",
            "entity_id",
            "created_by",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"implementation_target_spec_link_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "entity_type",
            _enum(
                self.entity_type,
                SpecEntityType,
                "implementation_target_spec_link_entity_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "implementation_target_spec_link_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ImplementationTargetEvidenceLink:
    id: str
    target_id: str
    evidence_id: str
    relation_type: ImplementationTargetEvidenceRelationType
    created_by: str
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "target_id", "evidence_id", "created_by"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"implementation_target_evidence_link_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "relation_type",
            _enum(
                self.relation_type,
                ImplementationTargetEvidenceRelationType,
                "implementation_target_evidence_link_relation_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "implementation_target_evidence_link_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResolutionCandidate:
    relative_path: str
    qualified_symbol: str | None
    symbol_signature: str | None
    symbol_fingerprint: str | None
    confidence: float
    reason_code: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "relative_path",
            normalize_code_relative_path(self.relative_path),
        )
        for name in ("qualified_symbol", "symbol_signature"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"implementation_resolution_candidate_{name}_invalid",
                    max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
                ),
            )
        object.__setattr__(
            self,
            "symbol_fingerprint",
            _optional_sha256(
                self.symbol_fingerprint,
                "implementation_resolution_candidate_fingerprint_invalid",
            ),
        )
        object.__setattr__(
            self,
            "confidence",
            _confidence(
                self.confidence,
                "implementation_resolution_candidate_confidence_invalid",
            ),
        )
        object.__setattr__(
            self,
            "reason_code",
            _required_text(
                self.reason_code,
                "implementation_resolution_candidate_reason_code_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ImplementationTargetResolution:
    id: str
    board_id: str
    target_id: str
    investigation_receipt_id: str
    source_ref: str
    receipt_generation: int
    subject_version: int
    target_revision: int
    workspace_state: ObservedWorkspaceStateRef
    state: ImplementationTargetResolutionState
    resolved_relative_path: str | None
    resolved_language: str | None
    resolved_symbol_kind: str | None
    resolved_qualified_symbol: str | None
    resolved_symbol_signature: str | None
    resolved_line_start: int | None
    resolved_line_end: int | None
    symbol_fingerprint: str | None
    declared_file_blob_sha256: str | None
    selector_fingerprint: str
    confidence: float | None
    reason_code: str | None
    candidate_count: int
    candidates: tuple[ResolutionCandidate, ...]
    declared_tool_id: str
    declared_tool_version: str
    submitted_by: str
    agent_observed_at: datetime
    received_at: datetime
    payload_sha256: str
    idempotency_key: str

    def __post_init__(self) -> None:
        for name in (
            "id",
            "board_id",
            "target_id",
            "investigation_receipt_id",
            "declared_tool_id",
            "declared_tool_version",
            "submitted_by",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"implementation_target_resolution_{name}_invalid",
                ),
            )
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        for name in (
            "receipt_generation",
            "subject_version",
            "target_revision",
        ):
            object.__setattr__(
                self,
                name,
                _positive_int(
                    getattr(self, name),
                    f"implementation_target_resolution_{name}_invalid",
                ),
            )
        if not isinstance(self.workspace_state, ObservedWorkspaceStateRef):
            raise CodeEvidenceAttestationRequired(details={"field": "workspace_state"})
        state = _enum(
            self.state,
            ImplementationTargetResolutionState,
            "implementation_target_resolution_state_invalid",
        )
        object.__setattr__(self, "state", state)
        resolved_relative_path = (
            None
            if self.resolved_relative_path is None
            else normalize_code_relative_path(self.resolved_relative_path)
        )
        object.__setattr__(
            self,
            "resolved_relative_path",
            resolved_relative_path,
        )
        for name in ("resolved_language", "resolved_symbol_kind"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"implementation_target_resolution_{name}_invalid",
                ),
            )
        for name in ("resolved_qualified_symbol", "resolved_symbol_signature"):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"implementation_target_resolution_{name}_invalid",
                    max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
                ),
            )
        line_start, line_end = _line_pair(
            self.resolved_line_start,
            self.resolved_line_end,
            "implementation_target_resolution_lines_invalid",
        )
        if line_start is not None and resolved_relative_path is None:
            raise CodeTraceabilityContractError(
                "implementation_target_resolution_path_required"
            )
        object.__setattr__(self, "resolved_line_start", line_start)
        object.__setattr__(self, "resolved_line_end", line_end)
        object.__setattr__(
            self,
            "symbol_fingerprint",
            _optional_sha256(
                self.symbol_fingerprint,
                "implementation_target_resolution_symbol_fingerprint_invalid",
            ),
        )
        object.__setattr__(
            self,
            "declared_file_blob_sha256",
            _optional_sha256(
                self.declared_file_blob_sha256,
                "implementation_target_resolution_file_blob_sha256_invalid",
            ),
        )
        object.__setattr__(
            self,
            "selector_fingerprint",
            _sha256(
                self.selector_fingerprint,
                "implementation_target_resolution_selector_fingerprint_invalid",
            ),
        )
        confidence = (
            None
            if self.confidence is None
            else _confidence(
                self.confidence,
                "implementation_target_resolution_confidence_invalid",
            )
        )
        object.__setattr__(self, "confidence", confidence)
        reason_code = _optional_text(
            self.reason_code,
            "implementation_target_resolution_reason_code_invalid",
        )
        object.__setattr__(self, "reason_code", reason_code)
        candidates = _typed_tuple(
            self.candidates,
            ResolutionCandidate,
            "implementation_target_resolution_candidates_invalid",
            max_items=DEFAULT_CODE_TRACEABILITY_LIMITS.resolution_candidates,
        )
        candidate_count = _non_negative_int(
            self.candidate_count,
            "implementation_target_resolution_candidate_count_invalid",
        )
        if candidate_count != len(candidates):
            raise CodeTraceabilityContractError(
                "implementation_target_resolution_candidate_count_mismatch"
            )
        candidate_keys = {
            (
                item.relative_path,
                item.qualified_symbol,
                item.symbol_signature,
            )
            for item in candidates
        }
        if len(candidate_keys) != len(candidates):
            raise CodeTraceabilityContractError(
                "implementation_target_resolution_candidate_duplicate"
            )
        object.__setattr__(self, "candidate_count", candidate_count)
        object.__setattr__(self, "candidates", candidates)
        has_resolved_coordinate = resolved_relative_path is not None
        if state in {
            ImplementationTargetResolutionState.RESOLVED,
            ImplementationTargetResolutionState.MOVED,
        }:
            if (
                not has_resolved_coordinate
                or confidence is None
                or confidence < 0.95
                or candidate_count > 1
            ):
                raise CodeTraceabilityContractError(
                    "implementation_target_resolution_resolved_threshold_invalid"
                )
        elif state is ImplementationTargetResolutionState.STALE:
            if (
                reason_code is None
                or not (has_resolved_coordinate or candidates)
                or confidence is None
                or not 0.80 <= confidence < 0.95
            ):
                raise CodeTraceabilityContractError(
                    "implementation_target_resolution_stale_threshold_invalid"
                )
        elif state is ImplementationTargetResolutionState.AMBIGUOUS:
            ranked_confidence = sorted(
                (item.confidence for item in candidates),
                reverse=True,
            )
            if (
                candidate_count < 2
                or has_resolved_coordinate
                or confidence is not None
                or ranked_confidence[0] - ranked_confidence[1] > 0.05
            ):
                raise CodeTraceabilityContractError(
                    "implementation_target_resolution_ambiguous_threshold_invalid"
                )
        elif state in {
            ImplementationTargetResolutionState.MISSING,
            ImplementationTargetResolutionState.UNAVAILABLE,
        }:
            if (
                reason_code is None
                or has_resolved_coordinate
                or candidate_count
                or confidence is not None
            ):
                raise CodeTraceabilityContractError(
                    "implementation_target_resolution_terminal_incoherent"
                )
            coordinate_values = (
                self.resolved_language,
                self.resolved_symbol_kind,
                self.resolved_qualified_symbol,
                self.resolved_symbol_signature,
                self.symbol_fingerprint,
                self.declared_file_blob_sha256,
            )
            if any(value is not None for value in coordinate_values):
                raise CodeTraceabilityContractError(
                    "implementation_target_resolution_terminal_incoherent"
                )
        agent_observed_at = _aware_utc(
            self.agent_observed_at,
            "implementation_target_resolution_agent_observed_at_invalid",
        )
        if agent_observed_at != self.workspace_state.observed_at:
            raise CodeTraceabilityContractError(
                "implementation_target_resolution_observed_at_mismatch"
            )
        object.__setattr__(self, "agent_observed_at", agent_observed_at)
        object.__setattr__(
            self,
            "received_at",
            _aware_utc(
                self.received_at,
                "implementation_target_resolution_received_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(
                self.payload_sha256,
                "implementation_target_resolution_payload_sha256_invalid",
            ),
        )
        _enforce_envelope_size(
            self,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.resolution_envelope_bytes,
            envelope="target_resolution",
        )


@dataclass(frozen=True, slots=True)
class ImplementationTargetExecutionRecord:
    id: str
    board_id: str
    card_id: str
    target_id: str
    target_revision: int
    result_investigation_receipt_id: str
    disposition: ImplementationTargetExecutionDisposition
    source_ref: str
    result_declared_revision: str | None
    result_workspace_state_id: str | None
    actual_relative_path: str | None
    actual_qualified_symbol: str | None
    replacement_target_id: str | None
    justification: str
    submitted_by: str
    received_at: datetime
    payload_sha256: str
    idempotency_key: str

    def __post_init__(self) -> None:
        for name in (
            "id",
            "board_id",
            "card_id",
            "target_id",
            "result_investigation_receipt_id",
            "justification",
            "submitted_by",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"implementation_target_execution_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "target_revision",
            _positive_int(
                self.target_revision,
                "implementation_target_execution_target_revision_invalid",
            ),
        )
        disposition = _enum(
            self.disposition,
            ImplementationTargetExecutionDisposition,
            "implementation_target_execution_disposition_invalid",
        )
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        object.__setattr__(
            self,
            "result_declared_revision",
            _optional_text(
                self.result_declared_revision,
                "implementation_target_execution_declared_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "result_workspace_state_id",
            _optional_text(
                self.result_workspace_state_id,
                "implementation_target_execution_workspace_state_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "actual_relative_path",
            (
                None
                if self.actual_relative_path is None
                else normalize_code_relative_path(self.actual_relative_path)
            ),
        )
        object.__setattr__(
            self,
            "actual_qualified_symbol",
            _optional_text(
                self.actual_qualified_symbol,
                "implementation_target_execution_qualified_symbol_invalid",
                max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            ),
        )
        replacement_target_id = _optional_text(
            self.replacement_target_id,
            "implementation_target_execution_replacement_target_id_invalid",
        )
        if disposition is ImplementationTargetExecutionDisposition.REPLACED:
            if replacement_target_id is None or replacement_target_id == self.target_id:
                raise TargetExecutionDispositionRequired(
                    details={"field": "replacement_target_id"}
                )
        elif replacement_target_id is not None:
            raise CodeTraceabilityContractError(
                "implementation_target_execution_replacement_incoherent"
            )
        object.__setattr__(self, "replacement_target_id", replacement_target_id)
        object.__setattr__(
            self,
            "received_at",
            _aware_utc(
                self.received_at,
                "implementation_target_execution_received_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(
                self.payload_sha256,
                "implementation_target_execution_payload_sha256_invalid",
            ),
        )
        _enforce_envelope_size(
            self,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.execution_envelope_bytes,
            envelope="execution_receipt",
        )


@dataclass(frozen=True, slots=True)
class TargetOverlapAcknowledgement:
    id: str
    board_id: str
    target_a_id: str
    target_b_id: str
    resolution_a_id: str
    resolution_b_id: str
    disposition: TargetOverlapDisposition
    justification: str
    created_by: str
    created_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "board_id",
            "target_a_id",
            "target_b_id",
            "resolution_a_id",
            "resolution_b_id",
            "justification",
            "created_by",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"target_overlap_acknowledgement_{name}_invalid",
                ),
            )
        if self.target_a_id == self.target_b_id:
            raise CodeTraceabilityContractError(
                "target_overlap_acknowledgement_targets_duplicate"
            )
        if self.resolution_a_id == self.resolution_b_id:
            raise CodeTraceabilityContractError(
                "target_overlap_acknowledgement_resolutions_duplicate"
            )
        if self.target_b_id < self.target_a_id:
            target_a_id, target_b_id = self.target_b_id, self.target_a_id
            resolution_a_id = self.resolution_b_id
            resolution_b_id = self.resolution_a_id
            object.__setattr__(self, "target_a_id", target_a_id)
            object.__setattr__(self, "target_b_id", target_b_id)
            object.__setattr__(self, "resolution_a_id", resolution_a_id)
            object.__setattr__(self, "resolution_b_id", resolution_b_id)
        object.__setattr__(
            self,
            "disposition",
            _enum(
                self.disposition,
                TargetOverlapDisposition,
                "target_overlap_acknowledgement_disposition_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "target_overlap_acknowledgement_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class CodeTraceabilityWaiver:
    id: str
    board_id: str
    entity_type: CodeTraceabilityWaiverEntityType
    entity_id: str
    scope: CodeTraceabilityWaiverScope
    reason_code: CodeTraceabilityWaiverReason
    justification: str
    active: bool
    created_by: str
    created_at: datetime
    cleared_by: str | None
    cleared_at: datetime | None

    def __post_init__(self) -> None:
        for name in ("id", "board_id", "entity_id", "justification", "created_by"):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"code_traceability_waiver_{name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "entity_type",
            _enum(
                self.entity_type,
                CodeTraceabilityWaiverEntityType,
                "code_traceability_waiver_entity_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "scope",
            _enum(
                self.scope,
                CodeTraceabilityWaiverScope,
                "code_traceability_waiver_scope_invalid",
            ),
        )
        object.__setattr__(
            self,
            "reason_code",
            _enum(
                self.reason_code,
                CodeTraceabilityWaiverReason,
                "code_traceability_waiver_reason_invalid",
            ),
        )
        active = _strict_bool(
            self.active,
            "code_traceability_waiver_active_invalid",
        )
        object.__setattr__(self, "active", active)
        created_at = _aware_utc(
            self.created_at,
            "code_traceability_waiver_created_at_invalid",
        )
        object.__setattr__(self, "created_at", created_at)
        cleared_by = _optional_text(
            self.cleared_by,
            "code_traceability_waiver_cleared_by_invalid",
        )
        cleared_at = (
            None
            if self.cleared_at is None
            else _aware_utc(
                self.cleared_at,
                "code_traceability_waiver_cleared_at_invalid",
            )
        )
        if active and (cleared_by is not None or cleared_at is not None):
            raise CodeTraceabilityContractError(
                "code_traceability_waiver_clearance_incoherent"
            )
        if not active and (cleared_by is None or cleared_at is None):
            raise CodeTraceabilityContractError(
                "code_traceability_waiver_clearance_incoherent"
            )
        if cleared_at is not None and cleared_at < created_at:
            raise CodeTraceabilityContractError(
                "code_traceability_waiver_cleared_at_invalid"
            )
        object.__setattr__(self, "cleared_by", cleared_by)
        object.__setattr__(self, "cleared_at", cleared_at)


@dataclass(frozen=True, slots=True)
class TargetOverlap:
    """Derived overlap projection; only its acknowledgement is persisted."""

    board_id: str
    target_a_id: str
    target_b_id: str
    resolution_a_id: str
    resolution_b_id: str
    severity: TargetOverlapSeverity
    reason_code: str
    relative_path: str | None = None
    qualified_symbol: str | None = None
    acknowledgement: TargetOverlapAcknowledgement | None = None

    def __post_init__(self) -> None:
        for name in (
            "board_id",
            "target_a_id",
            "target_b_id",
            "resolution_a_id",
            "resolution_b_id",
            "reason_code",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(
                    getattr(self, name),
                    f"target_overlap_{name}_invalid",
                ),
            )
        if self.target_a_id == self.target_b_id:
            raise CodeTraceabilityContractError("target_overlap_targets_duplicate")
        if self.target_b_id < self.target_a_id:
            target_a_id, target_b_id = self.target_b_id, self.target_a_id
            resolution_a_id = self.resolution_b_id
            resolution_b_id = self.resolution_a_id
            object.__setattr__(self, "target_a_id", target_a_id)
            object.__setattr__(self, "target_b_id", target_b_id)
            object.__setattr__(self, "resolution_a_id", resolution_a_id)
            object.__setattr__(self, "resolution_b_id", resolution_b_id)
        object.__setattr__(
            self,
            "severity",
            _enum(
                self.severity,
                TargetOverlapSeverity,
                "target_overlap_severity_invalid",
            ),
        )
        object.__setattr__(
            self,
            "relative_path",
            (
                None
                if self.relative_path is None
                else normalize_code_relative_path(self.relative_path)
            ),
        )
        object.__setattr__(
            self,
            "qualified_symbol",
            _optional_text(
                self.qualified_symbol,
                "target_overlap_qualified_symbol_invalid",
                max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.symbol_text_bytes,
            ),
        )
        if self.acknowledgement is not None:
            acknowledgement = self.acknowledgement
            if not isinstance(acknowledgement, TargetOverlapAcknowledgement):
                raise CodeTraceabilityContractError(
                    "target_overlap_acknowledgement_invalid"
                )
            if {
                acknowledgement.target_a_id,
                acknowledgement.target_b_id,
            } != {self.target_a_id, self.target_b_id} or {
                acknowledgement.resolution_a_id,
                acknowledgement.resolution_b_id,
            } != {self.resolution_a_id, self.resolution_b_id}:
                raise ImplementationOverlapAcknowledgementStale()


def classify_implementation_target_overlap(
    target_a: ImplementationTarget,
    resolution_a: ImplementationTargetResolution,
    target_b: ImplementationTarget,
    resolution_b: ImplementationTargetResolution,
) -> TargetOverlap:
    """Classify overlap solely from current agent-attested coordinates."""

    if not isinstance(target_a, ImplementationTarget) or not isinstance(
        target_b, ImplementationTarget
    ):
        raise ImplementationTargetInvalid()
    if not isinstance(resolution_a, ImplementationTargetResolution) or not isinstance(
        resolution_b, ImplementationTargetResolution
    ):
        raise ImplementationTargetResolutionRequired()
    if target_a.id == target_b.id:
        raise ImplementationTargetInvalid(details={"reason": "duplicate_target"})
    pairs = ((target_a, resolution_a), (target_b, resolution_b))
    for target, resolution in pairs:
        if (
            resolution.board_id != target.board_id
            or resolution.target_id != target.id
            or resolution.target_revision != target.revision
        ):
            raise ImplementationTargetResolutionOutdated(
                details={"target_id": target.id}
            )
    if target_a.board_id != target_b.board_id:
        raise CodeInvestigationSourceScopeMismatch(
            details={"reason": "cross_board_overlap"}
        )
    if target_b.id < target_a.id:
        target_a, target_b = target_b, target_a
        resolution_a, resolution_b = resolution_b, resolution_a
    same_source = target_a.source_ref == target_b.source_ref
    path_a = resolution_a.resolved_relative_path
    path_b = resolution_b.resolved_relative_path
    symbol_a = resolution_a.resolved_qualified_symbol
    symbol_b = resolution_b.resolved_qualified_symbol
    same_path = same_source and path_a is not None and path_a == path_b
    same_symbol = (
        same_source
        and symbol_a is not None
        and symbol_a == symbol_b
        and (same_path or path_a is None or path_b is None)
    )
    mutating_a = target_a.role in MUTATING_IMPLEMENTATION_TARGET_ROLES
    mutating_b = target_b.role in MUTATING_IMPLEMENTATION_TARGET_ROLES
    severity = TargetOverlapSeverity.NONE
    reason_code = "distinct_attested_coordinates"
    if not same_path and not same_symbol:
        reason_code = "different_source_ref" if not same_source else reason_code
    elif not mutating_a and not mutating_b:
        reason_code = "both_targets_non_mutating"
    elif mutating_a != mutating_b:
        severity = TargetOverlapSeverity.INFORMATIONAL
        reason_code = "mutating_and_non_mutating_overlap"
    elif same_symbol:
        severity = TargetOverlapSeverity.HIGH
        reason_code = "same_symbol_mutation"
    elif same_path and (
        target_a.selector_kind
        in {
            ImplementationTargetSelectorKind.FILE,
            ImplementationTargetSelectorKind.NEW_FILE,
        }
        or target_b.selector_kind
        in {
            ImplementationTargetSelectorKind.FILE,
            ImplementationTargetSelectorKind.NEW_FILE,
        }
    ):
        severity = TargetOverlapSeverity.HIGH
        reason_code = "file_scope_contains_mutation"
    elif same_path:
        severity = TargetOverlapSeverity.MEDIUM
        reason_code = "same_file_distinct_symbol_mutation"
    return TargetOverlap(
        board_id=target_a.board_id,
        target_a_id=target_a.id,
        target_b_id=target_b.id,
        resolution_a_id=resolution_a.id,
        resolution_b_id=resolution_b.id,
        severity=severity,
        reason_code=reason_code,
        relative_path=path_a if same_path else None,
        qualified_symbol=symbol_a if same_symbol else None,
    )


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CodeTraceabilityPageCursor:
    created_at: datetime
    item_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "code_traceability_cursor_created_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "code_traceability_cursor_item_id_invalid"),
        )


@dataclass(frozen=True, slots=True)
class CodeTraceabilityPage(Generic[T]):
    items: tuple[T, ...]
    limit: int
    next_cursor: CodeTraceabilityPageCursor | None = None

    def __post_init__(self) -> None:
        if isinstance(self.items, str | bytes) or not isinstance(self.items, Sequence):
            raise CodeTraceabilityContractError("code_traceability_page_invalid")
        items = tuple(self.items)
        limit = _positive_int(self.limit, "code_traceability_page_limit_invalid")
        if len(items) > limit:
            raise CodeTraceabilityContractError("code_traceability_page_invalid")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor, CodeTraceabilityPageCursor
        ):
            raise CodeTraceabilityContractError("code_traceability_cursor_invalid")
        object.__setattr__(self, "items", items)
        object.__setattr__(self, "limit", limit)


@dataclass(frozen=True, slots=True)
class CodeTraceabilityOmittedContent:
    """Server-owned disclosure that a bounded context collection was clipped."""

    collection: str
    hard_limit: int
    included_count: int
    omitted_at_least: int = 1
    reason_code: str = "projection_budget"

    def __post_init__(self) -> None:
        collection = _required_text(
            self.collection,
            "code_traceability_omitted_content_collection_invalid",
        )
        expected_limit = CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS.get(collection)
        if expected_limit is None or self.hard_limit != expected_limit:
            raise CodeTraceabilityContractError(
                "code_traceability_omitted_content_limit_invalid",
                details={"collection": collection},
            )
        object.__setattr__(self, "collection", collection)
        object.__setattr__(
            self,
            "hard_limit",
            _positive_int(
                self.hard_limit,
                "code_traceability_omitted_content_limit_invalid",
            ),
        )
        object.__setattr__(
            self,
            "omitted_at_least",
            _positive_int(
                self.omitted_at_least,
                "code_traceability_omitted_content_count_invalid",
            ),
        )
        included_count = _non_negative_int(
            self.included_count,
            "code_traceability_omitted_content_included_count_invalid",
        )
        if included_count > self.hard_limit:
            raise CodeTraceabilityContractError(
                "code_traceability_omitted_content_included_count_invalid",
                details={"collection": collection},
            )
        object.__setattr__(self, "included_count", included_count)
        if self.reason_code != "projection_budget":
            raise CodeTraceabilityContractError(
                "code_traceability_omitted_content_reason_invalid"
            )


@dataclass(frozen=True, slots=True)
class CodeTraceabilityContext:
    """Bounded read projection; profiles determine whether excerpts are present."""

    board_id: str
    subject_type: CodeTraceabilitySubjectType
    subject_id: str
    subject_version: int
    profile: CodeTraceabilityProjectionProfile
    context_scope: CodeTraceabilityContextScope = CodeTraceabilityContextScope.DEFAULT
    heads: tuple[CodeInvestigationHead, ...] = ()
    receipts: tuple[CodeInvestigationReceipt, ...] = ()
    receipt_revocations: tuple[CodeInvestigationReceiptRevocation, ...] = ()
    evidence: tuple[CodeEvidence, ...] = ()
    evidence_links: tuple[CodeEvidenceSpecLink, ...] = ()
    evidence_dispositions: tuple[CodeEvidenceDisposition, ...] = ()
    targets: tuple[ImplementationTarget, ...] = ()
    target_spec_links: tuple[ImplementationTargetSpecLink, ...] = ()
    target_evidence_links: tuple[ImplementationTargetEvidenceLink, ...] = ()
    resolutions: tuple[ImplementationTargetResolution, ...] = ()
    executions: tuple[ImplementationTargetExecutionRecord, ...] = ()
    overlaps: tuple[TargetOverlap, ...] = ()
    waivers: tuple[CodeTraceabilityWaiver, ...] = ()
    omitted_content_manifest: tuple[CodeTraceabilityOmittedContent, ...] = ()
    source_refinement_id: str | None = None
    source_refinement_snapshot_id: str | None = None
    source_refinement_version: int | None = None
    source_context: SourceContextSummaryV2 | None = None
    source_context_items: tuple[SourceContextEvidenceItemV2, ...] = ()
    source_context_classification_inputs: tuple[
        SourceContextClassificationInputV2, ...
    ] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "code_traceability_context_board_id_invalid"),
        )
        object.__setattr__(
            self,
            "subject_type",
            _enum(
                self.subject_type,
                CodeTraceabilitySubjectType,
                "code_traceability_context_subject_type_invalid",
            ),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "code_traceability_context_subject_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "subject_version",
            _positive_int(
                self.subject_version,
                "code_traceability_context_subject_version_invalid",
            ),
        )
        profile = _enum(
            self.profile,
            CodeTraceabilityProjectionProfile,
            "code_traceability_context_profile_invalid",
        )
        object.__setattr__(self, "profile", profile)
        context_scope = _enum(
            self.context_scope,
            CodeTraceabilityContextScope,
            "code_traceability_context_scope_invalid",
        )
        object.__setattr__(self, "context_scope", context_scope)
        collection_types = {
            "heads": CodeInvestigationHead,
            "receipts": CodeInvestigationReceipt,
            "receipt_revocations": CodeInvestigationReceiptRevocation,
            "evidence": CodeEvidence,
            "evidence_links": CodeEvidenceSpecLink,
            "evidence_dispositions": CodeEvidenceDisposition,
            "targets": ImplementationTarget,
            "target_spec_links": ImplementationTargetSpecLink,
            "target_evidence_links": ImplementationTargetEvidenceLink,
            "resolutions": ImplementationTargetResolution,
            "executions": ImplementationTargetExecutionRecord,
            "overlaps": TargetOverlap,
            "waivers": CodeTraceabilityWaiver,
        }
        for name, expected_type in collection_types.items():
            limit = CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS[name]
            object.__setattr__(
                self,
                name,
                _typed_tuple(
                    getattr(self, name),
                    expected_type,
                    f"code_traceability_context_{name}_invalid",
                    max_items=limit,
                ),
            )
        omitted_content = _typed_tuple(
            self.omitted_content_manifest,
            CodeTraceabilityOmittedContent,
            "code_traceability_context_omitted_content_manifest_invalid",
            max_items=len(CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS),
        )
        omitted_collections = tuple(item.collection for item in omitted_content)
        if len(set(omitted_collections)) != len(omitted_collections):
            raise CodeTraceabilityContractError(
                "code_traceability_context_omitted_content_manifest_invalid",
                details={"reason": "duplicate_collection"},
            )
        for item in omitted_content:
            if len(getattr(self, item.collection)) != item.included_count:
                raise CodeTraceabilityContractError(
                    "code_traceability_context_omitted_content_manifest_invalid",
                    details={"collection": item.collection},
                )
        object.__setattr__(
            self,
            "omitted_content_manifest",
            tuple(sorted(omitted_content, key=lambda item: item.collection)),
        )
        for name in (
            "source_refinement_id",
            "source_refinement_snapshot_id",
        ):
            object.__setattr__(
                self,
                name,
                _optional_text(
                    getattr(self, name),
                    f"code_traceability_context_{name}_invalid",
                ),
            )
        if self.source_refinement_version is not None:
            object.__setattr__(
                self,
                "source_refinement_version",
                _positive_int(
                    self.source_refinement_version,
                    "code_traceability_context_source_refinement_version_invalid",
                ),
            )
        lineage_values = (
            self.source_refinement_id,
            self.source_refinement_snapshot_id,
            self.source_refinement_version,
        )
        if any(value is not None for value in lineage_values) and not all(
            value is not None for value in lineage_values
        ):
            raise CodeTraceabilityContractError(
                "code_traceability_context_source_refinement_lineage_incoherent"
            )
        source_context = self.source_context
        if source_context is not None:
            if not isinstance(source_context, SourceContextSummaryV2):
                raise CodeTraceabilityContractError(
                    "code_traceability_context_source_context_invalid"
                )
            provenance = source_context.delivery_context_provenance
            if (
                provenance is not None
                and self.subject_type is CodeTraceabilitySubjectType.REFINEMENT
            ):
                if not isinstance(
                    provenance,
                    RefinementDeliveryContextProvenance,
                ):
                    raise CodeTraceabilityContractError(
                        "code_traceability_context_source_context_provenance_invalid",
                        details={"subject_type": self.subject_type.value},
                    )
                if (
                    provenance.source_refinement_id != self.subject_id
                    or provenance.source_refinement_version != self.subject_version
                ):
                    raise CodeTraceabilityContractError(
                        "code_traceability_context_source_context_provenance_invalid",
                        details={"reason": "refinement_subject_mismatch"},
                    )
            elif provenance is not None:
                if not isinstance(
                    provenance,
                    SpecDeliveryContextProvenance
                    | DirectSpecDeliveryContextProvenance,
                ):
                    raise CodeTraceabilityContractError(
                        "code_traceability_context_source_context_provenance_invalid",
                        details={"subject_type": self.subject_type.value},
                    )
                if isinstance(provenance, DirectSpecDeliveryContextProvenance):
                    if self.source_refinement_id is not None:
                        raise CodeTraceabilityContractError(
                            "code_traceability_context_source_context_provenance_invalid",
                            details={"reason": "direct_spec_has_refinement_lineage"},
                        )
                    if (
                        self.subject_type is CodeTraceabilitySubjectType.SPEC
                        and (
                            provenance.source_spec_id != self.subject_id
                            or provenance.source_spec_version > self.subject_version
                        )
                    ):
                        raise CodeTraceabilityContractError(
                            "code_traceability_context_source_context_provenance_invalid",
                            details={"reason": "direct_spec_subject_mismatch"},
                        )
                elif self.source_refinement_id is not None and (
                    provenance.source_refinement_id != self.source_refinement_id
                    or provenance.source_refinement_version
                    != self.source_refinement_version
                ):
                    raise CodeTraceabilityContractError(
                        "code_traceability_context_source_context_provenance_invalid",
                        details={"reason": "source_refinement_lineage_mismatch"},
                    )
        object.__setattr__(self, "source_context", source_context)
        source_context_items = _typed_tuple(
            self.source_context_items,
            SourceContextEvidenceItemV2,
            "code_traceability_context_source_context_items_invalid",
            max_items=DEFAULT_CODE_TRACEABILITY_LIMITS.context_evidence,
        )
        if len({item.evidence_id for item in source_context_items}) != len(
            source_context_items
        ):
            raise CodeTraceabilityContractError(
                "code_traceability_context_source_context_items_invalid",
                details={"reason": "duplicate_evidence_id"},
            )
        if (
            profile is CodeTraceabilityProjectionProfile.SUMMARY
            or context_scope is CodeTraceabilityContextScope.GATE
        ) and any(
            item.classified_by is not None or item.classified_at is not None
            for item in source_context_items
        ):
            raise CodeTraceabilityContractError(
                "code_traceability_context_source_context_actor_forbidden"
            )
        object.__setattr__(
            self,
            "source_context_items",
            tuple(sorted(source_context_items, key=lambda item: item.evidence_id)),
        )
        classification_inputs = _typed_tuple(
            self.source_context_classification_inputs,
            SourceContextClassificationInputV2,
            "code_traceability_context_classification_inputs_invalid",
            max_items=DEFAULT_CODE_TRACEABILITY_LIMITS.context_evidence,
        )
        classification_input_ids = tuple(
            item.evidence_id for item in classification_inputs
        )
        if (
            len(set(classification_input_ids)) != len(classification_input_ids)
            or not set(classification_input_ids).issubset(
                item.evidence_id for item in source_context_items
            )
            or (
                classification_inputs
                and self.subject_type is not CodeTraceabilitySubjectType.REFINEMENT
            )
            or (
                classification_inputs
                and (
                    profile is CodeTraceabilityProjectionProfile.SUMMARY
                    or context_scope is CodeTraceabilityContextScope.GATE
                )
            )
        ):
            raise CodeTraceabilityContractError(
                "code_traceability_context_classification_inputs_invalid"
            )
        object.__setattr__(
            self,
            "source_context_classification_inputs",
            tuple(
                sorted(classification_inputs, key=lambda item: item.evidence_id)
            ),
        )
        if (
            profile is CodeTraceabilityProjectionProfile.SUMMARY
            or context_scope is CodeTraceabilityContextScope.GATE
        ) and any(item.excerpt is not None for item in self.evidence):
            raise CodeTraceabilityContractError(
                "code_traceability_context_excerpt_forbidden"
            )
