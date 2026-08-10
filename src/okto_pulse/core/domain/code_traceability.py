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

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


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


class CodeInvestigationRequestStatus(str, Enum):
    OPEN = "open"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    REVOKED = "revoked"


class CodeInvestigationOutcome(str, Enum):
    ACCESSIBLE = "accessible"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


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
        if outcome is CodeInvestigationOutcome.ACCESSIBLE and omissions:
            raise CodeTraceabilityContractError(
                "code_investigation_outcome_omissions_incoherent"
            )
        if outcome is not CodeInvestigationOutcome.ACCESSIBLE and not omissions:
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
        _enforce_envelope_size(
            self,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.receipt_envelope_bytes,
            envelope="receipt",
        )


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
) -> CodeInvestigationReceiptCurrentness:
    """Classify ledger currentness without probing the underlying source."""

    if not isinstance(receipt, CodeInvestigationReceipt):
        raise CodeTraceabilityContractError("code_investigation_receipt_invalid")
    evaluated_at = _aware_utc(at, "code_investigation_currentness_at_invalid")
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
        _enforce_envelope_size(
            self,
            max_bytes=DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_envelope_bytes,
            envelope="evidence",
        )

    @property
    def content_sha256(self) -> str:
        """Immutable Evidence content digest used by snapshots and Spec links."""

        return self.payload_sha256


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
        if (
            profile is CodeTraceabilityProjectionProfile.SUMMARY
            or context_scope is CodeTraceabilityContextScope.GATE
        ) and any(item.excerpt is not None for item in self.evidence):
            raise CodeTraceabilityContractError(
                "code_traceability_context_excerpt_forbidden"
            )
