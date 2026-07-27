"""Pure governance contract for Knowledge Base metadata.

Knowledge Base bodies are advisory, untrusted reference material.  This module
validates only the explicit, versioned governance envelope; it deliberately
never derives metadata from a KB title, description, or body.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from typing import Any, Mapping


KNOWLEDGE_GOVERNANCE_CONTRACT_VERSION = 1
KNOWLEDGE_GOVERNANCE_ERROR_CODE = "knowledge_governance_invalid_metadata"
KNOWLEDGE_GOVERNANCE_MAX_ITEMS = 64

_ROOT_PATH = "governance_metadata"
# Requirement ordinals are authored as their public, upper-case labels.  Internal
# stable ids deliberately use lower-case prefixes (for example ``tr_33412250``),
# so matching case-insensitively would reject valid opaque ids.
_ISOLATED_ORDINAL = re.compile(r"^(?:FR|TR|BR|AC)[\s_-]?\d+$")
_RFC3339_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})$"
)


class KnowledgeClassification(str, Enum):
    OBSERVED_EVIDENCE = "observed_evidence"
    TECHNICAL_REFERENCE = "technical_reference"
    TECHNICAL_DETAIL = "technical_detail"
    PROCESS_REFERENCE = "process_reference"
    RUNBOOK_REFERENCE = "runbook_reference"
    HISTORICAL_DECISION = "historical_decision"


class KnowledgeProvenanceKind(str, Enum):
    CODE = "code"
    SYSTEM = "system"
    INCIDENT = "incident"
    EXTERNAL = "external"
    PROCESS = "process"
    USER_INPUT = "user_input"


class KnowledgeLifecycleState(str, Enum):
    CURRENT = "current"
    SUPERSEDED = "superseded"


class ExclusiveAuthorityCheck(str, Enum):
    PASSED = "passed"
    PROMOTED = "promoted"


class StableReferenceEntityType(str, Enum):
    IDEATION = "ideation"
    REFINEMENT = "refinement"
    SPEC = "spec"
    CARD = "card"
    FUNCTIONAL_REQUIREMENT = "functional_requirement"
    TECHNICAL_REQUIREMENT = "technical_requirement"
    BUSINESS_RULE = "business_rule"
    ACCEPTANCE_CRITERION = "acceptance_criterion"
    DECISION = "decision"
    API_CONTRACT = "api_contract"
    ARCHITECTURE_DESIGN = "architecture_design"
    TEST_SCENARIO = "test_scenario"
    WORKFLOW = "workflow"
    GUIDELINE = "guideline"
    KNOWLEDGE_BASE = "knowledge_base"
    REPOSITORY_COMMIT = "repository_commit"
    EXTERNAL_URI = "external_uri"


class NormativeDestinationEntityType(str, Enum):
    FUNCTIONAL_REQUIREMENT = "functional_requirement"
    TECHNICAL_REQUIREMENT = "technical_requirement"
    BUSINESS_RULE = "business_rule"
    ACCEPTANCE_CRITERION = "acceptance_criterion"
    DECISION = "decision"
    API_CONTRACT = "api_contract"
    ARCHITECTURE_DESIGN = "architecture_design"
    TEST_SCENARIO = "test_scenario"
    WORKFLOW = "workflow"
    GUIDELINE = "guideline"


@dataclass(frozen=True, slots=True, order=True)
class KnowledgeGovernanceIssue:
    """One deterministic validation issue in the public error envelope."""

    path: str
    code: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return {"path": self.path, "code": self.code, "detail": self.detail}


class KnowledgeGovernanceInvalidMetadata(ValueError):
    """Fail-closed error raised before an invalid envelope can be persisted."""

    code = KNOWLEDGE_GOVERNANCE_ERROR_CODE

    def __init__(self, issues: list[KnowledgeGovernanceIssue]) -> None:
        unique = {(item.path, item.code, item.detail): item for item in issues}
        self.issues = tuple(sorted(unique.values()))
        super().__init__(self.code)

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "issues": [issue.as_dict() for issue in self.issues],
        }

    def to_error_dict(self) -> dict[str, Any]:
        """Return the stable cross-surface error envelope."""

        return self.as_dict()


@dataclass(frozen=True, slots=True)
class KnowledgeProvenance:
    kind: KnowledgeProvenanceKind
    reference: str

    def as_dict(self) -> dict[str, str]:
        return {"kind": self.kind.value, "reference": self.reference}


@dataclass(frozen=True, slots=True)
class StableReference:
    entity_type: StableReferenceEntityType
    entity_id: str
    version_ref: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "version_ref": self.version_ref,
        }


@dataclass(frozen=True, slots=True)
class NormativeDestination:
    entity_type: NormativeDestinationEntityType
    entity_id: str
    version_ref: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "version_ref": self.version_ref,
        }


@dataclass(frozen=True, slots=True)
class SupersededBy:
    entity_id: str
    version_ref: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity_type": StableReferenceEntityType.KNOWLEDGE_BASE.value,
            "entity_id": self.entity_id,
            "version_ref": self.version_ref,
        }


@dataclass(frozen=True, slots=True)
class KnowledgeGovernanceMetadataV1:
    """Canonical, immutable representation of the only writable v1 contract."""

    classification: KnowledgeClassification
    purpose: str
    audience: tuple[str, ...]
    relevance_reason: str
    provenance: tuple[KnowledgeProvenance, ...]
    as_of: str
    version_ref: str | None
    version_not_applicable_reason: str | None
    scope: str
    limitations: str
    stable_references: tuple[StableReference, ...]
    lifecycle_state: KnowledgeLifecycleState
    superseded_by: SupersededBy | None
    superseded_reason: str | None
    exclusive_authority_check: ExclusiveAuthorityCheck
    normative_destinations: tuple[NormativeDestination, ...]
    contract_version: int = KNOWLEDGE_GOVERNANCE_CONTRACT_VERSION
    authority: str = "advisory"

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "authority": self.authority,
            "classification": self.classification.value,
            "purpose": self.purpose,
            "audience": list(self.audience),
            "relevance_reason": self.relevance_reason,
            "provenance": [item.as_dict() for item in self.provenance],
            "as_of": self.as_of,
            "version_ref": self.version_ref,
            "version_not_applicable_reason": self.version_not_applicable_reason,
            "scope": self.scope,
            "limitations": self.limitations,
            "stable_references": [item.as_dict() for item in self.stable_references],
            "lifecycle_state": self.lifecycle_state.value,
            "superseded_by": (
                self.superseded_by.as_dict() if self.superseded_by else None
            ),
            "superseded_reason": self.superseded_reason,
            "exclusive_authority_check": self.exclusive_authority_check.value,
            "normative_destinations": [
                item.as_dict() for item in self.normative_destinations
            ],
        }


@dataclass(frozen=True, slots=True)
class KnowledgeGovernanceProjection:
    """Read projection for both governed and historical Knowledge records."""

    metadata_status: str
    missing_fields: tuple[str, ...]
    metadata: Any
    authority: str = "advisory"

    def as_dict(self) -> dict[str, Any]:
        return {
            "authority": self.authority,
            "metadata_status": self.metadata_status,
            "missing_fields": list(self.missing_fields),
            "metadata": self.metadata,
        }


_TOP_LEVEL_FIELDS = frozenset(
    {
        "contract_version",
        "authority",
        "classification",
        "purpose",
        "audience",
        "relevance_reason",
        "provenance",
        "as_of",
        "version_ref",
        "version_not_applicable_reason",
        "scope",
        "limitations",
        "stable_references",
        "lifecycle_state",
        "superseded_by",
        "superseded_reason",
        "exclusive_authority_check",
        "normative_destinations",
    }
)

_MISSING = object()


def parse_knowledge_governance_metadata(
    raw: object | None,
) -> KnowledgeGovernanceMetadataV1 | None:
    """Validate and normalize an optional write envelope.

    ``None`` represents omitted/legacy metadata and is intentionally accepted.
    Any non-null envelope is validated in full and raises one deterministic,
    aggregate error before a caller performs persistence or propagation.
    """

    if raw is None:
        return None

    issues: list[KnowledgeGovernanceIssue] = []
    if not isinstance(raw, Mapping):
        _issue(issues, _ROOT_PATH, "invalid_type", "must be a JSON object")
        raise KnowledgeGovernanceInvalidMetadata(issues)

    _validate_object_keys(raw, _TOP_LEVEL_FIELDS, _ROOT_PATH, issues)

    contract_version = _required(raw, "contract_version", issues)
    if contract_version is not _MISSING and (
        type(contract_version) is not int
        or contract_version != KNOWLEDGE_GOVERNANCE_CONTRACT_VERSION
    ):
        _issue(
            issues,
            f"{_ROOT_PATH}.contract_version",
            "unsupported_contract_version",
            "must be exactly 1",
        )

    authority = _required(raw, "authority", issues)
    normalized_authority = authority.strip() if isinstance(authority, str) else authority
    if normalized_authority is not _MISSING and normalized_authority != "advisory":
        _issue(
            issues,
            f"{_ROOT_PATH}.authority",
            "invalid_authority",
            "must be exactly 'advisory'",
        )

    classification = _enum_field(
        raw, "classification", KnowledgeClassification, issues
    )
    purpose = _required_text(raw, "purpose", issues)
    audience = _string_array(raw, "audience", issues, min_items=1)
    relevance_reason = _required_text(raw, "relevance_reason", issues)
    provenance = _provenance_array(raw, issues)
    as_of = _rfc3339_field(raw, "as_of", issues)
    version_ref = _nullable_text(raw, "version_ref", issues)
    version_reason = _nullable_text(
        raw, "version_not_applicable_reason", issues
    )
    scope = _required_text(raw, "scope", issues)
    limitations = _required_text(raw, "limitations", issues)
    stable_references = _reference_array(
        raw,
        "stable_references",
        StableReferenceEntityType,
        StableReference,
        issues,
    )
    lifecycle_state = _enum_field(
        raw, "lifecycle_state", KnowledgeLifecycleState, issues
    )
    superseded_by = _superseded_by(raw, issues)
    superseded_reason = _nullable_text(raw, "superseded_reason", issues)
    authority_check = _enum_field(
        raw, "exclusive_authority_check", ExclusiveAuthorityCheck, issues
    )
    normative_destinations = _reference_array(
        raw,
        "normative_destinations",
        NormativeDestinationEntityType,
        NormativeDestination,
        issues,
    )

    if version_ref is not _MISSING and version_reason is not _MISSING:
        if (version_ref is None) == (version_reason is None):
            _issue(
                issues,
                f"{_ROOT_PATH}.version_ref",
                "xor_violation",
                "exactly one of version_ref and "
                "version_not_applicable_reason must be non-null",
            )
            _issue(
                issues,
                f"{_ROOT_PATH}.version_not_applicable_reason",
                "xor_violation",
                "exactly one of version_ref and "
                "version_not_applicable_reason must be non-null",
            )

    if (
        classification is KnowledgeClassification.HISTORICAL_DECISION
        and lifecycle_state is not _MISSING
        and lifecycle_state is not KnowledgeLifecycleState.SUPERSEDED
    ):
        _issue(
            issues,
            f"{_ROOT_PATH}.lifecycle_state",
            "historical_decision_requires_superseded",
            "historical_decision requires lifecycle_state='superseded'",
        )

    if lifecycle_state is KnowledgeLifecycleState.CURRENT:
        if superseded_by not in (_MISSING, None):
            _issue(
                issues,
                f"{_ROOT_PATH}.superseded_by",
                "current_cannot_be_superseded",
                "must be null while lifecycle_state='current'",
            )
        if superseded_reason not in (_MISSING, None):
            _issue(
                issues,
                f"{_ROOT_PATH}.superseded_reason",
                "current_cannot_be_superseded",
                "must be null while lifecycle_state='current'",
            )
    elif lifecycle_state is KnowledgeLifecycleState.SUPERSEDED:
        if superseded_by is None and superseded_reason is None:
            _issue(
                issues,
                f"{_ROOT_PATH}.lifecycle_state",
                "superseded_reference_required",
                "superseded requires superseded_by or superseded_reason",
            )

    if authority_check is ExclusiveAuthorityCheck.PASSED:
        if normative_destinations not in (_MISSING, ()):
            _issue(
                issues,
                f"{_ROOT_PATH}.normative_destinations",
                "destinations_forbidden",
                "must be empty when exclusive_authority_check='passed'",
            )
    elif authority_check is ExclusiveAuthorityCheck.PROMOTED:
        if normative_destinations == ():
            _issue(
                issues,
                f"{_ROOT_PATH}.normative_destinations",
                "destinations_required",
                "must contain at least one destination when "
                "exclusive_authority_check='promoted'",
            )

    if issues:
        raise KnowledgeGovernanceInvalidMetadata(issues)

    # All sentinels are excluded by the aggregate validation above.
    return KnowledgeGovernanceMetadataV1(
        classification=classification,
        purpose=purpose,
        audience=audience,
        relevance_reason=relevance_reason,
        provenance=provenance,
        as_of=as_of,
        version_ref=version_ref,
        version_not_applicable_reason=version_reason,
        scope=scope,
        limitations=limitations,
        stable_references=stable_references,
        lifecycle_state=lifecycle_state,
        superseded_by=superseded_by,
        superseded_reason=superseded_reason,
        exclusive_authority_check=authority_check,
        normative_destinations=normative_destinations,
    )


def normalize_knowledge_governance_metadata(
    raw: object | None,
) -> dict[str, Any] | None:
    """Return the canonical JSON form used by ports and adapters."""

    parsed = parse_knowledge_governance_metadata(raw)
    return parsed.as_dict() if parsed else None


def project_knowledge_governance(
    raw: object | None,
) -> KnowledgeGovernanceProjection:
    """Project a row without mutating or backfilling historical metadata."""

    if raw is None:
        return KnowledgeGovernanceProjection(
            metadata_status="legacy_incomplete",
            missing_fields=(_ROOT_PATH,),
            metadata=None,
        )
    try:
        parsed = parse_knowledge_governance_metadata(raw)
    except KnowledgeGovernanceInvalidMetadata as exc:
        paths = tuple(sorted({issue.path for issue in exc.issues})) or (_ROOT_PATH,)
        return KnowledgeGovernanceProjection(
            metadata_status="legacy_incomplete",
            missing_fields=paths,
            metadata=raw,
        )
    assert parsed is not None
    return KnowledgeGovernanceProjection(
        metadata_status="complete",
        missing_fields=(),
        metadata=parsed.as_dict(),
    )


def knowledge_governance_semantically_equal(
    left: object | None,
    right: object | None,
) -> bool:
    """Compare canonical metadata while preserving contractual array order."""

    if left is None or right is None:
        return left is None and right is None
    return normalize_knowledge_governance_metadata(
        left
    ) == normalize_knowledge_governance_metadata(right)


def _required(
    raw: Mapping[object, object],
    field: str,
    issues: list[KnowledgeGovernanceIssue],
) -> object:
    if field not in raw:
        _issue(
            issues,
            f"{_ROOT_PATH}.{field}",
            "missing_required_field",
            "field is required",
        )
        return _MISSING
    return raw[field]


def _required_text(
    raw: Mapping[object, object],
    field: str,
    issues: list[KnowledgeGovernanceIssue],
) -> str | object:
    value = _required(raw, field, issues)
    return _text_value(value, f"{_ROOT_PATH}.{field}", issues)


def _nullable_text(
    raw: Mapping[object, object],
    field: str,
    issues: list[KnowledgeGovernanceIssue],
) -> str | None | object:
    value = _required(raw, field, issues)
    if value in (_MISSING, None):
        return value
    return _text_value(value, f"{_ROOT_PATH}.{field}", issues)


def _text_value(
    value: object,
    path: str,
    issues: list[KnowledgeGovernanceIssue],
) -> str | object:
    if value is _MISSING:
        return _MISSING
    if not isinstance(value, str):
        _issue(issues, path, "invalid_type", "must be a string")
        return _MISSING
    normalized = value.strip()
    if not normalized:
        _issue(issues, path, "empty_string", "must be non-empty after trimming")
        return _MISSING
    return normalized


def _enum_field(
    raw: Mapping[object, object],
    field: str,
    enum_type: type[Enum],
    issues: list[KnowledgeGovernanceIssue],
) -> Enum | object:
    value = _required(raw, field, issues)
    if value is _MISSING:
        return _MISSING
    if not isinstance(value, str):
        _issue(issues, f"{_ROOT_PATH}.{field}", "invalid_type", "must be a string")
        return _MISSING
    normalized = value.strip()
    try:
        return enum_type(normalized)
    except ValueError:
        allowed = ", ".join(sorted(str(item.value) for item in enum_type))
        _issue(
            issues,
            f"{_ROOT_PATH}.{field}",
            "invalid_enum",
            f"must be one of: {allowed}",
        )
        return _MISSING


def _string_array(
    raw: Mapping[object, object],
    field: str,
    issues: list[KnowledgeGovernanceIssue],
    *,
    min_items: int,
) -> tuple[str, ...] | object:
    value = _required(raw, field, issues)
    path = f"{_ROOT_PATH}.{field}"
    if value is _MISSING:
        return _MISSING
    if not isinstance(value, list):
        _issue(issues, path, "invalid_type", "must be an array")
        return _MISSING
    _validate_array_size(value, path, issues, min_items=min_items)
    normalized: list[str] = []
    for index, item in enumerate(value):
        text = _text_value(item, f"{path}[{index}]", issues)
        if text is not _MISSING:
            normalized.append(text)
    _reject_duplicates(normalized, path, issues)
    return tuple(normalized)


def _provenance_array(
    raw: Mapping[object, object],
    issues: list[KnowledgeGovernanceIssue],
) -> tuple[KnowledgeProvenance, ...] | object:
    field = "provenance"
    value = _required(raw, field, issues)
    path = f"{_ROOT_PATH}.{field}"
    if value is _MISSING:
        return _MISSING
    if not isinstance(value, list):
        _issue(issues, path, "invalid_type", "must be an array")
        return _MISSING
    _validate_array_size(value, path, issues, min_items=1)
    result: list[KnowledgeProvenance] = []
    keys = frozenset({"kind", "reference"})
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, Mapping):
            _issue(issues, item_path, "invalid_type", "must be a JSON object")
            continue
        _validate_object_keys(item, keys, item_path, issues)
        kind = _nested_enum(item, "kind", KnowledgeProvenanceKind, item_path, issues)
        reference = _nested_text(item, "reference", item_path, issues)
        if kind is not _MISSING and reference is not _MISSING:
            result.append(KnowledgeProvenance(kind=kind, reference=reference))
    _reject_duplicates(
        [(item.kind.value, item.reference) for item in result], path, issues
    )
    return tuple(result)


def _reference_array(
    raw: Mapping[object, object],
    field: str,
    enum_type: type[Enum],
    result_type: type[StableReference] | type[NormativeDestination],
    issues: list[KnowledgeGovernanceIssue],
) -> tuple[StableReference, ...] | tuple[NormativeDestination, ...] | object:
    value = _required(raw, field, issues)
    path = f"{_ROOT_PATH}.{field}"
    if value is _MISSING:
        return _MISSING
    if not isinstance(value, list):
        _issue(issues, path, "invalid_type", "must be an array")
        return _MISSING
    _validate_array_size(value, path, issues, min_items=0)
    result: list[StableReference | NormativeDestination] = []
    keys = frozenset({"entity_type", "entity_id", "version_ref"})
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, Mapping):
            _issue(issues, item_path, "invalid_type", "must be a JSON object")
            continue
        _validate_object_keys(item, keys, item_path, issues)
        entity_type = _nested_enum(
            item, "entity_type", enum_type, item_path, issues
        )
        entity_id = _nested_text(item, "entity_id", item_path, issues)
        version_ref = _nested_nullable_text(
            item, "version_ref", item_path, issues
        )
        if entity_id is not _MISSING and _ISOLATED_ORDINAL.fullmatch(entity_id):
            _issue(
                issues,
                f"{item_path}.entity_id",
                "isolated_ordinal_forbidden",
                "must be an opaque stable identifier, not an isolated FR/TR/BR/AC ordinal",
            )
        if (
            entity_type is not _MISSING
            and entity_id is not _MISSING
            and version_ref is not _MISSING
        ):
            result.append(
                result_type(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    version_ref=version_ref,
                )
            )
    _reject_duplicates(
        [
            (item.entity_type.value, item.entity_id, item.version_ref)
            for item in result
        ],
        path,
        issues,
    )
    return tuple(result)


def _superseded_by(
    raw: Mapping[object, object],
    issues: list[KnowledgeGovernanceIssue],
) -> SupersededBy | None | object:
    value = _required(raw, "superseded_by", issues)
    path = f"{_ROOT_PATH}.superseded_by"
    if value in (_MISSING, None):
        return value
    if not isinstance(value, Mapping):
        _issue(issues, path, "invalid_type", "must be null or a JSON object")
        return _MISSING
    keys = frozenset({"entity_type", "entity_id", "version_ref"})
    _validate_object_keys(value, keys, path, issues)
    entity_type = _nested_text(value, "entity_type", path, issues)
    if entity_type is not _MISSING and entity_type != "knowledge_base":
        _issue(
            issues,
            f"{path}.entity_type",
            "invalid_enum",
            "must be exactly 'knowledge_base'",
        )
    entity_id = _nested_text(value, "entity_id", path, issues)
    version_ref = _nested_nullable_text(value, "version_ref", path, issues)
    if entity_id is not _MISSING and _ISOLATED_ORDINAL.fullmatch(entity_id):
        _issue(
            issues,
            f"{path}.entity_id",
            "isolated_ordinal_forbidden",
            "must be an opaque stable identifier, not an isolated FR/TR/BR/AC ordinal",
        )
    if (
        entity_type == "knowledge_base"
        and entity_id is not _MISSING
        and version_ref is not _MISSING
    ):
        return SupersededBy(entity_id=entity_id, version_ref=version_ref)
    return _MISSING


def _rfc3339_field(
    raw: Mapping[object, object],
    field: str,
    issues: list[KnowledgeGovernanceIssue],
) -> str | object:
    value = _required_text(raw, field, issues)
    path = f"{_ROOT_PATH}.{field}"
    if value is _MISSING:
        return _MISSING
    if not _RFC3339_TIMESTAMP.fullmatch(value):
        _issue(
            issues,
            path,
            "invalid_rfc3339",
            "must be an RFC3339 timestamp with timezone",
        )
        return _MISSING
    candidate = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        _issue(
            issues,
            path,
            "invalid_rfc3339",
            "must be an RFC3339 timestamp with timezone",
        )
        return _MISSING
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _issue(
            issues,
            path,
            "timezone_required",
            "must include an explicit timezone",
        )
        return _MISSING
    return value


def _nested_text(
    raw: Mapping[object, object],
    field: str,
    parent_path: str,
    issues: list[KnowledgeGovernanceIssue],
) -> str | object:
    if field not in raw:
        _issue(
            issues,
            f"{parent_path}.{field}",
            "missing_required_field",
            "field is required",
        )
        return _MISSING
    return _text_value(raw[field], f"{parent_path}.{field}", issues)


def _nested_nullable_text(
    raw: Mapping[object, object],
    field: str,
    parent_path: str,
    issues: list[KnowledgeGovernanceIssue],
) -> str | None | object:
    if field not in raw:
        _issue(
            issues,
            f"{parent_path}.{field}",
            "missing_required_field",
            "field is required",
        )
        return _MISSING
    value = raw[field]
    if value is None:
        return None
    return _text_value(value, f"{parent_path}.{field}", issues)


def _nested_enum(
    raw: Mapping[object, object],
    field: str,
    enum_type: type[Enum],
    parent_path: str,
    issues: list[KnowledgeGovernanceIssue],
) -> Enum | object:
    if field not in raw:
        _issue(
            issues,
            f"{parent_path}.{field}",
            "missing_required_field",
            "field is required",
        )
        return _MISSING
    value = raw[field]
    if not isinstance(value, str):
        _issue(
            issues,
            f"{parent_path}.{field}",
            "invalid_type",
            "must be a string",
        )
        return _MISSING
    try:
        return enum_type(value.strip())
    except ValueError:
        allowed = ", ".join(sorted(str(item.value) for item in enum_type))
        _issue(
            issues,
            f"{parent_path}.{field}",
            "invalid_enum",
            f"must be one of: {allowed}",
        )
        return _MISSING


def _validate_object_keys(
    raw: Mapping[object, object],
    expected: frozenset[str],
    path: str,
    issues: list[KnowledgeGovernanceIssue],
) -> None:
    for key in raw:
        if not isinstance(key, str):
            _issue(
                issues,
                path,
                "invalid_key_type",
                "all object keys must be strings",
            )
        elif key not in expected:
            _issue(
                issues,
                f"{path}.{key}",
                "unknown_field",
                "field is not allowed by contract version 1",
            )
    for key in sorted(expected):
        if key not in raw:
            _issue(
                issues,
                f"{path}.{key}",
                "missing_required_field",
                "field is required",
            )


def _validate_array_size(
    value: list[object],
    path: str,
    issues: list[KnowledgeGovernanceIssue],
    *,
    min_items: int,
) -> None:
    if len(value) < min_items:
        _issue(
            issues,
            path,
            "too_few_items",
            f"must contain at least {min_items} item(s)",
        )
    if len(value) > KNOWLEDGE_GOVERNANCE_MAX_ITEMS:
        _issue(
            issues,
            path,
            "too_many_items",
            f"must contain at most {KNOWLEDGE_GOVERNANCE_MAX_ITEMS} items",
        )


def _reject_duplicates(
    values: list[object],
    path: str,
    issues: list[KnowledgeGovernanceIssue],
) -> None:
    seen: set[object] = set()
    for index, value in enumerate(values):
        if value in seen:
            _issue(
                issues,
                f"{path}[{index}]",
                "duplicate_item",
                "duplicate array items are not allowed",
            )
        seen.add(value)


def _issue(
    issues: list[KnowledgeGovernanceIssue],
    path: str,
    code: str,
    detail: str,
) -> None:
    issues.append(KnowledgeGovernanceIssue(path=path, code=code, detail=detail))


__all__ = [
    "ExclusiveAuthorityCheck",
    "KNOWLEDGE_GOVERNANCE_CONTRACT_VERSION",
    "KNOWLEDGE_GOVERNANCE_ERROR_CODE",
    "KNOWLEDGE_GOVERNANCE_MAX_ITEMS",
    "KnowledgeClassification",
    "KnowledgeGovernanceInvalidMetadata",
    "KnowledgeGovernanceIssue",
    "KnowledgeGovernanceMetadataV1",
    "KnowledgeGovernanceProjection",
    "KnowledgeLifecycleState",
    "KnowledgeProvenance",
    "KnowledgeProvenanceKind",
    "NormativeDestination",
    "NormativeDestinationEntityType",
    "StableReference",
    "StableReferenceEntityType",
    "SupersededBy",
    "knowledge_governance_semantically_equal",
    "normalize_knowledge_governance_metadata",
    "parse_knowledge_governance_metadata",
    "project_knowledge_governance",
]
