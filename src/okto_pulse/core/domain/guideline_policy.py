"""Pure ``guideline-domain/v2`` contracts.

This module is the transport- and persistence-free source of truth for
versioned semantic guidelines, board bindings, assessment evidence, and
governed exceptions.  It intentionally does not import the legacy ORM
``Guideline`` model: the homonymous value object below is a domain identity
whose mutable content lives exclusively in immutable
:class:`GuidelineRevision` instances.

The unreleased executable policy/v1 values remain temporarily defined in this
module so dependent migration streams can still import while they are moved to
the semantic contract.  They are not part of the active revision or binding
surface: :class:`GuidelineRevision` contains only ``metrics`` and
:class:`BoardGuidelineBinding` contains only semantic configuration.
"""

from __future__ import annotations

import math
import re
import hashlib
import json
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import ClassVar, Generic, Mapping, TypeAlias, TypeVar

GUIDELINE_DOMAIN_CONTRACT_VERSION = "guideline-domain/v2"
GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION = "guideline-revision-digest/v2"
GUIDELINE_BINDING_CONFIGURATION_CONTRACT_VERSION = (
    "guideline-binding-configuration/v1"
)
GUIDELINE_IMPACT_CONTRACT_VERSION = "guideline-impact/v2"
GUIDELINE_PAGE_LIMIT_MAX = 200
GUIDELINE_REST_PAGE_LIMITS = (10, 25, 50, 100)
GUIDELINE_ID_MAX_LENGTH = 36
GUIDELINE_REVISION_ID_MAX_LENGTH = 36
GUIDELINE_BINDING_ID_MAX_LENGTH = 36
GUIDELINE_RETIREMENT_ID_MAX_LENGTH = 36
GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH = 64
GUIDELINE_TITLE_MAX_LENGTH = 500
GUIDELINE_BINDING_SOURCE_KIND_MAX_LENGTH = 40
GUIDELINE_BINDING_ORIGIN_MAX_LENGTH = 32
GUIDELINE_LEGACY_VERSION_MAX_LENGTH = 64
GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS = 128
# Entity identities cross the Core boundary as opaque edition-owned values.
# Do not couple the public policy contract to Community's current UUID storage
# width: other editions and legacy imports may use stable prefixed identities.
POLICY_ENTITY_ID_MAX_LENGTH = 255
POLICY_BOARD_ID_MAX_LENGTH = POLICY_ENTITY_ID_MAX_LENGTH
POLICY_ACTOR_ID_MAX_LENGTH = 255
POLICY_IDEMPOTENCY_KEY_MAX_LENGTH = 255
POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH = 64
POLICY_IMPACT_ITEM_ID_MAX_LENGTH = 64
POLICY_RECEIPT_ID_MAX_LENGTH = 64
POLICY_EVALUATION_ID_MAX_LENGTH = 255
POLICY_FINDING_ID_MAX_LENGTH = 64
POLICY_WAIVER_ID_MAX_LENGTH = 64
POLICY_WAIVER_EVENT_ID_MAX_LENGTH = 64
POLICY_SUBJECT_ID_MAX_LENGTH = POLICY_ENTITY_ID_MAX_LENGTH
POLICY_RULE_ID_MAX_LENGTH = 64
POLICY_METRIC_ID_MAX_LENGTH = 64
POLICY_METRIC_CODE_MAX_LENGTH = 128
POLICY_ENTITY_TYPE_MAX_LENGTH = 40
POLICY_VERSION_MAX_LENGTH = 128
POLICY_SQL_INTEGER_MAX = 2_147_483_647
POLICY_KEYSET_CONTRACT_VERSION = "policy-keyset/v1"
GUIDELINE_REVISION_ORDERING: tuple[str, str] = (
    "revision_number DESC",
    "revision_id DESC",
)

NON_WAIVABLE_POLICY_CLASSES = frozenset(
    {
        "coverage",
        "permissions",
        "reviewer_separation",
        "lineage",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SEMVER_RE = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
_RULE_CODE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]*$")
RESERVED_CONFIDENCE_FIELD = "confidence"

PolicyScalar: TypeAlias = str | int | float | bool | None
PolicyScalarCollection: TypeAlias = tuple[PolicyScalar, ...]
PolicyParameterValue: TypeAlias = PolicyScalar | PolicyScalarCollection
PolicyParameter: TypeAlias = tuple[str, PolicyParameterValue]


def _impact_canonical_value(value: object) -> object:
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise GuidelinePolicyContractError("guideline_impact_digest_invalid")
        return value
    if isinstance(value, str):
        return unicodedata.normalize(
            "NFC",
            value.replace("\r\n", "\n").replace("\r", "\n"),
        )
    if isinstance(value, Enum):
        return _impact_canonical_value(value.value)
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise GuidelinePolicyContractError("guideline_impact_digest_invalid")
        return {
            unicodedata.normalize("NFC", key): _impact_canonical_value(item)
            for key, item in value.items()
        }
    if isinstance(value, tuple | list):
        return [_impact_canonical_value(item) for item in value]
    raise GuidelinePolicyContractError("guideline_impact_digest_invalid")


def _impact_sha256(value: object) -> str:
    encoded = json.dumps(
        _impact_canonical_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class GuidelinePolicyContractError(ValueError):
    """A value violates the frozen guideline-domain contract."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise GuidelinePolicyContractError(code)
    normalized = value.strip()
    if not normalized:
        raise GuidelinePolicyContractError(code)
    return normalized


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def normalize_policy_bounded_text(
    value: object,
    *,
    max_length: int,
    code: str,
) -> str:
    """Normalize one durable text identifier against its physical maximum."""

    if (
        not isinstance(max_length, int)
        or isinstance(max_length, bool)
        or max_length < 1
    ):
        raise ValueError("policy_text_max_length_invalid")
    normalized = _required_text(value, code)
    if len(normalized) > max_length:
        raise GuidelinePolicyContractError(code)
    return normalized


def _bounded_optional_text(
    value: object,
    *,
    max_length: int,
    code: str,
) -> str | None:
    if value is None:
        return None
    return normalize_policy_bounded_text(
        value,
        max_length=max_length,
        code=code,
    )


def _strict_positive_int(value: object, code: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= POLICY_SQL_INTEGER_MAX
    ):
        raise GuidelinePolicyContractError(code)
    return value


def _strict_non_negative_int(value: object, code: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= POLICY_SQL_INTEGER_MAX
    ):
        raise GuidelinePolicyContractError(code)
    return value


def _strict_score(value: object, code: str) -> int:
    """Return one closed integer score/threshold in the inclusive 0..100 range."""

    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 100
    ):
        raise GuidelinePolicyContractError(code)
    return value


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelinePolicyContractError(code)
    return value.astimezone(timezone.utc)


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise GuidelinePolicyContractError(code)
    return normalized


def _semantic_version(value: object, code: str) -> str:
    normalized = _required_text(value, code)
    if len(normalized) > GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH:
        raise GuidelinePolicyContractError(code)
    match = _SEMVER_RE.fullmatch(normalized)
    if match is None:
        raise GuidelinePolicyContractError(code)
    if any(
        len(component) > GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS
        for component in match.group(1, 2, 3)
    ):
        raise GuidelinePolicyContractError(code)
    prerelease = match.group(4)
    if prerelease is not None and any(
        re.fullmatch(r"[0-9]+", identifier)
        and (
            len(identifier) > GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS
            or (len(identifier) > 1 and identifier.startswith("0"))
        )
        for identifier in prerelease.split(".")
    ):
        raise GuidelinePolicyContractError(code)
    return normalized


def normalize_guideline_sha256(value: object, code: str) -> str:
    """Public domain validator used by sibling guideline contracts."""

    return _sha256(value, code)


def normalize_guideline_semantic_version(
    value: object,
    code: str,
) -> str:
    """Public SemVer validator used by sibling guideline contracts."""

    return _semantic_version(value, code)


def _enum(value: object, enum_type: type[Enum], code: str) -> None:
    if not isinstance(value, enum_type):
        raise GuidelinePolicyContractError(code)


def _typed_tuple(
    value: object,
    expected_type: type,
    code: str,
) -> tuple:
    if not isinstance(value, tuple | list):
        raise GuidelinePolicyContractError(code)
    resolved = tuple(value)
    if any(not isinstance(item, expected_type) for item in resolved):
        raise GuidelinePolicyContractError(code)
    return resolved


def _text_tuple(
    value: object,
    code: str,
    *,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, tuple | list):
        raise GuidelinePolicyContractError(code)
    resolved = tuple(_required_text(item, code) for item in value)
    if not allow_empty and not resolved:
        raise GuidelinePolicyContractError(code)
    if len(set(resolved)) != len(resolved):
        raise GuidelinePolicyContractError(code)
    return resolved


def _metric_threshold_overrides(
    value: object,
    code: str,
) -> Mapping[str, int]:
    """Deep-freeze the wire-level ``metric_code -> threshold`` map."""

    if not isinstance(value, Mapping):
        raise GuidelinePolicyContractError(code)
    normalized: dict[str, int] = {}
    seen_casefolded: set[str] = set()
    for raw_metric_code, raw_threshold in value.items():
        metric_code = normalize_policy_bounded_text(
            raw_metric_code,
            max_length=POLICY_METRIC_CODE_MAX_LENGTH,
            code=code,
        )
        if (
            not _RULE_CODE_RE.fullmatch(metric_code)
            or metric_code.casefold() == RESERVED_CONFIDENCE_FIELD
            or metric_code.casefold() in seen_casefolded
        ):
            raise GuidelinePolicyContractError(code)
        seen_casefolded.add(metric_code.casefold())
        normalized[metric_code] = _strict_score(raw_threshold, code)
    return MappingProxyType(dict(sorted(normalized.items())))


def _parameters(
    value: object,
    code: str,
) -> tuple[PolicyParameter, ...]:
    if not isinstance(value, tuple | list):
        raise GuidelinePolicyContractError(code)
    resolved: list[PolicyParameter] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, tuple | list) or len(item) != 2:
            raise GuidelinePolicyContractError(code)
        key = _required_text(item[0], code)
        raw_parameter_value = item[1]
        if isinstance(raw_parameter_value, tuple | list):
            if not raw_parameter_value:
                raise GuidelinePolicyContractError(code)
            normalized_items: list[PolicyScalar] = []
            for collection_item in raw_parameter_value:
                if isinstance(collection_item, tuple | list | dict | set):
                    raise GuidelinePolicyContractError(code)
                if isinstance(collection_item, float) and not math.isfinite(
                    collection_item
                ):
                    raise GuidelinePolicyContractError(code)
                if not isinstance(
                    collection_item,
                    str | int | float | bool | None,
                ):
                    raise GuidelinePolicyContractError(code)
                normalized_items.append(collection_item)
            parameter_value: PolicyParameterValue = tuple(normalized_items)
        else:
            if isinstance(raw_parameter_value, float) and not math.isfinite(
                raw_parameter_value
            ):
                raise GuidelinePolicyContractError(code)
            if not isinstance(
                raw_parameter_value,
                str | int | float | bool | None,
            ):
                raise GuidelinePolicyContractError(code)
            parameter_value = raw_parameter_value
        if key in seen:
            raise GuidelinePolicyContractError(code)
        seen.add(key)
        resolved.append((key, parameter_value))
    return tuple(sorted(resolved, key=lambda pair: pair[0]))


class GuidelineScope(str, Enum):
    GLOBAL = "global"
    INLINE = "inline"


class GuidelineContextScope(str, Enum):
    """Context-only scope for prose guidance without executable rules."""

    ALL = "all"


class PolicyEntityType(str, Enum):
    """Closed set of semantic targets supported by guideline-domain/v2."""

    IDEATION = "ideation"
    REFINEMENT = "refinement"
    SPEC = "spec"
    SPRINT = "sprint"
    CARD = "card"
    TEST_SCENARIO = "test_scenario"


class GuidelineEnforcement(str, Enum):
    ADVISORY = "advisory"
    BLOCKING = "blocking"


class GuidelineMetricDirection(str, Enum):
    """How a bounded semantic score is compared with its effective threshold."""

    MINIMUM = "minimum"
    MAXIMUM = "maximum"


class GuidelineBindingState(str, Enum):
    """Materialized state of one append-only board binding revision."""

    ACTIVE = "active"
    UNLINKED = "unlinked"


class GuidelineBindingProvenance(str, Enum):
    """Structurally verifiable origin of a binding revision."""

    NATIVE = "native"
    DEFAULT_MATERIALIZATION = "default_materialization"


class GuidelineImpactItemKind(str, Enum):
    """Closed categories exposed by one board adoption impact preview."""

    BINDING = "binding"
    TARGET = "target"
    ARTIFACT = "artifact"
    WAIVER = "waiver"


class GuidelineLifecycleStatus(str, Enum):
    """Terminal logical states for an immutable guideline aggregate."""

    RETIRED = "retired"
    SUPERSEDED = "superseded"


class GuidelineRuleOperator(str, Enum):
    """Composition operator for the structured predicates in one rule."""

    ALL = "all"
    ANY = "any"


class PolicyEvaluationOutcome(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    NOT_APPLICABLE = "not_applicable"
    ERROR = "error"


class PolicyComplianceState(str, Enum):
    READY = "ready"
    BLOCKED = "blocked"
    READY_WITH_WAIVERS = "ready_with_waivers"
    NOT_APPLICABLE = "not_applicable"


class PolicyWaiverStatus(str, Enum):
    """Materialized waiver states, distinct from append-only lifecycle events.

    ``revalidate`` is an event rather than a sixth state: a successful,
    independently reviewed revalidation creates the next waiver revision in
    ``APPROVED`` state.
    """

    REQUESTED = "requested"
    APPROVED = "approved"
    REJECTED = "rejected"
    REVOKED = "revoked"
    EXPIRED = "expired"


class PolicyWaiverEventType(str, Enum):
    """Closed append-only operations for ``waiver-event/v1``."""

    REQUEST = "request"
    APPROVE = "approve"
    REJECT = "reject"
    REVOKE = "revoke"
    EXPIRE = "expire"
    REVALIDATE = "revalidate"


class PolicyWaiverExpireReasonCode(str, Enum):
    """Closed reasons for materializing an ``EXPIRE`` event.

    ``EXPIRE`` is the single non-discretionary terminalization operation.  It
    records either the scheduled time boundary or authoritative structural
    drift without expanding the closed waiver-event surface.
    """

    SCHEDULED_EXPIRY = "scheduled_expiry"
    SUBJECT_SCOPE_CHANGED = "subject_scope_changed"
    GUIDELINE_REVISION_CHANGED = "guideline_revision_changed"
    GUIDELINE_RULE_CHANGED = "guideline_rule_changed"


class PolicyCurrentness(str, Enum):
    CURRENT = "current"
    STALE = "stale"


class PolicyComplianceReasonCode(str, Enum):
    """Machine-readable aggregate conditions frozen on a receipt."""

    NO_APPLICABLE_RULES = "no_applicable_rules"
    POLICY_EVALUATION_UNAVAILABLE = "policy_evaluation_unavailable"
    POLICY_EVALUATION_DEGRADED = "policy_evaluation_degraded"


@dataclass(frozen=True, slots=True)
class Guideline:
    """Stable identity; title, content, and metrics live in immutable revisions."""

    guideline_id: str
    owner_id: str
    scope: GuidelineScope
    created_at: datetime
    board_id: str | None = None
    context_scope: GuidelineContextScope = GuidelineContextScope.ALL

    def __post_init__(self) -> None:
        _enum(self.scope, GuidelineScope, "guideline_scope_invalid")
        _enum(
            self.context_scope,
            GuidelineContextScope,
            "guideline_context_scope_invalid",
        )
        object.__setattr__(
            self,
            "guideline_id",
            normalize_policy_bounded_text(
                self.guideline_id,
                max_length=GUIDELINE_ID_MAX_LENGTH,
                code="guideline_id_required",
            ),
        )
        object.__setattr__(
            self,
            "owner_id",
            normalize_policy_bounded_text(
                self.owner_id,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="guideline_owner_id_required",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(self.created_at, "guideline_created_at_invalid"),
        )
        board_id = _bounded_optional_text(
            self.board_id,
            max_length=POLICY_BOARD_ID_MAX_LENGTH,
            code="guideline_board_id_invalid",
        )
        if self.scope is GuidelineScope.INLINE and board_id is None:
            raise GuidelinePolicyContractError("inline_guideline_board_id_required")
        if self.scope is GuidelineScope.GLOBAL and board_id is not None:
            raise GuidelinePolicyContractError("global_guideline_board_id_forbidden")
        object.__setattr__(self, "board_id", board_id)

    @property
    def id(self) -> str:
        """Compatibility-friendly read-only identity alias."""

        return self.guideline_id


@dataclass(frozen=True, slots=True)
class GuidelineMetric:
    """One author-owned semantic rubric with deterministic score boundaries."""

    metric_id: str
    code: str
    title: str
    description: str
    evaluation_rubric: str
    target_entity_types: tuple[PolicyEntityType, ...]
    direction: GuidelineMetricDirection
    default_threshold: int

    def __post_init__(self) -> None:
        _enum(
            self.direction,
            GuidelineMetricDirection,
            "guideline_metric_direction_invalid",
        )
        metric_id = normalize_policy_bounded_text(
            self.metric_id,
            max_length=POLICY_METRIC_ID_MAX_LENGTH,
            code="guideline_metric_id_required",
        )
        if metric_id.casefold() == RESERVED_CONFIDENCE_FIELD:
            raise GuidelinePolicyContractError(
                "guideline_metric_confidence_reserved"
            )
        object.__setattr__(
            self,
            "metric_id",
            metric_id,
        )
        code = normalize_policy_bounded_text(
            self.code,
            max_length=POLICY_METRIC_CODE_MAX_LENGTH,
            code="guideline_metric_code_required",
        )
        if not _RULE_CODE_RE.fullmatch(code):
            raise GuidelinePolicyContractError("guideline_metric_code_invalid")
        if code.casefold() == RESERVED_CONFIDENCE_FIELD:
            raise GuidelinePolicyContractError(
                "guideline_metric_confidence_reserved"
            )
        object.__setattr__(self, "code", code)
        title = normalize_policy_bounded_text(
            self.title,
            max_length=GUIDELINE_TITLE_MAX_LENGTH,
            code="guideline_metric_title_required",
        )
        if title.casefold() == RESERVED_CONFIDENCE_FIELD:
            raise GuidelinePolicyContractError(
                "guideline_metric_confidence_reserved"
            )
        object.__setattr__(
            self,
            "title",
            title,
        )
        for field_name in ("description", "evaluation_rubric"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_metric_{field_name}_required",
                ),
            )
        target_entity_types = _typed_tuple(
            self.target_entity_types,
            PolicyEntityType,
            "guideline_metric_target_entity_types_invalid",
        )
        if not target_entity_types:
            raise GuidelinePolicyContractError(
                "guideline_metric_target_entity_types_required"
            )
        if len(set(target_entity_types)) != len(target_entity_types):
            raise GuidelinePolicyContractError(
                "guideline_metric_target_entity_types_duplicate"
            )
        object.__setattr__(
            self,
            "target_entity_types",
            tuple(sorted(target_entity_types, key=lambda item: item.value)),
        )
        object.__setattr__(
            self,
            "default_threshold",
            _strict_score(
                self.default_threshold,
                "guideline_metric_default_threshold_invalid",
            ),
        )

    def applies_to(self, entity_type: PolicyEntityType) -> bool:
        _enum(entity_type, PolicyEntityType, "policy_entity_type_invalid")
        return entity_type in self.target_entity_types

    def digest_payload(self) -> dict[str, object]:
        return {
            "metric_id": self.metric_id,
            "code": self.code,
            "title": self.title,
            "description": self.description,
            "evaluation_rubric": self.evaluation_rubric,
            "target_entity_types": [
                entity_type.value for entity_type in self.target_entity_types
            ],
            "direction": self.direction.value,
            "default_threshold": self.default_threshold,
        }


def guideline_revision_digest_v2(
    *,
    semantic_version: str,
    title: str,
    content: str,
    metrics: tuple[GuidelineMetric, ...] | list[GuidelineMetric],
    tags: tuple[str, ...] | list[str] = (),
) -> str:
    """Digest only normative semantic content, never server-owned identities."""

    semantic_version = _semantic_version(
        semantic_version,
        "guideline_revision_semantic_version_invalid",
    )
    title = normalize_policy_bounded_text(
        title,
        max_length=GUIDELINE_TITLE_MAX_LENGTH,
        code="guideline_revision_title_required",
    )
    content = _required_text(content, "guideline_revision_content_required")
    metric_values = _typed_tuple(
        metrics,
        GuidelineMetric,
        "guideline_revision_metrics_invalid",
    )
    tag_values = tuple(
        sorted(_text_tuple(tags, "guideline_revision_tags_invalid"))
    )
    return _impact_sha256(
        {
            "contract": GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION,
            "semantic_version": semantic_version,
            "title": title,
            "content": content,
            "metrics": [
                metric.digest_payload() for metric in metric_values
            ],
            "tags": list(tag_values),
        }
    )


# Deprecated policy/v1 value objects retained only for migration-stream imports.
@dataclass(frozen=True, slots=True)
class GuidelinePredicate:
    """Catalog-resolved predicate invocation.

    ``predicate_code`` is interpreted by the deterministic catalog implemented
    outside this contract. Parameters use immutable scalars or flat scalar
    collections so a frozen rule cannot hide mutable transport data.
    """

    predicate_code: str
    parameters: tuple[PolicyParameter, ...] = ()

    def __post_init__(self) -> None:
        code = _required_text(
            self.predicate_code,
            "guideline_predicate_code_required",
        )
        if not _RULE_CODE_RE.fullmatch(code):
            raise GuidelinePolicyContractError("guideline_predicate_code_invalid")
        object.__setattr__(self, "predicate_code", code)
        object.__setattr__(
            self,
            "parameters",
            _parameters(
                self.parameters,
                "guideline_predicate_parameters_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class GuidelineRule:
    rule_id: str
    code: str
    title: str
    description: str
    target_entity_types: tuple[PolicyEntityType, ...]
    predicates: tuple[GuidelinePredicate, ...]
    enforcement: GuidelineEnforcement = GuidelineEnforcement.ADVISORY
    operator: GuidelineRuleOperator = GuidelineRuleOperator.ALL
    waivable: bool = True
    policy_class: str = "standard"

    def __post_init__(self) -> None:
        _enum(
            self.enforcement,
            GuidelineEnforcement,
            "guideline_rule_enforcement_invalid",
        )
        _enum(
            self.operator,
            GuidelineRuleOperator,
            "guideline_rule_operator_invalid",
        )
        object.__setattr__(
            self,
            "rule_id",
            normalize_policy_bounded_text(
                self.rule_id,
                max_length=POLICY_RULE_ID_MAX_LENGTH,
                code="guideline_rule_rule_id_required",
            ),
        )
        for field_name in ("title", "description"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_rule_{field_name}_required",
                ),
            )
        code = _required_text(self.code, "guideline_rule_code_required")
        if not _RULE_CODE_RE.fullmatch(code):
            raise GuidelinePolicyContractError("guideline_rule_code_invalid")
        object.__setattr__(self, "code", code)

        target_entity_types = _typed_tuple(
            self.target_entity_types,
            PolicyEntityType,
            "guideline_rule_target_entity_types_invalid",
        )
        if not target_entity_types:
            raise GuidelinePolicyContractError(
                "guideline_rule_target_entity_types_required"
            )
        if len(set(target_entity_types)) != len(target_entity_types):
            raise GuidelinePolicyContractError(
                "guideline_rule_target_entity_types_duplicate"
            )
        object.__setattr__(
            self,
            "target_entity_types",
            tuple(sorted(target_entity_types, key=lambda item: item.value)),
        )

        predicates = _typed_tuple(
            self.predicates,
            GuidelinePredicate,
            "guideline_rule_predicates_invalid",
        )
        if not predicates:
            raise GuidelinePolicyContractError("guideline_rule_predicates_required")
        object.__setattr__(self, "predicates", predicates)
        if not isinstance(self.waivable, bool):
            raise GuidelinePolicyContractError("guideline_rule_waivable_invalid")
        policy_class = _required_text(
            self.policy_class,
            "guideline_rule_policy_class_required",
        ).lower()
        if policy_class in NON_WAIVABLE_POLICY_CLASSES and self.waivable:
            raise GuidelinePolicyContractError(
                "guideline_rule_protected_class_must_be_non_waivable"
            )
        object.__setattr__(self, "policy_class", policy_class)

    def applies_to(self, entity_type: PolicyEntityType) -> bool:
        _enum(
            entity_type,
            PolicyEntityType,
            "policy_entity_type_invalid",
        )
        return entity_type in self.target_entity_types


@dataclass(frozen=True, slots=True)
class GuidelineRevision:
    """Immutable semantic revision of a stable guideline identity."""

    revision_id: str
    guideline_id: str
    revision_number: int
    semantic_version: str
    title: str
    content: str
    metrics: tuple[GuidelineMetric, ...]
    created_by: str
    created_at: datetime
    revision_digest: str | None = None
    parent_revision_id: str | None = None
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "revision_id",
            normalize_policy_bounded_text(
                self.revision_id,
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                code="guideline_revision_revision_id_required",
            ),
        )
        object.__setattr__(
            self,
            "guideline_id",
            normalize_policy_bounded_text(
                self.guideline_id,
                max_length=GUIDELINE_ID_MAX_LENGTH,
                code="guideline_revision_guideline_id_required",
            ),
        )
        object.__setattr__(
            self,
            "title",
            normalize_policy_bounded_text(
                self.title,
                max_length=GUIDELINE_TITLE_MAX_LENGTH,
                code="guideline_revision_title_required",
            ),
        )
        object.__setattr__(
            self,
            "created_by",
            normalize_policy_bounded_text(
                self.created_by,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="guideline_revision_created_by_required",
            ),
        )
        object.__setattr__(
            self,
            "content",
            _required_text(
                self.content,
                "guideline_revision_content_required",
            ),
        )
        object.__setattr__(
            self,
            "revision_number",
            _strict_positive_int(
                self.revision_number,
                "guideline_revision_number_invalid",
            ),
        )
        object.__setattr__(
            self,
            "semantic_version",
            _semantic_version(
                self.semantic_version,
                "guideline_revision_semantic_version_invalid",
            ),
        )
        metrics = _typed_tuple(
            self.metrics,
            GuidelineMetric,
            "guideline_revision_metrics_invalid",
        )
        if len({metric.metric_id for metric in metrics}) != len(metrics):
            raise GuidelinePolicyContractError(
                "guideline_revision_duplicate_metric_id"
            )
        if len({metric.code.casefold() for metric in metrics}) != len(metrics):
            raise GuidelinePolicyContractError(
                "guideline_revision_duplicate_metric_code"
            )
        # Metric order is authorial/normative and therefore intentionally
        # preserved rather than sorted.
        object.__setattr__(self, "metrics", metrics)
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "guideline_revision_created_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "tags",
            tuple(
                sorted(
                    _text_tuple(
                        self.tags,
                        "guideline_revision_tags_invalid",
                    )
                )
            ),
        )
        parent = _bounded_optional_text(
            self.parent_revision_id,
            max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
            code="guideline_revision_parent_id_invalid",
        )
        if self.revision_number == 1 and parent is not None:
            raise GuidelinePolicyContractError(
                "guideline_initial_revision_parent_forbidden"
            )
        if self.revision_number > 1 and parent is None:
            raise GuidelinePolicyContractError("guideline_revision_parent_required")
        object.__setattr__(self, "parent_revision_id", parent)
        expected_digest = guideline_revision_digest_v2(
            semantic_version=self.semantic_version,
            title=self.title,
            content=self.content,
            metrics=self.metrics,
            tags=self.tags,
        )
        if self.revision_digest is not None:
            supplied_digest = _sha256(
                self.revision_digest,
                "guideline_revision_digest_invalid",
            )
            if supplied_digest != expected_digest:
                raise GuidelinePolicyContractError(
                    "guideline_revision_digest_mismatch"
                )
        object.__setattr__(self, "revision_digest", expected_digest)

    @property
    def id(self) -> str:
        return self.revision_id

    @property
    def context_only(self) -> bool:
        return not self.metrics


@dataclass(frozen=True, slots=True)
class GuidelineHead:
    guideline_id: str
    revision_id: str
    revision_number: int
    semantic_version: str
    head_revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("guideline_id", "revision_id"):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                    code=f"guideline_head_{field_name}_required",
                ),
            )
        for field_name in ("revision_number", "head_revision"):
            object.__setattr__(
                self,
                field_name,
                _strict_positive_int(
                    getattr(self, field_name),
                    f"guideline_head_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "semantic_version",
            _semantic_version(
                self.semantic_version,
                "guideline_head_semantic_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "updated_at",
            _aware_utc(self.updated_at, "guideline_head_updated_at_invalid"),
        )


@dataclass(frozen=True, slots=True)
class GuidelineRetirement:
    """Immutable terminal snapshot of the exact guideline head retired.

    Retirement and supersedence never delete or rewrite the stable identity,
    revisions, or board-binding history.  The complete frozen head prevents a
    later reader from accidentally projecting a different revision as the one
    that was terminally retired.
    """

    retirement_id: str
    guideline_id: str
    status: GuidelineLifecycleStatus
    retired_revision_id: str
    retired_revision_number: int
    retired_semantic_version: str
    retired_revision_digest: str
    retired_head_revision: int
    reason: str
    retired_by: str
    retired_at: datetime
    superseded_by_guideline_id: str | None = None

    def __post_init__(self) -> None:
        _enum(
            self.status,
            GuidelineLifecycleStatus,
            "guideline_retirement_status_invalid",
        )
        for field_name in ("retirement_id", "guideline_id", "retired_revision_id"):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=(
                        GUIDELINE_RETIREMENT_ID_MAX_LENGTH
                        if field_name == "retirement_id"
                        else GUIDELINE_REVISION_ID_MAX_LENGTH
                    ),
                    code=f"guideline_retirement_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "guideline_retirement_reason_required"),
        )
        object.__setattr__(
            self,
            "retired_by",
            normalize_policy_bounded_text(
                self.retired_by,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="guideline_retirement_retired_by_required",
            ),
        )
        object.__setattr__(
            self,
            "retired_revision_number",
            _strict_positive_int(
                self.retired_revision_number,
                "guideline_retirement_revision_number_invalid",
            ),
        )
        object.__setattr__(
            self,
            "retired_semantic_version",
            _semantic_version(
                self.retired_semantic_version,
                "guideline_retirement_semantic_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "retired_revision_digest",
            _sha256(
                self.retired_revision_digest,
                "guideline_retirement_revision_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "retired_head_revision",
            _strict_positive_int(
                self.retired_head_revision,
                "guideline_retirement_head_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "retired_at",
            _aware_utc(
                self.retired_at,
                "guideline_retirement_retired_at_invalid",
            ),
        )
        successor = _bounded_optional_text(
            self.superseded_by_guideline_id,
            max_length=GUIDELINE_ID_MAX_LENGTH,
            code="guideline_retirement_successor_invalid",
        )
        if self.status is GuidelineLifecycleStatus.SUPERSEDED:
            if successor is None or successor == self.guideline_id:
                raise GuidelinePolicyContractError(
                    "guideline_supersedence_successor_required"
                )
        elif successor is not None:
            raise GuidelinePolicyContractError(
                "guideline_retirement_successor_forbidden"
            )
        object.__setattr__(self, "superseded_by_guideline_id", successor)

    @property
    def id(self) -> str:
        return self.retirement_id


def guideline_binding_configuration_digest_v1(
    *,
    binding_id: str,
    board_id: str,
    guideline_id: str,
    revision_id: str,
    revision_digest: str,
    priority: int,
    enforcement: GuidelineEnforcement,
    minimum_confidence: int,
    metric_threshold_overrides: Mapping[str, int],
) -> str:
    """Seal the exact board-effective semantic configuration."""

    _enum(
        enforcement,
        GuidelineEnforcement,
        "guideline_binding_enforcement_invalid",
    )
    normalized_overrides = _metric_threshold_overrides(
        metric_threshold_overrides,
        "guideline_binding_metric_threshold_overrides_invalid",
    )
    return _impact_sha256(
        {
            "contract": GUIDELINE_BINDING_CONFIGURATION_CONTRACT_VERSION,
            "binding_id": normalize_policy_bounded_text(
                binding_id,
                max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                code="guideline_binding_binding_id_required",
            ),
            "board_id": normalize_policy_bounded_text(
                board_id,
                max_length=POLICY_BOARD_ID_MAX_LENGTH,
                code="guideline_binding_board_id_required",
            ),
            "guideline_id": normalize_policy_bounded_text(
                guideline_id,
                max_length=GUIDELINE_ID_MAX_LENGTH,
                code="guideline_binding_guideline_id_required",
            ),
            "revision_id": normalize_policy_bounded_text(
                revision_id,
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                code="guideline_binding_revision_id_required",
            ),
            "revision_digest": _sha256(
                revision_digest,
                "guideline_binding_revision_digest_invalid",
            ),
            "priority": _strict_non_negative_int(
                priority,
                "guideline_binding_priority_invalid",
            ),
            "enforcement": enforcement.value,
            "minimum_confidence": _strict_score(
                minimum_confidence,
                "guideline_binding_minimum_confidence_invalid",
            ),
            "metric_threshold_overrides": dict(normalized_overrides),
        }
    )


@dataclass(frozen=True, slots=True)
class BoardGuidelineBinding:
    """Exact board-to-revision adoption; sync never mutates it implicitly."""

    binding_id: str
    board_id: str
    guideline_id: str
    revision_id: str
    semantic_version: str
    revision_digest: str
    priority: int
    binding_revision: int
    adopted_by: str
    adopted_at: datetime
    enforcement: GuidelineEnforcement = GuidelineEnforcement.ADVISORY
    minimum_confidence: int = 0
    metric_threshold_overrides: Mapping[str, int] = field(default_factory=dict)
    configuration_digest: str | None = None
    state: GuidelineBindingState = GuidelineBindingState.ACTIVE
    source_kind: GuidelineBindingProvenance = GuidelineBindingProvenance.NATIVE

    def __post_init__(self) -> None:
        _enum(
            self.enforcement,
            GuidelineEnforcement,
            "guideline_binding_enforcement_invalid",
        )
        _enum(
            self.state,
            GuidelineBindingState,
            "guideline_binding_state_invalid",
        )
        _enum(
            self.source_kind,
            GuidelineBindingProvenance,
            "guideline_binding_source_kind_invalid",
        )
        for field_name in (
            "binding_id",
            "board_id",
            "guideline_id",
            "revision_id",
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                    code=f"guideline_binding_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "adopted_by",
            normalize_policy_bounded_text(
                self.adopted_by,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="guideline_binding_adopted_by_required",
            ),
        )
        object.__setattr__(
            self,
            "semantic_version",
            _semantic_version(
                self.semantic_version,
                "guideline_binding_semantic_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "revision_digest",
            _sha256(
                self.revision_digest,
                "guideline_binding_revision_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "priority",
            _strict_non_negative_int(
                self.priority,
                "guideline_binding_priority_invalid",
            ),
        )
        object.__setattr__(
            self,
            "binding_revision",
            _strict_positive_int(
                self.binding_revision,
                "guideline_binding_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "adopted_at",
            _aware_utc(
                self.adopted_at,
                "guideline_binding_adopted_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "minimum_confidence",
            _strict_score(
                self.minimum_confidence,
                "guideline_binding_minimum_confidence_invalid",
            ),
        )
        overrides = _metric_threshold_overrides(
            self.metric_threshold_overrides,
            "guideline_binding_metric_threshold_overrides_invalid",
        )
        object.__setattr__(
            self,
            "metric_threshold_overrides",
            overrides,
        )
        expected_configuration_digest = guideline_binding_configuration_digest_v1(
            binding_id=self.binding_id,
            board_id=self.board_id,
            guideline_id=self.guideline_id,
            revision_id=self.revision_id,
            revision_digest=self.revision_digest,
            priority=self.priority,
            enforcement=self.enforcement,
            minimum_confidence=self.minimum_confidence,
            metric_threshold_overrides=overrides,
        )
        if self.configuration_digest is not None:
            supplied_digest = _sha256(
                self.configuration_digest,
                "guideline_binding_configuration_digest_invalid",
            )
            if supplied_digest != expected_configuration_digest:
                raise GuidelinePolicyContractError(
                    "guideline_binding_configuration_digest_mismatch"
                )
        object.__setattr__(
            self,
            "configuration_digest",
            expected_configuration_digest,
        )


@dataclass(frozen=True, slots=True)
class AdoptedGuidelineRevisionRef:
    """Minimal immutable revision evidence frozen on compliance receipts."""

    binding_id: str
    binding_revision: int
    guideline_id: str
    revision_id: str
    semantic_version: str
    revision_digest: str

    def __post_init__(self) -> None:
        for field_name in ("binding_id", "guideline_id", "revision_id"):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                    code=f"adopted_guideline_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "binding_revision",
            _strict_positive_int(
                self.binding_revision,
                "adopted_guideline_binding_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "semantic_version",
            _semantic_version(
                self.semantic_version,
                "adopted_guideline_semantic_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "revision_digest",
            _sha256(
                self.revision_digest,
                "adopted_guideline_revision_digest_invalid",
            ),
        )

    @classmethod
    def from_binding(
        cls,
        binding: BoardGuidelineBinding,
    ) -> AdoptedGuidelineRevisionRef:
        if not isinstance(binding, BoardGuidelineBinding):
            raise GuidelinePolicyContractError("adopted_guideline_binding_invalid")
        if binding.state is not GuidelineBindingState.ACTIVE:
            raise GuidelinePolicyContractError("adopted_guideline_binding_inactive")
        return cls(
            binding_id=binding.binding_id,
            binding_revision=binding.binding_revision,
            guideline_id=binding.guideline_id,
            revision_id=binding.revision_id,
            semantic_version=binding.semantic_version,
            revision_digest=binding.revision_digest,
        )


@dataclass(frozen=True, slots=True)
class GuidelineImpactItem:
    """One immutable, lightweight effect in an adoption preview.

    ``entity_type`` is deliberately broader than ``PolicyEntityType`` because
    the preview also contains the board binding itself. Semantic metric targets
    remain closed by ``PolicyEntityType`` on the receipt.
    """

    impact_item_id: str
    item_kind: GuidelineImpactItemKind
    entity_type: str
    entity_id: str
    details_digest: str
    related_id: str | None = None
    entity_version: int | None = None

    def __post_init__(self) -> None:
        _enum(
            self.item_kind,
            GuidelineImpactItemKind,
            "guideline_impact_item_kind_invalid",
        )
        for field_name, max_length in (
            ("impact_item_id", POLICY_IMPACT_ITEM_ID_MAX_LENGTH),
            ("entity_type", POLICY_ENTITY_TYPE_MAX_LENGTH),
            ("entity_id", POLICY_ENTITY_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"guideline_impact_item_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "details_digest",
            _sha256(
                self.details_digest,
                "guideline_impact_item_details_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "related_id",
            _bounded_optional_text(
                self.related_id,
                max_length=POLICY_ENTITY_ID_MAX_LENGTH,
                code="guideline_impact_item_related_id_invalid",
            ),
        )
        if self.entity_version is not None:
            object.__setattr__(
                self,
                "entity_version",
                _strict_non_negative_int(
                    self.entity_version,
                    "guideline_impact_item_entity_version_invalid",
                ),
            )

    @property
    def id(self) -> str:
        return self.impact_item_id

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (self.entity_type, self.entity_id, self.impact_item_id)

    def digest_payload(self) -> dict[str, object]:
        return {
            "id": self.impact_item_id,
            "item_kind": self.item_kind.value,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "related_id": self.related_id,
            "entity_version": self.entity_version,
            "details_digest": self.details_digest,
        }


def guideline_binding_snapshot_digest(
    binding: BoardGuidelineBinding | None,
    *,
    board_id: str,
    guideline_id: str,
) -> str:
    """Digest one exact binding head, including the authoritative absence."""

    board_id = _required_text(board_id, "guideline_impact_board_id_required")
    guideline_id = _required_text(
        guideline_id,
        "guideline_impact_guideline_id_required",
    )
    if binding is None:
        payload: dict[str, object] = {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "kind": "binding_fence",
            "board_id": board_id,
            "guideline_id": guideline_id,
            "state": "absent",
        }
    else:
        if (
            not isinstance(binding, BoardGuidelineBinding)
            or binding.board_id != board_id
            or binding.guideline_id != guideline_id
        ):
            raise GuidelinePolicyContractError(
                "guideline_impact_binding_scope_mismatch"
            )
        payload = {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "kind": "binding_fence",
            "board_id": binding.board_id,
            "guideline_id": binding.guideline_id,
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "revision_id": binding.revision_id,
            "semantic_version": binding.semantic_version,
            "revision_digest": binding.revision_digest,
            "priority": binding.priority,
            "enforcement": binding.enforcement.value,
            "minimum_confidence": binding.minimum_confidence,
            "metric_threshold_overrides": dict(
                binding.metric_threshold_overrides
            ),
            "configuration_digest": binding.configuration_digest,
            "state": binding.state.value,
            "source_kind": binding.source_kind.value,
        }
    return _impact_sha256(payload)


def guideline_impact_digest_v2(
    *,
    board_id: str,
    guideline_id: str,
    binding_id: str,
    from_revision_id: str | None,
    from_semantic_version: str | None,
    from_revision_digest: str | None,
    to_revision_id: str,
    to_revision_number: int,
    to_semantic_version: str,
    to_revision_digest: str,
    expected_head_revision: int,
    expected_binding_revision: int | None,
    expected_binding_state: GuidelineBindingState | None,
    binding_digest: str,
    binding_head_digest_before: str,
    binding_head_digest_after: str,
    policy_set_digest_before: str,
    policy_set_digest_after: str,
    artifact_snapshot_digest: str,
    waiver_snapshot_digest: str,
    proposed_priority: int,
    proposed_enforcement: GuidelineEnforcement,
    proposed_minimum_confidence: int,
    proposed_metric_threshold_overrides: Mapping[str, int],
    affected_entity_types: tuple[PolicyEntityType, ...],
    items: tuple[GuidelineImpactItem, ...],
    added_metric_ids: tuple[str, ...],
    changed_metric_ids: tuple[str, ...],
    removed_metric_ids: tuple[str, ...],
    requires_explicit_adoption: bool = True,
) -> str:
    """Seal one impact snapshot without request/time/receipt identity."""

    return _impact_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "board_id": board_id,
            "guideline_id": guideline_id,
            "binding_id": binding_id,
            "from_revision_id": from_revision_id,
            "from_semantic_version": from_semantic_version,
            "from_revision_digest": from_revision_digest,
            "to_revision_id": to_revision_id,
            "to_revision_number": to_revision_number,
            "to_semantic_version": to_semantic_version,
            "to_revision_digest": to_revision_digest,
            "expected_head_revision": expected_head_revision,
            "expected_binding_revision": expected_binding_revision,
            "expected_binding_state": (
                expected_binding_state.value
                if expected_binding_state is not None
                else None
            ),
            "binding_digest": binding_digest,
            "binding_head_digest_before": binding_head_digest_before,
            "binding_head_digest_after": binding_head_digest_after,
            "policy_set_digest_before": policy_set_digest_before,
            "policy_set_digest_after": policy_set_digest_after,
            "artifact_snapshot_digest": artifact_snapshot_digest,
            "waiver_snapshot_digest": waiver_snapshot_digest,
            "proposed_priority": proposed_priority,
            "proposed_enforcement": proposed_enforcement.value,
            "proposed_minimum_confidence": proposed_minimum_confidence,
            "proposed_metric_threshold_overrides": dict(
                sorted(proposed_metric_threshold_overrides.items())
            ),
            "affected_entity_types": [
                item.value
                for item in sorted(
                    affected_entity_types,
                    key=lambda candidate: candidate.value,
                )
            ],
            "items": [
                item.digest_payload()
                for item in sorted(items, key=lambda candidate: candidate.sort_key)
            ],
            "added_metric_ids": list(sorted(added_metric_ids)),
            "changed_metric_ids": list(sorted(changed_metric_ids)),
            "removed_metric_ids": list(sorted(removed_metric_ids)),
            "requires_explicit_adoption": requires_explicit_adoption,
        }
    )


@dataclass(frozen=True, slots=True)
class GuidelineImpactReceipt:
    """Immutable, self-sealed preview of an explicit board adoption."""

    impact_receipt_id: str
    board_id: str
    guideline_id: str
    binding_id: str
    to_revision_id: str
    to_revision_number: int
    to_semantic_version: str
    to_revision_digest: str
    expected_head_revision: int
    expected_binding_revision: int | None
    expected_binding_state: GuidelineBindingState | None
    binding_digest: str
    binding_head_digest_before: str
    binding_head_digest_after: str
    policy_set_digest_before: str
    policy_set_digest_after: str
    artifact_snapshot_digest: str
    waiver_snapshot_digest: str
    proposed_priority: int
    proposed_enforcement: GuidelineEnforcement
    proposed_minimum_confidence: int
    proposed_metric_threshold_overrides: Mapping[str, int]
    affected_entity_types: tuple[PolicyEntityType, ...]
    items: tuple[GuidelineImpactItem, ...]
    added_metric_ids: tuple[str, ...]
    changed_metric_ids: tuple[str, ...]
    removed_metric_ids: tuple[str, ...]
    requested_by: str
    created_at: datetime
    impact_digest: str
    from_revision_id: str | None = None
    from_semantic_version: str | None = None
    from_revision_digest: str | None = None
    requires_explicit_adoption: bool = True

    def __post_init__(self) -> None:
        _enum(
            self.proposed_enforcement,
            GuidelineEnforcement,
            "guideline_impact_proposed_enforcement_invalid",
        )
        if self.expected_binding_state is not None:
            _enum(
                self.expected_binding_state,
                GuidelineBindingState,
                "guideline_impact_expected_binding_state_invalid",
            )
        for field_name, max_length in (
            ("impact_receipt_id", POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH),
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
            ("to_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("requested_by", POLICY_ACTOR_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"guideline_impact_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "to_revision_number",
            _strict_positive_int(
                self.to_revision_number,
                "guideline_impact_to_revision_number_invalid",
            ),
        )
        object.__setattr__(
            self,
            "to_semantic_version",
            _semantic_version(
                self.to_semantic_version,
                "guideline_impact_to_semantic_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "to_revision_digest",
            _sha256(
                self.to_revision_digest,
                "guideline_impact_to_revision_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_head_revision",
            _strict_positive_int(
                self.expected_head_revision,
                "guideline_impact_expected_head_revision_invalid",
            ),
        )
        if self.expected_binding_revision is not None:
            object.__setattr__(
                self,
                "expected_binding_revision",
                _strict_positive_int(
                    self.expected_binding_revision,
                    "guideline_impact_expected_binding_revision_invalid",
                ),
            )
        for field_name in (
            "binding_digest",
            "binding_head_digest_before",
            "binding_head_digest_after",
            "policy_set_digest_before",
            "policy_set_digest_after",
            "artifact_snapshot_digest",
            "waiver_snapshot_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"guideline_impact_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "proposed_priority",
            _strict_non_negative_int(
                self.proposed_priority,
                "guideline_impact_proposed_priority_invalid",
            ),
        )
        object.__setattr__(
            self,
            "proposed_minimum_confidence",
            _strict_score(
                self.proposed_minimum_confidence,
                "guideline_impact_proposed_minimum_confidence_invalid",
            ),
        )
        object.__setattr__(
            self,
            "proposed_metric_threshold_overrides",
            _metric_threshold_overrides(
                self.proposed_metric_threshold_overrides,
                "guideline_impact_proposed_metric_threshold_overrides_invalid",
            ),
        )
        from_revision = _bounded_optional_text(
            self.from_revision_id,
            max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
            code="guideline_impact_from_revision_id_invalid",
        )
        from_semantic_version = (
            None
            if self.from_semantic_version is None
            else _semantic_version(
                self.from_semantic_version,
                "guideline_impact_from_semantic_version_invalid",
            )
        )
        from_revision_digest = (
            None
            if self.from_revision_digest is None
            else _sha256(
                self.from_revision_digest,
                "guideline_impact_from_revision_digest_invalid",
            )
        )
        if (
            len(
                {
                    value is None
                    for value in (
                        from_revision,
                        from_semantic_version,
                        from_revision_digest,
                    )
                }
            )
            != 1
        ):
            raise GuidelinePolicyContractError(
                "guideline_impact_from_revision_incomplete"
            )
        object.__setattr__(self, "from_revision_id", from_revision)
        object.__setattr__(
            self,
            "from_semantic_version",
            from_semantic_version,
        )
        object.__setattr__(
            self,
            "from_revision_digest",
            from_revision_digest,
        )
        if (self.expected_binding_revision is None) != (from_revision is None):
            raise GuidelinePolicyContractError(
                "guideline_impact_binding_fence_incomplete"
            )
        if (self.expected_binding_revision is None) != (
            self.expected_binding_state is None
        ):
            raise GuidelinePolicyContractError(
                "guideline_impact_binding_state_fence_incomplete"
            )
        affected = _typed_tuple(
            self.affected_entity_types,
            PolicyEntityType,
            "guideline_impact_entity_types_invalid",
        )
        if len(set(affected)) != len(affected):
            raise GuidelinePolicyContractError(
                "guideline_impact_entity_types_duplicate"
            )
        object.__setattr__(
            self,
            "affected_entity_types",
            tuple(sorted(affected, key=lambda item: item.value)),
        )
        items = _typed_tuple(
            self.items,
            GuidelineImpactItem,
            "guideline_impact_items_invalid",
        )
        if len({item.impact_item_id for item in items}) != len(items):
            raise GuidelinePolicyContractError("guideline_impact_items_duplicate")
        object.__setattr__(
            self,
            "items",
            tuple(sorted(items, key=lambda item: item.sort_key)),
        )
        for field_name in (
            "added_metric_ids",
            "changed_metric_ids",
            "removed_metric_ids",
        ):
            normalized_metric_ids = _text_tuple(
                getattr(self, field_name),
                f"guideline_impact_{field_name}_invalid",
            )
            object.__setattr__(
                self,
                field_name,
                tuple(
                    sorted(
                        normalize_policy_bounded_text(
                            metric_id,
                            max_length=POLICY_METRIC_ID_MAX_LENGTH,
                            code=f"guideline_impact_{field_name}_invalid",
                        )
                        for metric_id in normalized_metric_ids
                    )
                ),
            )
        changed_sets = (
            set(self.added_metric_ids),
            set(self.changed_metric_ids),
            set(self.removed_metric_ids),
        )
        if any(
            left & right
            for left in changed_sets
            for right in changed_sets
            if left is not right
        ):
            raise GuidelinePolicyContractError("guideline_impact_metric_sets_overlap")
        if self.requires_explicit_adoption is not True:
            raise GuidelinePolicyContractError("guideline_impact_adoption_flag_invalid")
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(self.created_at, "guideline_impact_created_at_invalid"),
        )
        object.__setattr__(
            self,
            "impact_digest",
            _sha256(
                self.impact_digest,
                "guideline_impact_digest_invalid",
            ),
        )
        if self.impact_digest != guideline_impact_receipt_digest(self):
            raise GuidelinePolicyContractError("guideline_impact_digest_mismatch")

    @property
    def id(self) -> str:
        return self.impact_receipt_id

    @property
    def target_binding_revision(self) -> int:
        return (self.expected_binding_revision or 0) + 1


def guideline_impact_receipt_digest(
    receipt: GuidelineImpactReceipt,
) -> str:
    """Return the canonical digest, excluding request/time/receipt identity."""

    if not isinstance(receipt, GuidelineImpactReceipt):
        raise GuidelinePolicyContractError("guideline_impact_receipt_invalid")
    return guideline_impact_digest_v2(
        board_id=receipt.board_id,
        guideline_id=receipt.guideline_id,
        binding_id=receipt.binding_id,
        from_revision_id=receipt.from_revision_id,
        from_semantic_version=receipt.from_semantic_version,
        from_revision_digest=receipt.from_revision_digest,
        to_revision_id=receipt.to_revision_id,
        to_revision_number=receipt.to_revision_number,
        to_semantic_version=receipt.to_semantic_version,
        to_revision_digest=receipt.to_revision_digest,
        expected_head_revision=receipt.expected_head_revision,
        expected_binding_revision=receipt.expected_binding_revision,
        expected_binding_state=receipt.expected_binding_state,
        binding_digest=receipt.binding_digest,
        binding_head_digest_before=receipt.binding_head_digest_before,
        binding_head_digest_after=receipt.binding_head_digest_after,
        policy_set_digest_before=receipt.policy_set_digest_before,
        policy_set_digest_after=receipt.policy_set_digest_after,
        artifact_snapshot_digest=receipt.artifact_snapshot_digest,
        waiver_snapshot_digest=receipt.waiver_snapshot_digest,
        proposed_priority=receipt.proposed_priority,
        proposed_enforcement=receipt.proposed_enforcement,
        proposed_minimum_confidence=receipt.proposed_minimum_confidence,
        proposed_metric_threshold_overrides=(
            receipt.proposed_metric_threshold_overrides
        ),
        affected_entity_types=receipt.affected_entity_types,
        items=receipt.items,
        added_metric_ids=receipt.added_metric_ids,
        changed_metric_ids=receipt.changed_metric_ids,
        removed_metric_ids=receipt.removed_metric_ids,
        requires_explicit_adoption=receipt.requires_explicit_adoption,
    )


# Transitional import alias while Community migrates to the v2 impact surface.
# It is byte-identical delegation, not a second evaluator or contract path.
guideline_impact_digest_v1 = guideline_impact_digest_v2


@dataclass(frozen=True, slots=True)
class PolicySubjectRef:
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    subject_version: int
    subject_edition: int | None = None

    def __post_init__(self) -> None:
        _enum(
            self.entity_type,
            PolicyEntityType,
            "policy_subject_entity_type_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            normalize_policy_bounded_text(
                self.board_id,
                max_length=POLICY_BOARD_ID_MAX_LENGTH,
                code="policy_subject_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "subject_id",
            normalize_policy_bounded_text(
                self.subject_id,
                max_length=POLICY_SUBJECT_ID_MAX_LENGTH,
                code="policy_subject_id_required",
            ),
        )
        object.__setattr__(
            self,
            "subject_version",
            _strict_positive_int(
                self.subject_version,
                "policy_subject_version_invalid",
            ),
        )
        if self.subject_edition is not None:
            object.__setattr__(
                self,
                "subject_edition",
                _strict_positive_int(
                    self.subject_edition,
                    "policy_subject_edition_invalid",
                ),
            )


@dataclass(frozen=True, slots=True)
class PolicySubjectSnapshot:
    subject: PolicySubjectRef
    content_digest: str
    last_semantic_editor_id: str
    captured_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError("policy_subject_snapshot_ref_invalid")
        object.__setattr__(
            self,
            "content_digest",
            _sha256(
                self.content_digest,
                "policy_subject_content_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "last_semantic_editor_id",
            normalize_policy_bounded_text(
                self.last_semantic_editor_id,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="policy_subject_last_semantic_editor_id_required",
            ),
        )
        object.__setattr__(
            self,
            "captured_at",
            _aware_utc(
                self.captured_at,
                "policy_subject_captured_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class PolicyEvaluationInput:
    evaluation_id: str
    subject_snapshot: PolicySubjectSnapshot
    bindings: tuple[BoardGuidelineBinding, ...]
    input_digest: str
    policy_set_digest: str
    binding_head_digest: str
    catalog_version: str
    ruleset_version: str
    evaluator_version: str
    requested_by: str
    requested_at: datetime
    idempotency_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject_snapshot, PolicySubjectSnapshot):
            raise GuidelinePolicyContractError(
                "policy_evaluation_subject_snapshot_invalid"
            )
        for field_name, max_length in (
            ("evaluation_id", POLICY_EVALUATION_ID_MAX_LENGTH),
            ("catalog_version", POLICY_VERSION_MAX_LENGTH),
            ("ruleset_version", POLICY_VERSION_MAX_LENGTH),
            ("evaluator_version", POLICY_VERSION_MAX_LENGTH),
            ("requested_by", POLICY_ACTOR_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"policy_evaluation_{field_name}_required",
                ),
            )
        bindings = _typed_tuple(
            self.bindings,
            BoardGuidelineBinding,
            "policy_evaluation_bindings_invalid",
        )
        if any(
            binding.board_id != self.subject_snapshot.subject.board_id
            for binding in bindings
        ):
            raise GuidelinePolicyContractError(
                "policy_evaluation_binding_board_mismatch"
            )
        if len({binding.binding_id for binding in bindings}) != len(bindings):
            raise GuidelinePolicyContractError("policy_evaluation_duplicate_binding")
        if len({binding.guideline_id for binding in bindings}) != len(bindings):
            raise GuidelinePolicyContractError(
                "policy_evaluation_duplicate_guideline_binding"
            )
        object.__setattr__(
            self,
            "bindings",
            tuple(
                sorted(
                    bindings,
                    key=lambda binding: (
                        binding.priority,
                        binding.binding_id,
                    ),
                )
            ),
        )
        for field_name in (
            "input_digest",
            "policy_set_digest",
            "binding_head_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_evaluation_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "requested_at",
            _aware_utc(
                self.requested_at,
                "policy_evaluation_requested_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class PolicyComplianceFinding:
    finding_id: str
    receipt_id: str
    subject: PolicySubjectRef
    guideline_id: str
    revision_id: str
    rule_id: str
    outcome: PolicyEvaluationOutcome
    enforcement: GuidelineEnforcement
    message: str
    created_at: datetime
    evidence_refs: tuple[str, ...] = ()
    waiver_id: str | None = None

    def __post_init__(self) -> None:
        _enum(
            self.outcome,
            PolicyEvaluationOutcome,
            "policy_finding_outcome_invalid",
        )
        if self.outcome not in {
            PolicyEvaluationOutcome.FAIL,
            PolicyEvaluationOutcome.ERROR,
        }:
            raise GuidelinePolicyContractError("policy_finding_outcome_invalid")
        _enum(
            self.enforcement,
            GuidelineEnforcement,
            "policy_finding_enforcement_invalid",
        )
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError("policy_finding_subject_invalid")
        for field_name, max_length in (
            ("finding_id", POLICY_FINDING_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("rule_id", POLICY_RULE_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"policy_finding_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "message",
            _required_text(self.message, "policy_finding_message_required"),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _text_tuple(
                self.evidence_refs,
                "policy_finding_evidence_refs_invalid",
            ),
        )
        object.__setattr__(
            self,
            "waiver_id",
            _bounded_optional_text(
                self.waiver_id,
                max_length=POLICY_WAIVER_ID_MAX_LENGTH,
                code="policy_finding_waiver_id_invalid",
            ),
        )
        if (
            self.waiver_id is not None
            and self.outcome is not PolicyEvaluationOutcome.FAIL
        ):
            raise GuidelinePolicyContractError("policy_finding_waiver_requires_failure")
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(self.created_at, "policy_finding_created_at_invalid"),
        )

    @property
    def blocking(self) -> bool:
        return (
            self.outcome
            in {
                PolicyEvaluationOutcome.FAIL,
                PolicyEvaluationOutcome.ERROR,
            }
            and self.enforcement is GuidelineEnforcement.BLOCKING
            and self.waiver_id is None
        )


@dataclass(frozen=True, slots=True)
class PolicyComplianceRuleResult:
    """Immutable outcome for every applicable rule, including passes."""

    guideline_id: str
    revision_id: str
    rule_id: str
    outcome: PolicyEvaluationOutcome
    enforcement: GuidelineEnforcement
    waiver_id: str | None = None

    def __post_init__(self) -> None:
        _enum(
            self.outcome,
            PolicyEvaluationOutcome,
            "policy_rule_result_outcome_invalid",
        )
        if self.outcome not in {
            PolicyEvaluationOutcome.PASS,
            PolicyEvaluationOutcome.FAIL,
            PolicyEvaluationOutcome.ERROR,
        }:
            raise GuidelinePolicyContractError("policy_rule_result_outcome_invalid")
        _enum(
            self.enforcement,
            GuidelineEnforcement,
            "policy_rule_result_enforcement_invalid",
        )
        for field_name, max_length in (
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("rule_id", POLICY_RULE_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"policy_rule_result_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "waiver_id",
            _bounded_optional_text(
                self.waiver_id,
                max_length=POLICY_WAIVER_ID_MAX_LENGTH,
                code="policy_rule_result_waiver_id_invalid",
            ),
        )
        if (
            self.waiver_id is not None
            and self.outcome is not PolicyEvaluationOutcome.FAIL
        ):
            raise GuidelinePolicyContractError(
                "policy_rule_result_waiver_requires_failure"
            )

    @property
    def blocking(self) -> bool:
        return (
            self.outcome
            in {
                PolicyEvaluationOutcome.FAIL,
                PolicyEvaluationOutcome.ERROR,
            }
            and self.enforcement is GuidelineEnforcement.BLOCKING
            and self.waiver_id is None
        )


@dataclass(frozen=True, slots=True)
class PolicyComplianceReceipt:
    receipt_id: str
    subject: PolicySubjectRef
    subject_content_digest: str
    input_digest: str
    policy_set_digest: str
    binding_head_digest: str
    catalog_version: str
    ruleset_version: str
    adopted_revisions: tuple[AdoptedGuidelineRevisionRef, ...]
    outcome: PolicyEvaluationOutcome
    state: PolicyComplianceState
    currentness: PolicyCurrentness
    findings: tuple[PolicyComplianceFinding, ...]
    evaluator_version: str
    evaluated_by: str
    evaluated_at: datetime
    rule_results: tuple[PolicyComplianceRuleResult, ...] = ()
    reason_codes: tuple[PolicyComplianceReasonCode, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError("policy_receipt_subject_invalid")
        _enum(
            self.outcome,
            PolicyEvaluationOutcome,
            "policy_receipt_outcome_invalid",
        )
        _enum(
            self.state,
            PolicyComplianceState,
            "policy_receipt_state_invalid",
        )
        _enum(
            self.currentness,
            PolicyCurrentness,
            "policy_receipt_currentness_invalid",
        )
        if self.currentness is not PolicyCurrentness.CURRENT:
            raise GuidelinePolicyContractError(
                "policy_receipt_recorded_currentness_must_be_current"
            )
        for field_name, max_length in (
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("catalog_version", POLICY_VERSION_MAX_LENGTH),
            ("ruleset_version", POLICY_VERSION_MAX_LENGTH),
            ("evaluator_version", POLICY_VERSION_MAX_LENGTH),
            ("evaluated_by", POLICY_ACTOR_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"policy_receipt_{field_name}_required",
                ),
            )
        for field_name in (
            "subject_content_digest",
            "input_digest",
            "policy_set_digest",
            "binding_head_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_receipt_{field_name}_invalid",
                ),
            )
        adopted_revisions = _typed_tuple(
            self.adopted_revisions,
            AdoptedGuidelineRevisionRef,
            "policy_receipt_adopted_revisions_invalid",
        )
        if len({item.binding_id for item in adopted_revisions}) != len(
            adopted_revisions
        ) or len({item.guideline_id for item in adopted_revisions}) != len(
            adopted_revisions
        ):
            raise GuidelinePolicyContractError(
                "policy_receipt_duplicate_adopted_revision"
            )
        object.__setattr__(
            self,
            "adopted_revisions",
            tuple(
                sorted(
                    adopted_revisions,
                    key=lambda item: (item.guideline_id, item.binding_id),
                )
            ),
        )
        rule_results = _typed_tuple(
            self.rule_results,
            PolicyComplianceRuleResult,
            "policy_receipt_rule_results_invalid",
        )
        rule_keys = tuple(
            (result.guideline_id, result.revision_id, result.rule_id)
            for result in rule_results
        )
        if len(set(rule_keys)) != len(rule_keys):
            raise GuidelinePolicyContractError("policy_receipt_duplicate_rule_result")
        for result in rule_results:
            if not any(
                adopted.guideline_id == result.guideline_id
                and adopted.revision_id == result.revision_id
                for adopted in adopted_revisions
            ):
                raise GuidelinePolicyContractError(
                    "policy_receipt_rule_revision_not_adopted"
                )
        object.__setattr__(
            self,
            "rule_results",
            tuple(
                sorted(
                    rule_results,
                    key=lambda result: (
                        result.guideline_id,
                        result.rule_id,
                    ),
                )
            ),
        )
        reason_codes = _typed_tuple(
            self.reason_codes,
            PolicyComplianceReasonCode,
            "policy_receipt_reason_codes_invalid",
        )
        if len(set(reason_codes)) != len(reason_codes):
            raise GuidelinePolicyContractError("policy_receipt_reason_codes_duplicate")
        object.__setattr__(
            self,
            "reason_codes",
            tuple(sorted(reason_codes, key=lambda reason: reason.value)),
        )
        findings = _typed_tuple(
            self.findings,
            PolicyComplianceFinding,
            "policy_receipt_findings_invalid",
        )
        if len({finding.finding_id for finding in findings}) != len(findings):
            raise GuidelinePolicyContractError("policy_receipt_duplicate_finding")
        finding_rule_keys: set[tuple[str, str, str]] = set()
        for finding in findings:
            if finding.receipt_id != self.receipt_id or finding.subject != self.subject:
                raise GuidelinePolicyContractError(
                    "policy_receipt_finding_scope_mismatch"
                )
            finding_rule_key = (
                finding.guideline_id,
                finding.revision_id,
                finding.rule_id,
            )
            if finding_rule_key in finding_rule_keys:
                raise GuidelinePolicyContractError(
                    "policy_receipt_duplicate_finding_for_rule"
                )
            finding_rule_keys.add(finding_rule_key)
            if not any(
                result.guideline_id == finding.guideline_id
                and result.revision_id == finding.revision_id
                and result.rule_id == finding.rule_id
                and result.outcome == finding.outcome
                and result.enforcement == finding.enforcement
                and result.waiver_id == finding.waiver_id
                for result in rule_results
            ):
                raise GuidelinePolicyContractError(
                    "policy_receipt_finding_rule_result_mismatch"
                )
        expected_finding_rule_keys = {
            (result.guideline_id, result.revision_id, result.rule_id)
            for result in rule_results
            if result.outcome
            in {
                PolicyEvaluationOutcome.FAIL,
                PolicyEvaluationOutcome.ERROR,
            }
        }
        if finding_rule_keys != expected_finding_rule_keys:
            raise GuidelinePolicyContractError("policy_receipt_findings_incomplete")
        object.__setattr__(
            self,
            "findings",
            tuple(sorted(findings, key=lambda finding: finding.finding_id)),
        )
        blocking = any(result.blocking for result in rule_results)
        waived = any(result.waiver_id is not None for result in rule_results)
        failed = any(
            result.outcome is PolicyEvaluationOutcome.FAIL for result in rule_results
        )
        errors = any(
            result.outcome is PolicyEvaluationOutcome.ERROR for result in rule_results
        )
        blocking_errors = any(
            result.outcome is PolicyEvaluationOutcome.ERROR
            and result.enforcement is GuidelineEnforcement.BLOCKING
            for result in rule_results
        )
        advisory_errors = errors and not blocking_errors
        if not rule_results:
            expected_state = PolicyComplianceState.NOT_APPLICABLE
            expected_outcome = PolicyEvaluationOutcome.NOT_APPLICABLE
        elif blocking:
            expected_state = PolicyComplianceState.BLOCKED
            expected_outcome = (
                PolicyEvaluationOutcome.ERROR
                if blocking_errors
                else PolicyEvaluationOutcome.FAIL
            )
        elif waived:
            expected_state = PolicyComplianceState.READY_WITH_WAIVERS
            expected_outcome = (
                PolicyEvaluationOutcome.ERROR
                if advisory_errors
                else PolicyEvaluationOutcome.FAIL
            )
        else:
            expected_state = PolicyComplianceState.READY
            expected_outcome = (
                PolicyEvaluationOutcome.ERROR
                if advisory_errors
                else (
                    PolicyEvaluationOutcome.FAIL
                    if failed
                    else PolicyEvaluationOutcome.PASS
                )
            )
        if self.state is not expected_state or self.outcome is not expected_outcome:
            raise GuidelinePolicyContractError(
                "policy_receipt_state_outcome_inconsistent"
            )
        reason_set = set(reason_codes)
        expected_conditions = {
            PolicyComplianceReasonCode.NO_APPLICABLE_RULES: not rule_results,
            PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE: (blocking_errors),
            PolicyComplianceReasonCode.POLICY_EVALUATION_DEGRADED: (advisory_errors),
        }
        for reason, required in expected_conditions.items():
            if (reason in reason_set) != required:
                raise GuidelinePolicyContractError(
                    "policy_receipt_reason_codes_inconsistent"
                )
        object.__setattr__(
            self,
            "evaluated_at",
            _aware_utc(self.evaluated_at, "policy_receipt_evaluated_at_invalid"),
        )

    @property
    def id(self) -> str:
        return self.receipt_id

    @property
    def rule_count(self) -> int:
        return len(self.rule_results)

    @property
    def failed_rule_count(self) -> int:
        return sum(
            1
            for result in self.rule_results
            if result.outcome is PolicyEvaluationOutcome.FAIL
        )

    @property
    def error_rule_count(self) -> int:
        return sum(
            1
            for result in self.rule_results
            if result.outcome is PolicyEvaluationOutcome.ERROR
        )

    @property
    def blocking_rule_count(self) -> int:
        return sum(1 for result in self.rule_results if result.blocking)

    @property
    def waived_rule_count(self) -> int:
        return sum(1 for result in self.rule_results if result.waiver_id is not None)


@dataclass(frozen=True, slots=True)
class PolicyEvaluationResult:
    evaluation_id: str
    input_digest: str
    receipt: PolicyComplianceReceipt

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evaluation_id",
            normalize_policy_bounded_text(
                self.evaluation_id,
                max_length=POLICY_EVALUATION_ID_MAX_LENGTH,
                code="policy_evaluation_result_id_required",
            ),
        )
        object.__setattr__(
            self,
            "input_digest",
            _sha256(
                self.input_digest,
                "policy_evaluation_result_digest_invalid",
            ),
        )
        if not isinstance(self.receipt, PolicyComplianceReceipt):
            raise GuidelinePolicyContractError(
                "policy_evaluation_result_receipt_invalid"
            )
        if self.receipt.input_digest != self.input_digest:
            raise GuidelinePolicyContractError(
                "policy_evaluation_result_digest_mismatch"
            )


@dataclass(frozen=True, slots=True)
class PolicyWaiver:
    """Materialized head of an append-only waiver lifecycle.

    Each lifecycle operation creates a new ``waiver_revision``.  In
    particular, ``revalidate`` records its own immutable event and materializes
    the resulting revision as ``PolicyWaiverStatus.APPROVED``; it does not
    expand the closed status enum.
    """

    waiver_id: str
    board_id: str
    finding_id: str
    receipt_id: str
    guideline_id: str
    revision_id: str
    rule_id: str
    subject: PolicySubjectRef
    status: PolicyWaiverStatus
    justification: str
    evidence_refs: tuple[str, ...]
    requested_by: str
    requested_at: datetime
    waiver_revision: int
    expires_at: datetime
    last_event_id: str
    last_event_type: PolicyWaiverEventType
    last_event_at: datetime
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_reason: str | None = None
    revoked_by: str | None = None
    revoked_at: datetime | None = None
    expire_reason_code: PolicyWaiverExpireReasonCode | None = None

    def __post_init__(self) -> None:
        _enum(
            self.status,
            PolicyWaiverStatus,
            "policy_waiver_status_invalid",
        )
        _enum(
            self.last_event_type,
            PolicyWaiverEventType,
            "policy_waiver_last_event_type_invalid",
        )
        if self.expire_reason_code is not None:
            _enum(
                self.expire_reason_code,
                PolicyWaiverExpireReasonCode,
                "policy_waiver_expire_reason_code_invalid",
            )
        if not isinstance(self.subject, PolicySubjectRef):
            raise GuidelinePolicyContractError("policy_waiver_subject_invalid")
        for field_name, max_length in (
            ("waiver_id", POLICY_WAIVER_ID_MAX_LENGTH),
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("finding_id", POLICY_FINDING_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("rule_id", POLICY_RULE_ID_MAX_LENGTH),
            ("requested_by", POLICY_ACTOR_ID_MAX_LENGTH),
            ("last_event_id", POLICY_WAIVER_EVENT_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"policy_waiver_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "justification",
            _required_text(
                self.justification,
                "policy_waiver_justification_required",
            ),
        )
        if self.board_id != self.subject.board_id:
            raise GuidelinePolicyContractError("policy_waiver_subject_board_mismatch")
        object.__setattr__(
            self,
            "waiver_revision",
            _strict_positive_int(
                self.waiver_revision,
                "policy_waiver_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "requested_at",
            _aware_utc(self.requested_at, "policy_waiver_requested_at_invalid"),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _text_tuple(
                self.evidence_refs,
                "policy_waiver_evidence_refs_invalid",
            ),
        )
        if not self.evidence_refs:
            raise GuidelinePolicyContractError("policy_waiver_evidence_refs_required")
        for field_name in (
            "expires_at",
            "last_event_at",
            "reviewed_at",
            "revoked_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _aware_utc(
                        value,
                        f"policy_waiver_{field_name}_invalid",
                    ),
                )
        for field_name in ("reviewed_by", "revoked_by"):
            object.__setattr__(
                self,
                field_name,
                _bounded_optional_text(
                    getattr(self, field_name),
                    max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                    code=f"policy_waiver_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "review_reason",
            _optional_text(
                self.review_reason,
                "policy_waiver_review_reason_invalid",
            ),
        )

        reviewed = self.status in {
            PolicyWaiverStatus.APPROVED,
            PolicyWaiverStatus.REJECTED,
            PolicyWaiverStatus.REVOKED,
            PolicyWaiverStatus.EXPIRED,
        }
        if reviewed and (
            self.reviewed_by is None
            or self.reviewed_at is None
            or self.review_reason is None
        ):
            raise GuidelinePolicyContractError("policy_waiver_review_required")
        if self.reviewed_by is not None and self.reviewed_by == self.requested_by:
            raise GuidelinePolicyContractError(
                "policy_waiver_independent_reviewer_required"
            )
        if not reviewed and any(
            value is not None
            for value in (
                self.reviewed_by,
                self.reviewed_at,
                self.review_reason,
            )
        ):
            raise GuidelinePolicyContractError("policy_waiver_review_forbidden")
        if self.status is PolicyWaiverStatus.REVOKED and (
            self.revoked_by is None or self.revoked_at is None
        ):
            raise GuidelinePolicyContractError("policy_waiver_revocation_required")
        if self.status is not PolicyWaiverStatus.REVOKED and any(
            value is not None for value in (self.revoked_by, self.revoked_at)
        ):
            raise GuidelinePolicyContractError("policy_waiver_revocation_forbidden")
        if self.status is PolicyWaiverStatus.EXPIRED:
            if self.expire_reason_code is None:
                raise GuidelinePolicyContractError(
                    "policy_waiver_expire_reason_code_required"
                )
        elif self.expire_reason_code is not None:
            raise GuidelinePolicyContractError(
                "policy_waiver_expire_reason_code_forbidden"
            )
        if self.expires_at <= self.requested_at:
            raise GuidelinePolicyContractError("policy_waiver_expiry_invalid")
        if self.reviewed_at is not None and self.reviewed_at < self.requested_at:
            raise GuidelinePolicyContractError("policy_waiver_review_before_request")
        if self.revoked_at is not None and (
            self.reviewed_at is None or self.revoked_at < self.reviewed_at
        ):
            raise GuidelinePolicyContractError("policy_waiver_revocation_before_review")
        if self.last_event_at < self.requested_at:
            raise GuidelinePolicyContractError("policy_waiver_event_before_request")
        if self.reviewed_at is not None and self.last_event_at < self.reviewed_at:
            raise GuidelinePolicyContractError("policy_waiver_event_before_review")
        if self.revoked_at is not None and self.last_event_at != self.revoked_at:
            raise GuidelinePolicyContractError(
                "policy_waiver_revocation_event_mismatch"
            )

        allowed_last_events = {
            PolicyWaiverStatus.REQUESTED: {PolicyWaiverEventType.REQUEST},
            PolicyWaiverStatus.APPROVED: {
                PolicyWaiverEventType.APPROVE,
                PolicyWaiverEventType.REVALIDATE,
            },
            PolicyWaiverStatus.REJECTED: {PolicyWaiverEventType.REJECT},
            PolicyWaiverStatus.REVOKED: {PolicyWaiverEventType.REVOKE},
            PolicyWaiverStatus.EXPIRED: {PolicyWaiverEventType.EXPIRE},
        }
        if self.last_event_type not in allowed_last_events[self.status]:
            raise GuidelinePolicyContractError("policy_waiver_status_event_mismatch")
        if (
            self.last_event_type is PolicyWaiverEventType.REQUEST
            and self.waiver_revision != 1
        ) or (
            self.last_event_type is not PolicyWaiverEventType.REQUEST
            and self.waiver_revision == 1
        ):
            raise GuidelinePolicyContractError("policy_waiver_event_revision_mismatch")
        if (
            self.status is PolicyWaiverStatus.APPROVED
            and self.expires_at <= self.reviewed_at
        ):
            raise GuidelinePolicyContractError("policy_waiver_expiry_not_after_review")
        if (
            self.status is PolicyWaiverStatus.EXPIRED
            and self.expire_reason_code is PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
            and self.last_event_at < self.expires_at
        ):
            raise GuidelinePolicyContractError("policy_waiver_expire_event_too_early")

    @property
    def id(self) -> str:
        return self.waiver_id

    def is_effective_at(self, evaluated_at: datetime) -> bool:
        """Return whether this exact materialized head grants privilege."""

        now = _aware_utc(
            evaluated_at,
            "policy_waiver_effective_at_invalid",
        )
        return (
            self.status is PolicyWaiverStatus.APPROVED
            and self.reviewed_by is not None
            and self.reviewed_at is not None
            and self.reviewed_at <= now < self.expires_at
        )


@dataclass(frozen=True, slots=True)
class PolicyWaiverEvent:
    """One immutable operation in a governed waiver lifecycle."""

    event_id: str
    waiver_id: str
    board_id: str
    waiver_revision: int
    event_type: PolicyWaiverEventType
    from_status: PolicyWaiverStatus | None
    to_status: PolicyWaiverStatus
    actor_id: str
    occurred_at: datetime
    reason: str
    evidence_refs: tuple[str, ...]
    expires_at: datetime
    scope_digest: str
    expire_reason_code: PolicyWaiverExpireReasonCode | None = None

    def __post_init__(self) -> None:
        _enum(
            self.event_type,
            PolicyWaiverEventType,
            "policy_waiver_event_type_invalid",
        )
        if self.from_status is not None:
            _enum(
                self.from_status,
                PolicyWaiverStatus,
                "policy_waiver_event_from_status_invalid",
            )
        _enum(
            self.to_status,
            PolicyWaiverStatus,
            "policy_waiver_event_to_status_invalid",
        )
        if self.expire_reason_code is not None:
            _enum(
                self.expire_reason_code,
                PolicyWaiverExpireReasonCode,
                "policy_waiver_event_expire_reason_code_invalid",
            )
        for field_name, max_length in (
            ("event_id", POLICY_WAIVER_EVENT_ID_MAX_LENGTH),
            ("waiver_id", POLICY_WAIVER_ID_MAX_LENGTH),
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("actor_id", POLICY_ACTOR_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"policy_waiver_event_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "policy_waiver_event_reason_required"),
        )
        object.__setattr__(
            self,
            "waiver_revision",
            _strict_positive_int(
                self.waiver_revision,
                "policy_waiver_event_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "policy_waiver_event_occurred_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expires_at",
            _aware_utc(
                self.expires_at,
                "policy_waiver_event_expires_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _text_tuple(
                self.evidence_refs,
                "policy_waiver_event_evidence_refs_invalid",
            ),
        )
        if not self.evidence_refs:
            raise GuidelinePolicyContractError(
                "policy_waiver_event_evidence_refs_required"
            )
        object.__setattr__(
            self,
            "scope_digest",
            _sha256(
                self.scope_digest,
                "policy_waiver_event_scope_digest_invalid",
            ),
        )

        transitions = {
            PolicyWaiverEventType.REQUEST: (
                None,
                PolicyWaiverStatus.REQUESTED,
            ),
            PolicyWaiverEventType.APPROVE: (
                PolicyWaiverStatus.REQUESTED,
                PolicyWaiverStatus.APPROVED,
            ),
            PolicyWaiverEventType.REJECT: (
                PolicyWaiverStatus.REQUESTED,
                PolicyWaiverStatus.REJECTED,
            ),
            PolicyWaiverEventType.REVOKE: (
                PolicyWaiverStatus.APPROVED,
                PolicyWaiverStatus.REVOKED,
            ),
            PolicyWaiverEventType.EXPIRE: (
                PolicyWaiverStatus.APPROVED,
                PolicyWaiverStatus.EXPIRED,
            ),
        }
        if self.event_type is PolicyWaiverEventType.REVALIDATE:
            if (
                self.from_status
                not in {
                    PolicyWaiverStatus.APPROVED,
                    PolicyWaiverStatus.EXPIRED,
                }
                or self.to_status is not PolicyWaiverStatus.APPROVED
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_event_transition_invalid"
                )
        elif (
            self.from_status,
            self.to_status,
        ) != transitions[self.event_type]:
            raise GuidelinePolicyContractError("policy_waiver_event_transition_invalid")
        if (
            self.event_type is PolicyWaiverEventType.REQUEST
            and self.waiver_revision != 1
        ) or (
            self.event_type is not PolicyWaiverEventType.REQUEST
            and self.waiver_revision == 1
        ):
            raise GuidelinePolicyContractError("policy_waiver_event_revision_mismatch")
        if self.event_type is PolicyWaiverEventType.EXPIRE:
            if self.expire_reason_code is None:
                raise GuidelinePolicyContractError(
                    "policy_waiver_event_expire_reason_code_required"
                )
            if (
                self.expire_reason_code is PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
                and self.occurred_at < self.expires_at
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_expire_event_too_early"
                )
            if (
                self.expire_reason_code
                is not PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY
                and self.occurred_at >= self.expires_at
            ):
                raise GuidelinePolicyContractError(
                    "policy_waiver_structural_expire_after_boundary"
                )
        elif self.expire_reason_code is not None:
            raise GuidelinePolicyContractError(
                "policy_waiver_event_expire_reason_code_forbidden"
            )
        if self.event_type is not PolicyWaiverEventType.EXPIRE and (
            self.event_type
            in {
                PolicyWaiverEventType.REQUEST,
                PolicyWaiverEventType.APPROVE,
                PolicyWaiverEventType.REVOKE,
                PolicyWaiverEventType.REVALIDATE,
            }
            and self.expires_at <= self.occurred_at
        ):
            raise GuidelinePolicyContractError("policy_waiver_event_expiry_not_future")

    @property
    def id(self) -> str:
        return self.event_id


@dataclass(frozen=True, slots=True)
class PolicyWaiverAuthorization:
    """Currentness-fenced privilege token consumed by the evaluator.

    Raw waiver heads are audit state, not authorization.  A persistence
    resolver may issue this value only after proving the source receipt current
    against the exact evaluation fences.
    """

    waiver: PolicyWaiver
    subject_content_digest: str
    input_digest: str
    policy_set_digest: str
    binding_head_digest: str
    catalog_version: str
    ruleset_version: str
    resolved_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.waiver, PolicyWaiver):
            raise GuidelinePolicyContractError(
                "policy_waiver_authorization_head_invalid"
            )
        for field_name in (
            "subject_content_digest",
            "input_digest",
            "policy_set_digest",
            "binding_head_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_waiver_authorization_{field_name}_invalid",
                ),
            )
        for field_name in ("catalog_version", "ruleset_version"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_waiver_authorization_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "resolved_at",
            _aware_utc(
                self.resolved_at,
                "policy_waiver_authorization_resolved_at_invalid",
            ),
        )
        if not self.waiver.is_effective_at(self.resolved_at):
            raise GuidelinePolicyContractError(
                "policy_waiver_authorization_head_not_effective"
            )

    def matches(self, evaluation_input: PolicyEvaluationInput) -> bool:
        if not isinstance(evaluation_input, PolicyEvaluationInput):
            raise GuidelinePolicyContractError(
                "policy_waiver_authorization_input_invalid"
            )
        snapshot = evaluation_input.subject_snapshot
        return (
            self.waiver.subject == snapshot.subject
            and self.subject_content_digest == snapshot.content_digest
            and self.input_digest == evaluation_input.input_digest
            and self.policy_set_digest == evaluation_input.policy_set_digest
            and self.binding_head_digest == evaluation_input.binding_head_digest
            and self.catalog_version == evaluation_input.catalog_version
            and self.ruleset_version == evaluation_input.ruleset_version
        )


@dataclass(frozen=True, slots=True)
class GuidelinePageCursor:
    created_at: datetime
    item_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(self.created_at, "guideline_cursor_created_at_invalid"),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "guideline_cursor_item_id_required"),
        )


@dataclass(frozen=True, slots=True)
class GuidelineRevisionPageCursor:
    """Context-bound keyset anchor for TR-8 ordinal revision ordering.

    The value object is never sent over a public transport directly.  REST and
    MCP encode it through ``PolicyCursorCodec`` so every anchor is opaque,
    authenticated, and bound to the exact filter and projection that created
    it.
    """

    revision_number: int
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str = POLICY_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str] = GUIDELINE_REVISION_ORDERING

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("policy_cursor_schema_version_invalid")
        if tuple(self.ordering) != GUIDELINE_REVISION_ORDERING:
            raise GuidelinePolicyContractError(
                "guideline_revision_cursor_ordering_invalid"
            )
        object.__setattr__(
            self,
            "revision_number",
            _strict_positive_int(
                self.revision_number,
                "guideline_revision_cursor_number_invalid",
            ),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(
                self.item_id,
                "guideline_revision_cursor_item_id_required",
            ),
        )
        for field_name in ("filter_digest", "projection_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"guideline_revision_cursor_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "ordering", GUIDELINE_REVISION_ORDERING)


_PageItemT = TypeVar("_PageItemT")


@dataclass(frozen=True, slots=True)
class GuidelinePage(Generic[_PageItemT]):
    """Stable keyset page ordered by ``created_at DESC, id DESC``."""

    items: tuple[_PageItemT, ...]
    limit: int
    next_cursor: GuidelinePageCursor | None
    has_more: bool

    ordering: ClassVar[tuple[str, str]] = ("created_at DESC", "id DESC")

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise GuidelinePolicyContractError("guideline_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        limit = _strict_positive_int(
            self.limit,
            "guideline_page_limit_invalid",
        )
        if limit > GUIDELINE_PAGE_LIMIT_MAX:
            raise GuidelinePolicyContractError("guideline_page_limit_invalid")
        object.__setattr__(self, "limit", limit)
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            GuidelinePageCursor,
        ):
            raise GuidelinePolicyContractError("guideline_page_cursor_invalid")
        if not isinstance(self.has_more, bool):
            raise GuidelinePolicyContractError("guideline_page_has_more_invalid")
        if len(self.items) > limit:
            raise GuidelinePolicyContractError("guideline_page_over_limit")
        if self.has_more != (self.next_cursor is not None):
            raise GuidelinePolicyContractError("guideline_page_cursor_mismatch")


@dataclass(frozen=True, slots=True)
class GuidelineRevisionPage:
    """Stable TR-8 page ordered by revision ordinal, never wall-clock time."""

    items: tuple[GuidelineRevision, ...]
    limit: int
    next_cursor: GuidelineRevisionPageCursor | None
    has_more: bool

    ordering: ClassVar[tuple[str, str]] = GUIDELINE_REVISION_ORDERING

    def __post_init__(self) -> None:
        items = _typed_tuple(
            self.items,
            GuidelineRevision,
            "guideline_revision_page_items_invalid",
        )
        object.__setattr__(self, "items", items)
        limit = _strict_positive_int(
            self.limit,
            "guideline_page_limit_invalid",
        )
        if limit > GUIDELINE_PAGE_LIMIT_MAX:
            raise GuidelinePolicyContractError("guideline_page_limit_invalid")
        object.__setattr__(self, "limit", limit)
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            GuidelineRevisionPageCursor,
        ):
            raise GuidelinePolicyContractError("guideline_revision_page_cursor_invalid")
        if not isinstance(self.has_more, bool):
            raise GuidelinePolicyContractError("guideline_page_has_more_invalid")
        if len(items) > limit:
            raise GuidelinePolicyContractError("guideline_page_over_limit")
        if self.has_more != (self.next_cursor is not None):
            raise GuidelinePolicyContractError("guideline_page_cursor_mismatch")


@dataclass(frozen=True, slots=True)
class GuidelineOffsetPage(Generic[_PageItemT]):
    """REST offset page with exact filtered and overall totals."""

    items: tuple[_PageItemT, ...]
    offset: int
    limit: int
    total_filtered: int
    total_overall: int

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise GuidelinePolicyContractError("guideline_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        offset = _strict_non_negative_int(
            self.offset,
            "guideline_page_offset_invalid",
        )
        limit = _strict_positive_int(
            self.limit,
            "guideline_page_limit_invalid",
        )
        if limit not in GUIDELINE_REST_PAGE_LIMITS:
            raise GuidelinePolicyContractError("guideline_page_limit_invalid")
        total_filtered = _strict_non_negative_int(
            self.total_filtered,
            "guideline_page_total_invalid",
        )
        total_overall = _strict_non_negative_int(
            self.total_overall,
            "guideline_page_total_invalid",
        )
        if total_filtered > total_overall or len(self.items) > limit:
            raise GuidelinePolicyContractError("guideline_page_total_invalid")
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "limit", limit)
        object.__setattr__(self, "total_filtered", total_filtered)
        object.__setattr__(self, "total_overall", total_overall)


__all__ = [
    "GUIDELINE_BINDING_ID_MAX_LENGTH",
    "GUIDELINE_BINDING_CONFIGURATION_CONTRACT_VERSION",
    "GUIDELINE_BINDING_ORIGIN_MAX_LENGTH",
    "GUIDELINE_BINDING_SOURCE_KIND_MAX_LENGTH",
    "GUIDELINE_DOMAIN_CONTRACT_VERSION",
    "GUIDELINE_ID_MAX_LENGTH",
    "GUIDELINE_LEGACY_VERSION_MAX_LENGTH",
    "GUIDELINE_IMPACT_CONTRACT_VERSION",
    "GUIDELINE_PAGE_LIMIT_MAX",
    "GUIDELINE_REST_PAGE_LIMITS",
    "GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION",
    "GUIDELINE_RETIREMENT_ID_MAX_LENGTH",
    "GUIDELINE_REVISION_ID_MAX_LENGTH",
    "GUIDELINE_REVISION_ORDERING",
    "GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH",
    "GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS",
    "GUIDELINE_TITLE_MAX_LENGTH",
    "POLICY_ACTOR_ID_MAX_LENGTH",
    "POLICY_BOARD_ID_MAX_LENGTH",
    "POLICY_ENTITY_ID_MAX_LENGTH",
    "POLICY_ENTITY_TYPE_MAX_LENGTH",
    "POLICY_EVALUATION_ID_MAX_LENGTH",
    "POLICY_FINDING_ID_MAX_LENGTH",
    "POLICY_IDEMPOTENCY_KEY_MAX_LENGTH",
    "POLICY_IMPACT_ITEM_ID_MAX_LENGTH",
    "POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH",
    "POLICY_KEYSET_CONTRACT_VERSION",
    "POLICY_METRIC_CODE_MAX_LENGTH",
    "POLICY_METRIC_ID_MAX_LENGTH",
    "POLICY_RECEIPT_ID_MAX_LENGTH",
    "POLICY_SQL_INTEGER_MAX",
    "POLICY_SUBJECT_ID_MAX_LENGTH",
    "POLICY_WAIVER_EVENT_ID_MAX_LENGTH",
    "POLICY_WAIVER_ID_MAX_LENGTH",
    "POLICY_VERSION_MAX_LENGTH",
    "BoardGuidelineBinding",
    "GuidelineBindingProvenance",
    "GuidelineBindingState",
    "Guideline",
    "GuidelineEnforcement",
    "GuidelineHead",
    "GuidelineImpactItem",
    "GuidelineImpactItemKind",
    "GuidelineImpactReceipt",
    "GuidelineLifecycleStatus",
    "GuidelineMetric",
    "GuidelineMetricDirection",
    "GuidelineContextScope",
    "GuidelineOffsetPage",
    "GuidelinePage",
    "GuidelinePageCursor",
    "GuidelinePolicyContractError",
    "GuidelineRevision",
    "GuidelineRevisionPage",
    "GuidelineRevisionPageCursor",
    "GuidelineRetirement",
    "GuidelineScope",
    "PolicyCurrentness",
    "PolicyEntityType",
    "PolicySubjectRef",
    "PolicySubjectSnapshot",
    "PolicyWaiver",
    "PolicyWaiverAuthorization",
    "PolicyWaiverEvent",
    "PolicyWaiverEventType",
    "PolicyWaiverExpireReasonCode",
    "PolicyWaiverStatus",
    "RESERVED_CONFIDENCE_FIELD",
    "guideline_binding_configuration_digest_v1",
    "guideline_binding_snapshot_digest",
    "guideline_impact_digest_v2",
    "guideline_impact_receipt_digest",
    "guideline_revision_digest_v2",
    "normalize_guideline_semantic_version",
    "normalize_guideline_sha256",
    "normalize_policy_bounded_text",
]
