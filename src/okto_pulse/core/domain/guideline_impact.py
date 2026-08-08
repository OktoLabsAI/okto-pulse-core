"""Deterministic impact-preview and explicit-adoption contracts for SK-B.

The module is pure.  It plans immutable evidence and mutations from caller
supplied snapshots; it owns no database transaction, transport, clock, UUID
generation, event dispatch, or KG write.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType

from okto_pulse.core.domain.guideline_lifecycle import (
    GuidelineBindingApplied,
    GuidelineBindingNoop,
    GuidelineBindingTransitionCommand,
    plan_guideline_binding_transition,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_IMPACT_CONTRACT_VERSION,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    POLICY_ACTOR_ID_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    BoardGuidelineBinding,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineImpactItem,
    GuidelineImpactItemKind,
    GuidelineImpactReceipt,
    GuidelinePolicyContractError,
    GuidelineRetirement,
    GuidelineRevision,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectRef,
    guideline_binding_snapshot_digest,
    guideline_impact_digest_v2,
    normalize_guideline_semantic_version,
    normalize_guideline_sha256,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    semantic_binding_head_digest_v1,
    semantic_policy_set_digest_v1,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiver,
    SemanticMetricWaiverStatus,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


GUIDELINE_ADOPTION_EVENT_TYPE = "board.semantic_guideline_adoption_changed.v2"
GUIDELINE_ADOPTION_ACTIVITY_ACTION = "guideline_revision_adopted"
GUIDELINE_UNLINK_ACTIVITY_ACTION = "guideline_unlinked"
GUIDELINE_RETIREMENT_EVENT_TYPE = "board.semantic_guideline_retirement_changed.v2"
GUIDELINE_RETIREMENT_ACTIVITY_ACTION = "guideline_retired"
_INITIAL_BINDING_NAMESPACE = uuid.UUID("607df50c-3d89-5bca-bfab-6e445d329c46")
_ACTIVITY_NAMESPACE = uuid.UUID("ec1be62a-87b6-55fd-89b9-10ff6071089d")
_RETIREMENT_EVENT_NAMESPACE = uuid.UUID("d79ddf58-c1f8-520a-9cf2-50cd36157abc")


class GuidelineImpactError(GuidelinePolicyContractError):
    """A preview/adoption invariant failed closed."""

    def __init__(
        self,
        code: str,
        *,
        currentness_reasons: tuple[GuidelineImpactCurrentnessReason, ...] = (),
    ) -> None:
        self.code = code
        self.currentness_reasons = currentness_reasons
        super().__init__(code)


class GuidelineImpactCurrentnessReason(str, Enum):
    SNAPSHOT_MISSING = "snapshot_missing"
    GUIDELINE_HEAD_CHANGED = "guideline_head_changed"
    TARGET_REVISION_CHANGED = "target_revision_changed"
    BINDING_CHANGED = "binding_changed"
    BOARD_BINDING_HEAD_CHANGED = "board_binding_head_changed"
    ARTIFACT_SNAPSHOT_CHANGED = "artifact_snapshot_changed"
    WAIVER_SNAPSHOT_CHANGED = "waiver_snapshot_changed"
    IMPACT_DIGEST_CHANGED = "impact_digest_changed"


_REASON_ORDER = (
    GuidelineImpactCurrentnessReason.SNAPSHOT_MISSING,
    GuidelineImpactCurrentnessReason.GUIDELINE_HEAD_CHANGED,
    GuidelineImpactCurrentnessReason.TARGET_REVISION_CHANGED,
    GuidelineImpactCurrentnessReason.BINDING_CHANGED,
    GuidelineImpactCurrentnessReason.BOARD_BINDING_HEAD_CHANGED,
    GuidelineImpactCurrentnessReason.ARTIFACT_SNAPSHOT_CHANGED,
    GuidelineImpactCurrentnessReason.WAIVER_SNAPSHOT_CHANGED,
    GuidelineImpactCurrentnessReason.IMPACT_DIGEST_CHANGED,
)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelineImpactError(code)
    return value.strip()


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelineImpactError(code)
    return value.astimezone(timezone.utc)


def _revision_evidence(
    revision_id: object,
    semantic_version: object,
    revision_digest: object,
    *,
    code: str,
) -> tuple[str | None, str | None, str | None]:
    present = (
        revision_id is not None,
        semantic_version is not None,
        revision_digest is not None,
    )
    if any(present) and not all(present):
        raise GuidelineImpactError(code)
    if not any(present):
        return None, None, None
    try:
        return (
            _required_text(revision_id, code),
            normalize_guideline_semantic_version(
                semantic_version,
                code,
            ),
            normalize_guideline_sha256(revision_digest, code),
        )
    except GuidelinePolicyContractError as exc:
        raise GuidelineImpactError(code) from exc


def _canonical_metric_ids(
    values: object,
    code: str,
) -> tuple[str, ...]:
    if not isinstance(values, tuple | list) or any(
        not isinstance(value, str) or not value.strip() for value in values
    ):
        raise GuidelineImpactError(code)
    normalized = tuple(value.strip() for value in values)
    if len(set(normalized)) != len(normalized):
        raise GuidelineImpactError(code)
    return tuple(sorted(normalized))


def _canonical_threshold_overrides(
    values: object,
) -> Mapping[str, int]:
    if not isinstance(values, Mapping):
        raise GuidelineImpactError(
            "guideline_impact_metric_threshold_overrides_invalid"
        )
    resolved: dict[str, int] = {}
    seen: set[str] = set()
    for raw_code, raw_threshold in values.items():
        code = _required_text(
            raw_code,
            "guideline_impact_metric_threshold_overrides_invalid",
        )
        if (
            code.casefold() in seen
            or not isinstance(raw_threshold, int)
            or isinstance(raw_threshold, bool)
            or not 0 <= raw_threshold <= 100
        ):
            raise GuidelineImpactError(
                "guideline_impact_metric_threshold_overrides_invalid"
            )
        seen.add(code.casefold())
        resolved[code] = raw_threshold
    return MappingProxyType(dict(sorted(resolved.items())))


def _semantic_binding_id(*, board_id: str, guideline_id: str) -> str:
    return str(
        uuid.uuid5(
            _INITIAL_BINDING_NAMESPACE,
            f"{board_id}:{guideline_id}",
        )
    )


def _item(
    *,
    item_kind: GuidelineImpactItemKind,
    entity_type: str,
    entity_id: str,
    details: dict[str, object],
    related_id: str | None = None,
    entity_version: int | None = None,
) -> GuidelineImpactItem:
    details_digest = canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "kind": "impact_item_details",
            **details,
        }
    )
    identity = {
        "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
        "kind": item_kind.value,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "related_id": related_id,
        "entity_version": entity_version,
        "details_digest": details_digest,
    }
    return GuidelineImpactItem(
        impact_item_id=f"gii_{canonical_sha256(identity)[:32]}",
        item_kind=item_kind,
        entity_type=entity_type,
        entity_id=entity_id,
        related_id=related_id,
        entity_version=entity_version,
        details_digest=details_digest,
    )


def _canonical_subjects(
    subjects: tuple[PolicySubjectRef, ...],
    *,
    board_id: str,
) -> tuple[PolicySubjectRef, ...]:
    if not isinstance(subjects, tuple | list) or any(
        not isinstance(subject, PolicySubjectRef) for subject in subjects
    ):
        raise GuidelineImpactError("guideline_impact_subjects_invalid")
    resolved = tuple(subjects)
    identities = tuple(
        (subject.entity_type.value, subject.subject_id) for subject in resolved
    )
    if len(set(identities)) != len(identities):
        raise GuidelineImpactError("guideline_impact_subjects_duplicate")
    if any(subject.board_id != board_id for subject in resolved):
        raise GuidelineImpactError("guideline_impact_subject_board_mismatch")
    return tuple(
        sorted(
            resolved,
            key=lambda subject: (
                subject.entity_type.value,
                subject.subject_id,
            ),
        )
    )


def _canonical_waivers(
    waivers: tuple[SemanticMetricWaiver, ...],
    *,
    board_id: str,
) -> tuple[SemanticMetricWaiver, ...]:
    if not isinstance(waivers, tuple | list) or any(
        not isinstance(waiver, SemanticMetricWaiver) for waiver in waivers
    ):
        raise GuidelineImpactError("guideline_impact_waivers_invalid")
    resolved = tuple(waivers)
    if len({waiver.waiver_id for waiver in resolved}) != len(resolved):
        raise GuidelineImpactError("guideline_impact_waivers_duplicate")
    if any(
        waiver.anchor.subject.board_id != board_id
        for waiver in resolved
    ):
        raise GuidelineImpactError("guideline_impact_waiver_board_mismatch")
    return tuple(sorted(resolved, key=lambda waiver: waiver.waiver_id))


def _revision_map(
    bindings: tuple[BoardGuidelineBinding, ...],
    revisions: tuple[GuidelineRevision, ...],
) -> dict[tuple[str, str], GuidelineRevision]:
    if not isinstance(bindings, tuple | list) or any(
        not isinstance(binding, BoardGuidelineBinding) for binding in bindings
    ):
        raise GuidelineImpactError("guideline_impact_bindings_invalid")
    if not isinstance(revisions, tuple | list) or any(
        not isinstance(revision, GuidelineRevision) for revision in revisions
    ):
        raise GuidelineImpactError("guideline_impact_revisions_invalid")
    mapping = {
        (revision.guideline_id, revision.revision_id): revision
        for revision in revisions
    }
    if len(mapping) != len(revisions):
        raise GuidelineImpactError("guideline_impact_revisions_duplicate")
    expected = {(binding.guideline_id, binding.revision_id) for binding in bindings}
    if expected != set(mapping):
        raise GuidelineImpactError("guideline_impact_revision_set_mismatch")
    return mapping


def _metric_deltas(
    before: GuidelineRevision | None,
    after: GuidelineRevision,
) -> tuple[
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[PolicyEntityType, ...],
]:
    before_by_id = (
        {metric.metric_id: metric for metric in before.metrics}
        if before is not None
        else {}
    )
    after_by_id = {metric.metric_id: metric for metric in after.metrics}
    added = tuple(sorted(set(after_by_id) - set(before_by_id)))
    removed = tuple(sorted(set(before_by_id) - set(after_by_id)))
    changed = tuple(
        sorted(
            metric_id
            for metric_id in set(before_by_id) & set(after_by_id)
            if before_by_id[metric_id] != after_by_id[metric_id]
        )
    )
    affected_ids = set(added) | set(changed) | set(removed)
    targets = {
        target
        for metric_id in affected_ids
        for metric in (
            before_by_id.get(metric_id),
            after_by_id.get(metric_id),
        )
        if metric is not None
        for target in metric.target_entity_types
    }
    return (
        added,
        changed,
        removed,
        tuple(sorted(targets, key=lambda target: target.value)),
    )


def _revision_targets(
    revision: GuidelineRevision,
) -> tuple[PolicyEntityType, ...]:
    """Return every entity type whose effective policy can change.

    A binding-only change does not alter any metric identity, so it is
    intentionally absent from ``_metric_deltas``. It still changes how every
    metric in the adopted revision is assessed and must
    therefore declare those targets in the impact receipt.
    """

    return tuple(
        sorted(
            {
                target
                for metric in revision.metrics
                for target in metric.target_entity_types
            },
            key=lambda target: target.value,
        )
    )


def _waiver_manifest(waiver: SemanticMetricWaiver) -> dict[str, object]:
    anchor = waiver.anchor
    return {
        "waiver_id": waiver.waiver_id,
        "waiver_revision": waiver.waiver_revision,
        "status": waiver.status.value,
        "scope_digest": waiver.scope_digest,
        "head_digest": waiver.head_digest,
        "receipt_id": anchor.receipt_id,
        "receipt_digest": anchor.receipt_digest,
        "finding_id": anchor.finding_id,
        "finding_digest": anchor.finding_digest,
        "guideline_id": anchor.guideline_id,
        "revision_id": anchor.guideline_revision_id,
        "revision_digest": anchor.guideline_revision_digest,
        "binding_id": anchor.binding_id,
        "binding_revision": anchor.binding_revision,
        "binding_configuration_digest": (
            anchor.binding_configuration_digest
        ),
        "metric_id": anchor.metric_id,
        "metric_code": anchor.metric_code,
        "metric_result_id": anchor.metric_result_id,
        "metric_result_digest": anchor.metric_result_digest,
        "subject_type": anchor.subject.entity_type.value,
        "subject_id": anchor.subject.subject_id,
        "subject_version": anchor.subject.subject_version,
        "subject_content_digest": anchor.subject_content_digest,
        "expires_at": (
            waiver.expires_at.isoformat()
            if waiver.expires_at is not None
            else None
        ),
    }


@dataclass(frozen=True, slots=True)
class GuidelineImpactFenceSnapshot:
    board_id: str
    guideline_id: str
    head_revision: int
    revision_id: str
    revision_number: int
    revision_digest: str
    binding_digest: str
    binding_head_digest: str
    artifact_snapshot_digest: str
    waiver_snapshot_digest: str
    impact_digest: str

    def __post_init__(self) -> None:
        for field_name in ("board_id", "guideline_id", "revision_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_impact_snapshot_{field_name}_required",
                ),
            )
        for field_name in ("head_revision", "revision_number"):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise GuidelineImpactError(
                    f"guideline_impact_snapshot_{field_name}_invalid"
                )
        for field_name in (
            "revision_digest",
            "binding_digest",
            "binding_head_digest",
            "artifact_snapshot_digest",
            "waiver_snapshot_digest",
            "impact_digest",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise GuidelineImpactError(
                    f"guideline_impact_snapshot_{field_name}_invalid"
                )


@dataclass(frozen=True, slots=True)
class GuidelineImpactCurrentnessAssessment:
    currentness: PolicyCurrentness
    reasons: tuple[GuidelineImpactCurrentnessReason, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.currentness, PolicyCurrentness):
            raise GuidelineImpactError("guideline_impact_currentness_invalid")
        if not isinstance(self.reasons, tuple | list) or any(
            not isinstance(reason, GuidelineImpactCurrentnessReason)
            for reason in self.reasons
        ):
            raise GuidelineImpactError("guideline_impact_currentness_reasons_invalid")
        normalized = tuple(reason for reason in _REASON_ORDER if reason in self.reasons)
        if len(normalized) != len(set(self.reasons)):
            raise GuidelineImpactError("guideline_impact_currentness_reasons_duplicate")
        if (self.currentness is PolicyCurrentness.CURRENT and normalized) or (
            self.currentness is PolicyCurrentness.STALE and not normalized
        ):
            raise GuidelineImpactError("guideline_impact_currentness_shape_invalid")
        object.__setattr__(self, "reasons", normalized)


def impact_fence_from_receipt(
    receipt: GuidelineImpactReceipt,
) -> GuidelineImpactFenceSnapshot:
    if not isinstance(receipt, GuidelineImpactReceipt):
        raise GuidelineImpactError("guideline_impact_receipt_invalid")
    return GuidelineImpactFenceSnapshot(
        board_id=receipt.board_id,
        guideline_id=receipt.guideline_id,
        head_revision=receipt.expected_head_revision,
        revision_id=receipt.to_revision_id,
        revision_number=receipt.to_revision_number,
        revision_digest=receipt.to_revision_digest,
        binding_digest=receipt.binding_digest,
        binding_head_digest=receipt.binding_head_digest_before,
        artifact_snapshot_digest=receipt.artifact_snapshot_digest,
        waiver_snapshot_digest=receipt.waiver_snapshot_digest,
        impact_digest=receipt.impact_digest,
    )


def assess_guideline_impact_currentness(
    receipt: GuidelineImpactReceipt,
    current: GuidelineImpactFenceSnapshot | None,
) -> GuidelineImpactCurrentnessAssessment:
    recorded = impact_fence_from_receipt(receipt)
    if current is None:
        return GuidelineImpactCurrentnessAssessment(
            PolicyCurrentness.STALE,
            (GuidelineImpactCurrentnessReason.SNAPSHOT_MISSING,),
        )
    if (
        current.board_id != recorded.board_id
        or current.guideline_id != recorded.guideline_id
    ):
        raise GuidelineImpactError("guideline_impact_snapshot_scope_mismatch")
    reasons: list[GuidelineImpactCurrentnessReason] = []
    if current.head_revision != recorded.head_revision:
        reasons.append(GuidelineImpactCurrentnessReason.GUIDELINE_HEAD_CHANGED)
    if (
        current.revision_id != recorded.revision_id
        or current.revision_number != recorded.revision_number
        or current.revision_digest != recorded.revision_digest
    ):
        reasons.append(GuidelineImpactCurrentnessReason.TARGET_REVISION_CHANGED)
    if current.binding_digest != recorded.binding_digest:
        reasons.append(GuidelineImpactCurrentnessReason.BINDING_CHANGED)
    if current.binding_head_digest != recorded.binding_head_digest:
        reasons.append(GuidelineImpactCurrentnessReason.BOARD_BINDING_HEAD_CHANGED)
    if current.artifact_snapshot_digest != recorded.artifact_snapshot_digest:
        reasons.append(GuidelineImpactCurrentnessReason.ARTIFACT_SNAPSHOT_CHANGED)
    if current.waiver_snapshot_digest != recorded.waiver_snapshot_digest:
        reasons.append(GuidelineImpactCurrentnessReason.WAIVER_SNAPSHOT_CHANGED)
    if not reasons and current.impact_digest != recorded.impact_digest:
        reasons.append(GuidelineImpactCurrentnessReason.IMPACT_DIGEST_CHANGED)
    return GuidelineImpactCurrentnessAssessment(
        PolicyCurrentness.CURRENT if not reasons else PolicyCurrentness.STALE,
        tuple(reasons),
    )


@dataclass(frozen=True, slots=True)
class GuidelineImpactPreviewCommand:
    impact_receipt_id: str
    board_id: str
    guideline_id: str
    head: GuidelineHead
    to_revision: GuidelineRevision
    current_binding: BoardGuidelineBinding | None
    from_revision: GuidelineRevision | None
    active_bindings: tuple[BoardGuidelineBinding, ...]
    active_revisions: tuple[GuidelineRevision, ...]
    subjects: tuple[PolicySubjectRef, ...]
    waivers: tuple[SemanticMetricWaiver, ...]
    proposed_priority: int
    proposed_enforcement: GuidelineEnforcement
    proposed_minimum_confidence: int
    proposed_metric_threshold_overrides: Mapping[str, int]
    requested_by: str
    created_at: datetime
    idempotency_key: str
    requested_to_revision_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "impact_receipt_id",
            "board_id",
            "guideline_id",
            "requested_by",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_impact_preview_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "guideline_impact_preview_created_at_invalid",
            ),
        )
        typed_collections = (
            ("active_bindings", self.active_bindings, BoardGuidelineBinding),
            ("active_revisions", self.active_revisions, GuidelineRevision),
            ("subjects", self.subjects, PolicySubjectRef),
            ("waivers", self.waivers, SemanticMetricWaiver),
        )
        for field_name, values, expected_type in typed_collections:
            if not isinstance(values, tuple | list) or any(
                not isinstance(value, expected_type) for value in values
            ):
                raise GuidelineImpactError(
                    f"guideline_impact_preview_{field_name}_invalid"
                )
            object.__setattr__(self, field_name, tuple(values))
        if (
            not isinstance(self.head, GuidelineHead)
            or not isinstance(self.to_revision, GuidelineRevision)
            or (
                self.current_binding is not None
                and not isinstance(self.current_binding, BoardGuidelineBinding)
            )
            or (
                self.from_revision is not None
                and not isinstance(self.from_revision, GuidelineRevision)
            )
            or not isinstance(self.proposed_enforcement, GuidelineEnforcement)
            or not isinstance(self.proposed_minimum_confidence, int)
            or isinstance(self.proposed_minimum_confidence, bool)
            or not 0 <= self.proposed_minimum_confidence <= 100
            or not isinstance(self.proposed_metric_threshold_overrides, Mapping)
            or not isinstance(self.proposed_priority, int)
            or isinstance(self.proposed_priority, bool)
            or self.proposed_priority < 0
            or self.proposed_priority > POLICY_SQL_INTEGER_MAX
        ):
            raise GuidelineImpactError("guideline_impact_preview_input_invalid")
        object.__setattr__(
            self,
            "proposed_metric_threshold_overrides",
            _canonical_threshold_overrides(
                self.proposed_metric_threshold_overrides
            ),
        )
        requested_to_revision_id = (
            None
            if self.requested_to_revision_id is None
            else normalize_policy_bounded_text(
                self.requested_to_revision_id,
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                code="guideline_impact_requested_revision_id_invalid",
            )
        )
        if (
            requested_to_revision_id is not None
            and requested_to_revision_id != self.to_revision.revision_id
        ):
            raise GuidelineImpactError(
                "guideline_impact_requested_revision_mismatch"
            )
        object.__setattr__(
            self,
            "requested_to_revision_id",
            requested_to_revision_id,
        )


@dataclass(frozen=True, slots=True)
class GuidelineImpactPreviewPlan:
    command: GuidelineImpactPreviewCommand
    receipt: GuidelineImpactReceipt
    proposed_binding: BoardGuidelineBinding
    expected_binding_revision: int | None
    idempotency_key: str
    request_digest: str


def guideline_impact_preview_request_digest_v1(
    *,
    board_id: str,
    guideline_id: str,
    proposed_priority: int,
    proposed_enforcement: GuidelineEnforcement,
    proposed_minimum_confidence: int,
    proposed_metric_threshold_overrides: Mapping[str, int],
    requested_by: str,
    requested_to_revision_id: str | None,
) -> str:
    """Bind preview idempotency to the exact caller-owned request payload."""

    board_id = normalize_policy_bounded_text(
        board_id,
        max_length=POLICY_BOARD_ID_MAX_LENGTH,
        code="guideline_impact_board_id_required",
    )
    guideline_id = normalize_policy_bounded_text(
        guideline_id,
        max_length=GUIDELINE_ID_MAX_LENGTH,
        code="guideline_impact_guideline_id_required",
    )
    requested_by = normalize_policy_bounded_text(
        requested_by,
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
        code="guideline_impact_requested_by_required",
    )
    if (
        not isinstance(proposed_priority, int)
        or isinstance(proposed_priority, bool)
        or not 0 <= proposed_priority <= POLICY_SQL_INTEGER_MAX
        or not isinstance(proposed_enforcement, GuidelineEnforcement)
        or not isinstance(proposed_minimum_confidence, int)
        or isinstance(proposed_minimum_confidence, bool)
        or not 0 <= proposed_minimum_confidence <= 100
        or not isinstance(proposed_metric_threshold_overrides, Mapping)
    ):
        raise GuidelineImpactError("guideline_impact_preview_input_invalid")
    requested_revision = (
        None
        if requested_to_revision_id is None
        else normalize_policy_bounded_text(
            requested_to_revision_id,
            max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
            code="guideline_impact_requested_revision_id_invalid",
        )
    )
    return canonical_sha256(
        {
            "contract": "guideline-impact-preview-request/v2",
            "operation": "preview",
            "board_id": board_id,
            "guideline_id": guideline_id,
            "proposed_priority": proposed_priority,
            "proposed_enforcement": proposed_enforcement.value,
            "proposed_minimum_confidence": proposed_minimum_confidence,
            "proposed_metric_threshold_overrides": dict(
                _canonical_threshold_overrides(
                    proposed_metric_threshold_overrides
                )
            ),
            "requested_by": requested_by,
            # ``None`` is an exact "resolve current head" intention, never a
            # wildcard for an earlier explicit historical target.
            "requested_to_revision_id": requested_revision,
        }
    )


def plan_guideline_impact_preview(
    command: GuidelineImpactPreviewCommand,
    *,
    retirement: GuidelineRetirement | None = None,
) -> GuidelineImpactPreviewPlan:
    """Build a self-sealed preview; caller persists only receipt and items."""

    if not isinstance(command, GuidelineImpactPreviewCommand):
        raise GuidelineImpactError("guideline_impact_preview_command_invalid")
    if retirement is not None:
        raise GuidelineImpactError("guideline_is_terminal")
    head = command.head
    target = command.to_revision
    if (
        head.guideline_id != command.guideline_id
        or target.guideline_id != command.guideline_id
    ):
        raise GuidelineImpactError("guideline_impact_target_scope_mismatch")
    known_metric_codes = {metric.code for metric in target.metrics}
    if (
        set(command.proposed_metric_threshold_overrides)
        - known_metric_codes
    ):
        raise GuidelineImpactError(
            "guideline_impact_threshold_override_unknown"
        )
    current = command.current_binding
    before = command.from_revision
    if current is None:
        if before is not None:
            raise GuidelineImpactError("guideline_impact_from_revision_unexpected")
        expected_binding_revision = None
        expected_binding_state = None
        binding_id = _semantic_binding_id(
            board_id=command.board_id,
            guideline_id=command.guideline_id,
        )
    else:
        if (
            current.board_id != command.board_id
            or current.guideline_id != command.guideline_id
            or before is None
            or before.guideline_id != current.guideline_id
            or before.revision_id != current.revision_id
            or before.semantic_version != current.semantic_version
            or before.revision_digest != current.revision_digest
        ):
            raise GuidelineImpactError("guideline_impact_binding_source_mismatch")
        expected_binding_revision = current.binding_revision
        expected_binding_state = current.state
        binding_id = current.binding_id
        if (
            current.state is GuidelineBindingState.ACTIVE
            and current.revision_id == target.revision_id
            and current.semantic_version == target.semantic_version
            and current.revision_digest == target.revision_digest
            and current.priority == command.proposed_priority
            and current.enforcement is command.proposed_enforcement
            and current.minimum_confidence == command.proposed_minimum_confidence
            and dict(current.metric_threshold_overrides)
            == command.proposed_metric_threshold_overrides
        ):
            raise GuidelineImpactError("guideline_impact_no_changes")

    subjects = _canonical_subjects(command.subjects, board_id=command.board_id)
    waivers = _canonical_waivers(command.waivers, board_id=command.board_id)
    active_bindings = tuple(
        sorted(
            (
                binding
                for binding in command.active_bindings
                if binding.state is GuidelineBindingState.ACTIVE
            ),
            key=lambda binding: (binding.priority, binding.binding_id),
        )
    )
    if any(binding.board_id != command.board_id for binding in active_bindings):
        raise GuidelineImpactError("guideline_impact_active_binding_board_mismatch")
    if len({binding.binding_id for binding in active_bindings}) != len(
        active_bindings
    ) or len({binding.guideline_id for binding in active_bindings}) != len(
        active_bindings
    ):
        raise GuidelineImpactError("guideline_impact_active_bindings_duplicate")
    current_inventory = tuple(
        binding
        for binding in active_bindings
        if binding.guideline_id == command.guideline_id
    )
    if current is not None and current.state is GuidelineBindingState.ACTIVE:
        if current_inventory != (current,):
            raise GuidelineImpactError(
                "guideline_impact_active_binding_inventory_mismatch"
            )
    elif current_inventory:
        raise GuidelineImpactError(
            "guideline_impact_inactive_binding_inventory_mismatch"
        )
    revision_by_identity = _revision_map(
        active_bindings,
        command.active_revisions,
    )
    before_binding_digest = guideline_binding_snapshot_digest(
        current,
        board_id=command.board_id,
        guideline_id=command.guideline_id,
    )
    proposed_binding = BoardGuidelineBinding(
        binding_id=binding_id,
        board_id=command.board_id,
        guideline_id=command.guideline_id,
        revision_id=target.revision_id,
        semantic_version=target.semantic_version,
        revision_digest=target.revision_digest,
        priority=command.proposed_priority,
        binding_revision=(expected_binding_revision or 0) + 1,
        adopted_by=command.requested_by,
        adopted_at=command.created_at,
        enforcement=command.proposed_enforcement,
        minimum_confidence=command.proposed_minimum_confidence,
        metric_threshold_overrides=command.proposed_metric_threshold_overrides,
        state=GuidelineBindingState.ACTIVE,
        source_kind=(
            current.source_kind
            if current is not None
            else GuidelineBindingProvenance.NATIVE
        ),
    )
    before_bindings = tuple(
        binding
        for binding in active_bindings
        if binding.guideline_id != command.guideline_id
    )
    if current is not None and current.state is GuidelineBindingState.ACTIVE:
        before_bindings += (current,)
    before_bindings = tuple(
        sorted(
            before_bindings, key=lambda binding: (binding.priority, binding.binding_id)
        )
    )
    before_revisions = tuple(
        revision_by_identity[(binding.guideline_id, binding.revision_id)]
        for binding in before_bindings
    )
    after_bindings = tuple(
        sorted(
            (
                *(
                    binding
                    for binding in active_bindings
                    if binding.guideline_id != command.guideline_id
                ),
                proposed_binding,
            ),
            key=lambda binding: (binding.priority, binding.binding_id),
        )
    )
    after_revisions = tuple(
        target
        if binding.guideline_id == command.guideline_id
        else revision_by_identity[(binding.guideline_id, binding.revision_id)]
        for binding in after_bindings
    )
    binding_head_before = semantic_binding_head_digest_v1(before_bindings)
    binding_head_after = semantic_binding_head_digest_v1(after_bindings)
    policy_set_before = semantic_policy_set_digest_v1(
        before_bindings,
        before_revisions,
    )
    policy_set_after = semantic_policy_set_digest_v1(
        after_bindings,
        after_revisions,
    )
    delta_before = (
        before
        if current is not None and current.state is GuidelineBindingState.ACTIVE
        else None
    )
    added, changed, removed, targets = _metric_deltas(delta_before, target)
    revision_changed = bool(
        delta_before is not None
        and (
            delta_before.revision_id != target.revision_id
            or delta_before.revision_digest != target.revision_digest
        )
    )
    if revision_changed:
        # Every assessment and exception is fenced to the exact semantic
        # revision, not only to its metric definitions.  Prose, tags, metric
        # order, or any other revision-level change therefore stales evidence
        # even when the individual metric payloads compare equal.
        targets = tuple(
            sorted(
                {
                    *targets,
                    *_revision_targets(delta_before),
                    *_revision_targets(target),
                },
                key=lambda entity_type: entity_type.value,
            )
        )
    binding_configuration_changed = bool(
        current is not None
        and current.state is GuidelineBindingState.ACTIVE
        and (
            current.priority != command.proposed_priority
            or current.enforcement is not command.proposed_enforcement
            or current.minimum_confidence != command.proposed_minimum_confidence
            or dict(current.metric_threshold_overrides)
            != command.proposed_metric_threshold_overrides
        )
    )
    if binding_configuration_changed:
        targets = tuple(
            sorted(
                {*targets, *_revision_targets(target)},
                key=lambda entity_type: entity_type.value,
            )
        )
    affected_target_types = frozenset(targets)
    affected_subjects = tuple(
        subject for subject in subjects if subject.entity_type in affected_target_types
    )
    affected_subject_identities = frozenset(
        (subject.entity_type, subject.subject_id) for subject in affected_subjects
    )

    items: list[GuidelineImpactItem] = [
        _item(
            item_kind=GuidelineImpactItemKind.BINDING,
            entity_type="board",
            entity_id=command.board_id,
            related_id=binding_id,
            entity_version=expected_binding_revision or 0,
            details={
                "guideline_id": command.guideline_id,
                "binding_digest_before": before_binding_digest,
                "binding_head_digest_before": binding_head_before,
                "binding_head_digest_after": binding_head_after,
                "policy_set_digest_before": policy_set_before,
                "policy_set_digest_after": policy_set_after,
                "to_revision_id": target.revision_id,
                "proposed_priority": command.proposed_priority,
                "proposed_enforcement": command.proposed_enforcement.value,
                "proposed_minimum_confidence": (
                    command.proposed_minimum_confidence
                ),
                "proposed_metric_threshold_overrides": (
                    command.proposed_metric_threshold_overrides
                ),
            },
        )
    ]
    items.extend(
        _item(
            item_kind=GuidelineImpactItemKind.TARGET,
            entity_type=target_type.value,
            entity_id=target_type.value,
            details={
                "target_entity_type": target_type.value,
                "added_metric_ids": added,
                "changed_metric_ids": changed,
                "removed_metric_ids": removed,
            },
        )
        for target_type in targets
    )
    artifact_items = tuple(
        _item(
            item_kind=GuidelineImpactItemKind.ARTIFACT,
            entity_type=subject.entity_type.value,
            entity_id=subject.subject_id,
            entity_version=subject.subject_version,
            details={
                "board_id": subject.board_id,
                "subject_type": subject.entity_type.value,
                "subject_id": subject.subject_id,
                "subject_version": subject.subject_version,
                "policy_set_digest_before": policy_set_before,
                "policy_set_digest_after": policy_set_after,
            },
        )
        for subject in affected_subjects
    )
    items.extend(artifact_items)
    live_waivers = tuple(
        waiver
        for waiver in waivers
        if waiver.status
        in {
            SemanticMetricWaiverStatus.REQUESTED,
            SemanticMetricWaiverStatus.APPROVED,
        }
        and (
            waiver.status is SemanticMetricWaiverStatus.REQUESTED
            or waiver.expires_at is None
            or command.created_at < waiver.expires_at
        )
        and waiver.anchor.guideline_id == command.guideline_id
        and (
            waiver.anchor.subject.entity_type,
            waiver.anchor.subject.subject_id,
        )
        in affected_subject_identities
    )
    waiver_items = tuple(
        _item(
            item_kind=GuidelineImpactItemKind.WAIVER,
            entity_type=waiver.anchor.subject.entity_type.value,
            entity_id=waiver.anchor.subject.subject_id,
            related_id=waiver.waiver_id,
            entity_version=waiver.waiver_revision,
            details={
                **_waiver_manifest(waiver),
                "requires_revalidation": True,
                "policy_set_digest_after": policy_set_after,
            },
        )
        for waiver in live_waivers
    )
    items.extend(waiver_items)
    canonical_items = tuple(sorted(items, key=lambda item: item.sort_key))
    artifact_snapshot_digest = canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "kind": "artifact_snapshot",
            "subjects": [
                {
                    "entity_type": subject.entity_type.value,
                    "entity_id": subject.subject_id,
                    "entity_version": subject.subject_version,
                }
                for subject in affected_subjects
            ],
        }
    )
    waiver_snapshot_digest = canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "kind": "waiver_snapshot",
            "waivers": [_waiver_manifest(waiver) for waiver in live_waivers],
        }
    )
    digest_arguments = {
        "board_id": command.board_id,
        "guideline_id": command.guideline_id,
        "binding_id": binding_id,
        "from_revision_id": before.revision_id if before is not None else None,
        "from_semantic_version": (
            before.semantic_version if before is not None else None
        ),
        "from_revision_digest": (
            before.revision_digest if before is not None else None
        ),
        "to_revision_id": target.revision_id,
        "to_revision_number": target.revision_number,
        "to_semantic_version": target.semantic_version,
        "to_revision_digest": target.revision_digest,
        "expected_head_revision": head.head_revision,
        "expected_binding_revision": expected_binding_revision,
        "expected_binding_state": expected_binding_state,
        "binding_digest": before_binding_digest,
        "binding_head_digest_before": binding_head_before,
        "binding_head_digest_after": binding_head_after,
        "policy_set_digest_before": policy_set_before,
        "policy_set_digest_after": policy_set_after,
        "artifact_snapshot_digest": artifact_snapshot_digest,
        "waiver_snapshot_digest": waiver_snapshot_digest,
        "proposed_priority": command.proposed_priority,
        "proposed_enforcement": command.proposed_enforcement,
        "proposed_minimum_confidence": command.proposed_minimum_confidence,
        "proposed_metric_threshold_overrides": (
            command.proposed_metric_threshold_overrides
        ),
        "affected_entity_types": targets,
        "items": canonical_items,
        "added_metric_ids": added,
        "changed_metric_ids": changed,
        "removed_metric_ids": removed,
    }
    impact_digest = guideline_impact_digest_v2(**digest_arguments)
    receipt = GuidelineImpactReceipt(
        impact_receipt_id=command.impact_receipt_id,
        requested_by=command.requested_by,
        created_at=command.created_at,
        impact_digest=impact_digest,
        **digest_arguments,
    )
    request_digest = guideline_impact_preview_request_digest_v1(
        board_id=command.board_id,
        guideline_id=command.guideline_id,
        proposed_priority=command.proposed_priority,
        proposed_enforcement=command.proposed_enforcement,
        proposed_minimum_confidence=command.proposed_minimum_confidence,
        proposed_metric_threshold_overrides=(
            command.proposed_metric_threshold_overrides
        ),
        requested_by=command.requested_by,
        requested_to_revision_id=command.requested_to_revision_id,
    )
    return GuidelineImpactPreviewPlan(
        command=command,
        receipt=receipt,
        proposed_binding=proposed_binding,
        expected_binding_revision=expected_binding_revision,
        idempotency_key=command.idempotency_key,
        request_digest=request_digest,
    )


@dataclass(frozen=True, slots=True)
class GuidelineBindingChangeEvent:
    event_id: str
    event_type: str
    operation: str
    board_id: str
    guideline_id: str
    binding_id: str
    previous_binding_revision: int | None
    binding_revision: int
    from_revision_id: str | None
    from_semantic_version: str | None
    from_revision_digest: str | None
    to_revision_id: str | None
    to_semantic_version: str | None
    to_revision_digest: str | None
    impact_receipt_id: str | None
    impact_digest: str | None
    binding_digest_before: str
    binding_head_digest_before: str
    binding_head_digest_after: str
    policy_set_digest_before: str
    policy_set_digest_after: str
    added_metric_ids: tuple[str, ...]
    changed_metric_ids: tuple[str, ...]
    removed_metric_ids: tuple[str, ...]
    actor_id: str
    actor_type: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        event_type = _required_text(
            self.event_type,
            "guideline_adoption_event_type_invalid",
        )
        operation = _required_text(
            self.operation,
            "guideline_adoption_event_operation_invalid",
        )
        if event_type != GUIDELINE_ADOPTION_EVENT_TYPE:
            raise GuidelineImpactError("guideline_adoption_event_type_invalid")
        if operation not in {"adopt", "unlink"}:
            raise GuidelineImpactError("guideline_adoption_event_operation_invalid")
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "operation", operation)
        for field_name in (
            "event_id",
            "board_id",
            "guideline_id",
            "binding_id",
            "actor_id",
            "actor_type",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_adoption_event_{field_name}_required",
                ),
            )
        previous_binding_revision = self.previous_binding_revision
        if previous_binding_revision is not None and (
            not isinstance(previous_binding_revision, int)
            or isinstance(previous_binding_revision, bool)
            or previous_binding_revision < 1
        ):
            raise GuidelineImpactError(
                "guideline_adoption_event_previous_binding_revision_invalid"
            )
        if (
            not isinstance(self.binding_revision, int)
            or isinstance(self.binding_revision, bool)
            or self.binding_revision < 1
            or self.binding_revision != (previous_binding_revision or 0) + 1
        ):
            raise GuidelineImpactError(
                "guideline_adoption_event_binding_revision_invalid"
            )
        from_evidence = _revision_evidence(
            self.from_revision_id,
            self.from_semantic_version,
            self.from_revision_digest,
            code="guideline_adoption_event_from_revision_invalid",
        )
        to_evidence = _revision_evidence(
            self.to_revision_id,
            self.to_semantic_version,
            self.to_revision_digest,
            code="guideline_adoption_event_to_revision_invalid",
        )
        for field_name, value in zip(
            (
                "from_revision_id",
                "from_semantic_version",
                "from_revision_digest",
                "to_revision_id",
                "to_semantic_version",
                "to_revision_digest",
            ),
            (*from_evidence, *to_evidence),
            strict=True,
        ):
            object.__setattr__(self, field_name, value)
        impact_receipt_id = (
            None
            if self.impact_receipt_id is None
            else _required_text(
                self.impact_receipt_id,
                "guideline_adoption_event_receipt_invalid",
            )
        )
        impact_digest = (
            None
            if self.impact_digest is None
            else normalize_guideline_sha256(
                self.impact_digest,
                "guideline_adoption_event_impact_digest_invalid",
            )
        )
        if operation == "adopt":
            if (
                to_evidence[0] is None
                or (previous_binding_revision is None) != (from_evidence[0] is None)
                or impact_receipt_id is None
                or impact_digest is None
            ):
                raise GuidelineImpactError(
                    "guideline_adoption_event_adopt_shape_invalid"
                )
        elif (
            previous_binding_revision is None
            or from_evidence[0] is None
            or to_evidence[0] is not None
            or impact_receipt_id is not None
            or impact_digest is not None
        ):
            raise GuidelineImpactError("guideline_adoption_event_unlink_shape_invalid")
        object.__setattr__(
            self,
            "impact_receipt_id",
            impact_receipt_id,
        )
        object.__setattr__(self, "impact_digest", impact_digest)
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "guideline_adoption_event_occurred_at_invalid",
            ),
        )
        if self.actor_type not in {"agent", "user", "system"}:
            raise GuidelineImpactError("guideline_adoption_event_actor_type_invalid")
        for field_name in (
            "binding_digest_before",
            "binding_head_digest_before",
            "binding_head_digest_after",
            "policy_set_digest_before",
            "policy_set_digest_after",
        ):
            try:
                normalized_digest = normalize_guideline_sha256(
                    getattr(self, field_name),
                    f"guideline_adoption_event_{field_name}_invalid",
                )
            except GuidelinePolicyContractError as exc:
                raise GuidelineImpactError(
                    f"guideline_adoption_event_{field_name}_invalid"
                ) from exc
            object.__setattr__(self, field_name, normalized_digest)
        normalized_metric_sets: list[tuple[str, ...]] = []
        for field_name in (
            "added_metric_ids",
            "changed_metric_ids",
            "removed_metric_ids",
        ):
            values = getattr(self, field_name)
            if (
                not isinstance(values, tuple | list)
                or any(
                    not isinstance(value, str) or not value.strip() for value in values
                )
                or len(set(values)) != len(values)
            ):
                raise GuidelineImpactError(
                    f"guideline_adoption_event_{field_name}_invalid"
                )
            object.__setattr__(
                self,
                field_name,
                tuple(sorted(value.strip() for value in values)),
            )
            normalized_metric_sets.append(getattr(self, field_name))
        metric_sets = tuple(set(values) for values in normalized_metric_sets)
        if any(
            left & right
            for index, left in enumerate(metric_sets)
            for right in metric_sets[index + 1 :]
        ):
            raise GuidelineImpactError("guideline_adoption_event_metric_sets_overlap")
        if operation == "unlink" and (
            self.added_metric_ids or self.changed_metric_ids
        ):
            raise GuidelineImpactError(
                "guideline_adoption_event_unlink_metric_sets_invalid"
            )

    def payload(self) -> dict[str, object]:
        return {
            "event_schema_version": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "event_id": self.event_id,
            "operation": self.operation,
            "board_id": self.board_id,
            "guideline_id": self.guideline_id,
            "binding_id": self.binding_id,
            "previous_binding_revision": self.previous_binding_revision,
            "binding_revision": self.binding_revision,
            "from_revision_id": self.from_revision_id,
            "from_semantic_version": self.from_semantic_version,
            "from_revision_digest": self.from_revision_digest,
            "to_revision_id": self.to_revision_id,
            "to_semantic_version": self.to_semantic_version,
            "to_revision_digest": self.to_revision_digest,
            "impact_receipt_id": self.impact_receipt_id,
            "impact_digest": self.impact_digest,
            "binding_digest_before": self.binding_digest_before,
            "binding_head_digest_before": self.binding_head_digest_before,
            "binding_head_digest_after": self.binding_head_digest_after,
            "policy_set_digest_before": self.policy_set_digest_before,
            "policy_set_digest_after": self.policy_set_digest_after,
            "policy_set_digest": self.policy_set_digest_after,
            "added_metric_ids": self.added_metric_ids,
            "changed_metric_ids": self.changed_metric_ids,
            "removed_metric_ids": self.removed_metric_ids,
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "occurred_at": self.occurred_at.isoformat(),
        }


def guideline_adoption_request_digest_v1(
    *,
    receipt: GuidelineImpactReceipt,
    binding: BoardGuidelineBinding,
    actor_id: str,
    actor_type: str,
) -> str:
    if not isinstance(receipt, GuidelineImpactReceipt) or not isinstance(
        binding,
        BoardGuidelineBinding,
    ):
        raise GuidelineImpactError("guideline_adoption_request_digest_input_invalid")
    return canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "operation": "adopt",
            "receipt_id": receipt.impact_receipt_id,
            "impact_digest": receipt.impact_digest,
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "actor_id": _required_text(
                actor_id,
                "guideline_adoption_actor_required",
            ),
            "actor_type": _required_text(
                actor_type,
                "guideline_adoption_actor_type_required",
            ),
        }
    )


@dataclass(frozen=True, slots=True)
class GuidelineAdoptionMutation:
    receipt: GuidelineImpactReceipt
    previous_binding: BoardGuidelineBinding | None
    binding: BoardGuidelineBinding
    event: GuidelineBindingChangeEvent
    activity_id: str
    activity_action: str
    idempotency_key: str
    request_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.receipt, GuidelineImpactReceipt)
            or (
                self.previous_binding is not None
                and not isinstance(
                    self.previous_binding,
                    BoardGuidelineBinding,
                )
            )
            or not isinstance(self.binding, BoardGuidelineBinding)
            or not isinstance(self.event, GuidelineBindingChangeEvent)
        ):
            raise GuidelineImpactError("guideline_adoption_mutation_input_invalid")
        activity_id = _required_text(
            self.activity_id,
            "guideline_adoption_activity_id_required",
        )
        activity_action = _required_text(
            self.activity_action,
            "guideline_adoption_activity_action_required",
        )
        idempotency_key = _required_text(
            self.idempotency_key,
            "guideline_adoption_idempotency_key_required",
        )
        try:
            request_digest = normalize_guideline_sha256(
                self.request_digest,
                "guideline_adoption_request_digest_invalid",
            )
        except GuidelinePolicyContractError as exc:
            raise GuidelineImpactError(
                "guideline_adoption_request_digest_invalid"
            ) from exc
        expected_activity_id = str(uuid.uuid5(_ACTIVITY_NAMESPACE, self.event.event_id))
        if (
            activity_id != expected_activity_id
            or activity_action != GUIDELINE_ADOPTION_ACTIVITY_ACTION
            or self.event.operation != "adopt"
            or self.event.board_id != self.receipt.board_id
            or self.event.guideline_id != self.receipt.guideline_id
            or self.event.binding_id != self.binding.binding_id
            or self.event.binding_revision != self.binding.binding_revision
            or self.event.previous_binding_revision
            != self.receipt.expected_binding_revision
            or self.event.impact_receipt_id != self.receipt.impact_receipt_id
            or self.event.impact_digest != self.receipt.impact_digest
            or self.event.from_revision_id != self.receipt.from_revision_id
            or self.event.from_semantic_version != self.receipt.from_semantic_version
            or self.event.from_revision_digest != self.receipt.from_revision_digest
            or self.event.to_revision_id != self.receipt.to_revision_id
            or self.event.to_semantic_version != self.receipt.to_semantic_version
            or self.event.to_revision_digest != self.receipt.to_revision_digest
            or self.event.binding_digest_before != self.receipt.binding_digest
            or self.event.binding_head_digest_before
            != self.receipt.binding_head_digest_before
            or self.event.binding_head_digest_after
            != self.receipt.binding_head_digest_after
            or self.event.policy_set_digest_before
            != self.receipt.policy_set_digest_before
            or self.event.policy_set_digest_after
            != self.receipt.policy_set_digest_after
            or self.event.added_metric_ids != self.receipt.added_metric_ids
            or self.event.changed_metric_ids != self.receipt.changed_metric_ids
            or self.event.removed_metric_ids != self.receipt.removed_metric_ids
            or self.binding.board_id != self.receipt.board_id
            or self.binding.guideline_id != self.receipt.guideline_id
            or self.binding.revision_id != self.receipt.to_revision_id
            or self.binding.semantic_version != self.receipt.to_semantic_version
            or self.binding.revision_digest != self.receipt.to_revision_digest
            or self.binding.adopted_by != self.event.actor_id
            or self.binding.adopted_at != self.event.occurred_at
            or guideline_binding_snapshot_digest(
                self.previous_binding,
                board_id=self.receipt.board_id,
                guideline_id=self.receipt.guideline_id,
            )
            != self.receipt.binding_digest
            or request_digest
            != guideline_adoption_request_digest_v1(
                receipt=self.receipt,
                binding=self.binding,
                actor_id=self.event.actor_id,
                actor_type=self.event.actor_type,
            )
        ):
            raise GuidelineImpactError("guideline_adoption_mutation_payload_invalid")
        object.__setattr__(self, "activity_id", activity_id)
        object.__setattr__(self, "activity_action", activity_action)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_digest", request_digest)


def plan_guideline_adoption(
    *,
    receipt: GuidelineImpactReceipt,
    current_snapshot: GuidelineImpactFenceSnapshot | None,
    current_binding: BoardGuidelineBinding | None,
    retirement: GuidelineRetirement | None,
    actor_id: str,
    actor_type: str,
    occurred_at: datetime,
    event_id: str,
    idempotency_key: str,
) -> GuidelineAdoptionMutation:
    """Seal the one binding+Activity+event mutation after currentness proof."""

    if current_binding is not None and not isinstance(
        current_binding,
        BoardGuidelineBinding,
    ):
        raise GuidelineImpactError("guideline_adoption_binding_invalid")
    normalized_actor_id = _required_text(
        actor_id,
        "guideline_adoption_actor_required",
    )
    normalized_actor_type = _required_text(
        actor_type,
        "guideline_adoption_actor_type_required",
    )
    normalized_occurred_at = _aware_utc(
        occurred_at,
        "guideline_adoption_occurred_at_invalid",
    )
    normalized_event_id = _required_text(
        event_id,
        "guideline_adoption_event_id_required",
    )
    normalized_idempotency_key = _required_text(
        idempotency_key,
        "guideline_adoption_idempotency_key_required",
    )
    if retirement is not None:
        raise GuidelineImpactError("guideline_is_terminal")
    assessment = assess_guideline_impact_currentness(receipt, current_snapshot)
    if assessment.currentness is not PolicyCurrentness.CURRENT:
        raise GuidelineImpactError(
            "guideline_impact_stale",
            currentness_reasons=assessment.reasons,
        )
    if (
        guideline_binding_snapshot_digest(
            current_binding,
            board_id=receipt.board_id,
            guideline_id=receipt.guideline_id,
        )
        != receipt.binding_digest
    ):
        raise GuidelineImpactError("guideline_impact_binding_stale")
    transition = plan_guideline_binding_transition(
        GuidelineBindingTransitionCommand(
            binding_id=receipt.binding_id,
            board_id=receipt.board_id,
            guideline_id=receipt.guideline_id,
            state=GuidelineBindingState.ACTIVE,
            revision_id=receipt.to_revision_id,
            semantic_version=receipt.to_semantic_version,
            revision_digest=receipt.to_revision_digest,
            priority=receipt.proposed_priority,
            enforcement=receipt.proposed_enforcement,
            minimum_confidence=receipt.proposed_minimum_confidence,
            metric_threshold_overrides=(
                receipt.proposed_metric_threshold_overrides
            ),
            source_kind=(
                current_binding.source_kind
                if current_binding is not None
                else GuidelineBindingProvenance.NATIVE
            ),
            actor_id=normalized_actor_id,
            occurred_at=normalized_occurred_at,
            idempotency_key=normalized_idempotency_key,
            expected_binding_revision=receipt.expected_binding_revision,
        ),
        current=current_binding,
        retirement=retirement,
    )
    if isinstance(transition, GuidelineBindingNoop):
        raise GuidelineImpactError("guideline_adoption_noop")
    if not isinstance(transition, GuidelineBindingApplied):
        raise GuidelineImpactError("guideline_adoption_transition_invalid")
    event = GuidelineBindingChangeEvent(
        event_id=normalized_event_id,
        event_type=GUIDELINE_ADOPTION_EVENT_TYPE,
        operation="adopt",
        board_id=receipt.board_id,
        guideline_id=receipt.guideline_id,
        binding_id=transition.binding.binding_id,
        previous_binding_revision=receipt.expected_binding_revision,
        binding_revision=transition.binding.binding_revision,
        from_revision_id=receipt.from_revision_id,
        from_semantic_version=receipt.from_semantic_version,
        from_revision_digest=receipt.from_revision_digest,
        to_revision_id=receipt.to_revision_id,
        to_semantic_version=receipt.to_semantic_version,
        to_revision_digest=receipt.to_revision_digest,
        impact_receipt_id=receipt.impact_receipt_id,
        impact_digest=receipt.impact_digest,
        binding_digest_before=receipt.binding_digest,
        binding_head_digest_before=receipt.binding_head_digest_before,
        binding_head_digest_after=receipt.binding_head_digest_after,
        policy_set_digest_before=receipt.policy_set_digest_before,
        policy_set_digest_after=receipt.policy_set_digest_after,
        added_metric_ids=receipt.added_metric_ids,
        changed_metric_ids=receipt.changed_metric_ids,
        removed_metric_ids=receipt.removed_metric_ids,
        actor_id=normalized_actor_id,
        actor_type=normalized_actor_type,
        occurred_at=normalized_occurred_at,
    )
    request_digest = guideline_adoption_request_digest_v1(
        receipt=receipt,
        binding=transition.binding,
        actor_id=normalized_actor_id,
        actor_type=normalized_actor_type,
    )
    return GuidelineAdoptionMutation(
        receipt=receipt,
        previous_binding=current_binding,
        binding=transition.binding,
        event=event,
        activity_id=str(uuid.uuid5(_ACTIVITY_NAMESPACE, event.event_id)),
        activity_action=GUIDELINE_ADOPTION_ACTIVITY_ACTION,
        idempotency_key=normalized_idempotency_key,
        request_digest=request_digest,
    )


def guideline_unlink_request_digest_v1(
    *,
    previous_binding: BoardGuidelineBinding,
    binding: BoardGuidelineBinding,
    binding_head_digest_before: str,
    binding_head_digest_after: str,
    policy_set_digest_before: str,
    policy_set_digest_after: str,
    removed_metric_ids: tuple[str, ...],
    actor_id: str,
    actor_type: str,
) -> str:
    if not isinstance(previous_binding, BoardGuidelineBinding) or not isinstance(
        binding, BoardGuidelineBinding
    ):
        raise GuidelineImpactError("guideline_unlink_request_digest_input_invalid")
    return canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "operation": "unlink",
            "binding_digest_before": guideline_binding_snapshot_digest(
                previous_binding,
                board_id=previous_binding.board_id,
                guideline_id=previous_binding.guideline_id,
            ),
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "binding_head_digest_before": normalize_guideline_sha256(
                binding_head_digest_before,
                "guideline_unlink_binding_head_digest_before_invalid",
            ),
            "binding_head_digest_after": normalize_guideline_sha256(
                binding_head_digest_after,
                "guideline_unlink_binding_head_digest_after_invalid",
            ),
            "policy_set_digest_before": normalize_guideline_sha256(
                policy_set_digest_before,
                "guideline_unlink_policy_set_digest_before_invalid",
            ),
            "policy_set_digest_after": normalize_guideline_sha256(
                policy_set_digest_after,
                "guideline_unlink_policy_set_digest_after_invalid",
            ),
            "removed_metric_ids": list(
                _canonical_metric_ids(
                    removed_metric_ids,
                    "guideline_unlink_removed_metric_ids_invalid",
                )
            ),
            "actor_id": _required_text(
                actor_id,
                "guideline_unlink_actor_required",
            ),
            "actor_type": _required_text(
                actor_type,
                "guideline_unlink_actor_type_required",
            ),
        }
    )


@dataclass(frozen=True, slots=True)
class GuidelineUnlinkMutation:
    previous_binding: BoardGuidelineBinding
    binding: BoardGuidelineBinding
    event: GuidelineBindingChangeEvent
    binding_head_digest_before: str
    binding_head_digest_after: str
    policy_set_digest_before: str
    policy_set_digest_after: str
    removed_metric_ids: tuple[str, ...]
    activity_id: str
    activity_action: str
    idempotency_key: str
    request_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.previous_binding, BoardGuidelineBinding)
            or not isinstance(self.binding, BoardGuidelineBinding)
            or not isinstance(self.event, GuidelineBindingChangeEvent)
        ):
            raise GuidelineImpactError("guideline_unlink_mutation_input_invalid")
        activity_id = _required_text(
            self.activity_id,
            "guideline_unlink_activity_id_required",
        )
        activity_action = _required_text(
            self.activity_action,
            "guideline_unlink_activity_action_required",
        )
        idempotency_key = _required_text(
            self.idempotency_key,
            "guideline_unlink_idempotency_key_required",
        )
        lineage_digests: dict[str, str] = {}
        for field_name in (
            "binding_head_digest_before",
            "binding_head_digest_after",
            "policy_set_digest_before",
            "policy_set_digest_after",
        ):
            try:
                lineage_digests[field_name] = normalize_guideline_sha256(
                    getattr(self, field_name),
                    f"guideline_unlink_{field_name}_invalid",
                )
            except GuidelinePolicyContractError as exc:
                raise GuidelineImpactError(
                    f"guideline_unlink_{field_name}_invalid"
                ) from exc
        removed_metric_ids = _canonical_metric_ids(
            self.removed_metric_ids,
            "guideline_unlink_removed_metric_ids_invalid",
        )
        try:
            request_digest = normalize_guideline_sha256(
                self.request_digest,
                "guideline_unlink_request_digest_invalid",
            )
        except GuidelinePolicyContractError as exc:
            raise GuidelineImpactError(
                "guideline_unlink_request_digest_invalid"
            ) from exc
        if (
            self.previous_binding.state is not GuidelineBindingState.ACTIVE
            or self.binding.state is not GuidelineBindingState.UNLINKED
            or self.binding.binding_id != self.previous_binding.binding_id
            or self.binding.binding_revision
            != self.previous_binding.binding_revision + 1
            or self.binding.board_id != self.previous_binding.board_id
            or self.binding.guideline_id != self.previous_binding.guideline_id
            or self.binding.revision_id != self.previous_binding.revision_id
            or self.binding.semantic_version != self.previous_binding.semantic_version
            or self.binding.revision_digest != self.previous_binding.revision_digest
            or self.binding.priority != self.previous_binding.priority
            or self.binding.enforcement is not self.previous_binding.enforcement
            or self.binding.minimum_confidence
            != self.previous_binding.minimum_confidence
            or dict(self.binding.metric_threshold_overrides)
            != dict(self.previous_binding.metric_threshold_overrides)
            or self.binding.source_kind is not self.previous_binding.source_kind
            or self.event.operation != "unlink"
            or self.event.board_id != self.binding.board_id
            or self.event.guideline_id != self.binding.guideline_id
            or self.event.binding_id != self.binding.binding_id
            or self.event.previous_binding_revision
            != self.previous_binding.binding_revision
            or self.event.binding_revision != self.binding.binding_revision
            or self.event.from_revision_id != self.previous_binding.revision_id
            or self.event.from_semantic_version
            != self.previous_binding.semantic_version
            or self.event.from_revision_digest != self.previous_binding.revision_digest
            or self.event.binding_digest_before
            != guideline_binding_snapshot_digest(
                self.previous_binding,
                board_id=self.previous_binding.board_id,
                guideline_id=self.previous_binding.guideline_id,
            )
            or self.event.binding_head_digest_before
            != lineage_digests["binding_head_digest_before"]
            or self.event.binding_head_digest_after
            != lineage_digests["binding_head_digest_after"]
            or self.event.policy_set_digest_before
            != lineage_digests["policy_set_digest_before"]
            or self.event.policy_set_digest_after
            != lineage_digests["policy_set_digest_after"]
            or self.event.added_metric_ids
            or self.event.changed_metric_ids
            or self.event.removed_metric_ids != removed_metric_ids
            or self.event.actor_id != self.binding.adopted_by
            or self.event.occurred_at != self.binding.adopted_at
            or activity_id != str(uuid.uuid5(_ACTIVITY_NAMESPACE, self.event.event_id))
            or activity_action != GUIDELINE_UNLINK_ACTIVITY_ACTION
            or request_digest
            != guideline_unlink_request_digest_v1(
                previous_binding=self.previous_binding,
                binding=self.binding,
                binding_head_digest_before=lineage_digests[
                    "binding_head_digest_before"
                ],
                binding_head_digest_after=lineage_digests["binding_head_digest_after"],
                policy_set_digest_before=lineage_digests["policy_set_digest_before"],
                policy_set_digest_after=lineage_digests["policy_set_digest_after"],
                removed_metric_ids=removed_metric_ids,
                actor_id=self.event.actor_id,
                actor_type=self.event.actor_type,
            )
        ):
            raise GuidelineImpactError("guideline_unlink_mutation_payload_invalid")
        object.__setattr__(self, "activity_id", activity_id)
        object.__setattr__(self, "activity_action", activity_action)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_digest", request_digest)
        for field_name, value in lineage_digests.items():
            object.__setattr__(self, field_name, value)
        object.__setattr__(self, "removed_metric_ids", removed_metric_ids)


def plan_guideline_unlink(
    *,
    current_binding: BoardGuidelineBinding,
    current_revision: GuidelineRevision,
    active_bindings: tuple[BoardGuidelineBinding, ...],
    active_revisions: tuple[GuidelineRevision, ...],
    retirement: GuidelineRetirement | None,
    actor_id: str,
    actor_type: str,
    occurred_at: datetime,
    event_id: str,
    idempotency_key: str,
) -> GuidelineUnlinkMutation:
    """Plan one append-only unlink with exact policy-set lineage."""

    if (
        not isinstance(current_binding, BoardGuidelineBinding)
        or current_binding.state is not GuidelineBindingState.ACTIVE
        or not isinstance(current_revision, GuidelineRevision)
        or current_revision.guideline_id != current_binding.guideline_id
        or current_revision.revision_id != current_binding.revision_id
        or current_revision.semantic_version != current_binding.semantic_version
        or current_revision.revision_digest != current_binding.revision_digest
    ):
        raise GuidelineImpactError("guideline_unlink_current_binding_invalid")
    if not isinstance(active_bindings, tuple | list) or any(
        not isinstance(binding, BoardGuidelineBinding) for binding in active_bindings
    ):
        raise GuidelineImpactError("guideline_unlink_active_bindings_invalid")
    if not isinstance(active_revisions, tuple | list) or any(
        not isinstance(revision, GuidelineRevision) for revision in active_revisions
    ):
        raise GuidelineImpactError("guideline_unlink_active_revisions_invalid")
    normalized_actor_id = _required_text(
        actor_id,
        "guideline_unlink_actor_required",
    )
    normalized_actor_type = _required_text(
        actor_type,
        "guideline_unlink_actor_type_required",
    )
    normalized_occurred_at = _aware_utc(
        occurred_at,
        "guideline_unlink_occurred_at_invalid",
    )
    normalized_event_id = _required_text(
        event_id,
        "guideline_unlink_event_id_required",
    )
    normalized_idempotency_key = _required_text(
        idempotency_key,
        "guideline_unlink_idempotency_key_required",
    )
    active = tuple(
        sorted(
            (
                binding
                for binding in active_bindings
                if binding.state is GuidelineBindingState.ACTIVE
            ),
            key=lambda binding: (binding.priority, binding.binding_id),
        )
    )
    current_inventory = tuple(
        binding
        for binding in active
        if binding.guideline_id == current_binding.guideline_id
    )
    if (
        (retirement is None and current_inventory != (current_binding,))
        or (
            retirement is not None
            and current_inventory not in ((), (current_binding,))
        )
        or len({binding.binding_id for binding in active}) != len(active)
        or len({binding.guideline_id for binding in active}) != len(active)
        or any(binding.board_id != current_binding.board_id for binding in active)
    ):
        raise GuidelineImpactError("guideline_unlink_active_binding_inventory_mismatch")
    revisions = tuple(active_revisions)
    revision_by_identity = _revision_map(active, revisions)
    before_head_digest = semantic_binding_head_digest_v1(active)
    before_policy_digest = semantic_policy_set_digest_v1(active, revisions)
    after = tuple(
        binding
        for binding in active
        if binding.guideline_id != current_binding.guideline_id
    )
    after_revisions = tuple(
        revision_by_identity[(binding.guideline_id, binding.revision_id)]
        for binding in after
    )
    after_head_digest = semantic_binding_head_digest_v1(after)
    after_policy_digest = semantic_policy_set_digest_v1(after, after_revisions)
    transition = plan_guideline_binding_transition(
        GuidelineBindingTransitionCommand(
            binding_id=current_binding.binding_id,
            board_id=current_binding.board_id,
            guideline_id=current_binding.guideline_id,
            state=GuidelineBindingState.UNLINKED,
            source_kind=current_binding.source_kind,
            actor_id=normalized_actor_id,
            occurred_at=normalized_occurred_at,
            idempotency_key=normalized_idempotency_key,
            expected_binding_revision=current_binding.binding_revision,
        ),
        current=current_binding,
        retirement=retirement,
    )
    if isinstance(transition, GuidelineBindingNoop):
        raise GuidelineImpactError("guideline_unlink_noop")
    if not isinstance(transition, GuidelineBindingApplied):
        raise GuidelineImpactError("guideline_unlink_transition_invalid")
    removed = tuple(
        sorted(metric.metric_id for metric in current_revision.metrics)
    )
    event = GuidelineBindingChangeEvent(
        event_id=normalized_event_id,
        event_type=GUIDELINE_ADOPTION_EVENT_TYPE,
        operation="unlink",
        board_id=current_binding.board_id,
        guideline_id=current_binding.guideline_id,
        binding_id=current_binding.binding_id,
        previous_binding_revision=current_binding.binding_revision,
        binding_revision=transition.binding.binding_revision,
        from_revision_id=current_binding.revision_id,
        from_semantic_version=current_binding.semantic_version,
        from_revision_digest=current_binding.revision_digest,
        to_revision_id=None,
        to_semantic_version=None,
        to_revision_digest=None,
        impact_receipt_id=None,
        impact_digest=None,
        binding_digest_before=guideline_binding_snapshot_digest(
            current_binding,
            board_id=current_binding.board_id,
            guideline_id=current_binding.guideline_id,
        ),
        binding_head_digest_before=before_head_digest,
        binding_head_digest_after=after_head_digest,
        policy_set_digest_before=before_policy_digest,
        policy_set_digest_after=after_policy_digest,
        added_metric_ids=(),
        changed_metric_ids=(),
        removed_metric_ids=removed,
        actor_id=normalized_actor_id,
        actor_type=normalized_actor_type,
        occurred_at=normalized_occurred_at,
    )
    request_digest = guideline_unlink_request_digest_v1(
        previous_binding=current_binding,
        binding=transition.binding,
        binding_head_digest_before=before_head_digest,
        binding_head_digest_after=after_head_digest,
        policy_set_digest_before=before_policy_digest,
        policy_set_digest_after=after_policy_digest,
        removed_metric_ids=removed,
        actor_id=normalized_actor_id,
        actor_type=normalized_actor_type,
    )
    return GuidelineUnlinkMutation(
        previous_binding=current_binding,
        binding=transition.binding,
        event=event,
        binding_head_digest_before=before_head_digest,
        binding_head_digest_after=after_head_digest,
        policy_set_digest_before=before_policy_digest,
        policy_set_digest_after=after_policy_digest,
        removed_metric_ids=removed,
        activity_id=str(uuid.uuid5(_ACTIVITY_NAMESPACE, normalized_event_id)),
        activity_action=GUIDELINE_UNLINK_ACTIVITY_ACTION,
        idempotency_key=normalized_idempotency_key,
        request_digest=request_digest,
    )


@dataclass(frozen=True, slots=True)
class GuidelineRetirementBoardEvent:
    """One exact board-policy tombstone emitted by a guideline retirement."""

    event_id: str
    event_type: str
    operation: str
    board_id: str
    guideline_id: str
    retirement_id: str
    retirement_status: str
    superseded_by_guideline_id: str | None
    binding_id: str
    binding_revision: int
    revision_id: str
    revision_number: int
    semantic_version: str
    revision_digest: str
    binding_digest_before: str
    binding_head_digest_before: str
    binding_head_digest_after: str
    policy_set_digest_before: str
    policy_set_digest_after: str
    removed_metric_ids: tuple[str, ...]
    actor_id: str
    actor_type: str
    occurred_at: datetime
    request_digest: str

    def __post_init__(self) -> None:
        text_fields = (
            "event_id",
            "event_type",
            "operation",
            "board_id",
            "guideline_id",
            "retirement_id",
            "retirement_status",
            "binding_id",
            "revision_id",
            "actor_id",
            "actor_type",
        )
        for field_name in text_fields:
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_retirement_event_{field_name}_required",
                ),
            )
        if (
            self.event_type != GUIDELINE_RETIREMENT_EVENT_TYPE
            or self.operation != "retire"
        ):
            raise GuidelineImpactError("guideline_retirement_event_type_invalid")
        expected_event_id = str(
            uuid.uuid5(
                _RETIREMENT_EVENT_NAMESPACE,
                f"{self.retirement_id}:{self.board_id}",
            )
        )
        if self.event_id != expected_event_id:
            raise GuidelineImpactError("guideline_retirement_event_id_invalid")
        if self.retirement_status not in {"retired", "superseded"}:
            raise GuidelineImpactError("guideline_retirement_event_status_invalid")
        successor = (
            None
            if self.superseded_by_guideline_id is None
            else _required_text(
                self.superseded_by_guideline_id,
                "guideline_retirement_event_successor_invalid",
            )
        )
        if (self.retirement_status == "retired" and successor is not None) or (
            self.retirement_status == "superseded"
            and (successor is None or successor == self.guideline_id)
        ):
            raise GuidelineImpactError("guideline_retirement_event_successor_invalid")
        object.__setattr__(
            self,
            "superseded_by_guideline_id",
            successor,
        )
        for field_name in ("binding_revision", "revision_number"):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise GuidelineImpactError(
                    f"guideline_retirement_event_{field_name}_invalid"
                )
        try:
            object.__setattr__(
                self,
                "semantic_version",
                normalize_guideline_semantic_version(
                    self.semantic_version,
                    "guideline_retirement_event_semver_invalid",
                ),
            )
            for field_name in (
                "revision_digest",
                "binding_digest_before",
                "binding_head_digest_before",
                "binding_head_digest_after",
                "policy_set_digest_before",
                "policy_set_digest_after",
                "request_digest",
            ):
                object.__setattr__(
                    self,
                    field_name,
                    normalize_guideline_sha256(
                        getattr(self, field_name),
                        f"guideline_retirement_event_{field_name}_invalid",
                    ),
                )
        except GuidelinePolicyContractError as exc:
            raise GuidelineImpactError(
                "guideline_retirement_event_digest_invalid"
            ) from exc
        object.__setattr__(
            self,
            "removed_metric_ids",
            _canonical_metric_ids(
                self.removed_metric_ids,
                "guideline_retirement_event_removed_metrics_invalid",
            ),
        )
        if self.actor_type not in {"agent", "user", "system"}:
            raise GuidelineImpactError("guideline_retirement_event_actor_type_invalid")
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "guideline_retirement_event_time_invalid",
            ),
        )

    def payload(self) -> dict[str, object]:
        return {
            "event_schema_version": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "event_id": self.event_id,
            "operation": self.operation,
            "board_id": self.board_id,
            "guideline_id": self.guideline_id,
            "retirement_id": self.retirement_id,
            "retirement_status": self.retirement_status,
            "superseded_by_guideline_id": (self.superseded_by_guideline_id),
            "binding_id": self.binding_id,
            "binding_revision": self.binding_revision,
            "revision_id": self.revision_id,
            "revision_number": self.revision_number,
            "semantic_version": self.semantic_version,
            "revision_digest": self.revision_digest,
            "binding_digest_before": self.binding_digest_before,
            "binding_head_digest_before": (self.binding_head_digest_before),
            "binding_head_digest_after": self.binding_head_digest_after,
            "policy_set_digest_before": self.policy_set_digest_before,
            "policy_set_digest_after": self.policy_set_digest_after,
            "policy_set_digest": self.policy_set_digest_after,
            "removed_metric_ids": self.removed_metric_ids,
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "occurred_at": self.occurred_at.isoformat(),
            "request_digest": self.request_digest,
        }


def guideline_retirement_impact_digest_v2(
    event: GuidelineRetirementBoardEvent,
) -> str:
    if not isinstance(event, GuidelineRetirementBoardEvent):
        raise GuidelineImpactError("guideline_retirement_impact_event_invalid")
    return canonical_sha256(
        {
            "contract": GUIDELINE_IMPACT_CONTRACT_VERSION,
            "operation": "retire",
            "event": event.payload(),
        }
    )


@dataclass(frozen=True, slots=True)
class GuidelineRetirementImpactMutation:
    retirement: GuidelineRetirement
    current_binding: BoardGuidelineBinding
    current_revision: GuidelineRevision
    event: GuidelineRetirementBoardEvent
    activity_id: str
    activity_action: str
    impact_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.retirement, GuidelineRetirement)
            or not isinstance(
                self.current_binding,
                BoardGuidelineBinding,
            )
            or not isinstance(self.current_revision, GuidelineRevision)
            or not isinstance(
                self.event,
                GuidelineRetirementBoardEvent,
            )
        ):
            raise GuidelineImpactError("guideline_retirement_impact_mutation_invalid")
        activity_id = _required_text(
            self.activity_id,
            "guideline_retirement_activity_id_required",
        )
        activity_action = _required_text(
            self.activity_action,
            "guideline_retirement_activity_action_required",
        )
        try:
            impact_digest = normalize_guideline_sha256(
                self.impact_digest,
                "guideline_retirement_impact_digest_invalid",
            )
        except GuidelinePolicyContractError as exc:
            raise GuidelineImpactError(
                "guideline_retirement_impact_digest_invalid"
            ) from exc
        retirement = self.retirement
        binding = self.current_binding
        revision = self.current_revision
        event = self.event
        if (
            binding.state is not GuidelineBindingState.ACTIVE
            or binding.guideline_id != retirement.guideline_id
            or revision.guideline_id != retirement.guideline_id
            or revision.revision_id != binding.revision_id
            or revision.semantic_version != binding.semantic_version
            or revision.revision_digest != binding.revision_digest
            or event.board_id != binding.board_id
            or event.guideline_id != retirement.guideline_id
            or event.retirement_id != retirement.retirement_id
            or event.retirement_status != retirement.status.value
            or event.superseded_by_guideline_id != retirement.superseded_by_guideline_id
            or event.binding_id != binding.binding_id
            or event.binding_revision != binding.binding_revision
            or event.revision_id != revision.revision_id
            or event.revision_number != revision.revision_number
            or event.semantic_version != revision.semantic_version
            or event.revision_digest != revision.revision_digest
            or event.binding_digest_before
            != guideline_binding_snapshot_digest(
                binding,
                board_id=binding.board_id,
                guideline_id=binding.guideline_id,
            )
            or event.removed_metric_ids
            != tuple(sorted(metric.metric_id for metric in revision.metrics))
            or event.actor_id != retirement.retired_by
            or event.occurred_at != retirement.retired_at
            or activity_id != str(uuid.uuid5(_ACTIVITY_NAMESPACE, event.event_id))
            or activity_action != GUIDELINE_RETIREMENT_ACTIVITY_ACTION
            or impact_digest != guideline_retirement_impact_digest_v2(event)
        ):
            raise GuidelineImpactError("guideline_retirement_impact_payload_invalid")
        object.__setattr__(self, "activity_id", activity_id)
        object.__setattr__(self, "activity_action", activity_action)
        object.__setattr__(self, "impact_digest", impact_digest)


def plan_guideline_retirement_impact(
    *,
    retirement: GuidelineRetirement,
    current_binding: BoardGuidelineBinding,
    current_revision: GuidelineRevision,
    active_bindings: tuple[BoardGuidelineBinding, ...],
    active_revisions: tuple[GuidelineRevision, ...],
    actor_type: str,
    request_digest: str,
) -> GuidelineRetirementImpactMutation:
    """Plan one board-scoped retirement event from an exact policy snapshot."""

    if (
        not isinstance(retirement, GuidelineRetirement)
        or not isinstance(current_binding, BoardGuidelineBinding)
        or current_binding.state is not GuidelineBindingState.ACTIVE
        or not isinstance(current_revision, GuidelineRevision)
    ):
        raise GuidelineImpactError("guideline_retirement_impact_input_invalid")
    normalized_actor_type = _required_text(
        actor_type,
        "guideline_retirement_actor_type_required",
    )
    try:
        normalized_request_digest = normalize_guideline_sha256(
            request_digest,
            "guideline_retirement_request_digest_invalid",
        )
    except GuidelinePolicyContractError as exc:
        raise GuidelineImpactError(
            "guideline_retirement_request_digest_invalid"
        ) from exc
    if not isinstance(active_bindings, tuple | list) or any(
        not isinstance(binding, BoardGuidelineBinding) for binding in active_bindings
    ):
        raise GuidelineImpactError("guideline_retirement_active_bindings_invalid")
    if not isinstance(active_revisions, tuple | list) or any(
        not isinstance(revision, GuidelineRevision) for revision in active_revisions
    ):
        raise GuidelineImpactError("guideline_retirement_active_revisions_invalid")
    active = tuple(
        sorted(
            (
                binding
                for binding in active_bindings
                if binding.state is GuidelineBindingState.ACTIVE
            ),
            key=lambda binding: (binding.priority, binding.binding_id),
        )
    )
    if (
        tuple(
            binding
            for binding in active
            if binding.guideline_id == retirement.guideline_id
        )
        != (current_binding,)
        or any(binding.board_id != current_binding.board_id for binding in active)
        or len({binding.binding_id for binding in active}) != len(active)
        or len({binding.guideline_id for binding in active}) != len(active)
    ):
        raise GuidelineImpactError("guideline_retirement_binding_inventory_mismatch")
    revisions = tuple(active_revisions)
    revision_by_identity = _revision_map(active, revisions)
    if (
        current_revision.guideline_id != current_binding.guideline_id
        or current_revision.revision_id != current_binding.revision_id
        or current_revision.semantic_version != current_binding.semantic_version
        or current_revision.revision_digest != current_binding.revision_digest
        or retirement.guideline_id != current_binding.guideline_id
    ):
        raise GuidelineImpactError("guideline_retirement_revision_mismatch")
    before_head_digest = semantic_binding_head_digest_v1(active)
    before_policy_digest = semantic_policy_set_digest_v1(active, revisions)
    after = tuple(
        binding for binding in active if binding.guideline_id != retirement.guideline_id
    )
    after_revisions = tuple(
        revision_by_identity[(binding.guideline_id, binding.revision_id)]
        for binding in after
    )
    after_head_digest = semantic_binding_head_digest_v1(after)
    after_policy_digest = semantic_policy_set_digest_v1(after, after_revisions)
    event_id = str(
        uuid.uuid5(
            _RETIREMENT_EVENT_NAMESPACE,
            f"{retirement.retirement_id}:{current_binding.board_id}",
        )
    )
    event = GuidelineRetirementBoardEvent(
        event_id=event_id,
        event_type=GUIDELINE_RETIREMENT_EVENT_TYPE,
        operation="retire",
        board_id=current_binding.board_id,
        guideline_id=retirement.guideline_id,
        retirement_id=retirement.retirement_id,
        retirement_status=retirement.status.value,
        superseded_by_guideline_id=(retirement.superseded_by_guideline_id),
        binding_id=current_binding.binding_id,
        binding_revision=current_binding.binding_revision,
        revision_id=current_revision.revision_id,
        revision_number=current_revision.revision_number,
        semantic_version=current_revision.semantic_version,
        revision_digest=current_revision.revision_digest,
        binding_digest_before=guideline_binding_snapshot_digest(
            current_binding,
            board_id=current_binding.board_id,
            guideline_id=current_binding.guideline_id,
        ),
        binding_head_digest_before=before_head_digest,
        binding_head_digest_after=after_head_digest,
        policy_set_digest_before=before_policy_digest,
        policy_set_digest_after=after_policy_digest,
        removed_metric_ids=tuple(
            sorted(metric.metric_id for metric in current_revision.metrics)
        ),
        actor_id=retirement.retired_by,
        actor_type=normalized_actor_type,
        occurred_at=retirement.retired_at,
        request_digest=normalized_request_digest,
    )
    return GuidelineRetirementImpactMutation(
        retirement=retirement,
        current_binding=current_binding,
        current_revision=current_revision,
        event=event,
        activity_id=str(uuid.uuid5(_ACTIVITY_NAMESPACE, event.event_id)),
        activity_action=GUIDELINE_RETIREMENT_ACTIVITY_ACTION,
        impact_digest=guideline_retirement_impact_digest_v2(event),
    )


__all__ = [
    "GUIDELINE_ADOPTION_ACTIVITY_ACTION",
    "GUIDELINE_ADOPTION_EVENT_TYPE",
    "GUIDELINE_RETIREMENT_ACTIVITY_ACTION",
    "GUIDELINE_RETIREMENT_EVENT_TYPE",
    "GUIDELINE_UNLINK_ACTIVITY_ACTION",
    "GuidelineAdoptionMutation",
    "GuidelineBindingChangeEvent",
    "GuidelineImpactCurrentnessAssessment",
    "GuidelineImpactCurrentnessReason",
    "GuidelineImpactError",
    "GuidelineImpactFenceSnapshot",
    "GuidelineImpactPreviewCommand",
    "GuidelineImpactPreviewPlan",
    "GuidelineRetirementBoardEvent",
    "GuidelineRetirementImpactMutation",
    "GuidelineUnlinkMutation",
    "assess_guideline_impact_currentness",
    "impact_fence_from_receipt",
    "guideline_adoption_request_digest_v1",
    "guideline_impact_preview_request_digest_v1",
    "guideline_retirement_impact_digest_v2",
    "guideline_unlink_request_digest_v1",
    "plan_guideline_adoption",
    "plan_guideline_impact_preview",
    "plan_guideline_retirement_impact",
    "plan_guideline_unlink",
]
