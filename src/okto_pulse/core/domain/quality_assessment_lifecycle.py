"""Pure lifecycle and purge contracts for SK-A relational artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentReceiptState,
    AssessmentSubjectRef,
    AssessmentSubjectType,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256

QUALITY_ASSESSMENT_LIFECYCLE_CONTRACT_VERSION = (
    "quality-assessment-lifecycle/v2"
)


class AssessmentLifecycleContractError(ValueError):
    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise AssessmentLifecycleContractError(code)
    normalized = value.strip()
    if not normalized:
        raise AssessmentLifecycleContractError(code)
    return normalized


def _strict_non_negative_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AssessmentLifecycleContractError(code)
    return value


def _strict_positive_int(value: object, code: str) -> int:
    resolved = _strict_non_negative_int(value, code)
    if resolved < 1:
        raise AssessmentLifecycleContractError(code)
    return resolved


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise AssessmentLifecycleContractError(code)
    return value.astimezone(timezone.utc)


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if len(normalized) != 64:
        raise AssessmentLifecycleContractError(code)
    try:
        int(normalized, 16)
    except ValueError as exc:
        raise AssessmentLifecycleContractError(code) from exc
    return normalized


class AssessmentLifecycleAction(str, Enum):
    ADMIT_VALIDATION = "admit_validation"
    ARCHIVE = "archive"
    CANCEL = "cancel"
    RESTORE = "restore"
    REOPEN = "reopen"


class AssessmentHeadStrategy(str, Enum):
    PRESERVE = "preserve"
    RECOMPUTE = "recompute"


class AssessmentProjectionAction(str, Enum):
    HIDE = "hide"
    REBUILD = "rebuild"


class AssessmentKgAction(str, Enum):
    TOMBSTONE = "tombstone"
    RECONCILE = "reconcile"


class AssessmentPurgeScope(str, Enum):
    SUBJECT = "subject"
    BOARD = "board"


class AssessmentPurgeResource(str, Enum):
    QUALITY_PROJECTIONS = "quality_projections"
    QUALITY_OUTBOX = "quality_outbox"
    QUALITY_HEADS = "quality_heads"
    QUALITY_FINDING_QA_LINKS = "quality_finding_qa_links"
    QUALITY_FINDINGS = "quality_findings"
    QUALITY_PROPOSED_QUESTIONS = "quality_proposed_questions"
    OPERATIONAL_QA_ITEMS = "operational_qa_items"
    CHECKLIST_ITEM_RESULTS = "checklist_item_results"
    CHECKLIST_RECEIPTS = "checklist_receipts"
    CHECKLIST_EXECUTION_HEADS = "checklist_execution_heads"
    CHECKLIST_EXECUTIONS = "checklist_executions"
    CHECKLIST_BINDING_HEADS = "checklist_binding_heads"
    CHECKLIST_BINDING_VERSIONS = "checklist_binding_versions"
    RESEARCH_IDEMPOTENCY = "research_idempotency"
    RESEARCH_DERIVATIONS = "research_derivations"
    RESEARCH_SNAPSHOTS = "research_snapshots"
    RESEARCH_OUTBOX = "research_outbox"
    RESEARCH_EVENTS = "research_events"
    RESEARCH_HISTORY = "research_history"
    RESEARCH_HEADS = "research_heads"
    RESEARCH_ENTRIES = "research_entries"
    QUALITY_RECEIPTS = "quality_receipts"
    QUALITY_LIFECYCLE_STALE_TRANSITIONS = (
        "quality_lifecycle_stale_transitions"
    )
    QUALITY_LIFECYCLE_TRANSITIONS = "quality_lifecycle_transitions"
    SUBJECT_HISTORY = "subject_history"
    KG_PROJECTIONS = "kg_projections"
    LEGACY_IMPORT_RESOLUTIONS = "legacy_import_resolutions"
    LEGACY_IMPORT_CHECKPOINTS = "legacy_import_checkpoints"
    LEGACY_IMPORT_COMPLETIONS = "legacy_import_completions"
    LEGACY_IMPORT_CANDIDATES = "legacy_import_candidates"
    LEGACY_IMPORT_RUNS = "legacy_import_runs"


# Children and pointers precede their immutable parents.  The epoch ledger is
# intentionally absent: subject purge must not reopen a completed epoch.
ASSESSMENT_SUBJECT_PURGE_ORDER: tuple[AssessmentPurgeResource, ...] = (
    AssessmentPurgeResource.QUALITY_PROJECTIONS,
    AssessmentPurgeResource.QUALITY_OUTBOX,
    AssessmentPurgeResource.QUALITY_HEADS,
    AssessmentPurgeResource.QUALITY_FINDING_QA_LINKS,
    AssessmentPurgeResource.QUALITY_FINDINGS,
    AssessmentPurgeResource.QUALITY_PROPOSED_QUESTIONS,
    AssessmentPurgeResource.OPERATIONAL_QA_ITEMS,
    AssessmentPurgeResource.CHECKLIST_ITEM_RESULTS,
    AssessmentPurgeResource.CHECKLIST_EXECUTION_HEADS,
    AssessmentPurgeResource.CHECKLIST_RECEIPTS,
    AssessmentPurgeResource.CHECKLIST_EXECUTIONS,
    AssessmentPurgeResource.RESEARCH_IDEMPOTENCY,
    AssessmentPurgeResource.RESEARCH_DERIVATIONS,
    AssessmentPurgeResource.RESEARCH_SNAPSHOTS,
    AssessmentPurgeResource.RESEARCH_OUTBOX,
    AssessmentPurgeResource.RESEARCH_EVENTS,
    AssessmentPurgeResource.RESEARCH_HISTORY,
    AssessmentPurgeResource.RESEARCH_HEADS,
    AssessmentPurgeResource.RESEARCH_ENTRIES,
    AssessmentPurgeResource.QUALITY_RECEIPTS,
    AssessmentPurgeResource.QUALITY_LIFECYCLE_STALE_TRANSITIONS,
    AssessmentPurgeResource.QUALITY_LIFECYCLE_TRANSITIONS,
    AssessmentPurgeResource.SUBJECT_HISTORY,
    AssessmentPurgeResource.KG_PROJECTIONS,
)

# A board erasure is the only operation allowed to delete the durable epoch.
ASSESSMENT_BOARD_PURGE_ORDER: tuple[AssessmentPurgeResource, ...] = (
    *ASSESSMENT_SUBJECT_PURGE_ORDER,
    AssessmentPurgeResource.CHECKLIST_BINDING_HEADS,
    AssessmentPurgeResource.CHECKLIST_BINDING_VERSIONS,
    AssessmentPurgeResource.LEGACY_IMPORT_RESOLUTIONS,
    AssessmentPurgeResource.LEGACY_IMPORT_CHECKPOINTS,
    AssessmentPurgeResource.LEGACY_IMPORT_COMPLETIONS,
    AssessmentPurgeResource.LEGACY_IMPORT_CANDIDATES,
    AssessmentPurgeResource.LEGACY_IMPORT_RUNS,
)


@dataclass(frozen=True, slots=True)
class AssessmentLifecycleCurrentInput:
    assessment_kind: AssessmentKind
    input_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.assessment_kind, AssessmentKind):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_kind_invalid"
            )
        object.__setattr__(
            self,
            "input_digest",
            _sha256(
                self.input_digest,
                "assessment_lifecycle_input_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentLifecycleSubjectSnapshot:
    subject: AssessmentSubjectRef
    status: str
    archived: bool
    current_inputs: tuple[AssessmentLifecycleCurrentInput, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.subject, AssessmentSubjectRef):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_subject_invalid"
            )
        object.__setattr__(
            self,
            "status",
            _required_text(
                self.status,
                "assessment_lifecycle_status_required",
            ),
        )
        if not isinstance(self.archived, bool):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_archived_invalid"
            )
        inputs = tuple(self.current_inputs)
        object.__setattr__(self, "current_inputs", inputs)
        if any(
            not isinstance(item, AssessmentLifecycleCurrentInput)
            for item in inputs
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_current_input_invalid"
            )
        kinds = [item.assessment_kind for item in inputs]
        if len(set(kinds)) != len(kinds):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_current_input_duplicate"
            )

    @property
    def identity(self) -> tuple[str, AssessmentSubjectType, str]:
        return (
            self.subject.board_id,
            self.subject.subject_type,
            self.subject.subject_id,
        )


@dataclass(frozen=True, slots=True)
class AssessmentLifecycleTransition:
    action: AssessmentLifecycleAction
    before: AssessmentLifecycleSubjectSnapshot
    after: AssessmentLifecycleSubjectSnapshot
    idempotency_key: str
    actor_id: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.action, AssessmentLifecycleAction):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_action_invalid"
            )
        if not isinstance(
            self.before,
            AssessmentLifecycleSubjectSnapshot,
        ) or not isinstance(
            self.after,
            AssessmentLifecycleSubjectSnapshot,
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_transition_snapshot_invalid"
            )
        if self.before.identity != self.after.identity:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_transition_scope_mismatch"
            )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "assessment_lifecycle_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "actor_id",
            _required_text(
                self.actor_id,
                "assessment_lifecycle_actor_id_required",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "assessment_lifecycle_occurred_at_must_be_aware",
            ),
        )
        if self.action is AssessmentLifecycleAction.ADMIT_VALIDATION:
            validation_status = {
                AssessmentSubjectType.IDEATION: "evaluating",
                AssessmentSubjectType.REFINEMENT: "approved",
                AssessmentSubjectType.SPEC: "approved",
            }[self.after.subject.subject_type]
            valid = (
                self.after.status == validation_status
                and self.before.status != validation_status
                and self.before.subject.subject_version
                == self.after.subject.subject_version
                and self.before.subject.subject_edition
                == self.after.subject.subject_edition
                and not self.after.archived
            )
        elif self.action is AssessmentLifecycleAction.ARCHIVE:
            valid = not self.before.archived and self.after.archived
        elif self.action is AssessmentLifecycleAction.CANCEL:
            valid = (
                self.after.status == "cancelled"
                and self.before.status != "cancelled"
            )
        elif self.action is AssessmentLifecycleAction.RESTORE:
            valid = self.before.archived and not self.after.archived
        else:
            before_edition = self.before.subject.subject_edition
            after_edition = self.after.subject.subject_edition
            if before_edition is not None or after_edition is not None:
                valid = (
                    self.after.status == "draft"
                    and self.before.status != "draft"
                    and before_edition is not None
                    and after_edition == before_edition + 1
                    and not self.after.archived
                )
            else:
                # Compatibility for pre-edition lifecycle events.
                valid = (
                    self.after.status == "draft"
                    and self.before.status in {"done", "cancelled"}
                    and self.after.subject.subject_version
                    == self.before.subject.subject_version + 1
                    and not self.after.archived
                )
        if not valid:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_transition_invalid"
            )

    @property
    def transition_digest(self) -> str:
        return canonical_sha256(
            {
                "action": self.action.value,
                "board_id": self.after.subject.board_id,
                "subject_type": self.after.subject.subject_type.value,
                "subject_id": self.after.subject.subject_id,
                "before_version": self.before.subject.subject_version,
                "before_edition": self.before.subject.subject_edition,
                "before_status": self.before.status,
                "before_archived": self.before.archived,
                "after_version": self.after.subject.subject_version,
                "after_edition": self.after.subject.subject_edition,
                "after_status": self.after.status,
                "after_archived": self.after.archived,
                "idempotency_key": self.idempotency_key,
            }
        )


@dataclass(frozen=True, slots=True)
class AssessmentLifecycleHead:
    assessment_kind: AssessmentKind
    receipt_id: str
    revision: int

    def __post_init__(self) -> None:
        if not isinstance(self.assessment_kind, AssessmentKind):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_kind_invalid"
            )
        object.__setattr__(
            self,
            "receipt_id",
            _required_text(
                self.receipt_id,
                "assessment_lifecycle_head_receipt_required",
            ),
        )
        object.__setattr__(
            self,
            "revision",
            _strict_positive_int(
                self.revision,
                "assessment_lifecycle_head_revision_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentLifecycleReceipt:
    receipt_id: str
    subject: AssessmentSubjectRef
    assessment_kind: AssessmentKind
    input_digest: str
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "receipt_id",
            _required_text(
                self.receipt_id,
                "assessment_lifecycle_receipt_id_required",
            ),
        )
        if not isinstance(self.subject, AssessmentSubjectRef):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_receipt_subject_invalid"
            )
        if not isinstance(self.assessment_kind, AssessmentKind):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_receipt_kind_invalid"
            )
        object.__setattr__(
            self,
            "input_digest",
            _sha256(
                self.input_digest,
                "assessment_lifecycle_receipt_input_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "assessment_lifecycle_receipt_created_at_must_be_aware",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentHeadRebuild:
    assessment_kind: AssessmentKind
    expected_revision: int
    previous_receipt_id: str | None
    selected_receipt_id: str | None
    selected_state: AssessmentReceiptState | None
    resulting_revision: int
    stale_transition_required: bool
    stale_transition_key: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.assessment_kind, AssessmentKind):
            raise AssessmentLifecycleContractError(
                "assessment_head_rebuild_kind_invalid"
            )
        for field_name in ("previous_receipt_id", "selected_receipt_id"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _required_text(
                        value,
                        f"assessment_head_rebuild_{field_name}_invalid",
                    ),
                )
        object.__setattr__(
            self,
            "expected_revision",
            _strict_non_negative_int(
                self.expected_revision,
                "assessment_head_rebuild_expected_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "resulting_revision",
            _strict_non_negative_int(
                self.resulting_revision,
                "assessment_head_rebuild_resulting_revision_invalid",
            ),
        )
        changed = self.previous_receipt_id != self.selected_receipt_id
        expected_result = self.expected_revision + int(changed)
        if self.resulting_revision != expected_result:
            raise AssessmentLifecycleContractError(
                "assessment_head_rebuild_revision_mismatch"
            )
        if not isinstance(self.stale_transition_required, bool):
            raise AssessmentLifecycleContractError(
                "assessment_head_rebuild_stale_flag_invalid"
            )
        if self.selected_receipt_id is None:
            if self.selected_state is not None:
                raise AssessmentLifecycleContractError(
                    "assessment_head_rebuild_selected_state_invalid"
                )
        elif self.selected_state not in {
            AssessmentReceiptState.CURRENT,
            AssessmentReceiptState.STALE,
        }:
            raise AssessmentLifecycleContractError(
                "assessment_head_rebuild_selected_state_invalid"
            )
        if self.stale_transition_required:
            object.__setattr__(
                self,
                "stale_transition_key",
                _sha256(
                    self.stale_transition_key,
                    "assessment_head_rebuild_stale_key_invalid",
                ),
            )
            if (
                self.previous_receipt_id is None
                or self.selected_state is not AssessmentReceiptState.STALE
            ):
                raise AssessmentLifecycleContractError(
                    "assessment_head_rebuild_stale_flag_mismatch"
                )
        elif self.stale_transition_key is not None:
            raise AssessmentLifecycleContractError(
                "assessment_head_rebuild_stale_key_unexpected"
            )


@dataclass(frozen=True, slots=True)
class AssessmentLifecyclePlan:
    transition: AssessmentLifecycleTransition
    head_strategy: AssessmentHeadStrategy
    projection_action: AssessmentProjectionAction
    kg_action: AssessmentKgAction
    head_rebuilds: tuple[AssessmentHeadRebuild, ...]
    preserve_immutable_history: bool
    event_and_outbox_same_uow: bool
    # A Spec reopen also removes its mutable checklist execution head in the
    # same UoW. Immutable executions/receipts remain historical evidence.
    clear_checklist_execution_head: bool = False
    contract_version: str = QUALITY_ASSESSMENT_LIFECYCLE_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.transition, AssessmentLifecycleTransition):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_plan_transition_invalid"
            )
        if not isinstance(self.head_strategy, AssessmentHeadStrategy):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_strategy_invalid"
            )
        if not isinstance(
            self.projection_action,
            AssessmentProjectionAction,
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_projection_action_invalid"
            )
        if not isinstance(self.kg_action, AssessmentKgAction):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_kg_action_invalid"
            )
        rebuilds = tuple(self.head_rebuilds)
        object.__setattr__(self, "head_rebuilds", rebuilds)
        if any(not isinstance(item, AssessmentHeadRebuild) for item in rebuilds):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_rebuild_invalid"
            )
        kinds = [item.assessment_kind for item in rebuilds]
        if len(set(kinds)) != len(kinds):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_rebuild_duplicate"
            )
        recover = self.transition.action in {
            AssessmentLifecycleAction.RESTORE,
            AssessmentLifecycleAction.REOPEN,
        }
        reconcile = recover or (
            self.transition.action
            is AssessmentLifecycleAction.ADMIT_VALIDATION
        )
        if recover != (self.head_strategy is AssessmentHeadStrategy.RECOMPUTE):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_head_strategy_mismatch"
            )
        if reconcile != (
            self.projection_action is AssessmentProjectionAction.REBUILD
        ):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_projection_action_mismatch"
            )
        if reconcile != (self.kg_action is AssessmentKgAction.RECONCILE):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_kg_action_mismatch"
            )
        if not recover and rebuilds:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_unexpected_head_rebuild"
            )
        if self.preserve_immutable_history is not True:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_history_must_be_preserved"
            )
        if self.event_and_outbox_same_uow is not True:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_atomic_audit_required"
            )
        if not isinstance(self.clear_checklist_execution_head, bool):
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_checklist_head_flag_invalid"
            )
        checklist_reset_required = (
            self.transition.action is AssessmentLifecycleAction.REOPEN
            and self.transition.after.subject.subject_type
            is AssessmentSubjectType.SPEC
        )
        if self.clear_checklist_execution_head is not checklist_reset_required:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_checklist_head_strategy_mismatch"
            )
        if self.contract_version != QUALITY_ASSESSMENT_LIFECYCLE_CONTRACT_VERSION:
            raise AssessmentLifecycleContractError(
                "assessment_lifecycle_contract_version_unsupported"
            )


@dataclass(frozen=True, slots=True)
class AssessmentPurgeTarget:
    board_id: str
    scope: AssessmentPurgeScope
    subject_type: AssessmentSubjectType | None = None
    subject_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "assessment_purge_board_id_required"),
        )
        if not isinstance(self.scope, AssessmentPurgeScope):
            raise AssessmentLifecycleContractError(
                "assessment_purge_scope_invalid"
            )
        if self.scope is AssessmentPurgeScope.SUBJECT:
            if not isinstance(self.subject_type, AssessmentSubjectType):
                raise AssessmentLifecycleContractError(
                    "assessment_purge_subject_type_required"
                )
            object.__setattr__(
                self,
                "subject_id",
                _required_text(
                    self.subject_id,
                    "assessment_purge_subject_id_required",
                ),
            )
        elif self.subject_type is not None or self.subject_id is not None:
            raise AssessmentLifecycleContractError(
                "assessment_board_purge_subject_forbidden"
            )


@dataclass(frozen=True, slots=True)
class AssessmentPurgePlan:
    target: AssessmentPurgeTarget
    deletion_order: tuple[AssessmentPurgeResource, ...]
    board_erasure_permit_id: str | None
    idempotent: bool
    event_and_outbox_same_uow: bool

    def __post_init__(self) -> None:
        if not isinstance(self.target, AssessmentPurgeTarget):
            raise AssessmentLifecycleContractError(
                "assessment_purge_target_invalid"
            )
        order = tuple(self.deletion_order)
        object.__setattr__(self, "deletion_order", order)
        expected = (
            ASSESSMENT_BOARD_PURGE_ORDER
            if self.target.scope is AssessmentPurgeScope.BOARD
            else ASSESSMENT_SUBJECT_PURGE_ORDER
        )
        if order != expected:
            raise AssessmentLifecycleContractError(
                "assessment_purge_order_invalid"
            )
        if self.target.scope is AssessmentPurgeScope.BOARD:
            object.__setattr__(
                self,
                "board_erasure_permit_id",
                _required_text(
                    self.board_erasure_permit_id,
                    "assessment_board_erasure_permit_required",
                ),
            )
        elif self.board_erasure_permit_id is not None:
            raise AssessmentLifecycleContractError(
                "assessment_subject_purge_permit_forbidden"
            )
        if self.idempotent is not True:
            raise AssessmentLifecycleContractError(
                "assessment_purge_must_be_idempotent"
            )
        if self.event_and_outbox_same_uow is not True:
            raise AssessmentLifecycleContractError(
                "assessment_purge_atomic_audit_required"
            )


@dataclass(frozen=True, slots=True)
class AssessmentPurgeResidual:
    resource: AssessmentPurgeResource
    count: int

    def __post_init__(self) -> None:
        if not isinstance(self.resource, AssessmentPurgeResource):
            raise AssessmentLifecycleContractError(
                "assessment_purge_residual_resource_invalid"
            )
        object.__setattr__(
            self,
            "count",
            _strict_non_negative_int(
                self.count,
                "assessment_purge_residual_count_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentPurgePostcondition:
    """Evidence produced by the inner quality-assessment purge slice.

    Permit ownership intentionally does not appear in this contract.  The
    quality adapter executes while the outer board-erasure permit is still
    active, so it can prove only its own rows, orphans, projections, outbox,
    and epoch consistency.  Only the outer board-erasure orchestrator may
    attest that every purge finished and the permit was released.
    """

    target: AssessmentPurgeTarget
    residuals: tuple[AssessmentPurgeResidual, ...]
    zero_orphans: bool
    projections_reconciled: bool
    outbox_reconciled: bool
    epoch_consistency_preserved: bool
    verified_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.target, AssessmentPurgeTarget):
            raise AssessmentLifecycleContractError(
                "assessment_purge_postcondition_target_invalid"
            )
        residuals = tuple(self.residuals)
        object.__setattr__(self, "residuals", residuals)
        if any(
            not isinstance(item, AssessmentPurgeResidual)
            for item in residuals
        ):
            raise AssessmentLifecycleContractError(
                "assessment_purge_postcondition_residual_invalid"
            )
        resources = [item.resource for item in residuals]
        if len(set(resources)) != len(resources):
            raise AssessmentLifecycleContractError(
                "assessment_purge_postcondition_residual_duplicate"
            )
        for field_name in (
            "zero_orphans",
            "projections_reconciled",
            "outbox_reconciled",
            "epoch_consistency_preserved",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise AssessmentLifecycleContractError(
                    f"assessment_purge_postcondition_{field_name}_invalid"
                )
        object.__setattr__(
            self,
            "verified_at",
            _aware_utc(
                self.verified_at,
                "assessment_purge_verified_at_must_be_aware",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentBoardErasureCompletion:
    """Final evidence owned exclusively by the outer board-erasure boundary."""

    target: AssessmentPurgeTarget
    quality_purge_postcondition: AssessmentPurgePostcondition
    board_erasure_permit_id: str
    all_board_purges_completed: bool
    permit_released: bool
    verified_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target, AssessmentPurgeTarget)
            or self.target.scope is not AssessmentPurgeScope.BOARD
        ):
            raise AssessmentLifecycleContractError(
                "assessment_board_erasure_completion_target_invalid"
            )
        if not isinstance(
            self.quality_purge_postcondition,
            AssessmentPurgePostcondition,
        ):
            raise AssessmentLifecycleContractError(
                "assessment_board_erasure_quality_postcondition_invalid"
            )
        if self.quality_purge_postcondition.target != self.target:
            raise AssessmentLifecycleContractError(
                "assessment_board_erasure_quality_target_mismatch"
            )
        object.__setattr__(
            self,
            "board_erasure_permit_id",
            _required_text(
                self.board_erasure_permit_id,
                "assessment_board_erasure_completion_permit_required",
            ),
        )
        for field_name in (
            "all_board_purges_completed",
            "permit_released",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise AssessmentLifecycleContractError(
                    f"assessment_board_erasure_completion_{field_name}_invalid"
                )
        object.__setattr__(
            self,
            "verified_at",
            _aware_utc(
                self.verified_at,
                "assessment_board_erasure_completion_verified_at_must_be_aware",
            ),
        )
