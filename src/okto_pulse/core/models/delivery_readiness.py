"""Closed derived read contracts. Null is meaningful and must be serialized."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictBool, model_validator

from okto_pulse.core.domain.delivery_assessment import (
    DeliveryState, MetricStatus, ReadinessPhase, ReadinessReason, UnitState,
)
from okto_pulse.core.domain.delivery_diagnostics import ActionKind

Count = Annotated[int, Field(strict=True, ge=0)]
Version = Annotated[int, Field(strict=True, ge=1)]
Identity = Annotated[str, Field(strict=True, min_length=1, max_length=512)]
Digest = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]
PhaseFilter = Literal["all", "implementation", "verification", "lifecycle"]


class ReadinessDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StateCounts(ReadinessDTO):
    satisfied: Count = 0
    missing: Count = 0
    partial: Count = 0
    failed: Count = 0
    stale: Count = 0
    conflict: Count = 0
    waived: Count = 0
    not_applicable: Count = 0
    unknown: Count = 0


class DeliveryMetric(ReadinessDTO):
    unit: Literal["obligation", "scenario"]
    status: MetricStatus
    percent: Annotated[int, Field(strict=True, ge=0, le=100)] | None
    total: Count | None
    applicable: Count | None
    satisfied: Count
    not_applicable: Count
    waived: Count
    unresolved: Count | None
    unknown: Count
    counts_by_state: StateCounts
    reason_codes: tuple[ReadinessReason, ...]
    known_counts: StateCounts | None

    @model_validator(mode="after")
    def coherent_counts(self):
        counts = self.counts_by_state
        for name in ("satisfied", "not_applicable", "waived", "unknown"):
            if getattr(self, name) != getattr(counts, name):
                raise ValueError("readiness_metric_count_mismatch")
        if self.total is None:
            if self.status != MetricStatus.UNKNOWN or any(value is not None for value in
                    (self.percent, self.applicable, self.unresolved)):
                raise ValueError("readiness_population_unknown")
            return self
        total = sum(counts.model_dump().values())
        if (self.applicable is None or self.unresolved is None or self.total != total
            or self.total != self.applicable + self.not_applicable
            or self.applicable != self.satisfied + self.unresolved):
            raise ValueError("readiness_metric_equation_invalid")
        if self.known_counts is not None:
            raise ValueError("readiness_partial_counts_on_known_population")
        if self.status == MetricStatus.KNOWN:
            if not self.applicable or self.unknown or self.percent != 100 * self.satisfied // self.applicable:
                raise ValueError("readiness_metric_percent_invalid")
        elif self.status == MetricStatus.NOT_APPLICABLE:
            if self.applicable != 0 or self.percent is not None:
                raise ValueError("readiness_metric_applicability_invalid")
        elif self.percent is not None:
            raise ValueError("readiness_unknown_percent")
        return self


class ReadinessScope(ReadinessDTO):
    board_id: Identity
    spec_id: Identity
    card_id: Identity | None
    card_type: Literal["normal", "bug", "test"] | None
    spec_edition: Version
    spec_version: Version
    card_version: Version | None

    @model_validator(mode="after")
    def card_scope(self):
        if (self.card_id is None) != (self.card_type is None) or (self.card_id is None) != (self.card_version is None):
            raise ValueError("readiness_card_scope_incomplete")
        return self


class DeliveryGate(ReadinessDTO):
    status: Literal["allowed", "blocked", "unknown"]
    mode: Literal["blocking", "advisory"]
    exception_applied: StrictBool
    exception_refs: tuple[Identity, ...]

    @model_validator(mode="after")
    def effective_exceptions(self):
        if self.exception_applied != bool(self.exception_refs):
            raise ValueError("readiness_effective_exception_mismatch")
        return self


class DeliveryLifecycle(ReadinessDTO):
    stored_status: Identity
    target: Literal["done"] = "done"
    check_status: Literal["allowed", "blocked", "not_evaluated"]
    other_blocker_count: Count | None
    qualification_is_current: StrictBool | None
    reason_codes: tuple[Identity, ...] = ()
    context_refs: tuple[Identity, ...] = ()


class ReadinessSummary(ReadinessDTO):
    message_code: Identity
    message: str
    unresolved_units: Count | None
    root_causes: Count
    lifecycle_pending: Count | None
    exceptions: Count


class ReadinessPage(ReadinessDTO):
    total: Count
    returned: Annotated[int, Field(strict=True, ge=0, le=20)]
    has_more: StrictBool
    next_cursor: str | None
    response_truncated: StrictBool
    phase_filter: PhaseFilter

    @model_validator(mode="after")
    def cursor_matches_page(self):
        if self.returned > self.total or self.has_more != bool(self.next_cursor):
            raise ValueError("readiness_page_invalid")
        return self


class ReadinessUnitRef(ReadinessDTO):
    unit: Literal["obligation", "scenario"]
    ref: Identity
    semantic_sha256: Digest
    spec_edition: Version
    card_id: Identity | None


class ReadinessEvidenceRef(ReadinessDTO):
    kind: Literal["record", "receipt", "execution", "inspection", "exception", "scenario", "target"]
    id: Identity
    source_ref: Identity | None
    revision: Identity | None


class ReadinessPinpoint(ReadinessDTO):
    kind: Literal["entity", "target", "source", "symbol", "span", "scenario", "observation"]
    entity_type: Identity
    entity_id: Identity
    source_ref: Identity | None
    revision: Identity | None
    relative_path: str | None
    qualified_symbol: str | None
    line_start: Version | None
    line_end: Version | None
    target_id: Identity | None
    execution_id: Identity | None
    record_id: Identity | None
    observation_id: Identity | None
    precision: Literal["entity_only", "target", "symbol", "exact_span"]
    origin: Literal["native_entity", "target_resolution", "execution_receipt", "verification_report"]

    @model_validator(mode="after")
    def factual_location(self):
        if self.relative_path is not None:
            path = self.relative_path.replace("\\", "/")
            if not path or path.startswith("/") or ":" in path or ".." in path.split("/"):
                raise ValueError("readiness_relative_path_required")
        has_span = self.line_start is not None or self.line_end is not None
        if has_span or self.kind == "span" or self.precision == "exact_span":
            if (not self.revision or not self.source_ref or not self.relative_path
                or not (self.execution_id or self.record_id or self.observation_id)
                or self.line_start is None or self.line_end is None
                or self.line_end < self.line_start):
                raise ValueError("readiness_span_evidence_required")
        return self


class ReadinessFact(ReadinessDTO):
    """Versioned factual mismatch, not an arbitrary evidence payload."""

    source_ref: Identity | None = None
    revision: Identity | None = None
    record_id: Identity | None = None
    receipt_id: Identity | None = None
    scenario_id: Identity | None = None
    criterion_id: Identity | None = None
    verification_method: Identity | None = None
    outcome: Identity | None = None
    scope_sha256: Digest | None = None
    native_reason_codes: tuple[str, ...] = ()
    obligation_ref: Identity | None = None
    semantic_sha256: Digest | None = None
    source_obligation_ref: Identity | None = None
    source_scope_sha256: Digest | None = None
    execution_id: Identity | None = None
    target_id: Identity | None = None
    observation_id: Identity | None = None
    card_id: Identity | None = None
    text: str | None = None

    @model_validator(mode='after')
    def contribution_source_is_complete(self):
        if (self.source_obligation_ref is None) != (self.source_scope_sha256 is None):
            raise ValueError('readiness_contribution_source_incomplete')
        if self.source_obligation_ref is not None and not all(
            (self.obligation_ref, self.scope_sha256, self.card_id)
        ):
            raise ValueError('readiness_contribution_destination_required')
        return self


class ReadinessDetailQuery(ReadinessDTO):
    board_id: Identity
    spec_id: Identity
    card_id: Identity | None
    view: Literal["readiness"] = "readiness"
    phase: PhaseFilter = "all"
    snapshot_id: Digest
    issue_id: Identity
    limit: Annotated[int, Field(strict=True, ge=1, le=20)] = 20
    cursor: str | None = None


class ReadinessReadQuery(ReadinessDTO):
    """Internal current-edition query, shared by summary and detail paging."""
    board_id: Identity
    spec_id: Identity
    card_id: Identity | None = None
    view: Literal['readiness'] = 'readiness'
    phase: PhaseFilter = 'all'
    snapshot_id: Digest | None = None
    issue_id: Identity | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=20)] = 20
    cursor: Annotated[str, Field(strict=True, min_length=1, max_length=8192)] | None = None

    @model_validator(mode='after')
    def detail_requires_snapshot(self):
        if self.issue_id is not None and self.snapshot_id is None:
            raise ValueError('readiness_query_invalid')
        return self


class ReadinessCursorBinding(ReadinessDTO):
    actor_id: Identity
    board_id: Identity
    spec_id: Identity
    card_id: Identity | None
    spec_edition: Version
    snapshot_id: Digest
    projection_id: Digest
    view: Literal['readiness'] = 'readiness'
    phase: PhaseFilter
    issue_id: Identity | None
    limit: Annotated[int, Field(strict=True, ge=1, le=20)]


class ReadinessAction(ReadinessDTO):
    id: Identity
    kind: ActionKind
    message_code: Identity
    message_params: dict[str, JsonValue]
    message: str
    issue_ids: tuple[Identity, ...]
    state: Literal["ready", "waiting_dependency", "requires_input", "requires_human", "forbidden", "unavailable"]
    depends_on_action_ids: tuple[Identity, ...]
    responsible_card_ids: tuple[Identity, ...]
    permission: Identity | None
    execution_surface: Literal["pulse_tool", "external_agent", "human_decision", "read_only"]
    pulse_tool: Identity | None
    arguments: dict[str, JsonValue]
    missing_inputs: tuple[Identity, ...]
    context_refs: tuple[Identity, ...]
    resolves_unit_count: Count

    @model_validator(mode="after")
    def action_surface(self):
        if self.execution_surface == "external_agent" and self.pulse_tool is not None:
            raise ValueError("readiness_external_action_tool_forbidden")
        if self.execution_surface == "pulse_tool" and self.pulse_tool is None:
            raise ValueError("readiness_action_tool_required")
        return self


class ReadinessIssue(ReadinessDTO):
    detail_state: Literal['available', 'restricted'] = 'available'
    id: Identity
    code: ReadinessReason
    phase: ReadinessPhase
    score_impact: Literal["implementation", "verification", "both", "none", "unknown"]
    intrinsic_state: UnitState
    blocks_proof: StrictBool
    # An unevaluated native transition is neither allowed nor blocked.
    blocks_target_transition: StrictBool | None
    message_code: Identity
    message_params: dict[str, JsonValue]
    message: str
    native_reason_codes: tuple[str, ...]
    affected_unit_count: Count
    affected_units: Annotated[tuple[ReadinessUnitRef, ...], Field(max_length=10)]
    affected_units_truncated: StrictBool
    criterion_ids: Annotated[tuple[Identity, ...], Field(max_length=10)]
    criterion_count: Count
    criterion_ids_truncated: StrictBool
    scenario_ids: Annotated[tuple[Identity, ...], Field(max_length=10)]
    scenario_count: Count
    scenario_ids_truncated: StrictBool
    implementation_card_ids: Annotated[tuple[Identity, ...], Field(max_length=10)]
    implementation_card_count: Count
    implementation_card_ids_truncated: StrictBool
    test_card_ids: Annotated[tuple[Identity, ...], Field(max_length=10)]
    test_card_count: Count
    test_card_ids_truncated: StrictBool
    owner: Identity | None
    owner_state: Literal["known", "unassigned", "restricted"]
    expected: Annotated[tuple[ReadinessFact, ...], Field(max_length=10)]
    expected_count: Count
    expected_truncated: StrictBool
    observed: Annotated[tuple[ReadinessFact, ...], Field(max_length=10)]
    observed_count: Count
    observed_truncated: StrictBool
    evidence_refs: Annotated[tuple[ReadinessEvidenceRef, ...], Field(max_length=10)]
    evidence_count: Count
    evidence_refs_truncated: StrictBool
    pinpoints: Annotated[tuple[ReadinessPinpoint, ...], Field(max_length=10)]
    pinpoint_count: Count
    pinpoints_truncated: StrictBool
    root_cause_id: Identity
    depends_on_issue_ids: tuple[Identity, ...]
    action_ids: tuple[Identity, ...]
    detail_query: ReadinessDetailQuery

    @model_validator(mode="after")
    def truthful_summary(self):
        pairs = (("affected_units", "affected_unit_count"), ("criterion_ids", "criterion_count"),
            ("scenario_ids", "scenario_count"), ("implementation_card_ids", "implementation_card_count"),
            ("test_card_ids", "test_card_count"), ("evidence_refs", "evidence_count"),
            ("pinpoints", "pinpoint_count"), ("expected", "expected_count"), ("observed", "observed_count"))
        for field, count in pairs:
            values, total = getattr(self, field), getattr(self, count)
            if len(values) > total or getattr(self, field + "_truncated") != (len(values) < total):
                raise ValueError("readiness_issue_truncation_invalid")
        if (self.owner_state == "known") != (self.owner is not None):
            raise ValueError("readiness_owner_visibility_invalid")
        if self.detail_query.issue_id != self.id:
            raise ValueError("readiness_issue_detail_mismatch")
        return self


class DeliveryReadinessContext(ReadinessDTO):
    contract_version: Literal["delivery-readiness/v1"] = "delivery-readiness/v1"
    metrics_version: Literal[2] = 2
    scope: ReadinessScope
    snapshot_id: Digest
    evaluated_at: datetime
    scope_complete: StrictBool
    evaluation_complete: StrictBool
    implementation: DeliveryMetric
    verification: DeliveryMetric
    delivery_state: DeliveryState
    proof_complete: StrictBool | None
    delivery_gate: DeliveryGate
    lifecycle: DeliveryLifecycle
    summary: ReadinessSummary
    mutation_reauthorization_required: Literal[True] = True

    @model_validator(mode='after')
    def utc_time(self):
        if self.evaluated_at.utcoffset() is None or self.evaluated_at.utcoffset().total_seconds() != 0:
            raise ValueError('readiness_utc_time_required')
        return self


class DeliveryReadinessCardProjection(ReadinessDTO):
    card_id: Identity
    delivery_readiness: DeliveryReadinessContext


class DeliveryReadinessBoardSummary(ReadinessDTO):
    """One authorized Spec snapshot shared by its compact Card projections."""
    contract_version: Literal['delivery-readiness/v1'] = 'delivery-readiness/v1'
    metrics_version: Literal[2] = 2
    delivery_readiness: DeliveryReadinessContext
    cards: tuple[DeliveryReadinessCardProjection, ...]

    @model_validator(mode='after')
    def one_snapshot(self):
        scope = self.delivery_readiness.scope
        if scope.card_id is not None:
            raise ValueError('readiness_summary_spec_scope_required')
        ids = set()
        for row in self.cards:
            card = row.delivery_readiness
            if (card.scope.board_id, card.scope.spec_id, card.scope.spec_edition,
                card.scope.spec_version, card.snapshot_id) != (
                scope.board_id, scope.spec_id, scope.spec_edition, scope.spec_version, self.delivery_readiness.snapshot_id
            ) or not card.scope.card_id or card.scope.card_id in ids or row.card_id != card.scope.card_id:
                raise ValueError('readiness_summary_scope_mismatch')
            ids.add(card.scope.card_id)
        return self


class DeliveryReadiness(DeliveryReadinessContext):
    issues: Annotated[tuple[ReadinessIssue, ...], Field(max_length=20)]
    next_actions: Annotated[tuple[ReadinessAction, ...], Field(max_length=3)]
    page: ReadinessPage

    @model_validator(mode="after")
    def coherent_snapshot(self):
        if self.page.returned != len(self.issues):
            raise ValueError("readiness_page_count_mismatch")
        for issue in self.issues:
            query = issue.detail_query
            if (query.snapshot_id, query.board_id, query.spec_id, query.card_id) != (
                    self.snapshot_id, self.scope.board_id, self.scope.spec_id, self.scope.card_id):
                raise ValueError("readiness_issue_snapshot_mismatch")
        return self


class ReadinessRelation(ReadinessDTO):
    kind: Literal['criterion', 'scenario', 'implementation_card', 'test_card', 'native_reason',
        'issue_dependency', 'cause_cycle', 'action_cycle']
    source_id: Identity
    target_id: Identity | None = None

    @model_validator(mode='after')
    def edge_shape(self):
        edge = self.kind in {'issue_dependency', 'cause_cycle', 'action_cycle'}
        if edge != (self.target_id is not None):
            raise ValueError('readiness_relation_invalid')
        return self


class ReadinessDetailEntry(ReadinessDTO):
    id: Identity
    type: Literal['affected_unit', 'evidence', 'pinpoint', 'action']
    section: Literal['affected_units', 'evidence_refs', 'expected', 'observed', 'relations', 'pinpoints', 'actions']
    value: ReadinessUnitRef | ReadinessEvidenceRef | ReadinessFact | ReadinessRelation | ReadinessPinpoint | ReadinessAction

    @model_validator(mode='after')
    def closed_entry_shape(self):
        expected = {
            'affected_units': ('affected_unit', ReadinessUnitRef),
            'evidence_refs': ('evidence', ReadinessEvidenceRef),
            'expected': ('evidence', ReadinessFact), 'observed': ('evidence', ReadinessFact),
            'relations': ('evidence', ReadinessRelation),
            'pinpoints': ('pinpoint', ReadinessPinpoint), 'actions': ('action', ReadinessAction),
        }[self.section]
        if self.type != expected[0] or not isinstance(self.value, expected[1]):
            raise ValueError('readiness_detail_entry_invalid')
        return self


class ReadinessDetailCounts(ReadinessDTO):
    affected_unit: Count
    evidence: Count
    pinpoint: Count
    action: Count


class DeliveryReadinessDetail(DeliveryReadinessContext):
    detail_state: Literal['available', 'restricted'] = 'available'
    issue_id: Identity
    entry_counts: ReadinessDetailCounts
    entries: Annotated[tuple[ReadinessDetailEntry, ...], Field(max_length=20)]
    page: ReadinessPage

    @model_validator(mode='after')
    def entry_page(self):
        if self.page.returned != len(self.entries) or self.page.total != sum(self.entry_counts.model_dump().values()):
            raise ValueError('readiness_detail_count_mismatch')
        if len({e.id for e in self.entries}) != len(self.entries):
            raise ValueError('readiness_detail_duplicate_entry')
        return self
