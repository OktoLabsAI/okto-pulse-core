"""Closed coverage variant for REST and the existing traceability facade."""
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field


class Closed(BaseModel):
    model_config = ConfigDict(extra='forbid')


class SpecCoverageRequest(Closed):
    view: Literal['coverage']
    subject_ref: str = Field(pattern=r'^spec:[^:\s]+$', max_length=4096)
    limit: int = Field(default=200, ge=1, le=1000)
    cursor: str | None = Field(default=None, max_length=256)
    timeout_ms: int | None = Field(default=None, ge=1, le=30000)


class CoverageSummary(Closed):
    ac_coverage_pct: float
    ac_covered: int
    ac_total: int
    ac_uncovered_indices: list[int]
    fr_coverage_pct: float
    fr_covered: int
    fr_total: int
    fr_uncovered_indices: list[int]
    scenario_task_linkage_pct: float
    scenarios_linked: int
    scenarios_total: int
    br_task_linkage_pct: float
    brs_linked: int
    brs_total: int
    contract_task_linkage_pct: float
    contracts_linked: int
    contracts_total: int
    tr_task_linkage_pct: float
    trs_linked: int
    trs_total: int
    decisions_planning_pct: float
    decisions_planned: int
    decisions_total: int
    decisions_pending_ids: list[str]
    ir_task_linkage_pct: float
    irs_linked: int
    irs_total: int
    irs_uncovered_ids: list[str]
    or_task_linkage_pct: float
    ors_linked: int
    ors_total: int
    ors_uncovered_ids: list[str]
    cards_total: int
    cards_done: int
    cards_total_raw: int
    cards_done_raw: int
    cards_total_effective: int
    cards_done_effective: int
    skip_test_coverage: bool
    skip_rules_coverage: bool
    skip_decisions_coverage: bool
    skip_ir_coverage: bool
    skip_or_coverage: bool


class GraphObservation(Closed):
    state: Literal['observed', 'restricted', 'unavailable']
    complete_for_scope: Literal[False]
    expected_nodes: int | None
    observed_nodes: int | None
    missing_nodes: int | None
    missing_relations: int | None
    truncated: bool = False
    comparison_scope: str | None = None
    interpretation: str | None = None


class Structure(Closed):
    authority: Literal['spec_coverage_summary']
    interpretation: Literal['planning_links_not_delivery_proof']
    complete_for_scope: bool
    summary: CoverageSummary | None
    graph: GraphObservation


class DeliveryCounts(Closed):
    obligations: int | None
    implementation_proven: int | None
    verification_proven: int | None
    observed_obligations: int | None
    decisions: int | None
    decisions_verified: int | None


class Delivery(Closed):
    state: Literal['available', 'restricted', 'unavailable']
    authority: Literal['evaluate_delivery_coverage']
    complete_for_scope: bool
    counts: DeliveryCounts
    blockers: list[str]
    rejected_record_refs: list[str]
    interpretation: Literal['admitted_proof_and_authorized_waivers_do_not_approve_other_gates']


ProofStatus = Literal['unknown', 'proven', 'partial', 'missing', 'satisfied_with_waiver', 'not_applicable']


class DeliveryItem(Closed):
    kind: Literal['delivery']
    obligation_ref: str
    semantic_sha256: str
    title: str = Field(max_length=240)
    implementation: ProofStatus
    verification: ProofStatus
    decision_verification_status: str | None
    decision_review_refs: list[str]
    implementation_record_refs: list[str]
    verification_record_refs: list[str]
    implementation_waiver_refs: list[str]
    verification_waiver_refs: list[str]
    required_card_refs: list[str]
    missing_card_refs: list[str]
    missing_criteria: list[list[str]]


class NodeItem(Closed):
    kind: Literal['structure_node']
    node_type: str
    subject_ref: str
    observation: Literal['observed', 'not_found_in_projection']


class RelationItem(Closed):
    kind: Literal['structure_relation']
    source_type: str
    source_ref: str
    relation: str
    target_type: str
    target_ref: str
    rule_id: str
    layer: str
    created_by: str
    observation: Literal['observed_expected', 'graph_only', 'not_found_in_projection']
    authority: Literal['structural_observation_not_delivery_proof']


class Freshness(Closed):
    state: Literal['unknown', 'incomplete', 'unavailable']
    graph_generation: str | None
    source_checkpoint: str
    projection_checkpoint: None
    checked_at: str


class Completeness(Closed):
    complete_for_scope: Literal[False]
    truncated: bool
    limitations: list[str]


class SpecCoverageResponse(Closed):
    view: Literal['coverage']
    subject_ref: str
    authority: Literal['informational']
    data_source: Literal['relational', 'composed']
    edition: int
    projection_freshness: Freshness
    completeness: Completeness
    structure: Structure
    delivery: Delivery
    items: list[Annotated[DeliveryItem | NodeItem | RelationItem, Field(discriminator='kind')]]
    next_cursor: str | None
