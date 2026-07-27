"""Zero-orphan connectivity guard for KG node write batches.

The guard is deliberately independent from the MCP Pydantic DTOs and from the
Layer 1 worker dataclasses. Both paths pass objects with candidate_id,
node_type and source_artifact_ref-shaped attributes, so this module consumes
duck-typed objects or dictionaries and returns serializable results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Literal, Protocol

from okto_pulse.core.kg.cognitive_policy import (
    FORBIDDEN_DETERMINISTIC_EDGE_REASON,
    LEARNING_RELATES_TO_TARGETS,
)
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL


CONNECTIVITY_ERROR_CODE = "kg_node_connectivity_violation"

# S-KG-01 / BR-KG-02 — fail-closed reason for a cognitive artifact
# (Learning/Alternative/Assumption) that resolves its source but carries no
# allowed cognitive-taxonomy relation (validates -> canonical Bug, or relates_to
# -> one of the seven canonical endpoints). Bounded vocabulary so it rides the
# safe metric labels and API payloads alongside the existing reasons. The sibling
# reason ``forbidden_deterministic_edge`` is owned by cognitive_policy (a cognitive
# writer must never emit belongs_to); it is re-exported here so the closed
# vocabulary lives in one import for callers of the guard.
MISSING_COGNITIVE_PROVENANCE_REASON = "missing_cognitive_provenance"

# R7 (canonical Learning layer integrity) — reason codes for the layer-aware
# bug_learning completeness check. Closed vocabulary; the guard is the single
# classifier and the async/worker layer persists the go-forward hold.
CANONICAL_LEARNING_WORKING_ONLY_REASON = (
    "canonical_learning_working_only_bug_evidence_pending"
)
CANONICAL_LEARNING_MIXED_DEFERRED_REASON = (
    "canonical_learning_mixed_working_edge_deferred"
)
CANONICAL_LEARNING_PROVENANCE_ONLY_REASON = (
    "canonical_learning_provenance_only_observed"
)

# A semantic edge whose resolved endpoint is the guarded node itself never
# provides connectivity — a node cannot justify its own existence. Bounded
# reason (no new enum) so API payloads and metric labels stay closed-vocabulary.
SELF_LOOP_NOT_CONNECTIVITY_REASON = "self_loop_not_connectivity"

SAFE_CONNECTIVITY_METRIC_LABELS: tuple[str, ...] = (
    "board_id",
    "node_type",
    "writer_path",
    "outcome",
    "reason",
    "source_resolution_status",
    "generation_id",
)

DEGRADED_KG_STATES: frozenset[str] = frozenset(
    {"recovery_needed", "quarantined"}
)


class SourceResolutionStatus(str, Enum):
    RESOLVED_IN_BATCH = "resolved_in_batch"
    RESOLVED_EXISTING_NODE = "resolved_existing_node"
    UNRESOLVED_SOURCE_REF = "unresolved_source_ref"
    AMBIGUOUS_SOURCE_REF = "ambiguous_source_ref"
    SOURCE_TYPE_NOT_SUPPORTED = "source_type_not_supported"
    DEFERRED_DEGRADED_GRAPH = "deferred_degraded_graph"
    ALLOWLISTED_TECHNICAL_ROOT = "allowlisted_technical_root"


class KGConnectivityOutcome(str, Enum):
    PASSED = "passed"
    REJECTED = "rejected"
    DEFERRED = "deferred"
    ALLOWLISTED = "allowlisted"


class KGConnectivityOwner(str, Enum):
    DETERMINISTIC = "deterministic"
    COGNITIVE = "cognitive"
    DUAL = "dual"


class WriterClass(str, Enum):
    DETERMINISTIC = "deterministic"
    COGNITIVE = "cognitive"
    UNKNOWN = "unknown"


class MetricSinkProtocol(Protocol):
    def emit(self, event: "KGConnectivityMetricEvent") -> None:
        """Record one safe connectivity metric event."""


@dataclass(frozen=True)
class KGConnectivityMetricEvent:
    board_id: str
    node_type: str
    writer_path: str
    outcome: str
    reason: str
    source_resolution_status: str
    generation_id: str

    def labels(self) -> dict[str, str]:
        return {
            "board_id": self.board_id,
            "node_type": self.node_type,
            "writer_path": self.writer_path,
            "outcome": self.outcome,
            "reason": self.reason,
            "source_resolution_status": self.source_resolution_status,
            "generation_id": self.generation_id,
        }


@dataclass
class InMemoryConnectivityMetricSink:
    events: list[KGConnectivityMetricEvent] = field(default_factory=list)

    def emit(self, event: KGConnectivityMetricEvent) -> None:
        self.events.append(event)


@dataclass(frozen=True)
class KGConnectivityEdgeRequirement:
    edge_type: str
    direction: Literal["outgoing", "incoming", "any"] = "outgoing"
    target_node_types: tuple[str, ...] = ()
    description: str = ""
    # R7: when set, the resolved endpoint must carry this graph_layer to count
    # toward completeness. Missing/NULL/other layer is fail-closed (does NOT
    # satisfy the requirement) — consistent with cypher_templates.layer_filter.
    required_target_layer: str | None = None

    def label(self) -> str:
        target = "|".join(self.target_node_types) if self.target_node_types else "*"
        base = f"{self.edge_type}:{self.direction}:{target}"
        if self.required_target_layer:
            base = f"{base}:layer={self.required_target_layer}"
        return base


@dataclass(frozen=True)
class KGConnectivityEdgeGroup:
    name: str
    alternatives: tuple[KGConnectivityEdgeRequirement, ...]
    remediation_hint: str
    # S-KG-01: when set, a terminal "no satisfying edge" failure of this group
    # reports this bounded reason instead of the generic missing_required_edge /
    # unsupported_endpoint_type. Used by the cognitive_provenance group so a
    # missing/off-taxonomy cognitive relation surfaces as
    # ``missing_cognitive_provenance``. Self-loop / unresolved-endpoint failures
    # keep their own dedicated diagnostics.
    failure_reason: str | None = None

    def label(self) -> str:
        return " OR ".join(req.label() for req in self.alternatives)


@dataclass(frozen=True)
class KGTechnicalRootAllowlistEntry:
    node_type: str
    writer_path: str
    source_artifact_ref: str | None = None
    source_artifact_ref_prefix: str | None = None

    def matches(
        self,
        *,
        node_type: str,
        writer_path: str,
        source_artifact_ref: str | None,
    ) -> bool:
        if self.node_type != node_type or self.writer_path != writer_path:
            return False
        if self.source_artifact_ref_prefix is not None:
            return (source_artifact_ref or "").startswith(
                self.source_artifact_ref_prefix
            )
        if self.source_artifact_ref is None:
            return True
        return self.source_artifact_ref == (source_artifact_ref or "")


@dataclass(frozen=True)
class KGConnectivityRule:
    node_type: str
    owner: KGConnectivityOwner
    required_edge_groups: tuple[KGConnectivityEdgeGroup, ...]
    semantic_target_policy: str
    allowed_new_node_writers: tuple[WriterClass, ...]
    technical_root_allowed: bool = False


@dataclass(frozen=True)
class KGNodeRef:
    ref_id: str
    node_type: str
    source_artifact_ref: str | None = None
    # R7: endpoint layer metadata so the guard can enforce canonical
    # completeness without re-querying the graph. None = unknown (fail-closed).
    graph_layer: str | None = None
    maturity_status: str | None = None


@dataclass(frozen=True)
class _NodeSnapshot:
    candidate_id: str
    node_type: str
    source_artifact_ref: str | None
    raw: Any
    graph_layer: str | None = None
    maturity_status: str | None = None


@dataclass(frozen=True)
class _EdgeSnapshot:
    candidate_id: str
    edge_type: str
    from_candidate_id: str
    to_candidate_id: str
    raw: Any


@dataclass(frozen=True)
class _RequirementResolution:
    passed: bool
    status: SourceResolutionStatus
    missing_endpoint: str
    reason: str
    # R7: bounded endpoint descriptors ("<ref>@<layer>") observed at a
    # non-canonical layer. On a failure this is the working-only evidence; on a
    # pass it is the deferred working edge(s) of a mixed-evidence candidate.
    observed_endpoints: tuple[str, ...] = ()


@dataclass(frozen=True)
class KGConnectivityViolation:
    node_type: str
    candidate_id: str
    source_artifact_ref: str | None
    required_edge: str
    missing_endpoint: str
    writer_path: str
    source_resolution_status: SourceResolutionStatus
    remediation_hint: str
    reason: str
    error_code: str = CONNECTIVITY_ERROR_CODE
    # R7: bounded ("<ref>@<layer>") descriptors of the non-canonical endpoints
    # that were observed for this violation (e.g. validates -> working Bug).
    observed_endpoints: tuple[str, ...] = ()

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "node_type": self.node_type,
            "candidate_id": self.candidate_id,
            "source_artifact_ref": self.source_artifact_ref,
            "required_edge": self.required_edge,
            "missing_endpoint": self.missing_endpoint,
            "writer_path": self.writer_path,
            "source_resolution_status": self.source_resolution_status.value,
            "remediation_hint": self.remediation_hint,
            # Alias (spec f24c43f7 / card 4836a25b): expose the actionable remediation
            # under the contract-stable key `remediation` too, keeping `remediation_hint`
            # for backward compatibility.
            "remediation": self.remediation_hint,
            "reason": self.reason,
            "observed_endpoints": list(self.observed_endpoints),
        }

    def safe_for_api(self) -> bool:
        return True

    def safe_for_metric(self) -> bool:
        return True


@dataclass(frozen=True)
class KGConnectivityValidationResult:
    passed: bool
    violations: tuple[KGConnectivityViolation, ...] = ()
    allowlisted_roots: tuple[str, ...] = ()
    materializable_edges: tuple[dict[str, str], ...] = ()
    outcome: KGConnectivityOutcome = KGConnectivityOutcome.PASSED
    # R7: non-blocking notes. Mixed-evidence canonical Learning that was
    # accepted because it has >=1 canonical Bug but also carries working Bug
    # edges surfaces them here (reason canonical_learning_mixed_working_edge_deferred)
    # so the working edges are visibly deferred and never counted as completeness.
    advisories: tuple[dict[str, Any], ...] = ()

    @property
    def rejected_nodes(self) -> int:
        return len(self.violations)

    @property
    def connected_nodes(self) -> int:
        if not self.passed:
            return 0
        return 0 if self.outcome == KGConnectivityOutcome.DEFERRED else 1

    def to_response(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "violations": [v.to_safe_dict() for v in self.violations],
            "allowlisted_roots": list(self.allowlisted_roots),
            "materializable_edges": list(self.materializable_edges),
            "outcome": self.outcome.value,
            "advisories": [dict(a) for a in self.advisories],
        }


def classify_writer_path(writer_path: str) -> WriterClass:
    normalized = (writer_path or "").strip().lower()
    # Historical consolidation is the deterministic Layer-1 producer used by
    # the queue worker.  Keep this exact compatibility mapping ahead of the
    # generic ``consolidation``/``agent`` cognitive markers: persisted graph
    # nodes retain this producer id, so renaming only the current worker would
    # leave existing deterministic material classified as cognitive forever.
    if normalized == "system:historical_consolidation":
        return WriterClass.DETERMINISTIC
    if any(
        marker in normalized
        for marker in (
            "deterministic",
            "worker_layer1",
            "rebuild",
            "discovery",
            "bootstrap",
            "schema",
        )
    ):
        return WriterClass.DETERMINISTIC
    if any(
        marker in normalized
        for marker in (
            "cognitive",
            "commit_consolidation",
            "consolidation",
            "agent",
            "mcp",
        )
    ):
        return WriterClass.COGNITIVE
    return WriterClass.UNKNOWN


def _provenance_group() -> KGConnectivityEdgeGroup:
    return KGConnectivityEdgeGroup(
        name="provenance",
        alternatives=(
            KGConnectivityEdgeRequirement(
                "belongs_to",
                "outgoing",
                ("Entity", "Bug"),
                "Artifact node must be navigable to its source/root entity.",
            ),
        ),
        remediation_hint=(
            "Attach a belongs_to edge to the source/root Entity or use an "
            "explicit technical-root allowlist entry."
        ),
    )


def _decision_judgement_group() -> KGConnectivityEdgeGroup:
    return KGConnectivityEdgeGroup(
        name="decision_judgement",
        alternatives=(
            KGConnectivityEdgeRequirement("supersedes", "outgoing", ("Decision",)),
            KGConnectivityEdgeRequirement("contradicts", "outgoing", ("Decision",)),
            KGConnectivityEdgeRequirement("depends_on", "outgoing", ("Decision",)),
            KGConnectivityEdgeRequirement("relates_to", "outgoing", ("Alternative",)),
            KGConnectivityEdgeRequirement("derives_from", "outgoing", ("Requirement",)),
            KGConnectivityEdgeRequirement("mentions", "outgoing", ("Entity",)),
        ),
        remediation_hint=(
            "Provide a schema-valid judgement/provenance edge from the Decision "
            "to a structured endpoint."
        ),
    )


def _learning_bug_group() -> KGConnectivityEdgeGroup:
    return KGConnectivityEdgeGroup(
        name="bug_learning",
        alternatives=(
            # R7: a bug-derived canonical Learning only counts a validates edge
            # toward completeness when it points to a *canonical* Bug. A working
            # Bug endpoint is fail-closed (working-only hold, never satisfies).
            KGConnectivityEdgeRequirement(
                "validates",
                "outgoing",
                ("Bug",),
                required_target_layer=GRAPH_LAYER_CANONICAL,
            ),
        ),
        remediation_hint=(
            "Provide Learning -> validates -> canonical Bug when the learning is "
            "derived from a known bug; a working-layer Bug holds the learning as "
            "cognitive pending until its source bug matures to canonical."
        ),
    )


def _cognitive_provenance_group() -> KGConnectivityEdgeGroup:
    """S-KG-01 / BR-KG-02 — cognitive provenance connectivity.

    Learning/Alternative/Assumption are cognitive artifacts: their provenance is
    the resolved ``source_artifact_ref`` plus a cognitive-taxonomy relation, NOT
    the deterministic ``belongs_to``-to-Entity backbone the operational artifacts
    (Requirement/Constraint/APIContract/TestScenario/Criterion/Bug/Entity) use.
    This is the group for a cognitive node that is NOT bug-derived; a bug-derived
    Learning is routed to ``_learning_bug_group`` (validates -> canonical Bug)
    earlier in :meth:`KGNodeConnectivityGuard.validate`. A non-bug-derived
    cognitive node is connected when it carries ANY of:

    * ``belongs_to`` -> Entity/Bug — the deterministic root the commit path
      auto-attaches when the source ref resolves to a root entity. Kept as an
      accepted alternative so the established provenance path never regresses
      (it carries no layer constraint, so any-layer roots still satisfy);
    * ``relates_to`` -> one of the seven canonical endpoints
      (Entity|Decision|Requirement|Constraint|TestScenario|APIContract|Criterion)
      — the additive ``relates_to`` endpoint pairs (no new edge name).

    ``validates`` is deliberately NOT an alternative here: it is the exclusive
    proof for a *bug-derived* Learning (handled by ``_learning_bug_group``), so a
    node the resolver could not confirm as bug-derived must not pass by pointing a
    ``validates`` edge at some canonical Bug (RKG-02 anti-aliasing invariant).

    This replaces requiring ``belongs_to`` for cognitive nodes whose source ref
    is e.g. ``spec:``/``decision:`` (which never resolves to a root Entity/Bug),
    closing the unresolved_source_ref gap while keeping operational artifacts on
    the strict deterministic provenance group.
    """
    return KGConnectivityEdgeGroup(
        name="cognitive_provenance",
        alternatives=(
            KGConnectivityEdgeRequirement(
                "belongs_to",
                "outgoing",
                ("Entity", "Bug"),
                "Deterministic root auto-attached when the source ref resolves.",
            ),
            KGConnectivityEdgeRequirement(
                "relates_to",
                "outgoing",
                tuple(LEARNING_RELATES_TO_TARGETS),
                "Cognitive relation to a canonical taxonomy endpoint.",
                required_target_layer=GRAPH_LAYER_CANONICAL,
            ),
        ),
        remediation_hint=(
            "Attach a cognitive-taxonomy relation: relates_to -> a canonical "
            "Entity/Decision/Requirement/Constraint/TestScenario/APIContract/"
            "Criterion (or validates -> canonical Bug when the learning is "
            "bug-derived). belongs_to to a root Entity/Bug is auto-attached by "
            "the commit path when the source_artifact_ref resolves; cognitive "
            "writers must never emit belongs_to themselves."
        ),
        failure_reason=MISSING_COGNITIVE_PROVENANCE_REASON,
    )


class KGConnectivityRuleRegistry:
    """Registry of minimal connectivity and owner rules by node type."""

    def __init__(
        self,
        *,
        technical_root_allowlist: Iterable[KGTechnicalRootAllowlistEntry] | None = None,
    ) -> None:
        self._rules = self._build_default_rules()
        self._technical_root_allowlist = tuple(
            technical_root_allowlist
            if technical_root_allowlist is not None
            else (
                KGTechnicalRootAllowlistEntry(
                    node_type="Entity",
                    writer_path="deterministic_worker",
                    source_artifact_ref="tech_entities.yml",
                ),
                KGTechnicalRootAllowlistEntry(
                    node_type="Entity",
                    writer_path="deterministic_worker",
                    source_artifact_ref_prefix="board:",
                ),
                KGTechnicalRootAllowlistEntry(
                    node_type="Decision",
                    writer_path="commit_consolidation",
                    source_artifact_ref_prefix="final_report:",
                ),
                KGTechnicalRootAllowlistEntry(
                    node_type="Learning",
                    writer_path="commit_consolidation",
                    source_artifact_ref_prefix="final_report:",
                ),
                KGTechnicalRootAllowlistEntry(
                    node_type="Alternative",
                    writer_path="commit_consolidation",
                    source_artifact_ref_prefix="final_report:",
                ),
                KGTechnicalRootAllowlistEntry(
                    node_type="Assumption",
                    writer_path="commit_consolidation",
                    source_artifact_ref_prefix="final_report:",
                ),
            )
        )

    @staticmethod
    def _build_default_rules() -> dict[str, KGConnectivityRule]:
        provenance = _provenance_group()
        # S-KG-01: cognitive artifacts (Learning/Alternative/Assumption) prove
        # connectivity through the cognitive taxonomy, not the deterministic
        # belongs_to backbone. Operational artifacts stay on `provenance`.
        cognitive_provenance = _cognitive_provenance_group()
        deterministic_only = (WriterClass.DETERMINISTIC,)
        cognitive_only = (WriterClass.COGNITIVE,)
        cognitive_or_deterministic = (
            WriterClass.COGNITIVE,
            WriterClass.DETERMINISTIC,
        )
        return {
            "Entity": KGConnectivityRule(
                node_type="Entity",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="structured_provenance_only",
                allowed_new_node_writers=deterministic_only,
                technical_root_allowed=True,
            ),
            "Requirement": KGConnectivityRule(
                node_type="Requirement",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="deterministic_spec_source",
                allowed_new_node_writers=deterministic_only,
            ),
            "Criterion": KGConnectivityRule(
                node_type="Criterion",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="deterministic_spec_source",
                allowed_new_node_writers=deterministic_only,
            ),
            "APIContract": KGConnectivityRule(
                node_type="APIContract",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="deterministic_spec_source",
                allowed_new_node_writers=deterministic_only,
            ),
            "TestScenario": KGConnectivityRule(
                node_type="TestScenario",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="deterministic_spec_source",
                allowed_new_node_writers=deterministic_only,
            ),
            "Constraint": KGConnectivityRule(
                node_type="Constraint",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="deterministic_spec_source",
                allowed_new_node_writers=deterministic_only,
            ),
            "Bug": KGConnectivityRule(
                node_type="Bug",
                owner=KGConnectivityOwner.DETERMINISTIC,
                required_edge_groups=(provenance,),
                semantic_target_policy="bug_workflow_source",
                allowed_new_node_writers=deterministic_only,
            ),
            "Decision": KGConnectivityRule(
                node_type="Decision",
                owner=KGConnectivityOwner.DUAL,
                required_edge_groups=(provenance, _decision_judgement_group()),
                semantic_target_policy="structured_provenance_and_judgement",
                allowed_new_node_writers=cognitive_or_deterministic,
                technical_root_allowed=True,
            ),
            "Learning": KGConnectivityRule(
                node_type="Learning",
                owner=KGConnectivityOwner.COGNITIVE,
                required_edge_groups=(cognitive_provenance,),
                semantic_target_policy="bug_learning_requires_validates_when_known",
                allowed_new_node_writers=cognitive_or_deterministic,
                technical_root_allowed=True,
            ),
            "Alternative": KGConnectivityRule(
                node_type="Alternative",
                owner=KGConnectivityOwner.COGNITIVE,
                required_edge_groups=(cognitive_provenance,),
                semantic_target_policy="cognitive_per_concept_source",
                allowed_new_node_writers=cognitive_only,
                technical_root_allowed=True,
            ),
            "Assumption": KGConnectivityRule(
                node_type="Assumption",
                owner=KGConnectivityOwner.COGNITIVE,
                required_edge_groups=(cognitive_provenance,),
                semantic_target_policy="provenance_only_v1",
                allowed_new_node_writers=cognitive_only,
                technical_root_allowed=True,
            ),
        }

    def get_rule(self, node_type: str, writer_path: str = "") -> KGConnectivityRule:
        try:
            return self._rules[node_type]
        except KeyError as exc:
            raise KeyError(f"unknown KG node_type for connectivity guard: {node_type}") from exc

    def rule_node_types(self) -> tuple[str, ...]:
        """Return the canonical node types governed by this registry."""

        return tuple(self._rules)

    def is_technical_root_allowlisted(
        self,
        *,
        node_type: str,
        writer_path: str,
        source_artifact_ref: str | None,
    ) -> bool:
        return any(
            entry.matches(
                node_type=node_type,
                writer_path=writer_path,
                source_artifact_ref=source_artifact_ref,
            )
            for entry in self._technical_root_allowlist
        )


class KGNodeConnectivityGuard:
    def __init__(
        self,
        registry: KGConnectivityRuleRegistry | None = None,
        *,
        metric_sink: MetricSinkProtocol | None = None,
    ) -> None:
        self._registry = registry or KGConnectivityRuleRegistry()
        self._metric_sink = metric_sink

    def validate(
        self,
        *,
        board_id: str,
        writer_path: str,
        kg_health_state: str,
        nodes: Iterable[Any],
        edges: Iterable[Any],
        existing_node_refs: Iterable[Any] = (),
        generation_id: str = "",
    ) -> KGConnectivityValidationResult:
        node_snapshots = tuple(_snapshot_node(node) for node in nodes)
        edge_snapshots = tuple(_snapshot_edge(edge) for edge in edges)
        existing_refs = tuple(_snapshot_existing_ref(ref) for ref in existing_node_refs)

        if kg_health_state in DEGRADED_KG_STATES:
            violations = tuple(
                _violation(
                    node=node,
                    writer_path=writer_path,
                    required_edge="deferred_until_graph_recovers",
                    missing_endpoint="kg_health_state",
                    status=SourceResolutionStatus.DEFERRED_DEGRADED_GRAPH,
                    remediation_hint=(
                        "Do not commit candidates while the graph is recovery_needed "
                        "or quarantined; leave them pending and retry after recovery."
                    ),
                    reason="deferred_degraded_graph",
                )
                for node in node_snapshots
            )
            result = KGConnectivityValidationResult(
                passed=False,
                violations=violations,
                outcome=KGConnectivityOutcome.DEFERRED,
            )
            self._emit_metric(board_id, writer_path, generation_id, result)
            return result

        violations: list[KGConnectivityViolation] = []
        allowlisted_roots: list[str] = []
        advisories: list[dict[str, Any]] = []
        writer_class = classify_writer_path(writer_path)
        # RKG-02: the shared resolver decides if a Learning is bug-derived; a
        # plain card:<uuid> only counts when the canonical Bug probe confirms it.
        bug_probe = _build_canonical_bug_probe(existing_refs, node_snapshots)
        for node in node_snapshots:
            try:
                rule = self._registry.get_rule(node.node_type, writer_path)
            except KeyError:
                violations.append(
                    _violation(
                        node=node,
                        writer_path=writer_path,
                        required_edge="known_node_type",
                        missing_endpoint=node.node_type,
                        status=SourceResolutionStatus.SOURCE_TYPE_NOT_SUPPORTED,
                        remediation_hint=(
                            "Register an explicit connectivity rule before "
                            "writing this node type."
                        ),
                        reason="unknown_node_type",
                    )
                )
                continue

            if (
                rule.technical_root_allowed
                and self._registry.is_technical_root_allowlisted(
                    node_type=node.node_type,
                    writer_path=writer_path,
                    source_artifact_ref=node.source_artifact_ref,
                )
            ):
                allowlisted_roots.append(node.candidate_id)
                continue

            if writer_class not in rule.allowed_new_node_writers:
                violations.append(
                    _violation(
                        node=node,
                        writer_path=writer_path,
                        required_edge="connectivity_owner",
                        missing_endpoint=writer_class.value,
                        status=SourceResolutionStatus.SOURCE_TYPE_NOT_SUPPORTED,
                        remediation_hint=(
                            "This node_type is owned by a different writer path "
                            "(not permitted for this writer). Remediate by one of: "
                            "(1) remove the invalid candidate from the session; "
                            "(2) abort and recreate the session without it; "
                            "(3) route node creation through the connectivity owner "
                            "for this node_type, or reference an existing node."
                        ),
                        reason="writer_not_connectivity_owner",
                    )
                )
                continue

            if node.node_type == "Learning" and _learning_is_bug_derived(node.raw, bug_probe):
                required_groups = [_learning_bug_group()]
            else:
                required_groups = list(rule.required_edge_groups)

            for group in required_groups:
                resolution = self._resolve_group(
                    node=node,
                    group=group,
                    nodes=node_snapshots,
                    edges=edge_snapshots,
                    existing_refs=existing_refs,
                )
                if not resolution.passed:
                    violations.append(
                        _violation(
                            node=node,
                            writer_path=writer_path,
                            required_edge=group.label(),
                            missing_endpoint=resolution.missing_endpoint,
                            status=resolution.status,
                            remediation_hint=group.remediation_hint,
                            reason=resolution.reason,
                            observed_endpoints=resolution.observed_endpoints,
                        )
                    )
                    break
                if resolution.observed_endpoints:
                    # R7 mixed evidence: the group passed on a canonical endpoint
                    # but also carried working-layer endpoints — record them as
                    # deferred (never counted as completeness).
                    advisories.append(
                        {
                            "reason": CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
                            "node_type": node.node_type,
                            "candidate_id": node.candidate_id,
                            "source_artifact_ref": node.source_artifact_ref,
                            "deferred_endpoints": list(resolution.observed_endpoints),
                        }
                    )

        result = KGConnectivityValidationResult(
            passed=not violations,
            violations=tuple(violations),
            allowlisted_roots=tuple(allowlisted_roots),
            advisories=tuple(advisories),
            outcome=(
                KGConnectivityOutcome.ALLOWLISTED
                if allowlisted_roots and len(allowlisted_roots) == len(node_snapshots)
                else KGConnectivityOutcome.PASSED if not violations
                else KGConnectivityOutcome.REJECTED
            ),
        )
        self._emit_metric(board_id, writer_path, generation_id, result)
        return result

    def _resolve_group(
        self,
        *,
        node: _NodeSnapshot,
        group: KGConnectivityEdgeGroup,
        nodes: tuple[_NodeSnapshot, ...],
        edges: tuple[_EdgeSnapshot, ...],
        existing_refs: tuple[KGNodeRef, ...],
    ) -> _RequirementResolution:
        node_types_by_candidate = {item.candidate_id: item.node_type for item in nodes}
        node_layers_by_candidate = {
            item.candidate_id: item.graph_layer for item in nodes
        }
        existing_by_ref = _existing_ref_index(existing_refs)
        touched_unresolved = False
        touched_unsupported = False
        layer_required = any(req.required_target_layer for req in group.alternatives)
        # R7: classify across ALL edges before deciding so a mixed-evidence
        # candidate (>=1 canonical Bug + some working Bugs) is accepted while the
        # working endpoints are reported as deferred, and a working-only
        # candidate is held instead of silently satisfying completeness.
        satisfying_status: SourceResolutionStatus | None = None
        working_observed: list[str] = []
        # A self-loop edge matched a requirement but resolved to the node itself;
        # tracked so a self-loop-only candidate is rejected with a dedicated
        # bounded reason instead of the generic missing_required_edge.
        self_loop_matched = False
        for edge in edges:
            if edge.from_candidate_id == node.candidate_id:
                direction = "outgoing"
                other_ref = edge.to_candidate_id
            elif edge.to_candidate_id == node.candidate_id:
                direction = "incoming"
                other_ref = edge.from_candidate_id
            else:
                continue

            # A self-loop (resolved endpoint is the candidate node itself) can
            # never provide semantic connectivity for a guarded node.
            is_self_loop = other_ref == node.candidate_id or (
                bool(node.source_artifact_ref)
                and other_ref == node.source_artifact_ref
            )

            for req in group.alternatives:
                if req.edge_type != edge.edge_type:
                    continue
                if req.direction != "any" and req.direction != direction:
                    continue
                if is_self_loop:
                    # The edge matches this requirement's shape but its endpoint
                    # is the node itself; a self-loop can never establish
                    # connectivity, so record it and never resolve or count it.
                    self_loop_matched = True
                    continue
                endpoint_type, endpoint_layer, status = _resolve_endpoint_with_layer(
                    other_ref,
                    node_types_by_candidate,
                    node_layers_by_candidate,
                    existing_by_ref,
                )
                if status in (
                    SourceResolutionStatus.UNRESOLVED_SOURCE_REF,
                    SourceResolutionStatus.AMBIGUOUS_SOURCE_REF,
                ):
                    touched_unresolved = True
                    continue
                if req.target_node_types and endpoint_type not in req.target_node_types:
                    touched_unsupported = True
                    continue
                if (
                    req.required_target_layer is None
                    or endpoint_layer == req.required_target_layer
                ):
                    # Legacy (no layer constraint) or canonical-satisfying
                    # endpoint. Keep scanning so deferred working edges of a
                    # mixed-evidence candidate are still collected.
                    if satisfying_status is None:
                        satisfying_status = status
                else:
                    # R7: endpoint type matches but layer is non-canonical
                    # (working / NULL) — fail-closed; never counts toward
                    # canonical completeness.
                    working_observed.append(
                        f"{other_ref}@{endpoint_layer or 'unknown'}"
                    )

        if satisfying_status is not None:
            return _RequirementResolution(
                passed=True,
                status=satisfying_status,
                missing_endpoint="",
                reason="ok",
                observed_endpoints=tuple(working_observed),
            )
        if layer_required and working_observed:
            return _RequirementResolution(
                passed=False,
                status=SourceResolutionStatus.SOURCE_TYPE_NOT_SUPPORTED,
                missing_endpoint=group.label(),
                reason=CANONICAL_LEARNING_WORKING_ONLY_REASON,
                observed_endpoints=tuple(working_observed),
            )
        if self_loop_matched:
            # Only self-loop edge(s) would have satisfied the group; a node
            # cannot establish its own connectivity, so fail closed.
            return _RequirementResolution(
                passed=False,
                status=SourceResolutionStatus.UNRESOLVED_SOURCE_REF,
                missing_endpoint=group.label(),
                reason=SELF_LOOP_NOT_CONNECTIVITY_REASON,
            )
        if touched_unresolved:
            return _RequirementResolution(
                passed=False,
                status=SourceResolutionStatus.UNRESOLVED_SOURCE_REF,
                missing_endpoint=group.label(),
                reason="unresolved_required_endpoint",
            )
        if touched_unsupported:
            # S-KG-01: the cognitive_provenance group reports an off-taxonomy
            # relation (e.g. relates_to -> a non-taxonomy type) as the bounded
            # missing_cognitive_provenance reason; other groups keep the generic
            # unsupported_endpoint_type diagnostic.
            return _RequirementResolution(
                passed=False,
                status=SourceResolutionStatus.SOURCE_TYPE_NOT_SUPPORTED,
                missing_endpoint=group.label(),
                reason=group.failure_reason or "unsupported_endpoint_type",
            )
        return _RequirementResolution(
            passed=False,
            status=SourceResolutionStatus.UNRESOLVED_SOURCE_REF,
            missing_endpoint=group.label(),
            reason=group.failure_reason or "missing_required_edge",
        )

    def _emit_metric(
        self,
        board_id: str,
        writer_path: str,
        generation_id: str,
        result: KGConnectivityValidationResult,
    ) -> None:
        if self._metric_sink is None:
            return
        first = result.violations[0] if result.violations else None
        try:
            self._metric_sink.emit(
                KGConnectivityMetricEvent(
                    board_id=board_id,
                    node_type=first.node_type if first else "batch",
                    writer_path=writer_path,
                    outcome=result.outcome.value,
                    reason=first.reason if first else "ok",
                    source_resolution_status=(
                        first.source_resolution_status.value
                        if first
                        else (
                            SourceResolutionStatus.ALLOWLISTED_TECHNICAL_ROOT.value
                            if result.outcome == KGConnectivityOutcome.ALLOWLISTED
                            else SourceResolutionStatus.RESOLVED_IN_BATCH.value
                        )
                    ),
                    generation_id=generation_id,
                )
            )
        except Exception:
            return


def _violation(
    *,
    node: _NodeSnapshot,
    writer_path: str,
    required_edge: str,
    missing_endpoint: str,
    status: SourceResolutionStatus,
    remediation_hint: str,
    reason: str,
    observed_endpoints: tuple[str, ...] = (),
) -> KGConnectivityViolation:
    return KGConnectivityViolation(
        node_type=node.node_type,
        candidate_id=node.candidate_id,
        source_artifact_ref=node.source_artifact_ref,
        required_edge=required_edge,
        missing_endpoint=missing_endpoint,
        writer_path=writer_path,
        source_resolution_status=status,
        remediation_hint=remediation_hint,
        reason=reason,
        observed_endpoints=observed_endpoints,
    )


def _snapshot_node(node: Any) -> _NodeSnapshot:
    return _NodeSnapshot(
        candidate_id=str(_get_field(node, "candidate_id", "")),
        node_type=_enum_value(_get_field(node, "node_type", "")),
        source_artifact_ref=_optional_str(_get_field(node, "source_artifact_ref", None)),
        raw=node,
        graph_layer=_layer_value(_get_field(node, "graph_layer", None)),
        maturity_status=_layer_value(_get_field(node, "maturity_status", None)),
    )


def _snapshot_edge(edge: Any) -> _EdgeSnapshot:
    return _EdgeSnapshot(
        candidate_id=str(_get_field(edge, "candidate_id", "")),
        edge_type=_enum_value(_get_field(edge, "edge_type", "")),
        from_candidate_id=str(_get_field(edge, "from_candidate_id", "")),
        to_candidate_id=str(_get_field(edge, "to_candidate_id", "")),
        raw=edge,
    )


def _snapshot_existing_ref(ref: Any) -> KGNodeRef:
    if isinstance(ref, str):
        return KGNodeRef(ref_id=ref, node_type="", source_artifact_ref=None)
    node_id = (
        _get_field(ref, "ref_id", None)
        or _get_field(ref, "node_id", None)
        or _get_field(ref, "graph_node_id", None)
        or _get_field(ref, "graph_node_id", None)
        or _get_field(ref, "id", "")
    )
    return KGNodeRef(
        ref_id=str(node_id),
        node_type=_enum_value(_get_field(ref, "node_type", "")),
        source_artifact_ref=_optional_str(_get_field(ref, "source_artifact_ref", None)),
        graph_layer=_layer_value(_get_field(ref, "graph_layer", None)),
        maturity_status=_layer_value(_get_field(ref, "maturity_status", None)),
    )


def _existing_ref_index(existing_refs: tuple[KGNodeRef, ...]) -> dict[str, list[KGNodeRef]]:
    index: dict[str, list[KGNodeRef]] = {}
    for ref in existing_refs:
        keys = {ref.ref_id}
        if ref.ref_id and not ref.ref_id.startswith("kg:"):
            keys.add(f"kg:{ref.ref_id}")
        if ref.source_artifact_ref:
            keys.add(ref.source_artifact_ref)
        for key in keys:
            if key:
                bucket = index.setdefault(key, [])
                canonical_ref_id = _canonical_ref_id(ref.ref_id)
                if not any(
                    _canonical_ref_id(existing.ref_id) == canonical_ref_id
                    and existing.node_type == ref.node_type
                    and existing.source_artifact_ref == ref.source_artifact_ref
                    for existing in bucket
                ):
                    bucket.append(ref)
    return index


def _canonical_ref_id(ref_id: str) -> str:
    return ref_id[3:] if ref_id.startswith("kg:") else ref_id


def _resolve_endpoint_with_layer(
    ref: str,
    node_types_by_candidate: dict[str, str],
    node_layers_by_candidate: dict[str, str | None],
    existing_by_ref: dict[str, list[KGNodeRef]],
) -> tuple[str | None, str | None, SourceResolutionStatus]:
    """Resolve an edge endpoint to its (node_type, graph_layer, status).

    graph_layer is None when unknown (in-batch candidate without a layer, or an
    existing ref whose layer could not be resolved) — the R7 layer check treats
    that as fail-closed (non-canonical).
    """
    if ref in node_types_by_candidate:
        return (
            node_types_by_candidate[ref],
            node_layers_by_candidate.get(ref),
            SourceResolutionStatus.RESOLVED_IN_BATCH,
        )
    matches = existing_by_ref.get(ref, ())
    if len(matches) == 1:
        return (
            matches[0].node_type,
            matches[0].graph_layer,
            SourceResolutionStatus.RESOLVED_EXISTING_NODE,
        )
    if len(matches) > 1:
        return None, None, SourceResolutionStatus.AMBIGUOUS_SOURCE_REF
    return None, None, SourceResolutionStatus.UNRESOLVED_SOURCE_REF


def _candidate_has_known_bug(raw: Any) -> bool:
    for field_name in (
        "bug_id",
        "bug_ref",
        "known_bug_ref",
        "known_bug_source_ref",
        "target_bug_ref",
    ):
        if _get_field(raw, field_name, None):
            return True
    source_ref = _optional_str(_get_field(raw, "source_artifact_ref", None)) or ""
    return source_ref.startswith("bug:") or source_ref.startswith("card:bug:")


def _build_canonical_bug_probe(
    existing_refs: tuple[KGNodeRef, ...],
    node_snapshots: tuple[Any, ...],
) -> Any:
    """A type-aware probe (RKG-02 / FR2) telling whether a card uuid is backed by
    a *canonical* Bug. Built from the canonical Bug nodes already in the graph
    (and any canonical Bug created in the same batch), keyed by normalized
    identity so ``card:<uuid>``/``bug:<uuid>`` reconcile. Fail-closed: only
    canonical-layer Bugs count."""
    from okto_pulse.core.kg.cognitive_source_ref_resolver import strip_concept_suffix
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    canonical_bug_keys: set[str] = set()

    def _add(ref_id: Any, source_ref: Any, layer: Any) -> None:
        if _layer_value(layer) != GRAPH_LAYER_CANONICAL:
            return
        cid = _canonical_ref_id(str(ref_id or ""))
        if cid:
            canonical_bug_keys.add(normalize_cognitive_artifact_id(f"card:{cid}"))
        sref = _optional_str(source_ref)
        if sref:
            canonical_bug_keys.add(
                normalize_cognitive_artifact_id(strip_concept_suffix(sref)))

    for ref in existing_refs:
        if ref.node_type == "Bug":
            _add(ref.ref_id, ref.source_artifact_ref, ref.graph_layer)
    for snap in node_snapshots:
        if getattr(snap, "node_type", None) == "Bug":
            _add(
                getattr(snap, "candidate_id", None),
                getattr(snap, "source_artifact_ref", None),
                getattr(snap, "graph_layer", None),
            )

    def _probe(uuid: str) -> bool:
        return normalize_cognitive_artifact_id(f"card:{uuid}") in canonical_bug_keys

    return _probe


def _learning_is_bug_derived(raw: Any, bug_probe: Any) -> bool:
    """Type-aware bug-derived detection for a Learning node (RKG-02). Explicit
    bug fields and bug:/card:bug: forms are always bug-derived; a plain
    card:<uuid> is bug-derived ONLY when the probe confirms a canonical Bug."""
    for field_name in ("bug_id", "bug_ref", "known_bug_ref", "known_bug_source_ref", "target_bug_ref"):
        if _get_field(raw, field_name, None):
            return True
    from okto_pulse.core.kg.cognitive_source_ref_resolver import resolve_cognitive_source_ref

    source_ref = _optional_str(_get_field(raw, "source_artifact_ref", None)) or ""
    return resolve_cognitive_source_ref(source_ref, canonical_bug_probe=bug_probe).is_bug_derived


def _get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _enum_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _layer_value(value: Any) -> str | None:
    """Normalize a graph_layer/maturity_status to a plain string or None.

    Preserves None (so the guard stays fail-closed on missing layer) instead of
    coercing it to the literal string ``"None"`` the way ``_enum_value`` would.
    """
    if value is None:
        return None
    return _optional_str(_enum_value(value))


__all__ = [
    "CANONICAL_LEARNING_MIXED_DEFERRED_REASON",
    "CANONICAL_LEARNING_PROVENANCE_ONLY_REASON",
    "CANONICAL_LEARNING_WORKING_ONLY_REASON",
    "CONNECTIVITY_ERROR_CODE",
    "DEGRADED_KG_STATES",
    "FORBIDDEN_DETERMINISTIC_EDGE_REASON",
    "MISSING_COGNITIVE_PROVENANCE_REASON",
    "SAFE_CONNECTIVITY_METRIC_LABELS",
    "SELF_LOOP_NOT_CONNECTIVITY_REASON",
    "InMemoryConnectivityMetricSink",
    "KGConnectivityEdgeGroup",
    "KGConnectivityEdgeRequirement",
    "KGConnectivityMetricEvent",
    "KGConnectivityOutcome",
    "KGConnectivityOwner",
    "KGConnectivityRule",
    "KGConnectivityRuleRegistry",
    "KGConnectivityValidationResult",
    "KGConnectivityViolation",
    "KGNodeConnectivityGuard",
    "KGNodeRef",
    "KGTechnicalRootAllowlistEntry",
    "MetricSinkProtocol",
    "SourceResolutionStatus",
    "WriterClass",
    "classify_writer_path",
]
