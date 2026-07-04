"""RKG-01 — Canonical cognitive-effectiveness inventory (read-only).

Builds a read-only snapshot of cognitive-closeout effectiveness per board and
per done artifact. It cross-references KG health, technical DLQs, cognitive
readiness/consolidation items and persisted cognitive nodes to classify each
done artifact into ONE primary state, plus rolled-up totals.

Design constraints (spec RKG-01):
  * TR1 — 100% READ-ONLY. This module never reprocesses DLQs, opens a
    consolidation session, creates nodes/edges, or mutates readiness while
    generating the inventory. All graph access is ``MATCH ... RETURN`` only.
  * TR2 — ``extractor_triggered_but_not_persisted`` is the delta between
    emitted ``cognitive.extraction.*.candidate`` signals and what actually
    persisted. There is **no persisted candidate-log store yet** — that wiring
    is owned by RKG-03 (the candidate→persist consumer, deferred to a later
    wave). Until a literal source exists, the inventory derives this state by
    *eligibility inference* and labels it explicitly (``evidence_source =
    inferred_eligibility``, ``confidence = inferred``, ``candidate_log_refs =
    []``, ``inferred_candidate = True``) so a consumer can never confuse it with
    an observed log. The same contract accepts a stronger ``candidate_log``
    evidence source via :class:`CandidateLogSource` once RKG-03 ships one.
  * TR3 — the deterministic layer is kept separate: only the cognitive node
    labels (Decision/Alternative/Assumption/Learning) and judgement/provenance
    edges count toward closeout; Criterion/Constraint are never counted.
  * BR1 — an open technical DLQ invalidates effectiveness for that artifact;
    the board-level snapshot reports ``cognitively_effective = False`` whenever
    any effectiveness blocker (technical DLQ or persistence gap) is present.

Artifact correlation (validation finding, codex 2026-06-25):
  Learning is keyed to the bug (``bug:<id>`` → normalises to ``card:<id>``);
  Alternative/Assumption are keyed to the SPEC (``spec:<spec_id>`` with a
  per-concept suffix ``:alternative:<hash>`` / ``:assumption:<hash>``). The
  artifact universe therefore covers done BUG cards (Learning) and done SPECS
  (Alternative/Assumption); a non-bug card is NOT itself a cognitive artifact.
  Persisted nodes attribute to an artifact by their *base* ref (per-concept
  suffix stripped, ``bug:``/``card:bug:`` collapsed to ``card:``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.agent.extractors import LEARNING_MIN_ACTION_PLAN_CHARS
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    normalize_cognitive_artifact_id,
)
from okto_pulse.core.domain.enums import CardStatus, SpecStatus
from okto_pulse.core.models.db import (
    Card,
    ConsolidationDeadLetter,
    Spec,
)

logger = logging.getLogger("okto_pulse.core.services.cognitive_effectiveness")

VALID_GRAPH_LAYERS: tuple[str, ...] = ("canonical", "working", "all")

# TR3: ONLY these node labels are cognitive closeout. Criterion/Constraint are
# deterministic materialization and must never be counted here.
COGNITIVE_NODE_LABELS: tuple[str, ...] = ("Decision", "Alternative", "Assumption", "Learning")

# Judgement / provenance edges that represent navigable cognitive closeout.
COGNITIVE_EDGE_TYPES: tuple[str, ...] = (
    "relates_to",
    "validates",
    "supersedes",
    "contradicts",
    "derives_from",
    "depends_on",
)

# Per-concept ref suffixes the extractor appends (spec:<id>:alternative:<hash>,
# card:bug:<id>:learning:<uuid>, ...). Stripped to recover the base artifact.
_CONCEPT_MARKERS: tuple[str, ...] = (":learning:", ":alternative:", ":assumption:", ":decision:")


class EffectivenessState(str, Enum):
    """The canonical inventory states (FR1)."""

    ATTEMPTED = "attempted"  # derived rollup, not a primary classification
    EXTRACTED_CANDIDATE = "extracted_candidate"
    PERSISTED_OR_CONSOLIDATED = "persisted_or_consolidated"
    DLQ = "dlq"
    SKIPPED = "skipped"
    NO_ACTION_REQUIRED = "no_action_required"
    NOT_APPLICABLE = "not_applicable"
    NO_MATERIAL = "no_material"
    EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED = "extractor_triggered_but_not_persisted"


_PRIMARY_STATES: tuple[EffectivenessState, ...] = (
    EffectivenessState.EXTRACTED_CANDIDATE,
    EffectivenessState.PERSISTED_OR_CONSOLIDATED,
    EffectivenessState.DLQ,
    EffectivenessState.SKIPPED,
    EffectivenessState.NO_ACTION_REQUIRED,
    EffectivenessState.NOT_APPLICABLE,
    EffectivenessState.NO_MATERIAL,
    EffectivenessState.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED,
)

_NOT_ATTEMPTED_STATES: frozenset[str] = frozenset({
    EffectivenessState.NO_MATERIAL.value,
    EffectivenessState.NOT_APPLICABLE.value,
})


class CognitiveEffectivenessError(Exception):
    """Typed error carrying the bounded ``code`` + HTTP status the API echoes."""

    def __init__(self, code: str, message: str, *, http_status: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {"error": self.code, "message": self.message, "status_code": self.http_status}


@runtime_checkable
class CandidateLogSource(Protocol):
    """Read-only source of literal candidate-emission evidence.

    RKG-03 will provide a real implementation backed by a persisted
    candidate-log store/consumer. Until then the inventory has no source and
    falls back to eligibility inference. The inventory NEVER writes through this
    interface — it only reads, honouring TR1.
    """

    def candidates_for(self, board_id: str, artifact_id: str) -> list[dict[str, Any]]:
        ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _card_type_value(card_type: Any) -> str:
    return getattr(card_type, "value", str(card_type or "")).lower()


def _base_artifact_ref(source_ref: str) -> str:
    """Recover the canonical base artifact ref a per-concept cognitive node ref
    belongs to: strip ``:alternative:``/``:assumption:``/``:learning:`` suffixes
    and collapse ``bug:``/``card:bug:`` to ``card:``."""
    s = source_ref or ""
    for marker in _CONCEPT_MARKERS:
        idx = s.find(marker)
        if idx != -1:
            s = s[:idx]
            break
    if s.startswith("card:bug:"):
        s = "card:" + s[len("card:bug:"):]
    return normalize_cognitive_artifact_id(s)


def _artifact_type_of(ref: str) -> str:
    head = ref.split(":", 1)[0] if ":" in ref else "unknown"
    return head or "unknown"


# ---------------------------------------------------------------------------
# Eligibility (mirrors CognitiveExtractionHandler).
# ---------------------------------------------------------------------------


def _card_eligibility(card: Card) -> tuple[str | None, str]:
    """A done card is a cognitive artifact ONLY when it is a bug with a
    substantial action_plan (→ Learning). A non-bug card (even with spec_id) is
    NOT itself the cognitive artifact — its Alternative/Assumption closeout is
    keyed to the SPEC, classified under the spec ref instead."""
    if _card_type_value(card.card_type) == "bug":
        if len((card.action_plan or "").strip()) >= LEARNING_MIN_ACTION_PLAN_CHARS:
            return "learning", "bug_with_action_plan"
        return None, "bug_without_sufficient_action_plan"
    return None, "non_bug_card_cognition_keyed_to_spec"


# ---------------------------------------------------------------------------
# Read-only graph projection
# ---------------------------------------------------------------------------


def _layer_matches(node_layer: Any, graph_layer: str) -> bool:
    if graph_layer == "all":
        return True
    return str(node_layer or "canonical") == graph_layer


def _project_cognitive_graph(board_id: str, graph_layer: str) -> dict[str, Any]:
    """Read-only projection: per-base-ref persisted node + edge counts, plus
    global node/edge type counts. Degrades to an empty projection on any KG
    error so the inventory stays available. NEVER mutates."""
    persisted_refs: dict[str, dict[str, int]] = {}
    edge_refs: dict[str, dict[str, int]] = {}
    node_type_counts: dict[str, int] = {label: 0 for label in COGNITIVE_NODE_LABELS}
    edge_type_counts: dict[str, int] = {etype: 0 for etype in COGNITIVE_EDGE_TYPES}
    available = True

    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        executor = get_kg_registry().cypher_executor
        for label in COGNITIVE_NODE_LABELS:
            try:
                result = executor.execute_read_only(
                    board_id,
                    f"MATCH (n:{label}) RETURN n.source_artifact_ref, n.graph_layer",
                    {},
                    max_rows=10000,
                )
            except Exception as exc:
                logger.debug("cognitive_effectiveness.node_query_failed board=%s label=%s err=%s",
                             board_id, label, exc)
                continue
            for ref, node_layer in result.get("rows") or []:
                if not _layer_matches(node_layer, graph_layer):
                    continue
                node_type_counts[label] += 1
                if not ref:
                    continue
                base = _base_artifact_ref(str(ref))
                bucket = persisted_refs.setdefault(base, {})
                bucket[label] = bucket.get(label, 0) + 1
        for etype in COGNITIVE_EDGE_TYPES:
            try:
                result = executor.execute_read_only(
                    board_id,
                    f"MATCH (n)-[r:{etype}]->() RETURN n.source_artifact_ref, count(r)",
                    {},
                    max_rows=10000,
                )
            except Exception as exc:
                logger.debug("cognitive_effectiveness.edge_query_failed board=%s edge=%s err=%s",
                             board_id, etype, exc)
                continue
            for ref, cnt in result.get("rows") or []:
                c = int(cnt or 0)
                edge_type_counts[etype] += c
                if ref:
                    base = _base_artifact_ref(str(ref))
                    eb = edge_refs.setdefault(base, {})
                    eb[etype] = eb.get(etype, 0) + c
    except Exception as exc:
        logger.warning("cognitive_effectiveness.kg_projection_failed board=%s err=%s", board_id, exc)
        available = False

    return {"persisted_refs": persisted_refs, "edge_refs": edge_refs,
            "node_type_counts": node_type_counts, "edge_type_counts": edge_type_counts,
            "available": available}


# ---------------------------------------------------------------------------
# Per-artifact classification
# ---------------------------------------------------------------------------


_REMEDIATION: dict[str, str] = {
    EffectivenessState.DLQ.value: "Fix the source_ref/connectivity root cause, then reprocess the dead letter (RKG-02/RKG-04).",
    EffectivenessState.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED.value:
        "RKG-03 must plug the candidate-log store + consumer and verify the candidate->persist wiring; until then this is inferred, not observed.",
    EffectivenessState.EXTRACTED_CANDIDATE.value: "Await consolidation worker or inspect the pending item.",
    EffectivenessState.SKIPPED.value: "Cognitive skip recorded; no action unless the skip is contested.",
    EffectivenessState.NO_ACTION_REQUIRED.value: "Closeout resolved with no cognitive node required.",
    EffectivenessState.PERSISTED_OR_CONSOLIDATED.value: "None.",
    EffectivenessState.NOT_APPLICABLE.value: "Register not_applicable; no cognitive material expected.",
    EffectivenessState.NO_MATERIAL.value: "Register no_material; legitimate absence of cognitive material is not a failure.",
}


def _entry(
    artifact_ref: str,
    artifact_type: str,
    source_ref_original: str,
    state: EffectivenessState,
    *,
    evidence_source: str,
    confidence: str,
    candidate_log_refs: list[Any] | None = None,
    inferred_candidate: bool = False,
    inference_reason: str | None = None,
    reason_code: str | None = None,
    node_counts: dict[str, int] | None = None,
    edge_counts: dict[str, int] | None = None,
    dead_letter_ids: list[str] | None = None,
    detail: str = "",
) -> dict[str, Any]:
    return {
        "artifact_ref": artifact_ref,
        "artifact_type": artifact_type,
        "source_ref_original": source_ref_original,
        "state": state.value,
        "evidence_source": evidence_source,
        "confidence": confidence,
        "inferred_candidate": inferred_candidate,
        "candidate_log_refs": list(candidate_log_refs or []),
        "inference_reason": inference_reason,
        "reason_code": reason_code,
        "node_counts": node_counts or {},
        "edge_counts": edge_counts or {},
        "dead_letter_ids": list(dead_letter_ids or []),
        "next_action": _REMEDIATION.get(state.value, "None."),
        "remediation": _REMEDIATION.get(state.value, "None."),
        "detail": detail,
    }


def _classify_artifact(
    *,
    artifact_ref: str,
    artifact_type: str,
    source_ref_original: str,
    eligibility_kind: str | None,
    eligibility_reason: str,
    dlq_rows: list[Any],
    persisted: dict[str, int] | None,
    edge_counts: dict[str, int] | None,
    items: list[CognitiveConsolidationItem],
    candidate_source: CandidateLogSource | None,
    board_id: str,
) -> dict[str, Any]:
    edge_counts = edge_counts or {}

    if dlq_rows:
        return _entry(artifact_ref, artifact_type, source_ref_original, EffectivenessState.DLQ,
                      evidence_source="technical_dlq", confidence="observed",
                      dead_letter_ids=[str(getattr(r, "dead_letter_id", getattr(r, "id", ""))) for r in dlq_rows],
                      edge_counts=edge_counts,
                      detail="open technical dead letter invalidates effectiveness")

    if persisted:
        return _entry(artifact_ref, artifact_type, source_ref_original,
                      EffectivenessState.PERSISTED_OR_CONSOLIDATED,
                      evidence_source="graph_node", confidence="observed",
                      node_counts=dict(persisted), edge_counts=edge_counts)

    consolidated = [i for i in items if i.status == CognitiveItemStatus.CONSOLIDATED.value]
    if consolidated:
        return _entry(artifact_ref, artifact_type, source_ref_original,
                      EffectivenessState.PERSISTED_OR_CONSOLIDATED,
                      evidence_source="consolidation_item", confidence="observed", edge_counts=edge_counts)
    if any(i.outcome_type == CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value for i in items):
        return _entry(artifact_ref, artifact_type, source_ref_original,
                      EffectivenessState.NO_ACTION_REQUIRED,
                      evidence_source="consolidation_item", confidence="observed", edge_counts=edge_counts)
    skipped = [i for i in items if i.status == CognitiveItemStatus.SKIPPED.value]
    if skipped:
        return _entry(artifact_ref, artifact_type, source_ref_original, EffectivenessState.SKIPPED,
                      evidence_source="consolidation_item", confidence="observed",
                      reason_code=skipped[0].reason_code, edge_counts=edge_counts)
    pending = [i for i in items if i.status in (
        CognitiveItemStatus.PENDING.value, CognitiveItemStatus.IN_PROGRESS.value)]
    if pending:
        return _entry(artifact_ref, artifact_type, source_ref_original,
                      EffectivenessState.EXTRACTED_CANDIDATE,
                      evidence_source="consolidation_item", confidence="observed",
                      edge_counts=edge_counts, detail="candidate registered, awaiting persist")

    literal: list[dict[str, Any]] = []
    if candidate_source is not None:
        try:
            literal = candidate_source.candidates_for(board_id, artifact_ref) or []
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("cognitive_effectiveness.candidate_source_failed err=%s", exc)
            literal = []
    if literal:
        return _entry(artifact_ref, artifact_type, source_ref_original,
                      EffectivenessState.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED,
                      evidence_source="candidate_log", confidence="observed",
                      candidate_log_refs=[c.get("ref") for c in literal if c.get("ref")],
                      inferred_candidate=False, edge_counts=edge_counts,
                      detail="candidate emitted (observed) without persisted node/edge")

    if eligibility_kind is not None:
        return _entry(artifact_ref, artifact_type, source_ref_original,
                      EffectivenessState.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED,
                      evidence_source="inferred_eligibility", confidence="inferred",
                      candidate_log_refs=[], inferred_candidate=True,
                      inference_reason=f"eligible_{eligibility_kind}:{eligibility_reason}_no_node_no_dlq_no_item",
                      edge_counts=edge_counts,
                      detail="extractor eligible but nothing persisted; inferred, not observed")

    return _entry(artifact_ref, artifact_type, source_ref_original, EffectivenessState.NO_MATERIAL,
                  evidence_source="eligibility", confidence="observed",
                  inference_reason=eligibility_reason, edge_counts=edge_counts)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _valid_ref_shape(ref: str) -> bool:
    head, _, rest = ref.partition(":")
    return bool(head) and bool(rest)


async def build_cognitive_effectiveness_inventory(
    db: AsyncSession,
    board_id: str,
    *,
    artifact_id: str | None = None,
    include_candidate_logs: bool = False,
    graph_layer: str = "canonical",
    candidate_source: CandidateLogSource | None = None,
    store: CognitiveConsolidationItemStore | None = None,
    metric_status: str | None = None,
    now=_now,
) -> dict[str, Any]:
    """Build the read-only cognitive-effectiveness inventory for a board.

    Raises :class:`CognitiveEffectivenessError` ``invalid_artifact_filter`` (400)
    when graph_layer or artifact_id shape is invalid, or ``kg_metric_unavailable``
    (503) when the KG metric status is not available.
    """
    if graph_layer not in VALID_GRAPH_LAYERS:
        raise CognitiveEffectivenessError(
            "invalid_artifact_filter", f"graph_layer must be one of {VALID_GRAPH_LAYERS}",
            http_status=400)
    if artifact_id is not None and not _valid_ref_shape(artifact_id):
        raise CognitiveEffectivenessError(
            "invalid_artifact_filter",
            "artifact_id must be a ref of the form <type>:<id> (e.g. card:<uuid>, spec:<id>)",
            http_status=400)
    if metric_status is not None and metric_status not in ("available", "ok"):
        raise CognitiveEffectivenessError(
            "kg_metric_unavailable",
            "KG metric status is unavailable; inventory cannot be trusted", http_status=503)

    norm_filter = normalize_cognitive_artifact_id(artifact_id) if artifact_id else None

    # ---- gather sources (all read-only) ----
    dlq_rows = (await db.execute(
        select(ConsolidationDeadLetter).where(ConsolidationDeadLetter.board_id == board_id)
    )).scalars().all()
    dlq_by_ref: dict[str, list[Any]] = {}
    original_by_ref: dict[str, str] = {}
    type_by_ref: dict[str, str] = {}
    for row in dlq_rows:
        raw = f"{row.artifact_type}:{row.artifact_id}"
        ref = normalize_cognitive_artifact_id(raw)
        dlq_by_ref.setdefault(ref, []).append(row)
        original_by_ref.setdefault(ref, raw)
        type_by_ref.setdefault(ref, str(row.artifact_type or _artifact_type_of(ref)))

    elig_by_ref: dict[str, tuple[str | None, str]] = {}

    done_cards = (await db.execute(
        select(Card).where(Card.board_id == board_id, Card.status == CardStatus.DONE)
    )).scalars().all()
    for card in done_cards:
        raw = f"card:{card.id}"
        ref = normalize_cognitive_artifact_id(raw)
        kind, reason = _card_eligibility(card)
        elig_by_ref[ref] = (kind, reason)
        original_by_ref.setdefault(ref, raw)
        type_by_ref.setdefault(ref, "bug" if _card_type_value(card.card_type) == "bug" else "card")

    # Done specs are cognitive artifacts in their own right (Alternative/Assumption).
    done_specs = (await db.execute(
        select(Spec).where(Spec.board_id == board_id, Spec.status == SpecStatus.DONE)
    )).scalars().all()
    for spec in done_specs:
        raw = f"spec:{spec.id}"
        ref = normalize_cognitive_artifact_id(raw)
        elig_by_ref[ref] = ("alternative_assumption", "spec_done_cognitive_context")
        original_by_ref.setdefault(ref, raw)
        type_by_ref.setdefault(ref, "spec")

    from okto_pulse.core.kg.rebuild_audit import require_rebuild_audit_artifact_store
    item_store = store or CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    items_by_ref: dict[str, list[CognitiveConsolidationItem]] = {}
    try:
        gen = item_store.latest_generation(board_id)
        if gen:
            for item in item_store.list_items(board_id, gen):
                ref = item.artifact_id or normalize_cognitive_artifact_id(item.source_ref)
                items_by_ref.setdefault(ref, []).append(item)
                original_by_ref.setdefault(ref, item.source_ref_original or item.source_ref)
                type_by_ref.setdefault(ref, str(item.artifact_type or _artifact_type_of(ref)))
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("cognitive_effectiveness.item_store_failed board=%s err=%s", board_id, exc)

    graph = _project_cognitive_graph(board_id, graph_layer)
    persisted_refs: dict[str, dict[str, int]] = graph["persisted_refs"]
    edge_refs: dict[str, dict[str, int]] = graph["edge_refs"]

    all_refs = set(elig_by_ref) | set(dlq_by_ref) | set(items_by_ref) | set(persisted_refs)
    for ref in all_refs:
        original_by_ref.setdefault(ref, ref)
        type_by_ref.setdefault(ref, _artifact_type_of(ref))
    if norm_filter is not None:
        all_refs = {r for r in all_refs if r == norm_filter}

    artifacts: list[dict[str, Any]] = []
    for ref in sorted(all_refs):
        kind, reason = elig_by_ref.get(ref, (None, "not_a_tracked_done_artifact"))
        entry = _classify_artifact(
            artifact_ref=ref,
            artifact_type=type_by_ref.get(ref, _artifact_type_of(ref)),
            source_ref_original=original_by_ref.get(ref, ref),
            eligibility_kind=kind,
            eligibility_reason=reason,
            dlq_rows=dlq_by_ref.get(ref, []),
            persisted=persisted_refs.get(ref),
            edge_counts=edge_refs.get(ref),
            items=items_by_ref.get(ref, []),
            candidate_source=candidate_source,
            board_id=board_id,
        )
        if not include_candidate_logs:
            entry.pop("candidate_log_refs", None)
        artifacts.append(entry)

    totals = {state.value: 0 for state in _PRIMARY_STATES}
    totals[EffectivenessState.ATTEMPTED.value] = 0
    for entry in artifacts:
        st = entry["state"]
        totals[st] = totals.get(st, 0) + 1
        if st not in _NOT_ATTEMPTED_STATES:
            totals[EffectivenessState.ATTEMPTED.value] += 1

    # Effectiveness blockers (BR1 + validation finding #5): a board is not
    # cognitively effective while a technical DLQ OR a persistence gap exists.
    effectiveness_blockers: list[dict[str, Any]] = []
    if totals[EffectivenessState.DLQ.value]:
        effectiveness_blockers.append(
            {"category": "technical_dlq", "count": totals[EffectivenessState.DLQ.value]})
    gap = totals[EffectivenessState.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED.value]
    if gap:
        effectiveness_blockers.append({"category": "persistence_gap", "count": gap})

    return {
        "board_id": board_id,
        "generated_at": now().isoformat(),
        "read_only": True,
        "graph_layer": graph_layer,
        "artifact_filter": norm_filter,
        "kg_projection_available": graph["available"],
        "cognitively_effective": len(effectiveness_blockers) == 0,
        "effectiveness_blockers": effectiveness_blockers,
        "totals": totals,
        "cognitive_graph": {
            "node_type_counts": graph["node_type_counts"],
            "edge_type_counts": graph["edge_type_counts"],
        },
        "artifacts": artifacts,
    }
