"""RKG-03 — cognitive closeout production rules + candidate→persist path.

Closes the gap RKG-01 measured: cognitive candidates were only *logged* by
``CognitiveExtractionHandler`` and never persisted (the downstream consumer was
deferred). This module is that consumer: it extracts cognitive candidates from a
done spec/bug, classifies the outcome HONESTLY, and persists applicable
candidates to the graph through an injectable :class:`CognitiveCandidatePersister`
(the real one wraps begin→add_node→add_edge→commit; tests use a fake).

Invariants:
  * TR1 — the cognitive path only ever produces Decision/Alternative/Assumption/
    Learning. Criterion/Constraint deterministic materialization is never created
    here.
  * TR2 — source_artifact_ref is stable + idempotent per cognitive concept and is
    resolved through the RKG-02 shared resolver (Learning→validates→canonical Bug).
  * TR3 — a Decision is only produced when there is a real choice + a valid
    judgement/provenance edge; isolated risk/uncertainty becomes Assumption/
    Alternative, never an artificial Decision.
  * BR2 — a candidate only counts as effective once persisted + queryable.
  * BR3/FR3 — absence is classified honestly (no_material / not_applicable /
    skipped_no_llm_config / extractor_not_triggered /
    extractor_triggered_but_not_persisted) and NEVER fabricates a node.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.kg.agent.extractors import (
    LEARNING_MIN_ACTION_PLAN_CHARS,
    extract_alternatives,
    extract_assumptions,
)
from okto_pulse.core.kg.cognitive_source_ref_resolver import (
    resolve_cognitive_source_ref,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    default_rebuild_base_dir,
)

# Stable generation id used to open cognitive-closeout pending work when a board
# has no rebuild generation yet (#4: latest when it exists, this otherwise — one
# stable id, never a fresh generation per item that would hide prior pending).
_COGNITIVE_CLOSEOUT_GENERATION = "cognitive_closeout"

# #5 (codex): the cognitive commit runs under the SAME write barrier as the
# deterministic worker so strict-mode require_write_token sees an active guard.
COGNITIVE_CLOSEOUT_COMMIT_OPERATION = "cognitive_closeout_commit"


def _default_store() -> CognitiveConsolidationItemStore:
    """The production-default ledger store (#2: base_dir is REQUIRED — never a
    no-arg constructor)."""
    return CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())


def _resolve_generation(store: CognitiveConsolidationItemStore, board_id: str,
                        kg_generation_id: str | None) -> str:
    return kg_generation_id or store.latest_generation(board_id) or _COGNITIVE_CLOSEOUT_GENERATION

logger = logging.getLogger("okto_pulse.core.kg.cognitive_closeout_production")

# TR1: the only node types the cognitive writer may produce.
COGNITIVE_NODE_TYPES: frozenset[str] = frozenset({"Decision", "Alternative", "Assumption", "Learning"})


class CloseoutOutcome(str, Enum):
    """Honest closeout classification (FR3)."""

    PERSISTED = "persisted"
    NO_MATERIAL = "no_material"
    NOT_APPLICABLE = "not_applicable"
    SKIPPED_NO_LLM_CONFIG = "skipped_no_llm_config"
    EXTRACTOR_NOT_TRIGGERED = "extractor_not_triggered"
    EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED = "extractor_triggered_but_not_persisted"


@dataclass(frozen=True)
class CloseoutEdge:
    edge_type: str            # validates | relates_to
    to_ref: str               # the OTHER endpoint's existing node ref (kg id)
    # Direction relative to the NEW cognitive node. validates is outgoing
    # (Learning→Bug); relates_to is incoming (Decision→Alternative).
    incoming: bool = False


@dataclass(frozen=True)
class CloseoutCandidate:
    node_type: str            # Alternative | Assumption | Learning | Decision (TR1)
    title: str
    content: str | None
    source_artifact_ref: str
    edges: tuple[CloseoutEdge, ...] = ()


@dataclass
class CloseoutResult:
    artifact_ref: str
    artifact_type: str
    outcome: str
    detail: str = ""
    candidates_emitted: int = 0
    persisted_refs: list[str] = field(default_factory=list)
    skipped_existing_refs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_ref": self.artifact_ref,
            "artifact_type": self.artifact_type,
            "outcome": self.outcome,
            "detail": self.detail,
            "candidates_emitted": self.candidates_emitted,
            "persisted_refs": list(self.persisted_refs),
            "skipped_existing_refs": list(self.skipped_existing_refs),
        }


@runtime_checkable
class CognitiveCandidatePersister(Protocol):
    """Persists a cognitive candidate (+ its edges) through the consolidation
    pipeline to graph.lbug and confirms it is queryable. The real implementation
    wraps begin→add_node→add_edge→commit; a fake records calls for tests. The
    closeout service NEVER writes the graph directly (BR2 lives here)."""

    def already_persisted(self, board_id: str, node_type: str, source_artifact_ref: str) -> bool:
        """True iff an equivalent node already exists (idempotency, AC1/replay)."""
        ...

    async def persist(self, board_id: str, artifact_type: str, candidate: CloseoutCandidate) -> bool:
        """Persist the candidate + edges through the consolidation pipeline; return
        True once committed AND confirmed queryable in graph.lbug (BR2)."""
        ...


# ---------------------------------------------------------------------------
# Production rules (extraction + classification)
# ---------------------------------------------------------------------------


def _spec_candidates(
    spec_context: str, source_ref: str, *, decision_ref: str | None = None,
) -> list[CloseoutCandidate]:
    """Alternative/Assumption from a done spec when real material exists (FR1).
    When the spec has a related Decision (``decision_ref`` = its existing node
    ref), each Alternative carries a relates_to edge to it (AC1, #2)."""
    out: list[CloseoutCandidate] = []
    alt_edges = (
        (CloseoutEdge(edge_type="relates_to", to_ref=decision_ref, incoming=True),)
        if decision_ref else ()
    )
    for alt in extract_alternatives(spec_context=spec_context or "", source_ref=source_ref):
        out.append(CloseoutCandidate(
            node_type="Alternative", title=alt.title,
            content=getattr(alt, "reasoning_against", None),
            source_artifact_ref=alt.source_ref, edges=alt_edges))
    for asm in extract_assumptions(spec_context=spec_context or "", source_ref=source_ref):
        out.append(CloseoutCandidate(
            node_type="Assumption", title=asm.title,
            content=getattr(asm, "body", None),
            source_artifact_ref=asm.source_ref))
    return out


def _learning_candidate(
    *,
    bug_card_id: str,
    bug_title: str,
    action_plan: str,
    summariser: Any,
    validates_ref: str,
) -> CloseoutCandidate | None:
    """Learning from a done bug with root cause/fix/evidence. ``validates_ref`` is
    the ALREADY-resolved canonical Bug ref (the caller fail-closes when no
    canonical Bug resolves — #4, RKG-02); this never fabricates a bug-like ref."""
    from okto_pulse.core.kg.agent.extractors import extract_learning_from_bug

    bug_node_id = f"bug_{bug_card_id[:12]}"
    extraction = extract_learning_from_bug(
        bug_node_id=bug_node_id, bug_title=bug_title, bug_status="done",
        card_type="bug", action_plan=action_plan, summariser=summariser,
        min_action_plan_chars=LEARNING_MIN_ACTION_PLAN_CHARS,
    )
    if extraction is None:
        return None
    return CloseoutCandidate(
        node_type="Learning",
        title=extraction.learning_title,
        content=getattr(extraction, "learning_body", None),
        source_artifact_ref=f"bug:{bug_card_id}",
        edges=(CloseoutEdge(edge_type="validates", to_ref=validates_ref),),
    )


# ---------------------------------------------------------------------------
# Orchestration: classify → persist → honest outcome
# ---------------------------------------------------------------------------


async def _persist_all(
    *,
    board_id: str,
    artifact_type: str,
    candidates: list[CloseoutCandidate],
    persister: CognitiveCandidatePersister,
    result: CloseoutResult,
) -> None:
    """Persist each candidate idempotently; record honest per-candidate outcome."""
    any_persisted = False
    any_failed = False
    for cand in candidates:
        if cand.node_type not in COGNITIVE_NODE_TYPES:  # TR1 guard
            logger.warning("cognitive_closeout.non_cognitive_node_type type=%s", cand.node_type)
            continue
        if persister.already_persisted(board_id, cand.node_type, cand.source_artifact_ref):
            result.skipped_existing_refs.append(cand.source_artifact_ref)
            any_persisted = True  # already effective (AC1 replay idempotency)
            continue
        ok = await persister.persist(board_id, artifact_type, cand)
        if ok:
            result.persisted_refs.append(cand.source_artifact_ref)
            any_persisted = True
        else:
            any_failed = True
    if any_persisted and not result.persisted_refs and not any_failed:
        result.outcome = CloseoutOutcome.PERSISTED.value
        result.detail = "all applicable candidates already persisted (idempotent replay)"
    elif result.persisted_refs:
        result.outcome = CloseoutOutcome.PERSISTED.value
    elif any_failed:
        result.outcome = CloseoutOutcome.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED.value
        result.detail = "candidates emitted but persistence did not confirm queryable"


async def run_cognitive_closeout(
    *,
    board_id: str,
    artifact_type: str,
    artifact_ref: str,
    persister: CognitiveCandidatePersister,
    spec_context: str | None = None,
    bug_card_id: str | None = None,
    bug_title: str = "",
    bug_action_plan: str | None = None,
    llm_config: dict | None = None,
    summariser: Any = None,
    bug_probe: Any = None,
    decision_ref: str | None = None,
) -> CloseoutResult:
    """Produce + persist cognitive closeout for one done artifact, returning an
    honest outcome. ``persister`` owns the candidate→graph.lbug→queryable step."""
    result = CloseoutResult(artifact_ref=artifact_ref, artifact_type=artifact_type,
                            outcome=CloseoutOutcome.NO_MATERIAL.value)

    if artifact_type == "spec":
        candidates = _spec_candidates(spec_context or "", artifact_ref, decision_ref=decision_ref)
        result.candidates_emitted = len(candidates)
        if not candidates:
            result.outcome = CloseoutOutcome.NO_MATERIAL.value
            result.detail = "spec done carries no considered alternative / risk / assumption"
            return result
        await _persist_all(board_id=board_id, artifact_type=artifact_type,
                     candidates=candidates, persister=persister, result=result)
        return result

    if artifact_type == "bug":
        action_plan = (bug_action_plan or "").strip()
        if len(action_plan) < LEARNING_MIN_ACTION_PLAN_CHARS:
            result.outcome = CloseoutOutcome.NOT_APPLICABLE.value
            result.detail = "bug done has no root-cause/fix narrative (action_plan below threshold)"
            return result
        if not (llm_config or {}).get("provider"):
            result.outcome = CloseoutOutcome.SKIPPED_NO_LLM_CONFIG.value
            result.detail = "Learning needs Board.settings.cognitive_llm_config; skipped (config gap, not a failure)"
            return result
        # #3/#4 (codex): the Learning must validate a RESOLVED canonical Bug.
        # Fail-closed whenever it is NOT bug-derived (a probe is mandatory on the
        # bug path to prove the canonical Bug) — never fabricate a bug-like ref.
        resolution = resolve_cognitive_source_ref(
            f"card:{bug_card_id}", canonical_bug_probe=bug_probe)
        if not resolution.is_bug_derived:
            result.outcome = CloseoutOutcome.NOT_APPLICABLE.value
            result.detail = ("no canonical Bug resolved for this card; a Learning cannot "
                             "validate a non-existent Bug (fail-closed, RKG-02)")
            return result
        candidate = _learning_candidate(
            bug_card_id=bug_card_id or "", bug_title=bug_title, action_plan=action_plan,
            summariser=summariser, validates_ref=resolution.canonical_artifact_ref)
        if candidate is None:
            result.outcome = CloseoutOutcome.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED.value
            result.detail = "extractor triggered but produced no Learning (empty summary)"
            return result
        result.candidates_emitted = 1
        await _persist_all(board_id=board_id, artifact_type=artifact_type,
                     candidates=[candidate], persister=persister, result=result)
        return result

    if artifact_type == "card":
        # #5 (codex): a done card that is neither a bug nor spec-backed has no
        # cognitive extractor branch — the extractor never triggers for it.
        result.outcome = CloseoutOutcome.EXTRACTOR_NOT_TRIGGERED.value
        result.detail = "card done is not a bug and carries no spec context; no cognitive extractor applies"
        return result

    result.outcome = CloseoutOutcome.NOT_APPLICABLE.value
    result.detail = f"artifact_type '{artifact_type}' has no cognitive closeout rule"
    return result


# ---------------------------------------------------------------------------
# Real persister — drives the consolidation pipeline to graph.lbug (FR2/BR2)
# ---------------------------------------------------------------------------


def _count_nodes_by_source_ref(board_id: str, node_type: str, source_artifact_ref: str) -> int:
    """Read-only count of persisted nodes of ``node_type`` for a source_ref."""
    from okto_pulse.core.kg.interfaces import get_kg_registry

    try:
        result = get_kg_registry().cypher_executor.execute_read_only(
            board_id,
            f"MATCH (n:{node_type}) WHERE n.source_artifact_ref = $ref "
            "RETURN count(n)",
            {"ref": source_artifact_ref},
            max_rows=1,
        )
        rows = result.get("rows", [])
        if rows:
            return int(rows[0][0] or 0)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("cognitive_closeout.count_failed ref=%s err=%s", source_artifact_ref, exc)
    return 0


def _resolve_existing_node_id(board_id: str, label: str, ref: str) -> str:
    """Resolve an edge endpoint to an existing node id. A bare id (no ':') is
    used as-is; a canonical ref (e.g. ``card:<uuid>`` for the Bug) is matched
    against the label's nodes by normalized source_ref / id (RKG-02 identity).
    Returns the ref unchanged when unresolved so the commit fails cleanly."""
    if ":" not in ref:
        return ref
    from okto_pulse.core.kg.cognitive_source_ref_resolver import strip_concept_suffix
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    target = normalize_cognitive_artifact_id(strip_concept_suffix(ref))
    try:
        result = get_kg_registry().cypher_executor.execute_read_only(
            board_id,
            f"MATCH (n:{label}) RETURN n.id, n.source_artifact_ref",
            {},
            max_rows=10000,
        )
        for nid, sref in result.get("rows", []):
            if sref and normalize_cognitive_artifact_id(
                    strip_concept_suffix(str(sref))) == target:
                return str(nid)
            if nid and normalize_cognitive_artifact_id(f"card:{nid}") == target:
                return str(nid)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("cognitive_closeout.endpoint_resolve_failed ref=%s err=%s", ref, exc)
    return ref


class ConsolidationPipelinePersister:
    """The production persister: drives begin→add_node→add_edge→propose→commit and
    confirms the node is queryable in graph.lbug. This is the consumer/worker that
    RKG-01 measured as missing. ``db_factory`` is the async session context manager
    (matching the consolidation worker's wiring)."""

    def __init__(self, db_factory, *, agent_id: str = "cognitive_closeout_worker") -> None:
        # #6 (codex): NOT a ``system:`` id — primitives treats system:* as a
        # deterministic Layer-1 writer (ownership bypass). The cognitive closeout
        # must stay on the cognitive writer boundary.
        self._db_factory = db_factory
        self._agent_id = agent_id

    def already_persisted(self, board_id: str, node_type: str, source_artifact_ref: str) -> bool:
        return _count_nodes_by_source_ref(board_id, node_type, source_artifact_ref) > 0

    async def persist(self, board_id: str, artifact_type: str, candidate: CloseoutCandidate) -> bool:
        import hashlib
        import uuid as _uuid

        from okto_pulse.core.kg.primitives import (
            add_edge_candidate,
            add_node_candidate,
            begin_consolidation,
            commit_consolidation,
            propose_reconciliation,
        )
        from okto_pulse.core.kg.write_barrier import under_safe_write
        from okto_pulse.core.kg.schemas import (
            AddEdgeCandidateRequest,
            AddNodeCandidateRequest,
            BeginConsolidationRequest,
            CommitConsolidationRequest,
            EdgeCandidate,
            KGEdgeType,
            KGNodeType,
            NodeCandidate,
            ProposeReconciliationRequest,
        )

        artifact_id = candidate.source_artifact_ref.split(":", 1)[-1]
        cid = "cog_" + hashlib.sha256(candidate.source_artifact_ref.encode()).hexdigest()[:16]
        # #6 (codex 2026-06-25): a failure in ANY pipeline step before/at commit
        # (begin/add_node/add_edge/propose/commit) must fail-closed to "not
        # persisted", never escape as an uncaught error or a fabricated success.
        try:
            async with self._db_factory() as db:
                begin = await begin_consolidation(
                    BeginConsolidationRequest(
                        board_id=board_id, artifact_type=artifact_type, artifact_id=artifact_id,
                        raw_content=(candidate.content or candidate.title or ""),
                    ),
                    agent_id=self._agent_id, db=db,
                )
            await add_node_candidate(
                AddNodeCandidateRequest(
                    session_id=begin.session_id,
                    candidate=NodeCandidate(
                        candidate_id=cid, node_type=KGNodeType(candidate.node_type),
                        title=candidate.title, content=candidate.content,
                        source_artifact_ref=candidate.source_artifact_ref, source_confidence=0.7,
                    ),
                ),
                agent_id=self._agent_id,
            )
            for i, edge in enumerate(candidate.edges):
                # The edge target may be a bare existing node id (e.g. a resolved
                # Decision id) OR a canonical ref (e.g. card:<uuid> for the Bug);
                # resolve the canonical ref to the real node id (RKG-02 identity).
                target_label = "Bug" if edge.edge_type == "validates" else "Decision"
                node_id = _resolve_existing_node_id(board_id, target_label, edge.to_ref)
                other = f"kg:{node_id}"
                # validates is outgoing (Learning→Bug); relates_to is incoming
                # (Decision→Alternative) so the existing node is the source.
                from_id, to_id = (other, cid) if edge.incoming else (cid, other)
                await add_edge_candidate(
                    AddEdgeCandidateRequest(
                        session_id=begin.session_id,
                        candidate=EdgeCandidate(
                            candidate_id=f"{cid}_e{i}", edge_type=KGEdgeType(edge.edge_type),
                            from_candidate_id=from_id, to_candidate_id=to_id,
                        ),
                    ),
                    agent_id=self._agent_id,
                )
            await propose_reconciliation(
                ProposeReconciliationRequest(session_id=begin.session_id),
                agent_id=self._agent_id, db=None, force_reprocess=True,
            )
            owner_token = f"cognitive-closeout-worker:{cid}:{_uuid.uuid4().hex}"
            async with self._db_factory() as db:
                with under_safe_write(board_id, owner_token, COGNITIVE_CLOSEOUT_COMMIT_OPERATION):
                    await commit_consolidation(
                        CommitConsolidationRequest(session_id=begin.session_id),
                        agent_id=self._agent_id, db=db,
                    )
        except Exception as exc:
            # BR2/FR3: any rejection/failure (begin/add_node/add_edge/propose/
            # commit) is NOT fabricated as success.
            logger.info("cognitive_closeout.persist_failed ref=%s step_err=%s",
                        candidate.source_artifact_ref, getattr(exc, "code", exc))
            return False
        # BR2: effective only when actually queryable in graph.lbug.
        return self.already_persisted(board_id, candidate.node_type, candidate.source_artifact_ref)


# ---------------------------------------------------------------------------
# Ledger-backed consumer/worker (#1 codex) — handler opens pending; a dedicated
# cognitive worker drains it OUTSIDE the event drain and persists to graph.lbug.
# The ledger IS the candidate→consumer→graph tracking (FR2/BR2).
# ---------------------------------------------------------------------------


# CloseoutOutcome → (ledger status, outcome_type). The ledger is updated by the
# worker; persisted→consolidated, honest-absence→skipped, persist-fail→failed.
_LEDGER_STATUS: dict[str, tuple[str, str | None]] = {
    CloseoutOutcome.PERSISTED.value: (
        CognitiveItemStatus.CONSOLIDATED.value, CognitivePendingOutcomeType.CANDIDATE_CREATED.value),
    CloseoutOutcome.NO_MATERIAL.value: (
        CognitiveItemStatus.SKIPPED.value, CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value),
    CloseoutOutcome.NOT_APPLICABLE.value: (
        CognitiveItemStatus.SKIPPED.value, CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value),
    CloseoutOutcome.SKIPPED_NO_LLM_CONFIG.value: (
        CognitiveItemStatus.SKIPPED.value, CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value),
    CloseoutOutcome.EXTRACTOR_NOT_TRIGGERED.value: (
        CognitiveItemStatus.SKIPPED.value, CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value),
    CloseoutOutcome.EXTRACTOR_TRIGGERED_BUT_NOT_PERSISTED.value: (
        CognitiveItemStatus.FAILED.value, None),
}


def open_cognitive_closeout_pending(
    *,
    board_id: str,
    source_ref: str,
    artifact_type: str,
    store: CognitiveConsolidationItemStore | None = None,
    kg_generation_id: str | None = None,
) -> int:
    """Handler-side: open DURABLE cognitive closeout work in the existing ledger
    WITHOUT any graph write (safe inside the event drain). Returns the pending
    count materialised. The dedicated worker drains it later.

    #4 (codex): the generation is centralised here — latest_generation when it
    exists, a single stable id otherwise (never a fresh generation per item)."""
    store = store or _default_store()
    gen = _resolve_generation(store, board_id, kg_generation_id)
    materialized = store.materialize_from_marker(
        board_id=board_id, kg_generation_id=gen,
        event_ref="cognitive.closeout.pending",
        source_set=[{"source_ref": source_ref, "artifact_type": artifact_type}],
    )
    return int(materialized.item_count)


async def drain_cognitive_closeout_pending(
    db_factory,
    board_id: str,
    *,
    input_loader,
    store: CognitiveConsolidationItemStore | None = None,
    persister: CognitiveCandidatePersister | None = None,
    agent_id: str = "cognitive_closeout_worker",
    kg_generation_id: str | None = None,
) -> list[CloseoutResult]:
    """The dedicated cognitive worker: drains PENDING ledger items, runs the
    closeout OUTSIDE the event drain, and advances the SAME ledger
    pending→in_progress→consolidated/skipped/failed carrying the outcome.

    ``input_loader(board_id, item) -> dict`` returns the closeout inputs
    (spec_context / bug_* / llm_config / bug_probe / decision_ref) for an item —
    injectable so the worker is testable without the full SQL/graph load."""
    store = store or _default_store()
    persister = persister or ConsolidationPipelinePersister(db_factory, agent_id=agent_id)
    gen = _resolve_generation(store, board_id, kg_generation_id)

    # Drain fresh pending AND in_progress left by a worker that crashed mid-tick
    # (recoverable; persistence is idempotent via already_persisted).
    pending = [
        item for item in store.list_items(board_id, gen)
        if item.status in (CognitiveItemStatus.PENDING.value,
                           CognitiveItemStatus.IN_PROGRESS.value)
    ]
    results: list[CloseoutResult] = []
    for item in pending:
        store.update_item(
            board_id=board_id, kg_generation_id=gen, item_id=item.item_id,
            new_status=CognitiveItemStatus.IN_PROGRESS.value, updated_by_agent_id=agent_id,
        )
        try:
            inputs = await input_loader(board_id, item)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("cognitive_closeout.input_load_failed item=%s err=%s", item.item_id, exc)
            store.update_item(
                board_id=board_id, kg_generation_id=gen, item_id=item.item_id,
                new_status=CognitiveItemStatus.FAILED.value, updated_by_agent_id=agent_id,
                reason=f"input_load_failed: {exc}")
            continue

        result = await run_cognitive_closeout(
            board_id=board_id, artifact_type=item.artifact_type,
            artifact_ref=item.source_ref, persister=persister, **inputs,
        )
        new_status, outcome_type = _LEDGER_STATUS.get(
            result.outcome, (CognitiveItemStatus.FAILED.value, None))
        store.update_item(
            board_id=board_id, kg_generation_id=gen, item_id=item.item_id,
            new_status=new_status, updated_by_agent_id=agent_id,
            outcome_type=outcome_type, reason=result.detail or result.outcome,
            evidence_refs=result.persisted_refs or None,
        )
        results.append(result)
    return results
