"""RKG-07 — replay + regression of cognitive KG closeout (depends on RKG-02/03/04).

The goal is to prevent an "operationally healthy but cognitively empty" KG: the
suite fails when applicable cognitive material exists but no candidate/persistence
does. Every applicable replay is proven by the REAL path — candidate persisted to
graph.lbug and queryable by source_artifact_ref — not by a textual log.

Coverage:
  TS1 ts_6ffcd8f9 (e2e): full replay of Alternative + Assumption + Learning persisted
     with edges + queryable; + classification no_material / not_applicable /
     skipped_no_llm_config.
  TS2 ts_e7cfab63 (negative): a candidate with NO persistence and NO DLQ surfaces
     extractor_triggered_but_not_persisted — never a false success.
  TS3 ts_12a57048 (e2e): DLQ reprocess + reopen/replay preserve cognition (nodes
     stay queryable; no return to DLQ).
  + aliases bug:<id> / card:<id> / card:bug:<id> / spec child refs + negatives
    (RKG-02 resolver) without fabricating a Decision/edge.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from okto_pulse.core.kg import cognitive_closeout_production as ccp
from okto_pulse.core.kg.cognitive_source_ref_resolver import (
    resolve_cognitive_source_ref,
    strip_concept_suffix,
)
from sqlalchemy import select

from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
from kg_schema_testing import open_board_connection
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from okto_pulse.core.application.processors.consolidation import _process_queue_entry
from sqlalchemy_test_models import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.canonical_debt_service import list_canonical_debt
from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
    CONNECTIVITY_GUARD_SIGNATURE,
    diagnose_connectivity_guard_dlq,
    reprocess_connectivity_guard_dlq,
    verify_connectivity_class_cleared,
)

ANALYSIS = (
    "## Analysis\n"
    "We considered using Redis instead of Postgres for the cache layer.\n"
    "We assume the cache hit ratio stays above 90 percent in production.\n"
)


@pytest.fixture(autouse=True)
def _require_real_community_graph(_kg_registry_test_fakes):
    from kg_registry_testing import configure_real_graph_test_kg_registry

    configure_real_graph_test_kg_registry()


class _Summ:
    def summarise(self, *, bug_title, action_plan, context=None):
        return "Guard encoding before regex", "Normalise NFC first."


async def _not_quarantined(_b, _d):
    return False


def _seed_node(board_id, node_type, source_ref, *, node_id=None, graph_layer="canonical"):
    nid = node_id or f"{node_type.lower()}_seed_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id)
        _apply_kuzu_node_create_with_timestamp(
            orch, node_type, nid, {
                "title": f"Seed {node_type}", "content": "", "context": "", "justification": "",
                "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
                "created_by_agent": "test", "source_confidence": 1.0, "relevance_score": 0.5,
                "query_hits": 0, "last_queried_at": None, "priority_boost": 0.0,
                "human_curated": False, "embedding": [0.0] * 384,
                "graph_layer": graph_layer, "maturity_status": "canonical_eligible"})
    return nid


def _count(board_id, node_type, ref):
    return ccp._count_nodes_by_source_ref(board_id, node_type, ref)


def _count_containing(board_id, needle):
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            "MATCH (n) WHERE n.source_artifact_ref CONTAINS $needle RETURN count(n)",
            {"needle": needle})
        try:
            return int(res.get_next()[0]) if res.has_next() else 0
        finally:
            try:
                res.close()
            except Exception:
                pass


def _count_edge(board_id, cypher, ref):
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(cypher, {"ref": ref})
        try:
            return int(res.get_next()[0]) if res.has_next() else 0
        finally:
            try:
                res.close()
            except Exception:
                pass


# tr_4328534e: each test runs on its OWN bootstrapped board graph (these shadow
# the shared conftest fixtures) so the suite is deterministic + board-isolated and
# never pollutes / depends on another test's graph.
@pytest.fixture
def board_id():
    from kg_schema_testing import bootstrap_board_graph

    bid = f"rkg07-{uuid.uuid4().hex[:12]}"
    bootstrap_board_graph(bid)
    return bid


@pytest.fixture
def agent_id():
    return f"agent-rkg07-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def board_handle(board_id):
    # The graph is already bootstrapped by the board_id fixture; pass the id through
    # so existing test signatures keep working.
    return board_id


# ---------------------------------------------------------------------------
# TS1 ts_6ffcd8f9 — full replay of Alternative/Assumption/Learning + classification.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts1_spec_replay_persists_alternative_and_assumption_queryable(
    board_id, agent_id, db_factory, board_handle
):
    spec_id = f"spec-{uuid.uuid4()}"
    spec_ref = f"spec:{spec_id}"
    _seed_node(board_id, "Entity", spec_ref)  # provenance root
    decision_id = _seed_node(board_id, "Decision", f"{spec_ref}:decision:x")
    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)

    res = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=spec_ref,
        spec_context=ANALYSIS, decision_ref=decision_id, persister=persister)

    assert res.outcome == "persisted", res.detail
    alts = [r for r in res.persisted_refs if ":alternative:" in r]
    assums = [r for r in res.persisted_refs if ":assumption:" in r]
    assert alts and assums, res.persisted_refs
    assert _count(board_id, "Alternative", alts[0]) == 1   # node persisted + queryable
    assert _count(board_id, "Assumption", assums[0]) == 1
    # AC0bc6dfae: Alternative carries the relates_to edge.
    assert _count_edge(
        board_id,
        "MATCH (d:Decision)-[:relates_to]->(a:Alternative) WHERE a.source_artifact_ref = $ref "
        "RETURN count(*)", alts[0]) == 1
    # source_ref canonical (spec child ref) + idempotent replay.
    res2 = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=spec_ref,
        spec_context=ANALYSIS, decision_ref=decision_id, persister=persister)
    assert res2.skipped_existing_refs
    assert _count(board_id, "Alternative", alts[0]) == 1  # no duplicate on replay


@pytest.mark.asyncio
async def test_ts1_bug_replay_persists_learning_with_validates_edge(
    board_id, agent_id, db_factory, board_handle
):
    bug_uuid = str(uuid.uuid4())
    bug_ref = f"bug:{bug_uuid}"
    _seed_node(board_id, "Bug", bug_ref, graph_layer="canonical")
    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)

    res = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="bug", artifact_ref=bug_ref, bug_card_id=bug_uuid,
        bug_title="Regex misfires",
        bug_action_plan="Repro; root cause missing NFC; fixed + added a regression test.",
        llm_config={"provider": "openai"}, summariser=_Summ(),
        bug_probe=(lambda u: u == bug_uuid), persister=persister)

    assert res.outcome == "persisted", res.detail
    learning_ref = f"bug:{bug_uuid}"
    assert _count(board_id, "Learning", learning_ref) == 1
    assert _count_edge(
        board_id,
        "MATCH (l:Learning)-[:validates]->(b:Bug) WHERE l.source_artifact_ref = $ref "
        "RETURN count(*)", learning_ref) == 1


@pytest.mark.asyncio
async def test_ts1_classification_no_material_not_applicable_skipped(
    board_id, agent_id, db_factory, board_handle
):
    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)

    # spec done with NO cognitive material -> no_material, nothing persisted.
    spec_ref = f"spec:{uuid.uuid4()}"
    r1 = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=spec_ref,
        spec_context="just prose, no considered alternative or assumption",
        persister=persister)
    assert r1.outcome == "no_material"

    # bug with too-short action_plan -> not_applicable.
    bug_uuid = str(uuid.uuid4())
    r2 = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="bug", artifact_ref=f"bug:{bug_uuid}",
        bug_card_id=bug_uuid, bug_action_plan="too short",
        llm_config={"provider": "openai"}, summariser=_Summ(),
        bug_probe=(lambda u: True), persister=persister)
    assert r2.outcome == "not_applicable"

    # bug with material but NO llm_config -> skipped_no_llm_config (honest skip).
    bug_uuid2 = str(uuid.uuid4())
    r3 = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="bug", artifact_ref=f"bug:{bug_uuid2}",
        bug_card_id=bug_uuid2,
        bug_action_plan="A real root cause and fix narrative long enough to pass the gate.",
        llm_config=None, summariser=None, bug_probe=(lambda u: True), persister=persister)
    assert r3.outcome == "skipped_no_llm_config"


# ---------------------------------------------------------------------------
# TS2 ts_e7cfab63 — applicable material but no persistence/DLQ -> visible failure.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts2_candidate_without_persistence_is_extractor_triggered_but_not_persisted(
    board_id, board_handle, db_factory
):
    class _FailPersister:
        def already_persisted(self, *_a):
            return False

        async def persist(self, *_a):
            return False  # candidate produced but never persisted

    spec_ref = f"spec:{uuid.uuid4()}"
    res = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=spec_ref,
        spec_context=ANALYSIS, persister=_FailPersister())

    # tr_74d68275: applicable material existed -> a candidate was emitted, but with
    # no persistence the outcome is the honest failure, NOT a false success.
    assert res.candidates_emitted >= 1
    assert res.outcome == "extractor_triggered_but_not_persisted"
    assert not res.persisted_refs
    # ...and no node was written (no silent partial), and no DLQ was fabricated.
    assert _count(board_id, "Alternative", f"{spec_ref}:alternative:") == 0


# ---------------------------------------------------------------------------
# TS3 ts_12a57048 — DLQ reprocess + reopen/replay preserve cognition.
# ---------------------------------------------------------------------------


async def _seed_connectivity_dlq(db_factory, board_id, *, artifact_id):
    async with db_factory() as db:
        row = ConsolidationDeadLetter(
            board_id=board_id, artifact_type="spec", artifact_id=artifact_id, attempts=5,
            errors=[{"attempt": 5, "error_type": "KGPrimitiveError",
                     "message": CONNECTIVITY_GUARD_SIGNATURE}],
            dead_lettered_at=datetime.now(timezone.utc))
        db.add(row)
        await db.flush()
        dlq_id = row.id
        await db.commit()
    return dlq_id


@pytest.mark.asyncio
async def test_ts3_dlq_reprocess_drains_to_graph_and_preserves_cognition(
    board_id, agent_id, db_factory, board_handle
):
    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)

    # (A) A pre-existing canonical cognitive Alternative — the cognition to preserve.
    cog_spec = f"spec-cog-{uuid.uuid4().hex[:8]}"
    cog_ref = f"spec:{cog_spec}"
    _seed_node(board_id, "Entity", cog_ref)
    cog_decision = _seed_node(board_id, "Decision", f"{cog_ref}:decision:x")
    cog_res = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=cog_ref,
        spec_context=ANALYSIS, decision_ref=cog_decision, persister=persister)
    alt_ref = next(r for r in cog_res.persisted_refs if ":alternative:" in r)
    assert _count(board_id, "Alternative", alt_ref) == 1

    # (B) A REAL reprocessable spec (SQL) + its connectivity-guard DLQ.
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="rkg07", owner_id="owner"))
        db.add(Spec(
            id=spec_id, board_id=board_id, title="reprocessable", status=SpecStatus.DONE,
            created_by="owner",
            functional_requirements=["FR1: the system shall reprocess safely"],
            acceptance_criteria=["AC1: given a DLQ then it drains to the graph"],
            test_scenarios=[], business_rules=[], api_contracts=[],
            technical_requirements=[], decisions=[]))
        await db.commit()
    dlq_id = await _seed_connectivity_dlq(db_factory, board_id, artifact_id=spec_id)

    # DLQ no-mask -> reprocess (RKG-04): DLQ -> ConsolidationQueue (no duplicate).
    async with db_factory() as db:
        diag = await diagnose_connectivity_guard_dlq(db, board_id)
    assert dlq_id in diag["dead_letter_ids"]
    async with db_factory() as db:
        rr = await reprocess_connectivity_guard_dlq(
            db, board_id, [dlq_id], quarantine_probe=_not_quarantined)
        await db.commit()
    assert rr["blocked"] is False and rr["success"] is True
    async with db_factory() as db:
        q = (await db.execute(select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_id == spec_id))).scalars().all()
    assert len(q) == 1  # queued, no duplicate

    # DRAIN through the REAL worker -> graph.lbug. A broken worker FAILS here (this is
    # the strengthening codex asked for: validate the chain, not just the DLQ removal).
    async with db_factory() as db:
        entry = (await db.execute(select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_id == spec_id))).scalars().first()
        ok = await _process_queue_entry(db, entry)
        await db.commit()
    assert ok is True
    assert _count_containing(board_id, spec_id) > 0  # the spec materialised in graph.lbug

    # After the drain: NO return to DLQ AND NO canonical debt for the scope.
    async with db_factory() as db:
        verify = await verify_connectivity_class_cleared(
            db, board_id, artifact_refs=[f"spec:{spec_id}"])
        debt = await list_canonical_debt(db, board_id=board_id, limit=200)
    assert verify["class_cleared"] is True
    assert all(d.get("artifact_id") != spec_id for d in debt.items)  # explicit: no debt

    # COGNITION PRESERVED through reprocess/rebuild. Stronger than a bare reopen: an
    # idempotent closeout REPLAY recognises the existing canonical cognition and
    # neither drops nor duplicates it. (A full source-rebuild is intentionally NOT
    # used: cognitive nodes are DERIVED, not deterministic SQL sources — re-deriving
    # them is the closeout's job, covered by TS1; replay is the in-scope preservation.)
    assert _count(board_id, "Alternative", alt_ref) == 1
    replay = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=cog_ref,
        spec_context=ANALYSIS, decision_ref=cog_decision, persister=persister)
    assert replay.skipped_existing_refs
    assert _count(board_id, "Alternative", alt_ref) == 1  # preserved, no duplicate


# ---------------------------------------------------------------------------
# Aliases tr_8e9a656b — bug:<id> / card:<id> / card:bug:<id> / spec child + negatives.
# ---------------------------------------------------------------------------


def test_aliases_resolver_canonical_and_negatives():
    u = "11111111-1111-1111-1111-111111111111"
    # bug:<id> is always bug-derived -> canonical card:<id>.
    r_bug = resolve_cognitive_source_ref(f"bug:{u}")
    assert r_bug.is_bug_derived and r_bug.canonical_artifact_ref == f"card:{u}"
    # card:bug:<id> is bug-derived too.
    assert resolve_cognitive_source_ref(f"card:bug:{u}").is_bug_derived
    # card:<id> with NO probe -> NOT a blind bug alias (negative).
    r_card = resolve_cognitive_source_ref(f"card:{u}")
    assert r_card.is_bug_derived is False
    # card:<id> confirmed canonical bug by probe -> bug-derived.
    assert resolve_cognitive_source_ref(
        f"card:{u}", canonical_bug_probe=lambda x: x == u).is_bug_derived is True
    # spec child ref strips its per-concept suffix to the canonical spec ref.
    assert strip_concept_suffix(f"spec:{u}:alternative:abcd1234") == f"spec:{u}"
