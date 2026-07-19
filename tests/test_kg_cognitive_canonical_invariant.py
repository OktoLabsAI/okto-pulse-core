"""Behavioral tests for the cognitive canonical invariant (spec 007d1308).

One test per spec test_scenario / board card:
    * ts_4fa16fe7 (card 5ba8555a) — cognitive working fails WITHOUT mutating the session.
    * ts_220ffd12 (card ef45ac29) — accepted cognitive candidate persists canonical + canonical_eligible.
    * ts_a26843c2 (card 57f87392) — deterministic worker still emits working for an immature source
      and is NOT blocked by the cognitive guard (system:* exemption / FR5).
    * ts_4c076331 (card 567f538e) — the MCP tool error is actionable (typed code + message, no
      generic traceback).

Each test exercises the scenario ``then`` against the real primitive / MCP wrapper — no stubs,
no source-text inspection. Tests 1/2/4 fail against the pre-invariant code (working accepted,
maturity not normalized, MCP returns accepted) and pass after; test 3 guards the system:*
exemption so a regression that dropped it would fail.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    _apply_graph_node_create,
    _require_open_session,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
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
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    classify_source_for_kg,
)
from okto_pulse.core.mcp.kg_tools import register_kg_tools
from kg_registry_testing import configure_real_graph_test_kg_registry

COGNITIVE_CODE = "cognitive_node_candidates_must_be_canonical"
SYSTEM_WORKER = "system:layer1_worker"


# ---------------------------------------------------------------------------
# Helpers (graph seeding + querying, mirrored from the connectivity-guard suite)
# ---------------------------------------------------------------------------


def _seed_node(kconn, orch, node_type: str, node_id: str, source_ref: str) -> None:
    _apply_graph_node_create(
        orch,
        node_type,
        node_id,
        {
            "title": f"Seed {node_type}",
            "content": "",
            "context": "",
            "justification": "",
            "source_artifact_ref": source_ref,
            "created_at": "2026-06-08T00:00:00+00:00",
            "created_by_agent": "test",
            "source_confidence": 1.0,
            "relevance_score": 0.5,
            "query_hits": 0,
            "last_queried_at": None,
            "priority_boost": 0.0,
            "human_curated": False,
            "embedding": [0.0] * 384,
        },
    )


def _seed_spec_root_and_decision(board_id: str, spec_ref: str) -> tuple[str, str]:
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    root_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    decision_id = f"decision_seed_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Entity", root_id, spec_ref)
        _seed_node(kconn, orch, "Decision", decision_id, f"{spec_ref}:decision:seed")
        orch.create_edge(
            edge_type="belongs_to",
            from_id=decision_id,
            to_id=root_id,
            attrs={"confidence": 1.0},
            from_type="Decision",
            to_type="Entity",
        )
    return root_id, decision_id


def _get_decision_layer_maturity(
    board_id: str, source_ref: str,
) -> tuple[str | None, str | None]:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Decision) WHERE n.source_artifact_ref = $ref "
            "RETURN coalesce(n.graph_layer, 'legacy_unknown'), n.maturity_status",
            {"ref": source_ref},
        )
        try:
            if res.has_next():
                row = res.get_next()
                return row[0], row[1]
        finally:
            try:
                res.close()
            except Exception:
                pass
    return None, None


async def _seed_spec_root_and_decision_async(
    board_id: str,
    spec_ref: str,
) -> tuple[str, str]:
    return await run_blocking_graph_io(
        lambda: _seed_spec_root_and_decision(board_id, spec_ref),
        task_name="tests.cognitive_canonical.seed_spec_root_and_decision",
    )


async def _get_decision_layer_maturity_async(
    board_id: str,
    source_ref: str,
) -> tuple[str | None, str | None]:
    return await run_blocking_graph_io(
        lambda: _get_decision_layer_maturity(board_id, source_ref),
        task_name="tests.cognitive_canonical.get_decision_layer_maturity",
    )


# ---------------------------------------------------------------------------
# ts_4fa16fe7 (card 5ba8555a) — cognitive working fails WITHOUT mutating session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_working_candidate_rejected_without_mutating_session(
    board_id, agent_id, db_factory, board_handle,
):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content="cognitive working rejection",
            ),
            agent_id=agent_id,
            db=db,
        )

    with pytest.raises(KGPrimitiveError) as exc_info:
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id="cog_working",
                    node_type=KGNodeType.DECISION,
                    title="Working decision from cognitive agent",
                    graph_layer="working",
                ),
            ),
            agent_id=agent_id,
        )

    assert exc_info.value.code == COGNITIVE_CODE
    assert "canonical" in exc_info.value.message.lower()
    assert exc_info.value.details["graph_layer"] == "working"

    # then: the rejected candidate left NO trace in the session (fail-closed
    # BEFORE mutation, not a post-hoc reject).
    session = await _require_open_session(begin.session_id, agent_id)
    assert "cog_working" not in session.node_candidates
    assert len(session.node_candidates) == 0


# ---------------------------------------------------------------------------
# ts_220ffd12 (card ef45ac29) — accepted cognitive persists canonical_eligible
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_accepted_cognitive_candidate_persists_canonical_eligible(
    board_id, agent_id, db_factory, board_handle,
):
    configure_real_graph_test_kg_registry()
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    spec_ref = f"spec:{spec_id}"
    _root_id, existing_decision_id = await _seed_spec_root_and_decision_async(
        board_id, spec_ref
    )
    decision_ref = f"{spec_ref}:decision:canon"

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                raw_content="canonical persistence",
            ),
            agent_id=agent_id,
            db=db,
        )

    # graph_layer omitted (defaults canonical) but maturity_status is set to a
    # NON-canonical value on purpose: the invariant must force it to
    # canonical_eligible on persist (FR3 / dec_26c5cc2d). Before the invariant
    # the working_immature value survived to the committed node.
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="cog_canonical",
                node_type=KGNodeType.DECISION,
                title="Canonical decision",
                source_artifact_ref=decision_ref,
                maturity_status="working_immature",
            ),
        ),
        agent_id=agent_id,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="cog_canonical_depends",
                edge_type=KGEdgeType.DEPENDS_ON,
                from_candidate_id="cog_canonical",
                to_candidate_id=f"kg:{existing_decision_id}",
                confidence=0.8,
                layer="cognitive",
                rule_id="test/cognitive_canonical_persist",
            ),
        ),
        agent_id=agent_id,
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
        force_reprocess=True,
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    assert commit.connectivity["passed"] is True
    layer, maturity = await _get_decision_layer_maturity_async(
        board_id, decision_ref
    )
    assert layer == "canonical"
    assert maturity == MATURITY_CANONICAL_ELIGIBLE


# ---------------------------------------------------------------------------
# ts_a26843c2 (card 57f87392) — deterministic worker still emits working
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deterministic_working_source_not_blocked_by_cognitive_guard(
    board_id, agent_id, db_factory, board_handle,
):
    # (a) source_maturity policy is untouched: immature (non-done) sources stay working.
    story = classify_source_for_kg(
        artifact_type="story", artifact_status="active", content_hash="h1",
    )
    assert story.graph_layer == GRAPH_LAYER_WORKING
    spec_draft = classify_source_for_kg(
        artifact_type="spec", artifact_status="draft", content_hash="h2",
    )
    assert spec_draft.graph_layer == GRAPH_LAYER_WORKING

    # (b) the deterministic worker (system:* agent_id) is EXEMPT from the
    # cognitive canonical guard: its working node is accepted and PRESERVED as
    # working (not normalized to canonical). A regression that dropped the
    # system:* exemption would raise here / flip the layer to canonical.
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="story",
                artifact_id=f"story-{uuid.uuid4().hex[:8]}",
                raw_content="immature deterministic source",
            ),
            agent_id=SYSTEM_WORKER,
            db=db,
        )

    resp = await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="det_working",
                node_type=KGNodeType.LEARNING,
                title="Deterministic working node",
                graph_layer="working",
                maturity_status="working_immature",
            ),
        ),
        agent_id=SYSTEM_WORKER,
    )
    assert resp.accepted is True

    session = await _require_open_session(begin.session_id, SYSTEM_WORKER)
    stored = session.node_candidates["det_working"]
    assert stored.graph_layer == "working"
    assert stored.maturity_status == "working_immature"


# ---------------------------------------------------------------------------
# ts_4c076331 (card 567f538e) — MCP error is actionable (typed code, no traceback)
# ---------------------------------------------------------------------------


class _FakeAgent:
    def __init__(self, agent_id: str) -> None:
        self.id = agent_id


class _MCPRegistryDouble:
    """Tiny in-memory double of FastMCP capturing @mcp.tool() closures."""

    def __init__(self) -> None:
        self.tools: dict = {}

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _decorator


def _register_add_node_tool(agent_id: str):
    class _NullDb:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    async def _get_agent():
        return _FakeAgent(agent_id)

    mcp = _MCPRegistryDouble()
    register_kg_tools(mcp, get_agent=_get_agent, get_uow=lambda: _NullDb())
    return mcp.tools["okto_pulse_kg_add_node_candidate"]


@pytest.mark.asyncio
async def test_mcp_cognitive_working_error_is_actionable(
    board_id, agent_id, db_factory, board_handle,
):
    import json

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content="mcp actionable error",
            ),
            agent_id=agent_id,
            db=db,
        )

    tool = _register_add_node_tool(agent_id)
    raw = await tool(
        session_id=begin.session_id,
        candidate={
            "candidate_id": "cog_mcp_working",
            "node_type": "Decision",
            "title": "Working decision via MCP",
            "graph_layer": "working",
        },
    )

    # then: typed/actionable error, NOT a generic internal failure or traceback.
    assert "Traceback" not in raw
    resp = json.loads(raw)
    assert "error" in resp
    err = resp["error"]
    assert err["code"] == COGNITIVE_CODE
    assert "canonical" in err["message"].lower()
    assert err["details"]["graph_layer"] == "working"
    assert err["details"]["required_graph_layer"] == "canonical"

    # the candidate was NOT accepted / stored.
    assert "accepted" not in resp
    session = await _require_open_session(begin.session_id, agent_id)
    assert "cog_mcp_working" not in session.node_candidates
