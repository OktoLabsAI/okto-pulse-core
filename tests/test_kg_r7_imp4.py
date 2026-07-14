"""R7 IMP4 — KG Health aggregate + read-only drilldown + human-only skip.

Teeth for spec 7e0a5a28 / card 434d8dcb (AC5/TS5, AC9/TS9):

ENFORCEMENT (human-only, fail-closed):
- an AGENT (actor_is_human=False) cannot skip/clear an R7 hold via the central
  service NOR via the direct-ledger MCP update tool; non-R7 skips still work;
- a HUMAN (actor_is_human=True) may skip an R7 hold (the documented human path);
- actor_is_human is set by the endpoint, never a client/agent-supplied field.

HEALTH (aggregate-only):
- canonical_partition_integrity is ONE aggregate health_issues[] entry with
  counts + drill_down_tool; NO per-node rows in Health; no double-count.

DRILLDOWN (read-only):
- the unified read model classifies the 4 sources + filters + 400/404 errors.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.canonical_learning_partition import HISTORICAL_DEBT_REASON
from okto_pulse.core.kg.canonical_partition_integrity import (
    STATUS_CANONICAL_DEBT,
    STATUS_COGNITIVE_PENDING,
    STATUS_MIXED_DEFERRED,
    STATUS_PROVENANCE_ONLY,
    get_canonical_partition_integrity_detail,
    list_canonical_partition_integrity,
)
from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
    CognitiveReasonCode,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.primitives import _apply_graph_node_create
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    compute_cognitive_item_id,
    record_cognitive_working_only_hold,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)
from sqlalchemy_test_models import Board
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

USER_ID = "user-r7-imp4"
HUMAN_ACTOR = "human-operator"
AGENT_ACTOR = "claude-coder"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch, _kg_registry_test_fakes):
    """Point every CognitiveConsolidationItemStore (service, health, read model,
    helpers) at an isolated tmp dir for the test."""
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.community.adapters.rebuild_audit_storage import (
        CommunityFileSystemRebuildAuditArtifactStore,
    )

    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    configure_test_kg_registry(
        rebuild_audit_artifact_store=(
            CommunityFileSystemRebuildAuditArtifactStore(tmp_path)
        )
    )
    return tmp_path


def _store(tmp_path) -> CognitiveConsolidationItemStore:
    return CognitiveConsolidationItemStore(base_dir=tmp_path)


def _service(tmp_path) -> CognitiveReadinessService:
    return CognitiveReadinessService(_store(tmp_path))


async def _setup_board(db_factory) -> str:
    from kg_schema_testing import bootstrap_board_graph

    board_id = f"r7imp4-{uuid.uuid4().hex[:12]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r7 imp4", owner_id=USER_ID))
            await db.commit()
    return board_id


def _make_r7_hold(board_id: str, tmp_path) -> tuple[str, str]:
    """Create a go-forward R7 HOLD (cognitive_pending item, reason=working_only)
    via the IMP1 helper. Returns (source_ref, generation_id)."""
    source_ref = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    result = record_cognitive_working_only_hold(
        board_id=board_id,
        hold_payload={
            "reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
            "source_ref": source_ref,
            "artifact_type": "bug",
            "observed_endpoints": [f"kg:somebug@{GRAPH_LAYER_WORKING}"],
            "session_id": "sess-imp4",
        },
        actor_id="system:historical_consolidation",
        base_dir=tmp_path,
    )
    assert result is not None
    return source_ref, result["generation_id"]


def _make_plain_pending(board_id: str, tmp_path) -> tuple[str, str]:
    """Materialize a NON-R7 pending item (reason_code None)."""
    store = _store(tmp_path)
    gen = store.latest_generation(board_id) or "gen-nonr7"
    source_ref = f"bug:{uuid.uuid4()}"
    store.materialize_from_marker(
        board_id=board_id,
        kg_generation_id=gen,
        event_ref="imp4-nonr7",
        source_set=[{"source_ref": source_ref, "artifact_type": "bug"}],
    )
    return source_ref, gen


def _node_attrs(source_ref, graph_layer, maturity):
    return {
        "title": "imp4 seed", "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "test", "source_confidence": 1.0, "relevance_score": 0.5,
        "query_hits": 0, "last_queried_at": None, "priority_boost": 0.0,
        "human_curated": False, "embedding": [0.0] * 384,
        "graph_layer": graph_layer, "maturity_status": maturity,
    }


def _seed_learning_with_bugs(board_id, *, source_ref, canonical=0, working=0) -> str:
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"r7i4l_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_graph_node_create(
            orch, "Learning", learning_id,
            _node_attrs(source_ref, GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE),
        )
        for _ in range(canonical):
            bug_id = f"r7i4cb_{uuid.uuid4().hex[:10]}"
            _apply_graph_node_create(
                orch, "Bug", bug_id,
                _node_attrs(f"bug:{bug_id}", GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE),
            )
            orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                             attrs={"confidence": 1.0}, from_type="Learning", to_type="Bug")
        for _ in range(working):
            bug_id = f"r7i4wb_{uuid.uuid4().hex[:10]}"
            _apply_graph_node_create(
                orch, "Bug", bug_id,
                _node_attrs(f"bug:{bug_id}", GRAPH_LAYER_WORKING, MATURITY_WORKING_IMMATURE),
            )
            orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                             attrs={"confidence": 0.9}, from_type="Learning", to_type="Bug")
    return learning_id


# ---------------------------------------------------------------------------
# AC9 / TS9 — human-only enforcement (service + direct-ledger MCP update tool)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_cannot_skip_r7_hold_via_service(db_factory, _tmp_rebuild_dir):
    board_id = await _setup_board(db_factory)
    source_ref, _gen = _make_r7_hold(board_id, _tmp_rebuild_dir)
    service = _service(_tmp_rebuild_dir)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await service.record_cognitive_skip(
                db, board_id=board_id, source_ref=source_ref,
                reason_code=CognitiveReasonCode.TRIVIAL_FIX.value,
                actor=AGENT_ACTOR, actor_is_human=False,
            )
    assert exc.value.code == "human_only_reason_code"
    assert exc.value.http_status == 403
    # The hold is untouched (still pending with the R7 reason).
    items = _store(_tmp_rebuild_dir).list_items(board_id, _gen)
    held = next(i for i in items if i.source_ref == source_ref)
    assert held.status == "pending"
    assert held.reason_code == CANONICAL_LEARNING_WORKING_ONLY_REASON


@pytest.mark.asyncio
async def test_human_can_skip_r7_hold_via_service(db_factory, _tmp_rebuild_dir):
    board_id = await _setup_board(db_factory)
    source_ref, _gen = _make_r7_hold(board_id, _tmp_rebuild_dir)
    service = _service(_tmp_rebuild_dir)
    async with db_factory() as db:
        item = await service.record_cognitive_skip(
            db, board_id=board_id, source_ref=source_ref,
            reason_code=CognitiveReasonCode.TRIVIAL_FIX.value,
            actor=HUMAN_ACTOR, actor_is_human=True,
        )
    assert item.status == "skipped"


@pytest.mark.asyncio
async def test_agent_skip_non_r7_item_still_works(db_factory, _tmp_rebuild_dir):
    board_id = await _setup_board(db_factory)
    source_ref, _gen = _make_plain_pending(board_id, _tmp_rebuild_dir)
    service = _service(_tmp_rebuild_dir)
    async with db_factory() as db:
        item = await service.record_cognitive_skip(
            db, board_id=board_id, source_ref=source_ref,
            reason_code=CognitiveReasonCode.TRIVIAL_FIX.value,
            actor=AGENT_ACTOR, actor_is_human=False,
        )
    assert item.status == "skipped"  # non-R7 unaffected by the human-only gate


class _MCPRegistryDouble:
    """Captures @mcp.tool()-decorated functions by name (no real FastMCP)."""

    def __init__(self) -> None:
        self.tools: dict[str, object] = {}

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _decorator


def _register_update_tool(agent_id: str):
    """Register the kg tools with a FAKE agent identity (agent-facing path) and
    return the direct-ledger update tool fn."""
    from okto_pulse.core.mcp.kg_tools import register_kg_tools

    class _Agent:
        id = agent_id

    async def _get_agent():
        return _Agent()

    class _NullDb:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    def _get_db():
        return _NullDb()

    mcp = _MCPRegistryDouble()
    register_kg_tools(mcp, get_agent=_get_agent, get_uow=_get_db)
    return mcp.tools["okto_pulse_kg_update_cognitive_pending_item"]


@pytest.mark.asyncio
async def test_mcp_update_tool_blocks_skip_of_r7_hold(db_factory, _tmp_rebuild_dir):
    import json

    board_id = await _setup_board(db_factory)
    source_ref, gen = _make_r7_hold(board_id, _tmp_rebuild_dir)
    item_id = compute_cognitive_item_id(board_id, gen, source_ref)

    tool = _register_update_tool(AGENT_ACTOR)
    payload = json.loads(await tool(
        board_id=board_id, kg_generation_id=gen, item_id=item_id,
        status="skipped", reason="agent tries to mask the R7 hold",
    ))
    assert payload["error"]["code"] == "human_only_reason_code"
    # The hold is untouched.
    held = next(
        i for i in _store(_tmp_rebuild_dir).list_items(board_id, gen)
        if i.item_id == item_id
    )
    assert held.status == "pending"


@pytest.mark.asyncio
async def test_mcp_update_tool_allows_non_r7_skip(db_factory, _tmp_rebuild_dir):
    import json

    board_id = await _setup_board(db_factory)
    source_ref, gen = _make_plain_pending(board_id, _tmp_rebuild_dir)
    item_id = compute_cognitive_item_id(board_id, gen, source_ref)

    tool = _register_update_tool(AGENT_ACTOR)
    payload = json.loads(await tool(
        board_id=board_id, kg_generation_id=gen, item_id=item_id,
        status="skipped", reason="non-R7 safe skip stays working",
    ))
    assert payload.get("updated") is True
    assert payload.get("error") is None


# ---------------------------------------------------------------------------
# AC5 / TS5 — Health aggregate (no per-node, no double-count)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_exposes_aggregate_partition_integrity(db_factory, _tmp_rebuild_dir):
    from okto_pulse.core.kg.canonical_learning_partition import PARTITION_TARGET_STATUS
    from okto_pulse.core.services.kg_health_service import get_kg_health

    board_id = await _setup_board(db_factory)
    # one R7 go-forward hold (cognitive_pending) ...
    _make_r7_hold(board_id, _tmp_rebuild_dir)
    # ... and one R7 historical debt.
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug", artifact_id="bug-hist",
            source_ref=f"card:bug:{uuid.uuid4()}:learning:x",
            content_hash="clp_hist", target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending", failure_reason=HISTORICAL_DEBT_REASON,
        )
        await db.commit()
        health = await get_kg_health(board_id, db)

    issues = [
        i for i in health.get("health_issues", [])
        if i.get("code") == "canonical_partition_integrity"
    ]
    assert len(issues) == 1, issues  # exactly ONE aggregate entry
    issue = issues[0]
    assert issue["drill_down_tool"] == "okto_pulse_kg_canonical_partition_integrity_list"
    assert issue["counts"]["cognitive_pending"] == 1
    assert issue["counts"]["canonical_debt"] == 1
    assert "precedence_explanation" in issue
    # AGGREGATE ONLY — no per-node rows leak into Health.
    assert "items" not in issue
    assert "node_id" not in issue


# ---------------------------------------------------------------------------
# Drilldown read model — classification + filters + errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drilldown_lists_four_sources(db_factory, _tmp_rebuild_dir):
    from okto_pulse.core.kg.canonical_learning_partition import PARTITION_TARGET_STATUS

    board_id = await _setup_board(db_factory)
    hold_ref, _g = _make_r7_hold(board_id, _tmp_rebuild_dir)
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug", artifact_id="bug-d",
            source_ref=f"card:bug:{uuid.uuid4()}:learning:d",
            content_hash="clp_d", target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending", failure_reason=HISTORICAL_DEBT_REASON,
        )
        await db.commit()
    # graph: a MIXED Learning (canonical + working) and a provenance-only one.
    _seed_learning_with_bugs(
        board_id, source_ref=f"card:bug:{uuid.uuid4()}:learning:mix",
        canonical=1, working=1,
    )
    _seed_learning_with_bugs(
        board_id, source_ref=f"learning:provenance:{uuid.uuid4()}",
        canonical=0, working=0,
    )

    async with db_factory() as db:
        result = await list_canonical_partition_integrity(db, board_id=board_id)

    counts = result["counts"]
    assert counts[STATUS_COGNITIVE_PENDING] == 1
    assert counts[STATUS_CANONICAL_DEBT] == 1
    assert counts[STATUS_MIXED_DEFERRED] == 1
    assert counts[STATUS_PROVENANCE_ONLY] == 1
    assert result["health_issue_code"] == "canonical_partition_integrity"
    statuses = {i["status"] for i in result["items"]}
    assert statuses == {
        STATUS_COGNITIVE_PENDING, STATUS_CANONICAL_DEBT,
        STATUS_MIXED_DEFERRED, STATUS_PROVENANCE_ONLY,
    }
    # filter by status returns only that bucket.
    async with db_factory() as db:
        only_pending = await list_canonical_partition_integrity(
            db, board_id=board_id, status=STATUS_COGNITIVE_PENDING,
        )
    assert {i["status"] for i in only_pending["items"]} == {STATUS_COGNITIVE_PENDING}
    assert only_pending["items"][0]["source_artifact_ref"] == hold_ref


@pytest.mark.asyncio
async def test_drilldown_invalid_filter_400(db_factory, _tmp_rebuild_dir):
    board_id = await _setup_board(db_factory)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await list_canonical_partition_integrity(
                db, board_id=board_id, reason_code="not_a_real_reason",
            )
    assert exc.value.code == "invalid_filter"
    assert exc.value.http_status == 400


@pytest.mark.asyncio
async def test_drilldown_detail_not_found_404(db_factory, _tmp_rebuild_dir):
    board_id = await _setup_board(db_factory)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await get_canonical_partition_integrity_detail(
                db, board_id=board_id, node_id="does-not-exist",
            )
    assert exc.value.code == "canonical_partition_item_not_found"
    assert exc.value.http_status == 404


@pytest.mark.asyncio
async def test_drilldown_detail_mixed_evidence(db_factory, _tmp_rebuild_dir):
    board_id = await _setup_board(db_factory)
    node_id = _seed_learning_with_bugs(
        board_id, source_ref=f"card:bug:{uuid.uuid4()}:learning:mixdetail",
        canonical=1, working=1,
    )
    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["status"] == STATUS_MIXED_DEFERRED
    assert len(detail["canonical_edges"]) == 1
    assert len(detail["working_edges"]) == 1
    assert detail["working_edges"][0]["to_graph_layer"] == GRAPH_LAYER_WORKING


# ---------------------------------------------------------------------------
# OR1 — dedicated metric kg_canonical_partition_integrity_total
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_or1_metric_queryable_by_reason_and_layer(db_factory, _tmp_rebuild_dir):
    from okto_pulse.core.kg.canonical_partition_integrity import (
        get_canonical_partition_integrity_count,
        get_canonical_partition_integrity_counter_labels,
        reset_canonical_partition_integrity_counter,
    )

    board_id = await _setup_board(db_factory)
    _make_r7_hold(board_id, _tmp_rebuild_dir)  # cognitive_pending / working_only / canonical
    _seed_learning_with_bugs(
        board_id, source_ref=f"card:bug:{uuid.uuid4()}:learning:m", canonical=1, working=1,
    )  # mixed_evidence_deferred / canonical

    reset_canonical_partition_integrity_counter()
    async with db_factory() as db:
        await list_canonical_partition_integrity(db, board_id=board_id)

    # Bounded labels (no free text) and queryable by reason_code + graph_layer.
    assert "reason_code" in get_canonical_partition_integrity_counter_labels()
    assert "graph_layer" in get_canonical_partition_integrity_counter_labels()
    assert get_canonical_partition_integrity_count(
        reason_code=CANONICAL_LEARNING_WORKING_ONLY_REASON, board_id=board_id,
    ) == 1
    assert get_canonical_partition_integrity_count(
        status=STATUS_MIXED_DEFERRED, board_id=board_id,
    ) == 1
    assert get_canonical_partition_integrity_count(
        graph_layer=GRAPH_LAYER_CANONICAL, board_id=board_id,
    ) >= 2
