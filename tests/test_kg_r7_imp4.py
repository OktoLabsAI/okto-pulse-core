"""F4 preserves aggregate partition Health and human-only hold authority.

The detailed repair inspector is retired. Agent skip/clear restrictions and
legitimate human decisions still follow the cognitive readiness controls.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.canonical_learning_partition import HISTORICAL_DEBT_REASON
from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
    CognitiveReasonCode,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    compute_cognitive_item_id,
    record_cognitive_working_only_hold,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_WORKING,
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

    async def _get_board_agent(_board_id: str):
        return await _get_agent()

    mcp = _MCPRegistryDouble()
    register_kg_tools(
        mcp,
        get_agent=_get_agent,
        get_uow=_get_db,
        get_board_agent=_get_board_agent,
    )
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
    assert "drill_down_tool" not in issue
    assert issue["operator_action"] == "inspect_kg_health"
    assert issue["counts"]["cognitive_pending"] == 1
    assert issue["counts"]["canonical_debt"] == 1
    assert "precedence_explanation" in issue
    # AGGREGATE ONLY — no per-node rows leak into Health.
    assert "items" not in issue
    assert "node_id" not in issue


# ---------------------------------------------------------------------------
# Drilldown read model — classification + filters + errors
# ---------------------------------------------------------------------------










# ---------------------------------------------------------------------------
# OR1 — dedicated metric kg_canonical_partition_integrity_total
# ---------------------------------------------------------------------------
