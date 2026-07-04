"""Behavioral tests for KG operational-signal visibility (spec 007d1308, card c5dec85a).

One test per spec test_scenario / board card:
    * ts_38419749 (card 23be1580) — cognitive pending listing payload carries the
      operational fields (generation_id, counts, status, artifact_type, source_ref, timestamps).
    * ts_c604a02b (card 72a66fa1) — DLQ listing payload carries total/items, dead_letter_id,
      artifact_type, artifact_id, attempts, error, dead_lettered_at.
    * ts_ae755801 (card e6207c4f) — KG Health points each operational signal at its correct
      drill-down tool via `health_issues[].drill_down_tool`.
    * ts_e9f74b4d (card 052c0e85) — cognitive pending, DLQ and canonical debt stay SEPARATE:
      each tool returns only its own domain and the counters are not merged (dec_68fd26a2).

Each test seeds REAL signals and exercises the real tool/health payload — no stubs, no
source-text inspection.
"""

from __future__ import annotations

import json
import uuid

import pytest
import pytest_asyncio

from okto_pulse.core.kg.rebuild_audit import (
    CognitivePendingMarker,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.mcp.kg_tools import register_kg_tools
from okto_pulse.core.models.db import (
    Board,
    CanonicalDebt,
    ConsolidationDeadLetter,
)
from okto_pulse.core.services.canonical_debt_service import (
    list_canonical_debt,
    upsert_canonical_debt,
)
from okto_pulse.core.services.dead_letter_inspector_service import (
    list_dead_letter_rows,
)
from okto_pulse.core.services.kg_health_service import get_kg_health


# ---------------------------------------------------------------------------
# Fixtures + seed helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def rebuild_base(tmp_path, monkeypatch):
    """Isolated file-backed cognitive-pending store for this test."""
    target = tmp_path / "opviz"
    target.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


def _row(artifact_type: str, id_: str) -> dict:
    return {"artifact_type": artifact_type, "id": id_, "source_ref": f"{artifact_type}:{id_}"}


def _seed_pending(base_dir, board_id: str, sources: list[dict]) -> str:
    del base_dir
    gen = generate_kg_generation_id()
    CognitivePendingMarker(
        artifact_store=require_rebuild_audit_artifact_store()
    ).mark_for_generation(
        board_id=board_id,
        kg_generation_id=gen,
        source_set=sources,
        event_ref="evt_opviz",
    )
    return gen


async def _insert_dlq_row(db, board_id: str, artifact_id: str) -> str:
    row_id = f"dlq_{uuid.uuid4().hex[:10]}"
    db.add(
        ConsolidationDeadLetter(
            id=row_id,
            board_id=board_id,
            artifact_type="spec",
            artifact_id=artifact_id,
            original_queue_id=f"q-{uuid.uuid4().hex[:6]}",
            attempts=3,
            errors=[
                {
                    "attempt": 1,
                    "occurred_at": "2026-04-27T10:00:00",
                    "error_type": "TestError",
                    "message": "boom",
                    "traceback": None,
                }
            ],
        )
    )
    await db.flush()
    return row_id


def _register_pending_tool():
    class _Agent:
        id = "agent-test-001"

    class _NullDb:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    class _Reg:
        def __init__(self):
            self.tools = {}

        def tool(self):
            def _d(fn):
                self.tools[fn.__name__] = fn
                return fn

            return _d

    async def _get_agent():
        return _Agent()

    reg = _Reg()
    register_kg_tools(reg, get_agent=_get_agent, get_uow=lambda: _NullDb())
    return reg.tools["okto_pulse_kg_list_cognitive_pending_items"]


@pytest_asyncio.fixture
async def opviz_board(db_factory):
    """Board row + clean DLQ/CanonicalDebt state for the health test."""
    board_id = "board-opviz-health"
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="opviz-health", owner_id="user-opviz"))
            await db.commit()
        await db.execute(
            ConsolidationDeadLetter.__table__.delete().where(
                ConsolidationDeadLetter.board_id == board_id
            )
        )
        await db.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == board_id
            )
        )
        await db.commit()
    return board_id


# ---------------------------------------------------------------------------
# ts_38419749 (card 23be1580) — pending payload operational fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_pending_payload_has_operational_fields(rebuild_base):
    board_id = f"board-pending-{uuid.uuid4().hex[:8]}"
    gen = _seed_pending(rebuild_base, board_id, [_row("refinement", "r1"), _row("spec", "s1")])

    tool = _register_pending_tool()
    resp = json.loads(await tool(board_id=board_id))

    # generation_id + counts at the top level
    assert resp["selected_kg_generation_id"] == gen
    counts = resp["counts"]
    assert counts["total"] == 2
    assert set(counts) >= {"pending", "in_progress", "consolidated", "skipped", "failed", "total"}

    assert len(resp["items"]) == 2
    for item in resp["items"]:
        assert item["status"]  # lifecycle status
        assert item["artifact_type"] in {"refinement", "spec"}
        assert item["source_ref"].startswith(item["artifact_type"] + ":")  # artifact_id/source_ref
        assert item["recorded_at"]  # timestamps
        assert "updated_at" in item


# ---------------------------------------------------------------------------
# ts_c604a02b (card 72a66fa1) — DLQ payload diagnostic fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dlq_payload_has_diagnostic_fields(db_factory):
    board_id = f"board-dlq-{uuid.uuid4().hex[:8]}"
    artifact_id = f"spec-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _insert_dlq_row(db, board_id, artifact_id)
        await db.commit()

    async with db_factory() as db:
        payload = await list_dead_letter_rows(db, board_id, limit=10, offset=0)

    # total + items (additive AC6 alias of rows)
    assert payload["total"] == 1
    assert "items" in payload
    assert payload["items"] == payload["rows"]
    item = payload["items"][0]
    assert item["dead_letter_id"] == item["id"]
    assert item["artifact_type"] == "spec"
    assert item["artifact_id"] == artifact_id
    assert item["attempts"] == 3
    # error: both the full errors[] history and the derived last_error/error_text
    assert item["errors"] and item["errors"][0]["message"] == "boom"
    assert item["last_error"] == "boom"
    assert item["error_text"] == "boom"
    assert item["dead_lettered_at"]  # timestamp


# ---------------------------------------------------------------------------
# ts_ae755801 (card e6207c4f) — KG Health points at the correct drill-down tool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kg_health_points_each_signal_at_its_drill_down_tool(
    rebuild_base, opviz_board, db_factory
):
    board_id = opviz_board
    # one signal of each operational domain
    _seed_pending(rebuild_base, board_id, [_row("refinement", "rp1")])
    async with db_factory() as db:
        await _insert_dlq_row(db, board_id, f"spec-{uuid.uuid4().hex[:8]}")
        await upsert_canonical_debt(
            db,
            board_id=board_id,
            artifact_type="spec",
            artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
            source_ref=f"spec:{uuid.uuid4().hex[:8]}",
            content_hash=uuid.uuid4().hex,
            target_status="done",
            canonical_state="failed",
        )
        await db.commit()

    async with db_factory() as db:
        health = await get_kg_health(board_id, db)

    tools_by_code = {
        issue["code"]: issue.get("drill_down_tool")
        for issue in health["health_issues"]
    }
    assert tools_by_code.get("dead_letter_backlog") == "okto_pulse_kg_dead_letter_list"
    assert tools_by_code.get("canonical_debt_open") == "okto_pulse_kg_canonical_debt_list"
    assert (
        tools_by_code.get("cognitive_consolidation_pending")
        == "okto_pulse_kg_list_cognitive_pending_items"
    )


# ---------------------------------------------------------------------------
# ts_e9f74b4d (card 052c0e85) — the three signals do NOT mix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operational_signals_stay_separate(rebuild_base, db_factory):
    board_id = f"board-sep-{uuid.uuid4().hex[:8]}"
    # one signal per domain, with DISTINCT artifact identifiers
    pending_ref = "refinement:pending-only"
    dlq_artifact = f"dlq-only-{uuid.uuid4().hex[:8]}"
    debt_artifact = f"debt-only-{uuid.uuid4().hex[:8]}"

    _seed_pending(rebuild_base, board_id, [_row("refinement", "pending-only")])
    async with db_factory() as db:
        await _insert_dlq_row(db, board_id, dlq_artifact)
        await upsert_canonical_debt(
            db,
            board_id=board_id,
            artifact_type="spec",
            artifact_id=debt_artifact,
            source_ref=f"spec:{debt_artifact}",
            content_hash=uuid.uuid4().hex,
            target_status="done",
            canonical_state="failed",
        )
        await db.commit()

    pending_tool = _register_pending_tool()
    pending = json.loads(await pending_tool(board_id=board_id))
    async with db_factory() as db:
        dlq = await list_dead_letter_rows(db, board_id, limit=50, offset=0)
        debt = await list_canonical_debt(db, board_id=board_id, limit=50, offset=0)

    # each tool returns ONLY its own domain — one row each, no cross-leak
    assert pending["counts"]["total"] == 1
    assert pending["items"][0]["source_ref"] == pending_ref
    assert all(dlq_artifact not in (i.get("source_ref") or "") for i in pending["items"])

    assert dlq["total"] == 1
    assert dlq["items"][0]["artifact_id"] == dlq_artifact
    assert all(i["artifact_id"] != debt_artifact for i in dlq["items"])

    assert debt.total == 1
    assert debt.items[0]["artifact_id"] == debt_artifact
    assert all(i["artifact_id"] != dlq_artifact for i in debt.items)

    # counters are per-domain buckets, never summed across domains
    assert pending["counts"]["total"] == 1
    assert dlq["total"] == 1
    assert debt.total == 1
