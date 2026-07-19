"""RKG-05 — non-maskable technical KG signals in health/readiness.

Real integration tests: seed a real ConsolidationDeadLetter (technical signal) and
prove the canonical /kg/health-readiness projection + the kg_health summary
profile never mask it, that advisory distinguishes blocking vs would_block_done,
and that the OR counter okto_pulse_kg_cognitive_technical_signal_total fires.

Coverage:
  TS1 ts_802a9a69 (integration): health summary preserves DLQ + drill_down_tool.
  TS2 ts_fcf6300e (integration): advisory with a technical blocker -> blocking=true,
     would_block_done=false, not declared ready.
  TS3 ts_2ef50880 (integration): the technical signal is derived from health, so a
     cognitive skip/no_action cannot reduce or hide it.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import rebuild_audit
from okto_pulse.core.mcp.kg_query_safety import KGHealthMCPProjection
from sqlalchemy_test_models import Board, ConsolidationDeadLetter
from okto_pulse.core.services.kg_health_readiness_service import (
    InvalidProfileError,
    build_health_readiness,
)
from okto_pulse.core.services.kg_health_service import get_kg_health

_DLQ_TOOL = "okto_pulse_kg_dead_letter_list"


async def _seed_board_with_dlq(db_factory, *, artifact_id=None):
    board_id = f"rkg05-{uuid.uuid4().hex[:10]}"
    aid = artifact_id or uuid.uuid4().hex
    async with db_factory() as db:
        db.add(Board(id=board_id, name="rkg05", owner_id="owner"))
        await db.flush()
        db.add(ConsolidationDeadLetter(
            board_id=board_id, artifact_type="spec", artifact_id=aid, attempts=5,
            errors=[{
                "attempt": 5, "occurred_at": "2026-06-25T00:00:00+00:00",
                "error_type": "KGPrimitiveError",
                "message": "KG node connectivity guard rejected the commit before graph mutation",
                "traceback": None,
            }],
            dead_lettered_at=datetime.now(timezone.utc),
        ))
        await db.commit()
    return board_id, aid


@pytest.mark.asyncio
async def test_ts1_health_summary_preserves_dlq_and_drilldown(db_factory):
    board_id, _aid = await _seed_board_with_dlq(db_factory)

    # (a) the kg_health SUMMARY projection no longer drops operational_domains.
    async with db_factory() as db:
        health = await get_kg_health(board_id, db)
    assert health["dead_letter_count"] >= 1
    summary = KGHealthMCPProjection().project(health, profile="summary")
    assert summary["dead_letter_count"] >= 1  # scalar counter survives
    assert "operational_domains" in summary  # RKG-05 fix: not masked
    dl = summary["operational_domains"]["dead_letter"]
    assert dl["count"] >= 1
    assert dl["drill_down_tool"] == _DLQ_TOOL  # locatable without a DB query

    # (b) the canonical projection exposes the counter + drill-down in BOTH profiles.
    for profile in ("summary", "full"):
        async with db_factory() as db:
            hr = await build_health_readiness(board_id, db, profile=profile)
        assert hr["technical_signals"]["dead_letter_count"] >= 1
        assert hr["technical_signals"]["technical_dlq_count"] >= 1
        items = [i for i in hr["non_maskable_items"] if i["signal"] == "technical_dlq"]
        assert items and items[0]["drill_down_tool"] == _DLQ_TOOL
        assert items[0]["next_action"] and items[0]["remediation"]


@pytest.mark.asyncio
async def test_ts2_advisory_blocking_distinct_from_would_block_done(db_factory):
    board_id, _aid = await _seed_board_with_dlq(db_factory)
    # A fresh board: cognitive enforcement is advisory by default.
    async with db_factory() as db:
        hr = await build_health_readiness(board_id, db, profile="full")

    assert hr["enforcement_active"] is False
    assert hr["cognitive_enforcement_mode"] == "advisory"
    r = hr["readiness"]
    assert r["blocking"] is True            # a technical problem IS visible
    assert r["would_block_done"] is False   # advisory -> the gate does not block
    assert "technical_dlq" in r["reasons"]
    assert r["policy_reason"]               # not declared ready; reason given


@pytest.mark.asyncio
async def test_ts3_skip_cannot_reduce_or_hide_technical_signal(db_factory):
    rebuild_audit.reset_cognitive_technical_signal_counter()
    board_id, aid = await _seed_board_with_dlq(db_factory)
    ref = f"spec:{aid}"

    # The signal is derived from health, NOT from any cognitive verdict, so even
    # scoped to the (potentially skipped) artifact the DLQ is fully surfaced.
    async with db_factory() as db:
        hr = await build_health_readiness(
            board_id, db, profile="summary", artifact_ref=ref)

    items = hr["non_maskable_items"]
    assert any(i["artifact_ref"] == ref and i["signal"] == "technical_dlq" for i in items)
    item = next(i for i in items if i["artifact_ref"] == ref)
    assert item["last_error"] and item["error_text"]
    assert item["next_action"] and item["remediation"] and item["drill_down_tool"]
    # technical counter still non-zero (no masking).
    assert hr["technical_signals"]["technical_dlq_count"] >= 1

    # OR or_36e0cd85: the surfaced technical signal emitted a counter sample.
    assert rebuild_audit.get_cognitive_technical_signal_event_count(
        signal="technical_dlq", board_id=board_id) >= 1


@pytest.mark.asyncio
async def test_domains_stay_separate_and_invalid_profile_rejected(db_factory):
    board_id, _aid = await _seed_board_with_dlq(db_factory)
    async with db_factory() as db:
        hr = await build_health_readiness(board_id, db, profile="summary")
    sig = hr["technical_signals"]
    # active_queue_count is a SEPARATE domain, not inferred from dead_letter_count.
    assert sig["active_queue_count"] == 0
    assert sig["dead_letter_count"] >= 1

    async with db_factory() as db:
        with pytest.raises(InvalidProfileError):
            await build_health_readiness(board_id, db, profile="bogus")


@pytest.mark.asyncio
async def test_mcp_health_readiness_tool_exposes_signals(db_factory, monkeypatch):
    import okto_pulse.core.mcp.server as mcp_server

    board_id, _aid = await _seed_board_with_dlq(db_factory)

    async def _fake_ctx(_board_id):
        return SimpleNamespace(
            agent_id="mcp-agent",
            permissions=["*"],
            realm_id="local",
        )

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_health_readiness")
    data = json.loads(await tool.fn(board_id=board_id, profile="summary"))
    assert data["technical_signals"]["technical_dlq_count"] >= 1
    assert data["readiness"]["blocking"] is True
    assert data["readiness"]["would_block_done"] is False
    assert any(i["signal"] == "technical_dlq" for i in data["non_maskable_items"])

    # Invalid profiles use the shared MCP projection error (not silent summary).
    bad = json.loads(await tool.fn(board_id=board_id, profile="bogus"))
    assert bad.get("error_code") == "unsupported_projection"
    assert bad.get("supported_profiles") == ["summary", "detail", "full", "legacy"]
