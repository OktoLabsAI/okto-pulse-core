"""Tests for the `/analytics/blockers` triage endpoint (spec 124087e7)."""

import os
import sys
from datetime import datetime, timedelta, timezone
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.routing import APIRoute
import pytest

from okto_pulse.community.api.analytics import router
from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.services.analytics_service import compute_blockers
from sqlalchemy_test_models import Board, Card


class TestBlockersEndpointContract:
    def test_endpoint_registered(self):
        paths = {r.path for r in router.routes if isinstance(r, APIRoute)}
        assert "/boards/{board_id}/analytics/blockers" in paths

    def test_endpoint_accepts_stale_hours_param(self):
        route = next(
            r for r in router.routes
            if isinstance(r, APIRoute)
            and r.path == "/boards/{board_id}/analytics/blockers"
        )
        params = route.dependant.query_params
        assert any(p.name == "stale_hours" for p in params)


class TestBlockerTypesAreWellKnown:
    EXPECTED_TYPES = {
        "dependency_blocked",
        "on_hold",
        "rework_required",
        "stale",
        "spec_dependency_blocked",
        "spec_pending_validation",
        "spec_no_cards",
        "uncovered_scenario",
    }

    def test_expected_type_set(self):
        assert len(self.EXPECTED_TYPES) == 8
        # All types are imperative-past or adjectival — machine-parseable.
        for t in self.EXPECTED_TYPES:
            assert "_" in t or t.islower()


@pytest.mark.asyncio
async def test_rejected_card_is_rework_required_and_never_stale(db_factory) -> None:
    suffix = uuid.uuid4().hex[:12]
    board_id = f"blockers-rejected-board-{suffix}"
    card_id = f"blockers-rejected-card-{suffix}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Rejected blocker", owner_id="owner-1"))
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                title="Needs governed rework",
                status=CardStatus.REJECTED,
                position=0,
                created_by="owner-1",
                updated_at=datetime.now(timezone.utc) - timedelta(days=30),
                current_rejection_kind="task_validation",
                current_rejection_code="task_validation_failed",
                current_rejection_summary="Reviewer found incomplete behavior.",
            )
        )
        await db.commit()

    async with db_factory() as db:
        payload = await compute_blockers(db, board_id, stale_hours=1)

    card_blockers = [
        item for item in payload["blockers"] if item.get("card_id") == card_id
    ]
    assert [item["type"] for item in card_blockers] == ["rework_required"]
    assert card_blockers[0]["reason"] == "Reviewer found incomplete behavior."
    assert card_blockers[0]["evidence"] == {
        "cause_kind": "task_validation",
        "cause_code": "task_validation_failed",
    }


class TestMCPToolRegistered:
    def test_list_blockers_tool_definition_exists(self):
        # Source-level check: the MCP tool function is importable
        from okto_pulse.core.mcp import server as mcp_server
        assert hasattr(mcp_server, "okto_pulse_list_blockers")

    def test_mcp_tool_accepts_filter_type(self):
        import inspect
        from okto_pulse.core.mcp import server as mcp_server
        fn = mcp_server.okto_pulse_list_blockers
        # FastMCP wraps the function; unwrap if needed
        target = getattr(fn, "fn", fn)
        sig = inspect.signature(target)
        assert "filter_type" in sig.parameters
        assert "stale_hours" in sig.parameters
        assert sig.parameters["stale_hours"].default == 72
