from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.router import api_router
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.core.kg.orphan_integrity import (
    OrphanBackfillResult,
    OrphanBackfillSample,
    OrphanNodeSample,
    OrphanScanReport,
    SAFE_ORPHAN_SAMPLE_FIELDS,
)

BOARD_ID = "board-orphan-api"
USER_ID = "user-orphan-api"


class _Boards:
    async def get(self, board_id: str):
        if board_id != BOARD_ID:
            return None
        return SimpleNamespace(id=BOARD_ID, owner_id=USER_ID)


class _Shares:
    async def get_user_permission(self, _board_id: str, _user_id: str):
        return None


class _KGOperations:
    async def invoke_health_reader(
        self,
        reader,
        board_id: str,
        *,
        scheduler_control,
    ):
        return await reader(board_id, None, scheduler_control=scheduler_control)


async def _resolve_user_permissions(_user_id: str, _board_id: str):
    return {
        "kg": {
            "operations": {
                "integrity": {
                    "read": True,
                    "backfill": True,
                }
            },
            "admin": {
                "settings_read": True,
                "settings_write": True,
            },
        }
    }


async def _fake_uow():
    yield SimpleNamespace(
        boards=_Boards(),
        services=SimpleNamespace(
            shares=_Shares(),
            kg=_KGOperations(),
            resolve_user_permissions=_resolve_user_permissions,
        ),
    )


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user():
        return USER_ID

    app.dependency_overrides[require_user] = _fake_user
    app.dependency_overrides[get_unit_of_work] = _fake_uow
    return TestClient(app)


def test_orphan_integrity_routes_are_registered() -> None:
    paths = set()
    for route in api_router.routes:
        path = getattr(route, "path", None)
        if path:
            paths.add(path)
        effective_route_contexts = getattr(route, "effective_route_contexts", None)
        if callable(effective_route_contexts):
            paths.update(
                context.path
                for context in effective_route_contexts()
                if getattr(context, "path", None)
            )
    assert "/api/v1/kg/orphan-integrity/report" in paths
    assert "/api/v1/kg/orphan-integrity/backfill" in paths


def test_get_orphan_integrity_report_returns_bounded_safe_payload(monkeypatch) -> None:
    import okto_pulse.community.api.kg_orphan_integrity as orphan_api

    class _FakeScanner:
        def scan(self, *, board_id, generation_id, limit):
            assert board_id == BOARD_ID
            assert generation_id == "gen-api"
            assert limit == 2
            return OrphanScanReport(
                board_id=board_id,
                generation_id=generation_id,
                orphan_count=1,
                orphan_count_by_type={"Learning": 1},
                orphan_count_by_writer_path={"agent:cognitive": 1},
                samples=(
                    OrphanNodeSample(
                        node_id="learning-1",
                        node_type="Learning",
                        writer_path="agent:cognitive",
                        source_artifact_ref="card:bug:bug-1:learning:0",
                        source_resolution_status="resolved_unique",
                        generation_id=generation_id,
                        reason="zero_graph_degree",
                        correlation_id="corr-report",
                    ),
                ),
                unresolved_reasons={"zero_graph_degree": 1},
                allowlisted_root_count=0,
                correlation_id="corr-report",
            )

    monkeypatch.setattr(orphan_api, "OrphanNodeScanner", _FakeScanner)

    response = _client().get(
        "/api/v1/kg/orphan-integrity/report",
        params={"board_id": BOARD_ID, "generation_id": "gen-api", "limit": 2},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["board_id"] == BOARD_ID
    assert body["generation_id"] == "gen-api"
    assert body["orphan_count_by_type"] == {"Learning": 1}
    assert body["backfill_summary"]["status"] == "not_run"
    assert body["correlation_id"] == "corr-report"
    assert set(body["samples"][0]) == set(SAFE_ORPHAN_SAMPLE_FIELDS)
    assert "Raw title" not in response.text
    assert "Raw content" not in response.text


def test_post_orphan_backfill_refuses_recovery_needed_health(monkeypatch) -> None:
    import okto_pulse.community.api.kg_orphan_integrity as orphan_api

    async def _recovery_needed(board_id, db, scheduler_control=None):
        return {
            "overall_state": "recovery_needed",
            "graph_state": "recovery_needed",
        }

    monkeypatch.setattr(orphan_api, "get_kg_health", _recovery_needed)

    response = _client().post(
        "/api/v1/kg/orphan-integrity/backfill",
        json={"board_id": BOARD_ID, "dry_run": True, "node_ids": ["learning-1"]},
    )

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail == {
        "error": "kg_orphan_backfill_refused_by_health",
        "board_id": BOARD_ID,
        "overall_state": "recovery_needed",
        "graph_state": "recovery_needed",
        "operator_action": "inspect_kg_health_recovery_flow",
    }


def test_post_orphan_backfill_returns_explicit_dry_run_summary(monkeypatch) -> None:
    import okto_pulse.community.api.kg_orphan_integrity as orphan_api

    async def _healthy(board_id, db, scheduler_control=None):
        return {"overall_state": "at_risk", "graph_state": "at_risk"}

    class _FakeReconciler:
        def run(self, *, board_id, generation_id, dry_run, node_ids, limit):
            assert board_id == BOARD_ID
            assert generation_id == "gen-api"
            assert dry_run is True
            assert node_ids == ["learning-1"]
            assert limit == 1
            return OrphanBackfillResult(
                detected=1,
                connected=1,
                noop=0,
                unresolved=0,
                ambiguous=0,
                semantic_pending=0,
                samples=(
                    OrphanBackfillSample(
                        node_id="learning-1",
                        node_type="Learning",
                        writer_path="agent:cognitive",
                        outcome="connected",
                        reason="bug_learning_validates_bug",
                        edge_type="validates",
                        target_node_type="Bug",
                        target_node_id="bug-1",
                        source_resolution_status="resolved_unique",
                        generation_id=generation_id,
                        correlation_id="corr-backfill",
                    ),
                ),
                correlation_id="corr-backfill",
            )

    monkeypatch.setattr(orphan_api, "get_kg_health", _healthy)
    monkeypatch.setattr(orphan_api, "OrphanBackfillReconciler", _FakeReconciler)

    response = _client().post(
        "/api/v1/kg/orphan-integrity/backfill",
        json={
            "board_id": BOARD_ID,
            "generation_id": "gen-api",
            "dry_run": True,
            "node_ids": ["learning-1"],
            "limit": 1,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["board_id"] == BOARD_ID
    assert body["dry_run"] is True
    assert body["correlation_id"] == "corr-backfill"
    assert body["backfill_summary"]["connected"] == 1
    assert body["backfill_summary"]["samples"][0]["edge_type"] == "validates"


def test_mcp_orphan_tools_are_registered_with_safe_contract() -> None:
    source = Path("src/okto_pulse/core/mcp/server.py").read_text(encoding="utf-8")

    for required in (
        "async def okto_pulse_kg_orphan_report",
        "async def okto_pulse_kg_orphan_backfill",
        "OrphanNodeScanner",
        "OrphanBackfillReconciler",
        "dry_run",
        "coerce_to_list_str(node_ids)",
        "kg_orphan_graph_unavailable",
        "kg_orphan_backfill_refused_by_health",
        "backfill_summary",
        "correlation_id",
    ):
        assert required in source

    assert source.count("async def okto_pulse_kg_orphan_report") == 1
    assert source.count("async def okto_pulse_kg_orphan_backfill") == 1
    assert "report.to_safe_dict()" in source
    assert "result.to_safe_dict()" in source
