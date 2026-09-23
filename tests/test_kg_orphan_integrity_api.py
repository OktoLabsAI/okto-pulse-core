"""F4: orphan maintenance is absent; Health retains its internal scanner."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.router import api_router
from okto_pulse.core.application.use_cases import operational_rest
from okto_pulse.core.kg import orphan_integrity
from okto_pulse.core.mcp import server


@pytest.mark.parametrize("board", ["missing", "foreign", "owned"])
@pytest.mark.parametrize("dry_run", [True, False])
@pytest.mark.parametrize("method,suffix", [("GET", "report"), ("POST", "backfill")])
def test_orphan_maintenance_absent_before_authentication_and_graph(monkeypatch, board, dry_run, method, suffix):
    def forbidden(*args, **kwargs):
        pytest.fail("Retired orphan maintenance reached authentication or storage")

    monkeypatch.setattr(orphan_integrity, "OrphanNodeScanner", forbidden)
    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[require_user] = forbidden
    app.dependency_overrides[get_unit_of_work] = forbidden
    path = f"/api/v1/kg/orphan-integrity/{suffix}"
    with TestClient(app) as client:
        response = client.request(method, path, params={"board_id": board}, json={
            "board_id": board, "dry_run": dry_run, "node_ids": ["old-orphan"],
            "generation_id": "old-generation", "limit": 100,
        })
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}
    assert path not in app.openapi()["paths"]
    assert "OrphanBackfillRequest" not in app.openapi()["components"]["schemas"]


def test_exclusive_handlers_and_use_cases_are_removed():
    for name in ("OrphanBackfillReconciler", "OrphanBackfillResult", "OrphanBackfillSample"):
        assert not hasattr(orphan_integrity, name)
    for name in ("okto_pulse_kg_orphan_report", "okto_pulse_kg_orphan_backfill",
                 "_kg_orphan_graph_unavailable_payload", "_kg_orphan_backfill_health_refusal"):
        assert not hasattr(server, name)
    for name in ("OrphanIntegrityReportCommand", "GetOrphanIntegrityReportUseCase",
                 "OrphanBackfillCommand", "RunOrphanBackfillUseCase"):
        assert not hasattr(operational_rest, name)
