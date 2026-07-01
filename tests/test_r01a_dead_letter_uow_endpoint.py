"""Spec R01A IMP2 — REST dead-letter inspector migrated to the UnitOfWork path.

Proves the REAL endpoint (GET /api/v1/kg/queue/dead-letter) now drives
endpoint -> ``get_unit_of_work`` (request-scoped ``PulseUnitOfWork``) ->
transport-free ``ListDeadLetterRowsUseCase`` -> ``list_dead_letter_rows``
service, preserving the payload/permission and keeping the FastAPI ``get_db``
dependency override intact. The handler no longer takes a raw ``AsyncSession``,
and the R01A inventory confirms the router is strangled (0 relational
call-sites). The pre-existing ``test_dlq_inspector`` suite continues to pass,
which is the end-to-end behavior-parity guarantee.
"""

from __future__ import annotations

import inspect

from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import dead_letter as dead_letter_api
from okto_pulse.core.api.dead_letter import router as dead_letter_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.repositories.relational_boundary_gate import (
    default_use_cases_path,
    run_relational_boundary_gate,
)
from okto_pulse.core.repositories.relational_consumer_inventory import (
    build_relational_consumer_inventory,
)

USER = "dlq-uow-r01a"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(dead_letter_router, prefix="/api/v1")
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


def test_dead_letter_endpoint_uses_uow_and_preserves_payload() -> None:
    """The real endpoint returns the DeadLetterListResponse payload unchanged,
    flowing through get_unit_of_work -> use case (the get_db override applies)."""
    client = _client()
    resp = client.get(
        "/api/v1/kg/queue/dead-letter",
        params={"board_id": "r01a-imp2-no-such-board", "limit": 25, "offset": 0},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # DeadLetterListResponse shape preserved exactly.
    assert set(body) == {"rows", "total", "limit", "offset"}
    assert body["rows"] == []  # board with no DLQ rows
    assert body["total"] == 0
    assert body["limit"] == 25  # query echoed back unchanged
    assert body["offset"] == 0


def test_dead_letter_handler_depends_on_unit_of_work_not_raw_session() -> None:
    """Strangler proof at the handler contract: no raw AsyncSession; the UoW comes
    from get_unit_of_work; the require_user permission is preserved."""
    sig = inspect.signature(dead_letter_api.get_dead_letter)
    assert "db" not in sig.parameters  # no raw AsyncSession in the migrated handler
    uow_param = sig.parameters["uow"]
    assert uow_param.default.dependency is get_unit_of_work
    assert "user_id" in sig.parameters  # require_user permission preserved


def test_dead_letter_router_is_strangled_in_inventory() -> None:
    """Tie-in with R01A IMP1: the inventory now lists ZERO relational call-sites
    for the migrated router (it was 5 before the migration)."""
    inv = build_relational_consumer_inventory()
    rows = [c for c in inv.consumers if c.file == "core/api/dead_letter.py"]
    assert rows == [], [c.symbol for c in rows]


def test_use_case_keeps_application_layer_relationally_clean() -> None:
    """The new use case must not re-introduce a relational coupling into the
    boundary-gated application/use_cases package."""
    report = run_relational_boundary_gate(root=default_use_cases_path())
    assert report.ok, [(v.file, v.symbol) for v in report.violations]
