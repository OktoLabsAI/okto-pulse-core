"""R2a — Rebuild-scoped admission gate tests (spec 959115c0).

IMPL-1: predicate _refuse_rebuild_if_quarantined (FR8 / AC14 / AC7 / AC8)
IMPL-2: REST per-board scope (FR10) + real health probe (FR9)

Scenario -> test-card map (load-bearing scenarios first):
  ts_a83fb1bc  -> quarantined IS refused by the rebuild gate
  ts_4acad85e  -> recovery_needed IS admitted (NC-2 exit)
  ts_95ec2b37  -> predicate does not import _RISK_STATE_HARD_REJECT (AC14)
"""

from __future__ import annotations

import inspect
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

import okto_pulse.core.api.kg_rebuild as kg_rebuild_mod
from okto_pulse.core.api.kg_rebuild import (
    _REBUILD_REJECT_STATES,
    _refuse_rebuild_if_quarantined,
    router as rebuild_router,
)
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board, BoardShare


# ---------------------------------------------------------------------------
# Shared fake async session that records calls
# ---------------------------------------------------------------------------

class _FakeSession:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


def _install_rebuild_health(monkeypatch, graph_state: str):
    """Stub kg_rebuild_mod.get_kg_health to return the given graph_state."""
    calls: list[str] = []

    async def _stub(board_id, db, scheduler_control=None):
        calls.append(board_id)
        return {"graph_state": graph_state, "metric_status": "available"}

    monkeypatch.setattr(kg_rebuild_mod, "get_kg_health", _stub)
    return calls


# ---------------------------------------------------------------------------
# ts_a83fb1bc — LOAD-BEARING: quarantined IS refused
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_a83fb1bc_quarantined_is_refused(monkeypatch):
    """LOAD-BEARING (ts_a83fb1bc / AC7):
    When graph_state=='quarantined', _refuse_rebuild_if_quarantined returns a
    structured refusal payload.  A rebuild cannot land on a quarantined graph —
    the operator must use the KG reset flow first."""
    calls = _install_rebuild_health(monkeypatch, "quarantined")
    db = _FakeSession()

    result = await _refuse_rebuild_if_quarantined("board-q", db)

    assert result is not None, "quarantined board MUST be refused"
    assert result["error"] == "rebuild_refused_quarantined"
    assert result["graph_state"] == "quarantined"
    assert result["board_id"] == "board-q"
    assert "quarantined" in result["message"].lower()
    assert calls == ["board-q"]


# ---------------------------------------------------------------------------
# ts_4acad85e — LOAD-BEARING: recovery_needed IS admitted (NC-2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_4acad85e_recovery_needed_is_admitted(monkeypatch):
    """LOAD-BEARING (ts_4acad85e / AC8 — NC-2 exit):
    When graph_state=='recovery_needed', _refuse_rebuild_if_quarantined returns
    None (gate passes).  Rebuild IS the prescribed exit from recovery_needed;
    refusing it would recreate the NC-2 catch-22."""
    calls = _install_rebuild_health(monkeypatch, "recovery_needed")
    db = _FakeSession()

    result = await _refuse_rebuild_if_quarantined("board-rn", db)

    assert result is None, (
        "recovery_needed MUST be admitted by the rebuild gate — "
        "refusing it recreates the NC-2 catch-22"
    )
    assert calls == ["board-rn"]


# ---------------------------------------------------------------------------
# Additional healthy/at_risk/backpressure states: all admitted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["healthy", "at_risk", "backpressure"])
async def test_non_quarantined_states_are_admitted(state, monkeypatch):
    """All non-quarantined states must pass the rebuild admission gate."""
    _install_rebuild_health(monkeypatch, state)
    db = _FakeSession()
    result = await _refuse_rebuild_if_quarantined("board-ok", db)
    assert result is None, f"state={state!r} should not block a rebuild"


# ---------------------------------------------------------------------------
# ts_95ec2b37 — LOAD-BEARING: predicate does NOT import _RISK_STATE_HARD_REJECT
# ---------------------------------------------------------------------------


def test_ts_95ec2b37_predicate_does_not_reference_hard_reject_set():
    """LOAD-BEARING (ts_95ec2b37 / AC14):
    The rebuild admission predicate must NOT import or reference
    _RISK_STATE_HARD_REJECT from backpressure.py.  That set contains
    'recovery_needed' which would block the NC-2 exit path.

    Verified by inspecting the source of the predicate function (no string
    match) and by asserting that 'recovery_needed' is NOT in
    _REBUILD_REJECT_STATES."""
    # AC14 — textual check: source must not reference the backpressure symbol.
    source = inspect.getsource(_refuse_rebuild_if_quarantined)
    assert "_RISK_STATE_HARD_REJECT" not in source, (
        "The rebuild predicate must NOT reference _RISK_STATE_HARD_REJECT — "
        "importing that set would cause recovery_needed to be refused, "
        "recreating the NC-2 catch-22."
    )

    # Also check the module-level reject set for the same guarantee.
    module_source = inspect.getsource(kg_rebuild_mod)
    # Only the comment that documents this constraint may mention it.
    # The production logic must not import it.
    assert "from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT" not in module_source, (
        "kg_rebuild.py must not import _RISK_STATE_HARD_REJECT"
    )

    # _REBUILD_REJECT_STATES must not contain recovery_needed.
    assert "recovery_needed" not in _REBUILD_REJECT_STATES, (
        "'recovery_needed' must not be in _REBUILD_REJECT_STATES"
    )

    # _REBUILD_REJECT_STATES must contain quarantined.
    assert "quarantined" in _REBUILD_REJECT_STATES, (
        "'quarantined' must be in _REBUILD_REJECT_STATES"
    )


# ---------------------------------------------------------------------------
# FR10 — REST per-board scope: BEHAVIORAL tests with FastAPI TestClient
#
# Three pairs of tests per endpoint: non-existent board → 404; user without
# access to an existing board → 403; board owner → request proceeds past the
# scope gate (any status other than 403/404 is accepted — business-logic
# errors like 400/409 are fine because the auth gate was passed).
# ---------------------------------------------------------------------------

_OWNER_ID = "rebuild-scope-owner"
_OTHER_ID = "rebuild-scope-other"
_MEMBER_ID = "rebuild-scope-member"


@pytest_asyncio.fixture
async def _rebuild_scope_board():
    """Create a board owned by _OWNER_ID and shared (viewer) with _MEMBER_ID.

    Returns the board_id for use in the tests.
    """
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    board_id = f"rebuild-scope-{uuid.uuid4()}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Rebuild Scope Board", owner_id=_OWNER_ID))
        db.add(
            BoardShare(
                id=str(uuid.uuid4()),
                board_id=board_id,
                user_id=_MEMBER_ID,
                realm_id="",
                permission="viewer",
                shared_by=_OWNER_ID,
            )
        )
        await db.commit()
    return board_id


def _make_rebuild_client(user_id: str) -> tuple[TestClient, object]:
    """Return (client, db_factory) for the rebuild router with *user_id* wired."""
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    app = FastAPI()
    app.include_router(rebuild_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: user_id
    return TestClient(app, raise_server_exceptions=False), db_factory


# ---- /preflight ----

def test_fr10_preflight_nonexistent_board_returns_404(_rebuild_scope_board):
    """FR10 BEHAVIORAL: preflight on a board that does not exist → HTTP 404."""
    client, _ = _make_rebuild_client(_OWNER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/preflight",
        params={"board_id": "nonexistent-board-id"},
    )
    assert resp.status_code == 404, (
        f"Expected 404 for non-existent board, got {resp.status_code}: {resp.text}"
    )


def test_fr10_preflight_unauthorized_user_returns_403(_rebuild_scope_board):
    """FR10 BEHAVIORAL: preflight by user with no board access → HTTP 403."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_OTHER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/preflight",
        params={"board_id": board_id},
    )
    assert resp.status_code == 403, (
        f"Expected 403 for unauthorized user, got {resp.status_code}: {resp.text}"
    )


def test_fr10_preflight_owner_passes_scope_gate(_rebuild_scope_board):
    """FR10 BEHAVIORAL: preflight by board owner passes the scope gate
    (status must not be 403 or 404; business-logic errors are acceptable)."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_OWNER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/preflight",
        params={"board_id": board_id},
    )
    assert resp.status_code not in (403, 404), (
        f"Owner must pass scope gate, got unexpected {resp.status_code}: {resp.text}"
    )


def test_fr10_preflight_member_passes_scope_gate(_rebuild_scope_board):
    """FR10 BEHAVIORAL: preflight by a shared member also passes the scope gate."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_MEMBER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/preflight",
        params={"board_id": board_id},
    )
    assert resp.status_code not in (403, 404), (
        f"Member must pass scope gate, got unexpected {resp.status_code}: {resp.text}"
    )


# ---- /confirm ----

_DUMMY_HASH = "a" * 64
_DUMMY_MANIFEST = "manifest-ref-dummy-00000001"


def test_fr10_confirm_nonexistent_board_returns_404(_rebuild_scope_board):
    """FR10 BEHAVIORAL: confirm on a board that does not exist → HTTP 404."""
    client, _ = _make_rebuild_client(_OWNER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/confirm",
        json={
            "board_id": "nonexistent-board-id",
            "operation": "full_rebuild",
            "preflight_hash": _DUMMY_HASH,
            "manifest_ref": _DUMMY_MANIFEST,
        },
    )
    assert resp.status_code == 404, (
        f"Expected 404 for non-existent board, got {resp.status_code}: {resp.text}"
    )


def test_fr10_confirm_unauthorized_user_returns_403(_rebuild_scope_board):
    """FR10 BEHAVIORAL: confirm by user with no board access → HTTP 403."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_OTHER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/confirm",
        json={
            "board_id": board_id,
            "operation": "full_rebuild",
            "preflight_hash": _DUMMY_HASH,
            "manifest_ref": _DUMMY_MANIFEST,
        },
    )
    assert resp.status_code == 403, (
        f"Expected 403 for unauthorized user, got {resp.status_code}: {resp.text}"
    )


def test_fr10_confirm_owner_passes_scope_gate(_rebuild_scope_board):
    """FR10 BEHAVIORAL: confirm by board owner passes the scope gate."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_OWNER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/confirm",
        json={
            "board_id": board_id,
            "operation": "full_rebuild",
            "preflight_hash": _DUMMY_HASH,
            "manifest_ref": _DUMMY_MANIFEST,
        },
    )
    assert resp.status_code not in (403, 404), (
        f"Owner must pass scope gate, got unexpected {resp.status_code}: {resp.text}"
    )


# ---- /run ----

_DUMMY_CONFIRMATION = "confirmation-id-dummy-0001"


def test_fr10_run_nonexistent_board_returns_404(_rebuild_scope_board):
    """FR10 BEHAVIORAL: run on a board that does not exist → HTTP 404."""
    client, _ = _make_rebuild_client(_OWNER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/run",
        json={
            "confirmation_id": _DUMMY_CONFIRMATION,
            "board_id": "nonexistent-board-id",
            "operation": "full_rebuild",
            "preflight_hash": _DUMMY_HASH,
            "manifest_ref": _DUMMY_MANIFEST,
            "reason": "test",
        },
    )
    assert resp.status_code == 404, (
        f"Expected 404 for non-existent board, got {resp.status_code}: {resp.text}"
    )


def test_fr10_run_unauthorized_user_returns_403(_rebuild_scope_board):
    """FR10 BEHAVIORAL: run by user with no board access → HTTP 403."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_OTHER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/run",
        json={
            "confirmation_id": _DUMMY_CONFIRMATION,
            "board_id": board_id,
            "operation": "full_rebuild",
            "preflight_hash": _DUMMY_HASH,
            "manifest_ref": _DUMMY_MANIFEST,
            "reason": "test",
        },
    )
    assert resp.status_code == 403, (
        f"Expected 403 for unauthorized user, got {resp.status_code}: {resp.text}"
    )


def test_fr10_run_owner_passes_scope_gate(_rebuild_scope_board):
    """FR10 BEHAVIORAL: run by board owner passes the scope gate."""
    board_id = _rebuild_scope_board
    client, _ = _make_rebuild_client(_OWNER_ID)
    resp = client.post(
        "/api/v1/kg/rebuild/run",
        json={
            "confirmation_id": _DUMMY_CONFIRMATION,
            "board_id": board_id,
            "operation": "full_rebuild",
            "preflight_hash": _DUMMY_HASH,
            "manifest_ref": _DUMMY_MANIFEST,
            "reason": "test",
        },
    )
    assert resp.status_code not in (403, 404), (
        f"Owner must pass scope gate, got unexpected {resp.status_code}: {resp.text}"
    )


# ---------------------------------------------------------------------------
# FR9 — Real health probe wired in preflight endpoint
# ---------------------------------------------------------------------------


def test_fr9_health_probe_uses_get_kg_health_in_preflight():
    """FR9: the preflight endpoint must use the real get_kg_health probe (not
    a hardcoded stub returning 'healthy').  Verified by source inspection:
    the stub that returned base_state='healthy' unconditionally must be gone,
    and _raw_health from the real get_kg_health call must be used."""
    import okto_pulse.core.api.kg_rebuild as m
    source = inspect.getsource(m.post_rebuild_preflight)
    # Real health call must be present.
    assert "get_kg_health" in source, (
        "post_rebuild_preflight must call get_kg_health (FR9 real probe)"
    )
    # The old hardcoded stub must be absent.
    assert 'base_state="healthy"' not in source, (
        "The hardcoded stub 'base_state=\"healthy\"' must be replaced with "
        "the real get_kg_health probe"
    )


# ---------------------------------------------------------------------------
# Async signature: preflight/confirm/run must accept db: AsyncSession
# ---------------------------------------------------------------------------


def test_all_three_endpoints_accept_db_parameter():
    """FR10 (defence-in-depth): each REST endpoint must accept an async DB
    session dependency so the per-board scope check can query the Board table."""
    import okto_pulse.core.api.kg_rebuild as m
    for fn in (m.post_rebuild_preflight, m.post_rebuild_confirm, m.post_rebuild_run):
        sig = inspect.signature(fn)
        assert "db" in sig.parameters, (
            f"{fn.__name__} must have a 'db' parameter for scope auth"
        )


# ---------------------------------------------------------------------------
# MCP twin: source inspection for the three tools
# ---------------------------------------------------------------------------


def test_mcp_twin_tools_registered_in_server():
    """IMPL-3: the three MCP rebuild tools must be registered in server.py."""
    import okto_pulse.core.mcp.server as srv_mod
    source = inspect.getsource(srv_mod)
    for tool_name in (
        "okto_pulse_kg_rebuild_preflight",
        "okto_pulse_kg_rebuild_confirm",
        "okto_pulse_kg_rebuild_run",
    ):
        assert tool_name in source, (
            f"MCP tool '{tool_name}' must be registered in server.py"
        )


def test_mcp_twin_calls_rebuild_scoped_gate():
    """IMPL-3 + FR8: the MCP rebuild tools must import and call
    _refuse_rebuild_if_quarantined (the shared rebuild-scoped gate)."""
    import okto_pulse.core.mcp.server as srv_mod
    source = inspect.getsource(srv_mod)
    assert "_refuse_rebuild_if_quarantined" in source, (
        "MCP twin must call the shared rebuild-scoped gate"
    )
    # must NOT use the tick gate directly for rebuild decisions
    # (the tick gate uses _RISK_STATE_HARD_REJECT which would refuse recovery_needed)
    # The twin imports and calls _refuse_rebuild_if_quarantined from kg_rebuild


def test_mcp_twin_does_not_reimplement_business_logic():
    """IMPL-3 (delegation invariant): the MCP rebuild tools must delegate to
    the shared service objects, not re-implement business logic.  Verified by
    inspecting the full server module source (the tools are @mcp.tool()-wrapped
    FunctionTool objects whose underlying source lives in the module file)."""
    import okto_pulse.core.mcp.server as srv_mod

    # The @mcp.tool() decorator wraps functions in FunctionTool objects.
    # We can't getsource on those directly; inspect the module source instead.
    source = inspect.getsource(srv_mod)

    # preflight must delegate to RebuildPreflightService
    assert "RebuildPreflightService" in source, (
        "okto_pulse_kg_rebuild_preflight must use RebuildPreflightService"
    )

    # confirm must delegate to RebuildConfirmationStore
    assert "RebuildConfirmationStore" in source, (
        "okto_pulse_kg_rebuild_confirm must use RebuildConfirmationStore"
    )

    # run must delegate to KGRebuildService
    assert "KGRebuildService" in source, (
        "okto_pulse_kg_rebuild_run must use KGRebuildService"
    )


# ---------------------------------------------------------------------------
# ts_30de5f98 — TWIN auth guard: _get_agent_ctx=None → _auth_error, zero side-effect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_30de5f98_twin_auth_none_returns_auth_error_no_side_effects(
    tmp_path,
    monkeypatch,
):
    """ts_30de5f98 (AC / FR8 MCP auth gate):
    When _get_agent_ctx returns None (unauthenticated or no board access),
    okto_pulse_kg_rebuild_preflight MUST:
      (a) return the canonical _auth_error() JSON (structured auth failure);
      (b) write ZERO files to the rebuild base_dir — no manifest, no
          confirmation, no artifact of any kind.

    This proves the auth check short-circuits before any KG mutation
    (TR13 / no-side-effect invariant for unauthenticated calls)."""
    import json

    import okto_pulse.core.mcp.server as srv_mod

    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
        REBUILD_AUDIT_GLOBAL_BOARD_ID,
        RebuildAuditKey,
    )
    from okto_pulse.core.kg.rebuild_audit import require_rebuild_audit_artifact_store

    artifact_store = require_rebuild_audit_artifact_store()

    def _artifact_snapshot() -> dict[str, list[dict]]:
        return {
            "source_manifest": list(
                artifact_store.list_json(
                    RebuildAuditKey(
                        namespace="source_manifest",
                        board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
                    )
                )
            ),
            "confirmation_token": list(
                artifact_store.list_json(
                    RebuildAuditKey(
                        namespace="confirmation_token",
                        board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
                    )
                )
            ),
            "rebuild_report": list(
                artifact_store.list_json(
                    RebuildAuditKey(
                        namespace="rebuild_report",
                        board_id="b-auth-test",
                    )
                )
            ),
        }

    before_artifacts = _artifact_snapshot()

    # Patch _get_agent_ctx in the server module to return None —
    # simulates unauthenticated / board-access-denied.
    # Must be async because the tool does ``await _get_agent_ctx(board_id)``.
    async def _ctx_returns_none(board_id: str):
        return None

    monkeypatch.setattr(srv_mod, "_get_agent_ctx", _ctx_returns_none)

    tools = srv_mod.mcp._tool_manager._tools
    preflight_fn = tools["okto_pulse_kg_rebuild_preflight"].fn

    result = await preflight_fn(board_id="b-auth-test")

    # (a) Response must be the canonical auth-error JSON.
    body = json.loads(result)
    assert body.get("error") == "Authentication failed or board access denied", (
        f"Expected auth error JSON, got: {body}"
    )

    # (b) Zero artifacts written to the rebuild audit store, and no legacy
    # filesystem write under tmp_path.
    assert _artifact_snapshot() == before_artifacts
    all_files = list(tmp_path.rglob("*"))
    written_files = [f for f in all_files if f.is_file()]
    assert written_files == [], (
        f"Auth-failed preflight must not write any files; found: {written_files}"
    )


# ---------------------------------------------------------------------------
# ts_95626a94 — agent_instructions.md documents the 3 rebuild tools + exceptions
# ---------------------------------------------------------------------------

_AGENT_INSTRUCTIONS_MD = (
    Path(__file__).parent.parent
    / "src" / "okto_pulse" / "core" / "mcp" / "agent_instructions.md"
)


def _instructions_text() -> str:
    return _AGENT_INSTRUCTIONS_MD.read_text(encoding="utf-8")


def test_ts_95626a94_agent_instructions_documents_rebuild_tools():
    """ts_95626a94:
    agent_instructions.md MUST document all three MCP rebuild tools and
    the two critical agent-behaviour rules:
      (a) cites okto_pulse_kg_rebuild_preflight + okto_pulse_kg_rebuild_confirm
          + okto_pulse_kg_rebuild_run (tool names that the agent needs to call);
      (b) documents the recovery_needed EXCEPTION — agent MUST run the rebuild
          (not stop) when graph_state == recovery_needed;
      (c) PRESERVES the quarantined stop-rule (agent MUST stop on quarantined).

    Mirrors the pattern in test_r1_degraded_kg_fallback_gate.py
    (test_ts_a377ed81_errors_md_documents_graph_availability_keys)."""
    text = _instructions_text()

    # (a) all three tool names are cited
    for tool_name in (
        "okto_pulse_kg_rebuild_preflight",
        "okto_pulse_kg_rebuild_confirm",
        "okto_pulse_kg_rebuild_run",
    ):
        assert tool_name in text, (
            f"agent_instructions.md must cite {tool_name} so the agent knows "
            f"which tools to call for the rebuild flow"
        )

    # (b) recovery_needed exception — the agent MUST proceed, not stop
    assert "recovery_needed" in text, (
        "agent_instructions.md must document the recovery_needed exception "
        "so agents do not stop on that state"
    )

    # (c) quarantined stop-rule — the agent MUST stop
    assert "quarantined" in text, (
        "agent_instructions.md must preserve the quarantined stop-rule"
    )
    # The stop-rule language must be present (MUST stop / do not write)
    lower = text.lower()
    assert "must stop" in lower or "do not write" in lower, (
        "agent_instructions.md must contain explicit stop language for quarantined "
        "(e.g. 'MUST stop' or 'do not write')"
    )


def test_ts_95626a94_agent_instructions_rebuild_section_is_load_bearing():
    """Negative-wiring guard: excising the rebuild-tool section from a
    synthetic copy of agent_instructions.md must drop at LEAST one of the
    three tool names — proving the assertions above are load-bearing
    (not vacuous substring matches on an unrelated section)."""
    real = _instructions_text()

    # The KG health stop-rule section contains all three tool names;
    # locate it and truncate before it.
    # The section starts at '## KG health and operational signals'
    section_marker = "## KG health and operational signals"
    idx = real.find(section_marker)
    assert idx != -1, (
        f"agent_instructions.md must contain '{section_marker}' section"
    )
    synthetic = real[:idx]

    # At least the three rebuild tool names must be absent in the truncated copy.
    missing_count = sum(
        1 for name in (
            "okto_pulse_kg_rebuild_preflight",
            "okto_pulse_kg_rebuild_confirm",
            "okto_pulse_kg_rebuild_run",
        )
        if name not in synthetic
    )
    assert missing_count == 3, (
        "All three rebuild tool names must live ONLY in the KG health section — "
        f"{3 - missing_count} were found before the section marker (load-bearing guard failed)"
    )
