"""F16+F17 (spec 690416ab) — health-aware closeout gate + tick admission.

F16: CognitiveCloseoutGate.evaluate() surfaces UNAVAILABLE on a degraded graph
(graph_state in _RISK_STATE_HARD_REJECT OR resolved_generation is None) while
staying sync/pure; the async service caller resolves graph_state via
get_kg_health and threads it in.
F17: run_tick_now (and its MCP twin) refuse a manual tick on a degraded concrete
board with a structured 409, after the advisory-lock check and before tick_id.

Scenario -> test-card map:
  ts_abd2cb82, ts_7fce86cf  -> 599d6cea (AC1/AC2: degraded -> UNAVAILABLE)
  ts_b054e4f7, ts_01d256b9  -> 62902967 (AC3/AC6: null-gen UNAVAILABLE; skip wins)
  ts_4663630d, ts_6fa3e068  -> 84d02ee1 (AC4/AC5: healthy no-regression)
  ts_80500309, ts_dedefaf9  -> eb5bf3f3 (AC8/AC13: sync/no-IO + predicate identity)
  ts_327df96b               -> 3bd086d0 (AC14: degraded telemetry sample)
  ts_3eda0dc9               -> c8e10019 (AC7: async plumbing blocks before mutation)
  ts_de1c347a, ts_23ae75b2  -> 5f9af5b8 (AC9/AC10: tick 409 / 202)
  ts_58aac88a, ts_21f124dd  -> 8e629790 (AC11/AC12: lock wins; global not refused)
  ts_67fe9fd2               -> b9523f4f (AC15: MCP twin inherits the refusal)
"""

from __future__ import annotations

import inspect
import json
import uuid
from types import SimpleNamespace

import pytest

from coordination_fakes import FakeLeaseProvider, FakeWriteLockPort
import okto_pulse.community.api.kg_tick as kg_tick
import okto_pulse.core.application.kg_tick as kg_tick_policy
import okto_pulse.core.kg.cognitive_closeout_gate as gate_mod
import okto_pulse.core.mcp.server as server
import okto_pulse.core.services.kg_health_service as kg_health_service
import okto_pulse.core.services.main as services_main
from okto_pulse.community.api.kg_tick import (
    TickRunNowRequest,
    TickRunNowResponse,
    run_tick_now,
)
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.kg.cognitive_closeout_gate import (
    CognitiveCloseoutGate,
    CognitiveCloseoutOutcome,
    CognitiveCloseoutReason,
    get_closeout_gate_samples,
    reset_closeout_gate_samples,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveItemStatus,
)
from okto_pulse.core.ports.advisory_lock import get_async_lock
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.main import CardService
from okto_pulse.core.ports.coordination import (
    get_lease_provider,
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)
from okto_pulse.core.ports.authentication import Principal


@pytest.fixture(autouse=True)
def _coordination_ports():
    reset_coordination_providers_for_tests()
    register_coordination_providers(
        lease_provider=FakeLeaseProvider(),
        write_lock_port=FakeWriteLockPort(),
    )
    yield
    reset_coordination_providers_for_tests()


# --------------------------------------------------------------------------- #
# F16 gate unit scaffolding
# --------------------------------------------------------------------------- #


def _item(source_ref: str, status: str) -> CognitiveConsolidationItem:
    return CognitiveConsolidationItem(
        item_id=f"item-{source_ref.replace(':', '-')}",
        board_id="board-1",
        kg_generation_id="kg-1",
        source_ref=source_ref,
        artifact_type=source_ref.split(":", 1)[0],
        status=status,
        recorded_at="2026-05-29T00:00:00Z",
    )


class _FakeStore:
    """latest_generation returns a non-None generation (healthy shape)."""

    def __init__(self, items: list[CognitiveConsolidationItem] | None = None):
        self.items = list(items or [])

    def latest_generation(self, board_id: str) -> str | None:
        return "kg-1"

    def list_items(self, board_id: str, kg_generation_id: str, **_kw):
        return list(self.items)


class _NoGenStore:
    """latest_generation returns None — the live recovery_needed shape (FR2)."""

    def latest_generation(self, board_id: str) -> str | None:
        return None

    def list_items(self, board_id: str, kg_generation_id: str, **_kw):
        return []


def _gate(store) -> CognitiveCloseoutGate:
    return CognitiveCloseoutGate(store=store)


# --------------------------------------------------------------------------- #
# AC1 / AC2 — 599d6cea (degraded graph_state surfaces UNAVAILABLE)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("degraded_state", ["recovery_needed", "quarantined"])
def test_degraded_graph_state_surfaces_unavailable(degraded_state):
    """ts_abd2cb82 (recovery_needed) + ts_7fce86cf (quarantined): NC-1 Degraded-KG
    Auto-Skip — a non-None graph_state in _RISK_STATE_HARD_REJECT (without
    board_skip) produces allowed=True + outcome=UNAVAILABLE + reason=
    DEGRADED_KG_AUTO_SKIP (auditable, not silent). The outcome value is reused
    from the existing UNAVAILABLE member; no new outcome enum member added (F16
    no-new-enum constraint)."""
    assert degraded_state in _RISK_STATE_HARD_REJECT
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state=degraded_state,
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.UNAVAILABLE.value
    assert result.reason == CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value


# --------------------------------------------------------------------------- #
# AC3 / AC6 — 62902967 (null generation UNAVAILABLE; skip precedence)
# --------------------------------------------------------------------------- #


def test_ts_b054e4f7_null_generation_surfaces_unavailable():
    """No graph_state arg, latest_generation()->None, skip=false: the empty
    ledger is NOT mapped to ALLOWED — this is the exact 3cf5dede failure mode."""
    result = _gate(_NoGenStore()).evaluate(
        board_id="board-1",
        entity_type="spec",
        entity_id="spec-1",
        target_status="done",
    )
    assert result.allowed is False
    assert result.outcome == CognitiveCloseoutOutcome.UNAVAILABLE.value
    assert result.reason == CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value


def test_ts_01d256b9_skip_wins_over_degraded():
    """board_skip_enabled=True short-circuits the degraded liveness branch to a
    SKIPPED result (allowed=True), byte-identical to the exception-path skip."""
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        board_skip_enabled=True,
        graph_state="recovery_needed",
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.SKIPPED.value
    assert result.reason == CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value


# --------------------------------------------------------------------------- #
# AC4 / AC5 — 84d02ee1 (healthy graph no-regression)
# --------------------------------------------------------------------------- #


def test_ts_4663630d_healthy_empty_stays_allowed():
    """Non-None generation, empty active items, healthy graph_state: terminal
    ALLOWED branch preserved (the new trigger must NOT shadow it)."""
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="healthy",
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.ALLOWED.value
    assert result.reason == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value


def test_non_hardreject_states_stay_allowed():
    """Boundary guard: at_risk / backpressure are DELIBERATELY not in the
    degraded predicate (_RISK_STATE_HARD_REJECT = {recovery_needed, quarantined},
    the single shared set per dec_e66161eb). A non-hard-reject graph_state with a
    real generation closes out normally — only the hard-reject states block. This
    pins the spec's hard-reject-only contract: a future re-broadening that blocks
    at_risk (which would re-break legitimate mid-SDLC done transitions) fails here.
    Extending the block to the at_risk-with-unreadable-telemetry shape is a
    deliberate non-goal of this spec / a potential follow-up."""
    for state in ("at_risk", "backpressure"):
        assert state not in _RISK_STATE_HARD_REJECT
        result = _gate(_FakeStore()).evaluate(
            board_id="board-1",
            entity_type="task",
            entity_id="card-1",
            target_status="done",
            graph_state=state,
        )
        assert result.allowed is True
        assert result.outcome == CognitiveCloseoutOutcome.ALLOWED.value


def test_ts_6fa3e068_healthy_pending_stays_blocked():
    """Non-None generation + one PENDING item matching the entity, healthy
    graph_state: existing BLOCKED path unaffected by the new trigger."""
    store = _FakeStore([_item("task:card-1", CognitiveItemStatus.PENDING.value)])
    result = _gate(store).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="healthy",
    )
    assert result.allowed is False
    assert result.outcome == CognitiveCloseoutOutcome.BLOCKED.value
    assert result.reason == CognitiveCloseoutReason.COGNITIVE_CONSOLIDATION_PENDING.value
    assert result.blocking_count == 1


# --------------------------------------------------------------------------- #
# AC14 — 3bd086d0 (degraded closeout emits an unavailable telemetry sample, TR6)
# --------------------------------------------------------------------------- #


def test_ts_327df96b_degraded_emits_unavailable_sample():
    """NC-1: degraded closeout now emits outcome=UNAVAILABLE + reason=
    DEGRADED_KG_AUTO_SKIP (the new type-safe reason member).  Outcome value
    reuses the existing UNAVAILABLE member — no new outcome enum (F16 constraint)."""
    reset_closeout_gate_samples()
    _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="recovery_needed",
    )
    sample = get_closeout_gate_samples()[-1]
    assert sample["outcome"] == CognitiveCloseoutOutcome.UNAVAILABLE.value
    assert sample["reason"] == CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value
    assert sample["skip_enabled"] == "false"


# --------------------------------------------------------------------------- #
# AC8 / AC13 — eb5bf3f3 (structural: gate sync/no-IO + single shared predicate)
# --------------------------------------------------------------------------- #


def test_ts_80500309_gate_stays_sync_no_io():
    assert not inspect.iscoroutinefunction(CognitiveCloseoutGate.evaluate)
    src = inspect.getsource(gate_mod)
    for forbidden in ("AsyncSession", "get_kg_health", "get_db", "open_board_connection"):
        assert forbidden not in src, (
            f"the closeout gate must stay sync/pure (no I/O) — found {forbidden!r}"
        )


def test_ts_dedefaf9_single_shared_predicate_identity():
    # both the gate and the tick reuse the SAME backpressure frozenset object
    assert gate_mod._RISK_STATE_HARD_REJECT is _RISK_STATE_HARD_REJECT
    assert kg_tick_policy._RISK_STATE_HARD_REJECT is _RISK_STATE_HARD_REJECT
    # neither module redefines a local/parallel predicate set
    for mod in (gate_mod, kg_tick_policy):
        src = inspect.getsource(mod)
        assert "_RISK_STATE_HARD_REJECT = " not in src, (
            "the degraded predicate must be IMPORTED, never redefined locally"
        )


# --------------------------------------------------------------------------- #
# AC7 — c8e10019 (async plumbing threads graph_state, blocks before mutation)
# --------------------------------------------------------------------------- #


class _SpyGate:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def evaluate(self, **kwargs):
        self.calls.append(dict(kwargs))
        return SimpleNamespace(
            allowed=False,
            reason="cognitive_status_unavailable",
            blocking_count=0,
            blocking_items=(),
        )


async def _seed_validation_card() -> tuple[str, str]:
    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F16 wiring board", owner_id="f16-agent", settings={}))
        db.add(Spec(id=spec_id, board_id=board_id, title="F16 wiring spec",
                    status=SpecStatus.IN_PROGRESS, created_by="f16-agent",
                    acceptance_criteria=[], test_scenarios=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="F16 card",
                    status=CardStatus.VALIDATION, card_type=CardType.NORMAL,
                    position=0, created_by="f16-agent"))
        await db.commit()
    return board_id, card_id


@pytest.mark.asyncio
async def test_ts_3eda0dc9_async_plumbing_blocks_before_mutation(monkeypatch):
    _, card_id = await _seed_validation_card()
    spy = _SpyGate()

    async def _stub_health(board_id, db, scheduler_control=None):
        return {"graph_state": "recovery_needed"}

    monkeypatch.setattr(kg_health_service, "get_kg_health", _stub_health)

    validation_data = {
        "confidence": 99, "confidence_justification": "strong",
        "estimated_completeness": 100, "completeness_justification": "complete",
        "estimated_drift": 0, "drift_justification": "none",
        "recommendation": "approve", "general_justification": "ready",
    }
    async with get_session_factory()() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: spy
        with pytest.raises(ValueError, match="cognitive_status_unavailable"):
            await service.submit_task_validation(
                card_id=card_id, reviewer_id="f16-agent",
                reviewer_name="f16-agent", data=validation_data,
            )
        await db.rollback()

    # the async caller resolved graph_state via get_kg_health and threaded it in
    assert spy.calls, "the closeout gate was never reached"
    assert spy.calls[0]["graph_state"] == "recovery_needed"
    assert spy.calls[0]["target_status"] == "done"

    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
    assert card.status == CardStatus.VALIDATION  # no status mutation
    assert card.validations in (None, [])  # no validation/conclusion append


@pytest.mark.asyncio
async def test_resolve_graph_state_fail_safe_returns_none(monkeypatch):
    """FR6 fail-safe: a get_kg_health failure resolves to None (never swallowed
    into ALLOWED — the gate's null-generation liveness then governs)."""
    async def _boom(board_id, db, scheduler_control=None):
        raise kg_health_service.BoardNotFoundError("nope")

    monkeypatch.setattr(kg_health_service, "get_kg_health", _boom)

    result = await services_main._resolve_closeout_graph_state("b", object())
    assert result is None


# --------------------------------------------------------------------------- #
# F17 tick admission scaffolding
# --------------------------------------------------------------------------- #


class _FakeSession:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False
        self.boards = SimpleNamespace(get=self._get_board)
        self.services = SimpleNamespace(
            kg=SimpleNamespace(dispatch_manual_tick=self._dispatch_manual_tick)
        )

    async def _get_board(self, board_id: str):
        return SimpleNamespace(id=board_id, owner_id="op")

    async def _dispatch_manual_tick(self, **kwargs) -> list[str]:
        return await kg_tick_policy.dispatch_manual_tick(
            relational_context=self,
            **kwargs,
        )

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


def _install_tick_health(monkeypatch, graph_state):
    """Stub kg_tick.get_kg_health; return the list that records probe calls."""
    calls: list[str] = []

    async def _stub(board_id, db, scheduler_control=None):
        calls.append(board_id)
        return {"graph_state": graph_state}

    monkeypatch.setattr(kg_tick, "get_kg_health", _stub)
    monkeypatch.setattr(kg_tick_policy, "get_kg_health", _stub)
    return calls


def _tick_principal() -> Principal:
    return Principal(
        "op",
        realm_id=LOCAL_REALM_ID,
        claims={
            "roles": ["admin"],
            "permissions": ["kg.admin.settings_write"],
        },
    )


def _request_without_scheduler():
    from starlette.requests import Request

    app = SimpleNamespace(state=SimpleNamespace(runtime_composition=None))
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/kg/tick/run-now",
            "headers": [],
            "app": app,
        }
    )


async def _assert_tick_lease_reacquirable() -> None:
    lease_provider = get_lease_provider()
    reacquired = await lease_provider.try_acquire(
        "kg_daily_tick",
        ttl_seconds=300,
    )
    assert reacquired is not None
    await lease_provider.release(reacquired)


def _spy_publish(monkeypatch):
    published: list[object] = []

    async def _publish_tick_events(
        session,  # noqa: ARG001
        *,
        board_id,
        actor_id=None,
        actor_type=None,
        scheduled_at=None,
        force_full_rebuild=False,
        tick_id=None,
    ):
        published.append(
            SimpleNamespace(
                board_id=board_id,
                actor_id=actor_id,
                actor_type=actor_type,
                scheduled_at=scheduled_at,
                force_full_rebuild=force_full_rebuild,
                tick_id=tick_id,
            )
        )
        return [str(tick_id)]

    import okto_pulse.core.events.handlers.kg_decay_tick as kg_decay_tick

    monkeypatch.setattr(kg_decay_tick, "publish_tick_events", _publish_tick_events)
    return published


# --------------------------------------------------------------------------- #
# AC9 / AC10 — 5f9af5b8 (tick 409 on degraded; 202 on healthy)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_ts_de1c347a_degraded_board_refused_409(monkeypatch):
    from fastapi import HTTPException

    _install_tick_health(monkeypatch, "recovery_needed")
    published = _spy_publish(monkeypatch)
    db = _FakeSession()
    payload = TickRunNowRequest(board_id="board-degraded", force_full_rebuild=False)

    with pytest.raises(HTTPException) as exc_info:
        await run_tick_now(
            payload,
            _request_without_scheduler(),
            principal=_tick_principal(),
            db=db,
        )

    assert exc_info.value.status_code == 409
    detail = exc_info.value.detail
    assert detail["error"] == "graph_recovery_needed"
    assert detail["graph_state"] == "recovery_needed"
    assert detail["board_id"] == "board-degraded"
    assert published == []  # refused before any dispatch
    assert db.committed is False


@pytest.mark.asyncio
async def test_ts_23ae75b2_healthy_board_proceeds_202(monkeypatch):
    probe_calls = _install_tick_health(monkeypatch, "healthy")
    published = _spy_publish(monkeypatch)
    db = _FakeSession()
    payload = TickRunNowRequest(board_id="board-healthy", force_full_rebuild=False)

    resp = await run_tick_now(
        payload,
        _request_without_scheduler(),
        principal=_tick_principal(),
        db=db,
    )

    assert isinstance(resp, TickRunNowResponse)
    assert resp.status == "running"
    assert resp.tick_id
    assert len(published) == 1
    assert db.committed is True
    # the gate DID probe the concrete board (it just admitted a healthy one)
    assert probe_calls == ["board-healthy"]


# --------------------------------------------------------------------------- #
# AC11 / AC12 — 8e629790 (lock contention wins; global tick not refused)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_ts_58aac88a_lock_contention_wins_over_health(monkeypatch):
    from fastapi import HTTPException

    probe_calls = _install_tick_health(monkeypatch, "recovery_needed")
    _spy_publish(monkeypatch)
    payload = TickRunNowRequest(board_id="board-degraded", force_full_rebuild=False)

    lease_provider = get_lease_provider()
    lease = await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=300)
    assert lease is not None
    try:
        with pytest.raises(HTTPException) as exc_info:
            await run_tick_now(
                payload,
                _request_without_scheduler(),
                principal=_tick_principal(),
                db=_FakeSession(),
            )
    finally:
        await lease_provider.release(lease)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["error"] == "tick_already_running"
    assert probe_calls == []  # health branch unreachable while the lock is held


@pytest.mark.asyncio
async def test_ts_21f124dd_global_tick_not_health_refused(monkeypatch):
    probe_calls = _install_tick_health(monkeypatch, "recovery_needed")
    published = _spy_publish(monkeypatch)
    db = _FakeSession()
    payload = TickRunNowRequest(board_id=None, force_full_rebuild=False)

    resp = await run_tick_now(
        payload,
        _request_without_scheduler(),
        principal=_tick_principal(),
        db=db,
    )

    assert resp.status == "running"
    assert len(published) == 1
    assert probe_calls == []  # no per-board probe for a global (board_id=None) tick


# --------------------------------------------------------------------------- #
# AC15 — b9523f4f (MCP twin inherits the degraded refusal via the shared gate)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_ts_67fe9fd2_mcp_twin_inherits_refusal(monkeypatch):
    # the MCP twin shares the SAME _refuse_tick_if_degraded gate as the REST path
    _install_tick_health(monkeypatch, "recovery_needed")

    async def _fake_ctx(board_id):
        return server.AgentContext(
            "agent-mcp",
            "Agent",
            board_id,
            ["kg.admin.settings_write"],
        )

    health_session = _FakeSession()

    class _SessionContext:
        async def __aenter__(self):
            return health_session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _SessionContext(),
    )

    # ensure the global advisory lock is free
    assert not get_async_lock("kg_daily_tick", "global").locked()

    raw = await server.okto_pulse_kg_tick_run_now.fn(board_id="board-degraded")

    payload = json.loads(raw)
    assert payload["error"] == "graph_recovery_needed"
    assert payload["graph_state"] == "recovery_needed"
    assert payload["board_id"] == "board-degraded"
    await _assert_tick_lease_reacquirable()


@pytest.mark.asyncio
async def test_mcp_tick_releases_lease_when_health_probe_raises(monkeypatch):
    async def _fake_ctx(_board_id):
        return server.AgentContext(
            "agent-mcp",
            "Agent",
            _board_id,
            ["kg.admin.settings_write"],
        )

    async def _failed_health(_board_id, _uow, scheduler_control=None):
        del scheduler_control
        raise RuntimeError("health probe unavailable")

    health_session = _FakeSession()

    class _SessionContext:
        async def __aenter__(self):
            return health_session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(kg_tick_policy, "get_kg_health", _failed_health)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _SessionContext(),
    )

    with pytest.raises(RuntimeError, match="health probe unavailable"):
        await server.okto_pulse_kg_tick_run_now.fn(board_id="board-health-error")

    await _assert_tick_lease_reacquirable()


@pytest.mark.asyncio
async def test_mcp_tick_maps_retryable_recovery_defer_and_releases_lease(
    monkeypatch,
):
    _install_tick_health(monkeypatch, "healthy")

    async def _fake_ctx(_board_id):
        return server.AgentContext(
            "agent-mcp",
            "Agent",
            _board_id,
            ["kg.admin.settings_write"],
        )

    class _DeferredSession(_FakeSession):
        async def _dispatch_manual_tick(self, **_kwargs) -> None:
            raise kg_tick_policy.KGTickAdmissionDeferred(
                reason_code="global_recovery_active"
            )

    deferred_session = _DeferredSession()

    class _SessionContext:
        async def __aenter__(self):
            return deferred_session

        async def __aexit__(self, exc_type, _exc, _tb):
            if exc_type is not None:
                await deferred_session.rollback()
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _SessionContext(),
    )

    raw = await server.okto_pulse_kg_tick_run_now.fn(board_id="board-deferred")

    payload = json.loads(raw)
    assert payload == {
        "error": "kg_tick_deferred_for_global_recovery",
        "reason": "global_recovery_active",
        "retryable": True,
        "message": (
            "KG tick deferred while Global Discovery recovery owns the "
            "mutation fence"
        ),
    }
    assert deferred_session.committed is False
    assert deferred_session.rolled_back is True
    await _assert_tick_lease_reacquirable()


@pytest.mark.asyncio
async def test_mcp_tick_releases_lease_after_success(monkeypatch):
    _install_tick_health(monkeypatch, "healthy")
    published = _spy_publish(monkeypatch)

    async def _fake_ctx(_board_id):
        return server.AgentContext(
            "agent-mcp",
            "Agent",
            _board_id,
            ["kg.admin.settings_write"],
        )

    session = _FakeSession()

    class _SessionContext:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _SessionContext(),
    )

    raw = await server.okto_pulse_kg_tick_run_now.fn(board_id="board-success")

    payload = json.loads(raw)
    assert payload["status"] == "running"
    assert payload["tick_id"]
    assert payload["correlation_id"] == payload["tick_id"]
    assert payload["tick_ids"] == [payload["tick_id"]]
    assert payload["scheduled_at"] == published[0].scheduled_at
    assert published[0].tick_id == payload["tick_id"]
    assert session.committed is True
    assert len(published) == 1
    await _assert_tick_lease_reacquirable()


@pytest.mark.asyncio
async def test_mcp_tick_release_failure_after_commit_does_not_mask_success(
    monkeypatch,
    caplog,
):
    _install_tick_health(monkeypatch, "healthy")
    published = _spy_publish(monkeypatch)

    class _ReleaseFailsAfterCleanup(FakeLeaseProvider):
        async def release(self, handle):
            await super().release(handle)
            raise RuntimeError("coordination cleanup unavailable")

    lease_provider = _ReleaseFailsAfterCleanup()
    register_coordination_providers(lease_provider=lease_provider)

    async def _fake_ctx(_board_id):
        return server.AgentContext(
            "agent-mcp",
            "Agent",
            _board_id,
            ["kg.admin.settings_write"],
        )

    session = _FakeSession()

    class _SessionContext:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _SessionContext(),
    )

    with caplog.at_level(
        "ERROR",
        logger="okto_pulse.mcp.tick",
    ):
        raw = await server.okto_pulse_kg_tick_run_now.fn(
            board_id="board-release-error"
        )

    payload = json.loads(raw)
    assert payload["status"] == "running"
    assert payload["tick_ids"] == [payload["tick_id"]]
    assert session.committed is True
    assert len(published) == 1
    assert lease_provider.is_held("kg_daily_tick") is False
    record = next(
        item
        for item in caplog.records
        if getattr(item, "event", None) == "kg.tick.lease_release_failed"
    )
    assert record.committed is True
    assert record.source == "mcp"


@pytest.mark.asyncio
async def test_mcp_tick_releases_lease_after_schedule_failure(monkeypatch):
    _install_tick_health(monkeypatch, "healthy")

    async def _fake_ctx(_board_id):
        return server.AgentContext(
            "agent-mcp",
            "Agent",
            _board_id,
            ["kg.admin.settings_write"],
        )

    class _FailedSession(_FakeSession):
        async def _dispatch_manual_tick(self, **_kwargs) -> None:
            raise RuntimeError("event store unavailable")

    failed_session = _FailedSession()

    class _SessionContext:
        async def __aenter__(self):
            return failed_session

        async def __aexit__(self, exc_type, _exc, _tb):
            if exc_type is not None:
                await failed_session.rollback()
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _SessionContext(),
    )

    raw = await server.okto_pulse_kg_tick_run_now.fn(
        board_id="board-schedule-error"
    )

    payload = json.loads(raw)
    assert payload["error"] == "tick_schedule_failed"
    assert payload["detail"] == "event store unavailable"
    assert failed_session.committed is False
    assert failed_session.rolled_back is True
    await _assert_tick_lease_reacquirable()
