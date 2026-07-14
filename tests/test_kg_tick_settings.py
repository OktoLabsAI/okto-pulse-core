"""Wave 2 NC f9732afc — KG decay tick controllability tests (spec 54399628).

Cobre os FRs/BRs críticos de IMPL-A (CoreSettings + RuntimeSettings),
IMPL-B (trigger swap + hot-reload), IMPL-C (endpoint POST run-now +
advisory lock 409).

Test scenarios mapeados:
- TS1: GET /settings/runtime retorna 3 campos novos com defaults
- TS2: PUT /settings/runtime persiste + dispara reschedule_job + log
- TS3: PUT com valor fora do range retorna 422
- TS4: POST /kg/tick/run-now retorna 202 + 409 retry imediato
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from coordination_fakes import FakeLeaseProvider, FakeWriteLockPort
from okto_pulse.core.ports.coordination import (
    get_lease_provider,
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)


pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _coordination_ports():
    reset_coordination_providers_for_tests()
    register_coordination_providers(
        lease_provider=FakeLeaseProvider(),
        write_lock_port=FakeWriteLockPort(),
    )
    yield
    reset_coordination_providers_for_tests()


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


async def test_ts1_get_settings_returns_three_new_defaults():
    """TS1 — GET /api/v1/settings/runtime expõe os 3 campos novos com
    defaults (1440, 7, 0).

    Hits the service layer directly to keep the test sync with the API
    contract without spinning a TestClient (avoids ASGI lifespan setup
    weight).
    """
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.settings_service import (
        get_runtime_settings,
    )

    factory = get_session_factory()
    async with factory() as db:
        snapshot = await get_runtime_settings(db)

    assert snapshot["kg_decay_tick_interval_minutes"] == 1440
    assert snapshot["kg_decay_tick_staleness_days"] == 7
    assert snapshot["kg_decay_tick_max_age_days"] == 0
    assert "restart_required" in snapshot


async def test_ts2_put_persists_tick_interval_and_emits_reschedule_log(caplog):
    """TS2 — PUT settings com kg_decay_tick_interval_minutes=60:
    - Persiste no AppSetting
    - Dispara structured log kg.tick.rescheduled (mesmo sem scheduler real,
      o branch try/except logga reschedule_failed quando singleton=None,
      mas o log de rescheduled deve aparecer quando scheduler está set)
    - Subsequent GET retorna 60.
    """
    import logging
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.settings_service import (
        get_runtime_settings,
        put_runtime_settings,
    )

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):  # noqa: D401
            captured.append(record)

    logger = logging.getLogger("okto_pulse.services.settings")
    logger.setLevel(logging.INFO)
    handler = _Capture(level=logging.INFO)
    logger.addHandler(handler)

    factory = get_session_factory()
    try:
        async with factory() as db:
            await put_runtime_settings(
                db, {"kg_decay_tick_interval_minutes": 60}
            )
        async with factory() as db:
            snapshot = await get_runtime_settings(db)
    finally:
        logger.removeHandler(handler)

    assert snapshot["kg_decay_tick_interval_minutes"] == 60

    # Either kg.tick.rescheduled (if scheduler singleton was set) OR
    # kg.tick.reschedule_failed (if singleton is None — test context).
    # Both prove the reschedule code path was invoked.
    relevant_events = {
        getattr(r, "event", None)
        for r in captured
        if getattr(r, "event", None) in (
            "kg.tick.rescheduled",
            "kg.tick.reschedule_failed",
            "kg.tick.reschedule_skipped",
        )
    }
    assert relevant_events, (
        f"expected reschedule log emission; got events {[getattr(r, 'event', None) for r in captured]}"
    )


async def test_ts3_put_outside_range_rejected_by_pydantic():
    """TS3 — RuntimeSettingsPayload rejeita valor abaixo do mínimo (5).

    Validação acontece na própria classe Pydantic, antes do request chegar
    no service. Verificamos diretamente via Pydantic ValidationError.
    """
    from pydantic import ValidationError
    from okto_pulse.community.api.settings import RuntimeSettingsPayload

    with pytest.raises(ValidationError) as exc_info:
        RuntimeSettingsPayload(kg_decay_tick_interval_minutes=4)
    msg = str(exc_info.value)
    assert "greater than or equal to 5" in msg or "ge=5" in msg or "5" in msg

    with pytest.raises(ValidationError):
        RuntimeSettingsPayload(kg_decay_tick_interval_minutes=10081)


async def test_ts6_reset_last_recomputed_at_handles_empty_scope():
    """TS6 — `reset_last_recomputed_at` não quebra quando o board-alvo não
    tem grafo local. Exercita o caminho de iteração + try/except por board
    sem depender do estado global acumulado pela suíte monolítica.

    Validação completa do comportamento force_full_rebuild=true requer
    Kuzu fixture com nodes pré-existentes — deferred para integration
    test em sessão futura.
    """
    from okto_pulse.core.application.kg_tick import reset_last_recomputed_at

    # Per-board scope com board inexistente tenta o GraphTransaction composto,
    # que falha de forma best-effort no bloco interno.
    await reset_last_recomputed_at(board_id="board-does-not-exist-uuid")


async def test_ts5_mcp_dispatch_helper_replicates_endpoint_behavior(monkeypatch):
    """TS5 — MCP tool gemellar `okto_pulse_kg_tick_run_now` reusa o
    mesmo `dispatch_manual_tick` da aplicação usado pelo endpoint REST. Não temos harness
    FastMCP completa em pytest, então validamos a sub-função compartilhada
    diretamente: ela deve aceitar tick_id + board_id + force_full_rebuild
    e completar sem exception (best-effort background).
    """
    from okto_pulse.core.application import kg_tick
    from okto_pulse.core.events.handlers import kg_decay_tick

    published: list[dict] = []

    async def _fake_publish_tick_events(session, *, board_id=None, **kwargs):
        assert session is not None
        published.append({"board_id": board_id, **kwargs})
        return ["fanout-tick-uuid"]

    monkeypatch.setattr(
        kg_decay_tick, "publish_tick_events", _fake_publish_tick_events
    )

    await kg_tick.dispatch_manual_tick(
        tick_id="ts5-test-uuid",
        board_id="board-does-not-exist-uuid",
        force_full_rebuild=False,
        relational_context=object(),
    )
    assert len(published) == 1
    assert published[0]["board_id"] == "board-does-not-exist-uuid"


async def test_ts5_dispatch_requires_explicit_relational_context(monkeypatch):
    """The Core helper must not open a concrete relational scope implicitly."""
    from okto_pulse.core.application import kg_tick
    from okto_pulse.core.events.handlers import kg_decay_tick

    published: list[dict] = []

    async def _fake_publish_tick_events(session, *, board_id=None, **kwargs):
        published.append({"board_id": board_id, **kwargs})
        return ["fanout-tick-uuid"]

    monkeypatch.setattr(
        kg_decay_tick, "publish_tick_events", _fake_publish_tick_events
    )
    with pytest.raises(TypeError, match="relational_context"):
        await kg_tick.dispatch_manual_tick(
            tick_id="ts5-missing-context",
            board_id="board-does-not-exist-uuid",
            force_full_rebuild=False,
        )
    assert published == []


async def test_ts5_dispatch_explicit_session_is_caller_owned(monkeypatch):
    """Explicit sessions are owned by the caller.

    The helper must publish with exactly that session and must not open an
    injected scope or assume commit/rollback.
    """
    from okto_pulse.core.application import kg_tick
    from okto_pulse.core.events.handlers import kg_decay_tick

    class _ExplicitSession:
        def __init__(self) -> None:
            self.committed = False
            self.rolled_back = False

        async def commit(self) -> None:
            self.committed = True

        async def rollback(self) -> None:
            self.rolled_back = True

    explicit = _ExplicitSession()
    published: list[dict] = []

    async def _fake_publish_tick_events(session, *, board_id=None, **kwargs):
        assert session is explicit
        published.append({"board_id": board_id, **kwargs})
        return ["fanout-tick-uuid"]

    monkeypatch.setattr(
        kg_decay_tick, "publish_tick_events", _fake_publish_tick_events
    )
    await kg_tick.dispatch_manual_tick(
        tick_id="ts5-explicit-session",
        board_id="board-does-not-exist-uuid",
        force_full_rebuild=False,
        relational_context=explicit,
    )

    assert len(published) == 1
    assert published[0]["board_id"] == "board-does-not-exist-uuid"
    assert explicit.committed is False
    assert explicit.rolled_back is False


async def test_ts4_endpoint_run_now_returns_202_and_409_on_retry(monkeypatch):
    """TS4 — POST /api/v1/kg/tick/run-now:
    - First call: 202 com tick_id, status=running, scheduled_at
    - Concurrent retry: 409 com error=tick_already_running

    Usa o módulo diretamente (sem TestClient) para evitar setup ASGI.
    Captura o lock manualmente para simular "já em execução".
    """
    from okto_pulse.community.api import kg_tick
    from okto_pulse.community.api.kg_tick import (
        TickRunNowRequest,
        TickRunNowResponse,
        run_tick_now,
    )
    from fastapi import HTTPException

    payload = TickRunNowRequest(
        board_id="board-does-not-exist-uuid",
        force_full_rebuild=False,
    )

    class _FakeSession:
        def __init__(self) -> None:
            self.committed = False
            self.rolled_back = False
            self.services = SimpleNamespace(
                kg=SimpleNamespace(dispatch_manual_tick=self._dispatch_manual_tick)
            )

        async def _dispatch_manual_tick(self, **kwargs) -> None:
            from okto_pulse.core.application.kg_tick import dispatch_manual_tick

            await dispatch_manual_tick(relational_context=self, **kwargs)

        async def commit(self) -> None:
            self.committed = True

        async def rollback(self) -> None:
            self.rolled_back = True

    from okto_pulse.core.events.handlers import kg_decay_tick

    published: list[dict] = []

    async def _fake_publish_tick_events(session, *, board_id=None, **kwargs):
        assert isinstance(session, _FakeSession)
        published.append({"board_id": board_id, **kwargs})
        return ["fanout-tick-uuid"]

    monkeypatch.setattr(
        kg_decay_tick, "publish_tick_events", _fake_publish_tick_events
    )

    # F17: run_tick_now now probes board health before allocating a tick. Stub
    # it healthy so this 202 path proceeds (a concrete board that is NOT degraded
    # is admitted); the advisory-lock 409 path below short-circuits before the
    # probe is reached.
    async def _healthy_health(board_id, db, scheduler_control=None):
        return {"graph_state": "healthy"}

    monkeypatch.setattr(kg_tick, "get_kg_health", _healthy_health)
    fake_db = _FakeSession()

    # First call: 202 success.
    response = await run_tick_now(
        payload,
        _request_without_scheduler(),
        user="test-user",
        db=fake_db,
    )
    assert isinstance(response, TickRunNowResponse)
    assert response.status == "running"
    assert response.tick_id  # non-empty uuid
    assert response.scheduled_at  # ISO datetime
    assert fake_db.committed is True
    assert fake_db.rolled_back is False
    assert len(published) == 1

    # Capture lease to simulate in-flight tick — second call must 409.
    lease_provider = get_lease_provider()
    lease = await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=300)
    assert lease is not None
    try:
        with pytest.raises(HTTPException) as exc_info:
            await run_tick_now(
                payload,
                _request_without_scheduler(),
                user="test-user-2",
                db=_FakeSession(),
            )
        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail.get("error") == "tick_already_running"
    finally:
        await lease_provider.release(lease)
