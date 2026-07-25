from __future__ import annotations

import ast
import json
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.kg.guarded_write import (
    GuardedWriteError,
    guarded_board_write,
)
from okto_pulse.core.kg.interfaces.graph_lifecycle import (
    GraphLifecycleStepResult,
)
from okto_pulse.core.kg.safe_write_lifecycle import (
    KGSafeWriteLifecycle,
    LockOwnerProbe,
)
from okto_pulse.core.kg.single_writer_lock import LockAcquisition
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    get_barrier_mode,
    get_unguarded_count,
    require_write_token,
    reset_unguarded_counter,
    set_barrier_mode,
)


class _Catalog:
    def __init__(self) -> None:
        self.tools: dict[str, object] = {}

    def tool(self):
        def _register(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _register


class _WriterLock:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.token = "writer-token"
        self.active = False

    def acquire(self, **_kwargs) -> LockAcquisition:
        self.events.append("lock_acquire")
        self.active = True
        return LockAcquisition(
            acquired=True,
            owner_token=self.token,
            expires_at=None,
            current_owner=None,
        )

    def is_owner(self, _board_id: str, owner_token: str) -> bool:
        return self.active and owner_token == self.token

    def renew(self, **_kwargs) -> bool:
        return self.active

    def release(self, **_kwargs) -> bool:
        self.events.append("lock_release")
        self.active = False
        return True


def _lifecycle(
    writer_lock: _WriterLock,
    events: list[str],
    *,
    fail_first_flush: bool = False,
) -> KGSafeWriteLifecycle:
    flush_failed = False

    def _step(_board_id: str, _graph_type: str, step: str):
        nonlocal flush_failed
        events.append(step)
        if fail_first_flush and step == "flush" and not flush_failed:
            flush_failed = True
            return GraphLifecycleStepResult(ok=False, detail="forced failure")
        return GraphLifecycleStepResult(ok=True)

    return KGSafeWriteLifecycle(
        step_adapter=_step,
        owner_probe=LockOwnerProbe(is_active_owner=writer_lock.is_owner),
    )


@contextmanager
def _strict_barrier():
    previous = get_barrier_mode()
    reset_unguarded_counter()
    set_barrier_mode(BarrierMode.STRICT)
    try:
        yield
    finally:
        set_barrier_mode(previous)
        reset_unguarded_counter()


def test_guarded_write_requires_durability_on_normal_exit() -> None:
    events: list[str] = []
    writer_lock = _WriterLock(events)

    with pytest.raises(GuardedWriteError) as caught:
        with guarded_board_write(
            "board-guarded",
            operation="test_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            writer_lock=writer_lock,
            lifecycle=_lifecycle(writer_lock, events),
        ):
            events.append("mutation")

    assert caught.value.code == "durability_not_applied"
    assert events == ["lock_acquire", "mutation", "lock_release"]


def test_guarded_write_strict_mode_has_no_unguarded_sample() -> None:
    events: list[str] = []
    writer_lock = _WriterLock(events)

    with _strict_barrier():
        with guarded_board_write(
            "board-guarded",
            operation="test_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            writer_lock=writer_lock,
            lifecycle=_lifecycle(writer_lock, events),
        ) as lease:
            require_write_token("board-guarded")
            events.append("mutation")
            lease.ensure_durable()

        assert get_unguarded_count("board-guarded") == 0

    assert events == [
        "lock_acquire",
        "mutation",
        "checkpoint",
        "flush",
        "fsync",
        "lock_release",
    ]


def test_guarded_write_release_failure_reports_post_durability_ambiguity() -> None:
    events: list[str] = []

    class _ReleaseFailureLock(_WriterLock):
        def release(self, **_kwargs) -> bool:
            self.events.append("lock_release_failed")
            return False

    writer_lock = _ReleaseFailureLock(events)
    with pytest.raises(GuardedWriteError) as caught:
        with guarded_board_write(
            "board-guarded",
            operation="test_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            writer_lock=writer_lock,
            lifecycle=_lifecycle(writer_lock, events),
        ) as lease:
            events.append("mutation")
            lease.ensure_durable()

    assert caught.value.code == "writer_lock_release_failed"
    assert caught.value.details["durability_applied"] is True
    assert caught.value.details["write_may_be_applied"] is True
    assert caught.value.details["failure_phase"] == "release_after_durability"


def test_guarded_write_lifecycle_revalidates_real_lock_owner() -> None:
    events: list[str] = []
    writer_lock = _WriterLock(events)

    with pytest.raises(GuardedWriteError) as caught:
        with guarded_board_write(
            "board-guarded",
            operation="test_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            writer_lock=writer_lock,
            lifecycle=_lifecycle(writer_lock, events),
        ) as lease:
            writer_lock.active = False
            lease.ensure_durable()

    assert caught.value.code == "writer_lease_lost"
    assert caught.value.details["failure_phase"] == "before_lifecycle"
    assert events == ["lock_acquire", "lock_release"]


def test_guarded_write_heartbeat_prevents_stale_takeover() -> None:
    events: list[str] = []

    class _ExpiringWriterLock:
        def __init__(self) -> None:
            self.token: str | None = None
            self.expires_at = 0.0
            self.renew_count = 0

        def acquire(self, **_kwargs) -> LockAcquisition:
            if self.token is not None and time.monotonic() < self.expires_at:
                return LockAcquisition(
                    acquired=False,
                    owner_token=None,
                    expires_at=None,
                    current_owner=self.token,
                )
            self.token = f"token-{time.monotonic_ns()}"
            self.expires_at = time.monotonic() + 0.05
            return LockAcquisition(
                acquired=True,
                owner_token=self.token,
                expires_at=None,
                current_owner=None,
            )

        def renew(self, *, owner_token: str, **_kwargs) -> bool:
            if not self.is_owner("", owner_token):
                return False
            self.renew_count += 1
            self.expires_at = time.monotonic() + 0.05
            return True

        def is_owner(self, _board_id: str, owner_token: str) -> bool:
            return (
                self.token == owner_token
                and time.monotonic() < self.expires_at
            )

        def release(self, *, owner_token: str, **_kwargs) -> bool:
            if self.token != owner_token:
                return False
            self.token = None
            return True

    writer_lock = _ExpiringWriterLock()
    with guarded_board_write(
        "board-renewed",
        operation="long_write",
        owner_id="agent-1",
        mutation_ref="mutation-1",
        ttl_seconds=1,
        renew_interval_seconds=0.01,
        writer_lock=writer_lock,
        lifecycle=_lifecycle(writer_lock, events),
    ) as lease:
        time.sleep(0.12)
        contender = writer_lock.acquire()
        assert contender.acquired is False
        lease.ensure_durable()

    assert writer_lock.renew_count >= 2


def test_worker_fence_blocks_mcp_writer_on_same_board() -> None:
    events: list[str] = []

    class _ContendedWriterLock(_WriterLock):
        def acquire(self, **kwargs) -> LockAcquisition:
            operation = str(kwargs["operation"])
            self.events.append(f"lock_acquire:{operation}")
            if self.active:
                return LockAcquisition(
                    acquired=False,
                    owner_token=None,
                    expires_at=None,
                    current_owner=self.token,
                )
            self.active = True
            self.token = f"writer-token:{operation}"
            return LockAcquisition(
                acquired=True,
                owner_token=self.token,
                expires_at=None,
                current_owner=None,
            )

    writer_lock = _ContendedWriterLock(events)
    lifecycle = _lifecycle(writer_lock, events)

    with guarded_board_write(
        "board-shared-writer",
        operation="consolidation_worker",
        owner_id="system:historical_consolidation",
        mutation_ref="queue-entry:session",
        writer_lock=writer_lock,
        lifecycle=lifecycle,
    ) as worker_lease:
        with pytest.raises(GuardedWriteError) as caught:
            with guarded_board_write(
                "board-shared-writer",
                operation="mcp_kg_commit",
                owner_id="agent:mcp",
                mutation_ref="mcp-session",
                writer_lock=writer_lock,
                lifecycle=lifecycle,
            ):
                raise AssertionError("contending MCP writer must not enter")
        assert caught.value.code == "lock_contention"
        worker_lease.ensure_durable()

    assert events == [
        "lock_acquire:consolidation_worker",
        "lock_acquire:mcp_kg_commit",
        "checkpoint",
        "flush",
        "fsync",
        "lock_release",
    ]


def test_guarded_write_renew_failure_prevents_ack() -> None:
    events: list[str] = []
    renewal_attempted = Event()

    class _FailingRenewLock(_WriterLock):
        def renew(self, **_kwargs) -> bool:
            renewal_attempted.set()
            return False

    writer_lock = _FailingRenewLock(events)
    with pytest.raises(GuardedWriteError) as caught:
        with guarded_board_write(
            "board-renew-failed",
            operation="long_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            ttl_seconds=1,
            renew_interval_seconds=0.01,
            writer_lock=writer_lock,
            lifecycle=_lifecycle(writer_lock, events),
        ) as lease:
            lease.ensure_durable()
            assert renewal_attempted.wait(timeout=0.5)
            lease.ensure_owned(failure_phase="before_ack")
            events.append("ack")

    assert caught.value.code == "writer_lease_lost"
    assert caught.value.details["failure_phase"] == "before_ack"
    assert "ack" not in events


def test_missing_lifecycle_remains_typed_when_lock_release_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write

    events: list[str] = []

    class _ReleaseExceptionLock(_WriterLock):
        def release(self, **_kwargs) -> bool:
            self.events.append("lock_release_failed")
            raise OSError("release unavailable")

    writer_lock = _ReleaseExceptionLock(events)
    monkeypatch.setattr(
        guarded_write,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_lifecycle=None),
    )

    with pytest.raises(GuardedWriteError) as caught:
        with guarded_board_write(
            "board-guarded",
            operation="test_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            writer_lock=writer_lock,
        ):
            raise AssertionError("boundary must not yield without lifecycle")

    assert caught.value.code == "safe_lifecycle_unavailable"
    assert caught.value.details["release_failure_type"] == "OSError"
    assert events == ["lock_acquire", "lock_release_failed"]


def test_lifecycle_resolution_failure_releases_lock_and_remains_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write

    events: list[str] = []
    writer_lock = _WriterLock(events)

    def _registry_failure():
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(guarded_write, "get_kg_registry", _registry_failure)

    with pytest.raises(GuardedWriteError) as caught:
        with guarded_board_write(
            "board-guarded",
            operation="test_write",
            owner_id="agent-1",
            mutation_ref="mutation-1",
            writer_lock=writer_lock,
        ):
            raise AssertionError("boundary must not yield without lifecycle")

    assert caught.value.code == "safe_lifecycle_unavailable"
    assert caught.value.details["provider_failure_type"] == "RuntimeError"
    assert caught.value.details["release_failure_type"] is None
    assert events == ["lock_acquire", "lock_release"]


@pytest.mark.asyncio
async def test_kg_begin_and_commit_deny_board_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_tools

    events: list[str] = []
    catalog = _Catalog()

    async def _agent():
        events.append("authenticate")
        return SimpleNamespace(id="agent-denied")

    async def _board_agent(board_id: str):
        events.append(f"acl:{board_id}")
        return None

    def _uow_factory():
        raise AssertionError("unauthorized request opened a UnitOfWork")

    async def _session(
        session_id: str,
        agent_id: str,
        *,
        allow_pending_commit: bool = False,
    ):
        events.append(
            f"session:{session_id}:{agent_id}:{allow_pending_commit}"
        )
        return SimpleNamespace(board_id="board-denied")

    def _guarded(*_args, **_kwargs):
        raise AssertionError("unauthorized request acquired a writer lock")

    monkeypatch.setattr(kg_tools, "_require_open_session", _session)
    monkeypatch.setattr(kg_tools, "guarded_board_write", _guarded)
    kg_tools.register_kg_tools(
        catalog,
        get_agent=_agent,
        get_uow=_uow_factory,
        get_board_agent=_board_agent,
    )

    begin = await catalog.tools["okto_pulse_kg_begin_consolidation"](
        board_id="board-denied",
        artifact_type="spec",
        artifact_id="spec-1",
        raw_content="content",
    )
    commit = await catalog.tools["okto_pulse_kg_commit_consolidation"](
        session_id="session-1",
    )

    assert json.loads(begin)["error"]["code"] == "unauthorized"
    assert json.loads(commit)["error"]["code"] == "unauthorized"
    assert events == [
        "authenticate",
        "acl:board-denied",
        "authenticate",
        "session:session-1:agent-denied:True",
        "acl:board-denied",
    ]


class _Uow:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.commits = 0

    async def commit(self) -> None:
        self.events.append("relational_commit")
        self.commits += 1


class _UowScope:
    def __init__(self, uow: _Uow) -> None:
        self.uow = uow

    async def __aenter__(self):
        self.uow.events.append("uow_enter")
        return self.uow

    async def __aexit__(self, exc_type, _exc, _tb) -> None:
        if exc_type is not None:
            self.uow.events.append("uow_rollback")
        self.uow.events.append("uow_exit")


async def _registered_guarded_commit(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_first_flush: bool,
):
    from okto_pulse.core.application import use_cases
    from okto_pulse.core.mcp import kg_tools

    events: list[str] = []
    writer_lock = _WriterLock(events)
    lifecycle = _lifecycle(
        writer_lock,
        events,
        fail_first_flush=fail_first_flush,
    )
    uow = _Uow(events)
    catalog = _Catalog()

    async def _agent():
        return SimpleNamespace(id="agent-guarded")

    async def _board_agent(_board_id: str):
        return SimpleNamespace(agent_id="agent-guarded")

    async def _session(*_args, **_kwargs):
        return SimpleNamespace(board_id="board-guarded")

    class _CommitUseCase:
        async def execute(self, *_args, **_kwargs):
            require_write_token("board-guarded")
            events.append("graph_commit")
            return SimpleNamespace(
                resp=SimpleNamespace(
                    model_dump_json=lambda: '{"committed":true}'
                )
            )

    async def _finalize(*_args, **_kwargs):
        events.append("finalize")

    async def _abort(*_args, **_kwargs):
        require_write_token("board-guarded")
        events.append("abort")

    def _guarded(*args, **kwargs):
        return guarded_board_write(
            *args,
            **kwargs,
            writer_lock=writer_lock,
            lifecycle=lifecycle,
        )

    monkeypatch.setattr(kg_tools, "_require_open_session", _session)
    monkeypatch.setattr(kg_tools, "guarded_board_write", _guarded)
    monkeypatch.setattr(
        use_cases,
        "CommitConsolidationUseCase",
        _CommitUseCase,
    )
    monkeypatch.setattr(
        kg_tools,
        "finalize_deferred_consolidation",
        _finalize,
    )
    monkeypatch.setattr(
        kg_tools,
        "abort_deferred_consolidation",
        _abort,
    )
    kg_tools.register_kg_tools(
        catalog,
        get_agent=_agent,
        get_uow=lambda: lambda **_kwargs: _UowScope(uow),
        get_board_agent=_board_agent,
    )
    return (
        catalog.tools["okto_pulse_kg_commit_consolidation"],
        uow,
        events,
    )


@pytest.mark.asyncio
async def test_mcp_commit_lifecycle_precedes_relational_ack_in_strict_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool, uow, events = await _registered_guarded_commit(
        monkeypatch,
        fail_first_flush=False,
    )

    with _strict_barrier():
        response = await tool(session_id="session-guarded")
        assert get_unguarded_count("board-guarded") == 0

    assert response == '{"committed":true}'
    assert uow.commits == 1
    assert events == [
        "lock_acquire",
        "uow_enter",
        "graph_commit",
        "checkpoint",
        "flush",
        "fsync",
        "relational_commit",
        "finalize",
        "uow_exit",
        "lock_release",
    ]


@pytest.mark.asyncio
async def test_mcp_lifecycle_failure_is_typed_and_compensated_before_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool, uow, events = await _registered_guarded_commit(
        monkeypatch,
        fail_first_flush=True,
    )

    with _strict_barrier():
        raw = await tool(session_id="session-guarded")
        assert get_unguarded_count("board-guarded") == 0

    payload = json.loads(raw)
    assert payload["error"]["code"] == "safe_lifecycle_failed"
    assert payload["error"]["retryable"] is True
    assert payload["error"]["details"]["failed_step"] == "flush"
    assert uow.commits == 0
    assert events == [
        "lock_acquire",
        "uow_enter",
        "graph_commit",
        "checkpoint",
        "flush",
        "uow_rollback",
        "uow_exit",
        "abort",
        "checkpoint",
        "flush",
        "fsync",
        "lock_release",
    ]


@pytest.mark.asyncio
async def test_power_and_export_tools_deny_board_before_provider_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_export_tools, kg_power_tools

    denied: list[str] = []

    async def _agent():
        return SimpleNamespace(id="agent-denied")

    async def _board_agent(board_id: str):
        denied.append(board_id)
        return None

    def _provider_called(*_args, **_kwargs):
        raise AssertionError("provider ran before board ACL")

    monkeypatch.setattr(
        kg_power_tools,
        "execute_cypher_read_only",
        _provider_called,
    )
    monkeypatch.setattr(
        kg_power_tools,
        "execute_natural_query",
        _provider_called,
    )
    monkeypatch.setattr(
        kg_power_tools,
        "get_schema_info",
        _provider_called,
    )

    power = _Catalog()
    export = _Catalog()
    kg_power_tools.register_kg_power_tools(
        power,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )
    kg_export_tools.register_kg_export_tools(
        export,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    calls = [
        power.tools["okto_pulse_kg_query_cypher"](
            board_id="board-denied",
            cypher="MATCH (n) RETURN n",
        ),
        power.tools["okto_pulse_kg_query_natural"](
            board_id="board-denied",
            nl_query="query",
        ),
        power.tools["okto_pulse_kg_schema_info"](
            board_id="board-denied",
        ),
        power.tools["okto_pulse_kg_verify_grounding"](
            board_id="board-denied",
            answer_text="answer",
            retrieved_rows_json="[]",
        ),
        power.tools["okto_pulse_kg_provenance_drift"](
            board_id="board-denied",
        ),
        export.tools["okto_pulse_kg_export_jsonld"](
            board_id="board-denied",
        ),
    ]
    responses = [json.loads(await call) for call in calls]

    assert all(
        (
            payload.get("error", {}).get("code")
            if isinstance(payload.get("error"), dict)
            else payload.get("error")
        )
        == "unauthorized"
        for payload in responses
    )
    assert denied == ["board-denied"] * 6


@pytest.mark.asyncio
async def test_power_tool_honors_effective_board_permission_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_power_tools

    provider_called = False

    async def _agent():
        return SimpleNamespace(id="agent-restricted")

    async def _board_agent(_board_id: str):
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet(
                {"kg": {"power": {"cypher": False}}}
            ),
        )

    def _provider(*_args, **_kwargs):
        nonlocal provider_called
        provider_called = True
        return {}

    monkeypatch.setattr(
        kg_power_tools,
        "execute_cypher_read_only",
        _provider,
    )
    catalog = _Catalog()
    kg_power_tools.register_kg_power_tools(
        catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_query_cypher"](
        board_id="board-restricted",
        cypher="MATCH (n) RETURN n",
    )

    payload = json.loads(raw)
    assert payload["error"]["code"] == "permission_denied"
    assert payload["error"]["required_permission"] == "kg.power.cypher"
    assert provider_called is False


@pytest.mark.asyncio
async def test_power_tool_requires_board_read_even_when_specific_flag_is_granted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_power_tools

    provider_called = False

    async def _agent():
        return SimpleNamespace(id="agent-board-read-revoked")

    async def _board_agent(_board_id: str):
        return SimpleNamespace(
            agent_id="agent-board-read-revoked",
            permissions=PermissionSet(
                {
                    "board": {"read": False},
                    "kg": {"power": {"cypher": True}},
                }
            ),
        )

    def _provider(*_args, **_kwargs):
        nonlocal provider_called
        provider_called = True
        return {}

    monkeypatch.setattr(
        kg_power_tools,
        "execute_cypher_read_only",
        _provider,
    )
    catalog = _Catalog()
    kg_power_tools.register_kg_power_tools(
        catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_query_cypher"](
        board_id="board-restricted",
        cypher="MATCH (n) RETURN n",
    )

    payload = json.loads(raw)
    assert payload["error"]["code"] == "permission_denied"
    assert payload["error"]["required_permission"] == "board.read"
    assert provider_called is False


@pytest.mark.asyncio
async def test_session_begin_honors_effective_board_permission_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_tools

    events: list[str] = []

    async def _agent():
        events.append("authenticate")
        return SimpleNamespace(id="agent-restricted")

    async def _board_agent(board_id: str):
        events.append(f"acl:{board_id}")
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet(
                {"kg": {"session": {"begin": False}}}
            ),
        )

    def _uow_factory():
        raise AssertionError("permission-denied request opened a UnitOfWork")

    def _guarded(*_args, **_kwargs):
        raise AssertionError("permission-denied request acquired a writer lock")

    monkeypatch.setattr(kg_tools, "guarded_board_write", _guarded)
    catalog = _Catalog()
    kg_tools.register_kg_tools(
        catalog,
        get_agent=_agent,
        get_uow=_uow_factory,
        get_board_agent=_board_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_begin_consolidation"](
        board_id="board-restricted",
        artifact_type="spec",
        artifact_id="spec-1",
        raw_content="content",
    )

    payload = json.loads(raw)
    assert payload["error"]["code"] == "permission_denied"
    assert payload["error"]["required_permission"] == "kg.session.begin"
    assert events == ["authenticate", "acl:board-restricted"]


@pytest.mark.asyncio
async def test_export_honors_effective_board_read_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import graph_export
    from okto_pulse.core.mcp import kg_export_tools

    events: list[str] = []

    async def _agent():
        events.append("authenticate")
        return SimpleNamespace(id="agent-restricted")

    async def _board_agent(board_id: str):
        events.append(f"acl:{board_id}")
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet({"board": {"read": False}}),
        )

    def _export(*_args, **_kwargs):
        raise AssertionError("permission-denied request accessed graph export")

    monkeypatch.setattr(graph_export, "export_board_jsonld", _export)
    catalog = _Catalog()
    kg_export_tools.register_kg_export_tools(
        catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_export_jsonld"](
        board_id="board-restricted",
    )

    payload = json.loads(raw)
    assert payload["error"] == "permission_denied"
    assert payload["required_permission"] == "board.read"
    assert events == ["authenticate", "acl:board-restricted"]


@pytest.mark.asyncio
async def test_intent_query_honors_board_read_override_before_service_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_query_tools

    events: list[str] = []

    async def _user_boards(*_args, **_kwargs):
        events.append("realm_membership")
        return SimpleNamespace(id="agent-restricted"), ["board-restricted"]

    async def _board_agent(board_id: str):
        events.append(f"effective:{board_id}")
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet({"board": {"read": False}}),
        )

    def _service():
        raise AssertionError(
            "permission-denied request resolved the KG service/provider"
        )

    monkeypatch.setattr(
        kg_query_tools,
        "_get_user_boards",
        _user_boards,
    )
    monkeypatch.setattr(kg_query_tools, "get_kg_service", _service)
    catalog = _Catalog()
    kg_query_tools.register_kg_query_tools(
        catalog,
        get_agent=lambda: None,
        get_uow=lambda: None,
        get_board_agent=_board_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_get_decision_history"](
        board_id="board-restricted",
        topic="authorization",
    )

    payload = json.loads(raw)
    assert payload["error"]["code"] == "permission_denied"
    assert payload["error"]["required_permission"] == "board.read"
    assert events == [
        "realm_membership",
        "effective:board-restricted",
    ]


@pytest.mark.asyncio
async def test_intent_query_honors_specific_override_before_service_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_query_tools

    async def _user_boards(*_args, **_kwargs):
        return SimpleNamespace(id="agent-restricted"), ["board-restricted"]

    async def _board_agent(_board_id: str):
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet(
                {
                    "board": {"read": True},
                    "kg": {"query": {"decision_history": False}},
                }
            ),
        )

    monkeypatch.setattr(
        kg_query_tools,
        "_get_user_boards",
        _user_boards,
    )
    monkeypatch.setattr(
        kg_query_tools,
        "get_kg_service",
        lambda: (_ for _ in ()).throw(
            AssertionError(
                "specific permission denial resolved the KG service"
            )
        ),
    )
    catalog = _Catalog()
    kg_query_tools.register_kg_query_tools(
        catalog,
        get_agent=lambda: None,
        get_uow=lambda: None,
        get_board_agent=_board_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_get_decision_history"](
        board_id="board-restricted",
        topic="authorization",
    )

    payload = json.loads(raw)
    assert payload["error"]["code"] == "permission_denied"
    assert (
        payload["error"]["required_permission"]
        == "kg.query.decision_history"
    )


@pytest.mark.asyncio
async def test_global_intent_query_filters_effective_board_read_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_query_tools

    events: list[str] = []

    async def _user_boards(*_args, **_kwargs):
        return SimpleNamespace(id="agent-restricted"), [
            "board-readable",
            "board-revoked",
        ]

    async def _board_agent(board_id: str):
        events.append(f"effective:{board_id}")
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet(
                {"board": {"read": board_id == "board-readable"}}
            ),
        )

    async def _global_agent():
        return SimpleNamespace(
            agent_id="agent-restricted",
            permissions=PermissionSet(
                {"kg": {"query": {"global": True}}}
            ),
        )

    class _Service:
        def query_global(
            self,
            _query: str,
            *,
            user_boards: list[str],
            **_kwargs,
        ) -> list[dict]:
            events.append(f"provider:{','.join(user_boards)}")
            return []

    monkeypatch.setattr(
        kg_query_tools,
        "_get_user_boards",
        _user_boards,
    )
    monkeypatch.setattr(
        kg_query_tools,
        "get_kg_service",
        lambda: _Service(),
    )
    catalog = _Catalog()
    kg_query_tools.register_kg_query_tools(
        catalog,
        get_agent=lambda: None,
        get_uow=lambda: None,
        get_board_agent=_board_agent,
        get_global_agent=_global_agent,
    )

    raw = await catalog.tools["okto_pulse_kg_query_global"](
        nl_query="authorization",
    )

    payload = json.loads(raw)
    assert payload["count"] == 0
    assert events == [
        "effective:board-readable",
        "effective:board-revoked",
        "provider:board-readable",
    ]


@pytest.mark.asyncio
async def test_schema_info_global_is_static_and_internal_requires_admin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_power_tools

    calls: list[tuple[str, bool]] = []

    raw_agent_calls = 0

    async def _raw_agent():
        nonlocal raw_agent_calls
        raw_agent_calls += 1
        return SimpleNamespace(
            id="raw-agent",
            permissions=None,
        )

    async def _global_agent():
        return SimpleNamespace(
            agent_id="raw-agent",
            permissions=PermissionSet(
                {
                    "kg": {
                        "power": {"schema_info": True},
                        "admin": {"settings_read": False},
                    }
                }
            ),
        )

    async def _unexpected_board_acl(_board_id: str):
        raise AssertionError("global schema must not resolve a board ACL")

    def _schema(board_id: str, *, include_internal: bool):
        calls.append((board_id, include_internal))
        return {"schema_version": "test"}

    monkeypatch.setattr(kg_power_tools, "get_schema_info", _schema)
    catalog = _Catalog()
    kg_power_tools.register_kg_power_tools(
        catalog,
        get_agent=_raw_agent,
        get_board_agent=_unexpected_board_acl,
        get_global_agent=_global_agent,
    )
    tool = catalog.tools["okto_pulse_kg_schema_info"]

    public = json.loads(await tool())
    internal = json.loads(await tool(include_internal="true"))

    assert public == {"schema_version": "test"}
    assert calls == [("", False)]
    assert internal["error"]["code"] == "permission_denied"
    assert (
        internal["error"]["required_permission"]
        == "kg.admin.settings_read"
    )
    assert raw_agent_calls == 0


def test_global_schema_contract_does_not_open_graph_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import tier_power

    class _GraphStore:
        def get_schema_info(self, *_args, **_kwargs):
            raise AssertionError("global schema opened a board graph")

    # get_schema_info imports the registry getter inside its body, so patch the
    # defining module used by that import rather than relying on a pseudo board.
    from okto_pulse.core.kg.interfaces import registry

    monkeypatch.setattr(
        registry,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_store=_GraphStore()),
    )
    result = tier_power.get_schema_info("")

    assert result["schema_version"]
    assert "stable_node_types" in result


def test_server_injects_board_acl_into_every_registered_kg_family() -> None:
    from okto_pulse.core.mcp import server

    tree = ast.parse(Path(server.__file__).read_text(encoding="utf-8"))
    for registration in (
        "_register_kg_tools",
        "_register_kg_power_tools",
        "_register_kg_export_tools",
    ):
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == registration
        ]
        assert len(calls) == 1
        keywords = {kw.arg: kw.value for kw in calls[0].keywords}
        callback = keywords.get("get_board_agent")
        assert isinstance(callback, ast.Name)
        assert callback.id == "_get_agent_ctx"
