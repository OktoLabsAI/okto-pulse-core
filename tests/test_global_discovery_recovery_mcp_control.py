from __future__ import annotations

import asyncio
import inspect
import json
from datetime import datetime, timedelta, timezone
from threading import Event
from time import monotonic
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.global_discovery_recovery_control import (
    RecoveryControlPlane,
    RecoveryPreparationCommand,
    RecoveryPreparedResult,
    RecoveryProgressCounts,
    RecoveryProgressInvariantViolation,
    RecoveryResumeRejected,
    RecoveryRunBinding,
    RecoveryStartCommand,
    register_recovery_control_plane,
    reset_recovery_control_plane,
)
from okto_pulse.core.kg.providers.testing.memory_recovery_run_store import (
    MemoryRecoveryControlStore as _MemoryRecoveryControlStore,
)
from okto_pulse.core.mcp import server


NOW = datetime.now(timezone.utc)


class MemoryRecoveryControlStore(_MemoryRecoveryControlStore):
    """Keep preparation expiry deterministic across long regression runs."""

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("clock", lambda: NOW)
        super().__init__(*args, **kwargs)


def test_global_recovery_progress_invariant_error_exposes_only_code() -> None:
    error = RecoveryProgressInvariantViolation(
        field="nodes_written",
        previous=10,
        proposed=9,
    )

    assert json.loads(server._global_recovery_control_error(error)) == {
        "error": "recovery_progress_invariant_violation"
    }


def test_global_recovery_generic_value_error_does_not_disclose_detail() -> None:
    secret_detail = "database password is hunter2"

    assert json.loads(
        server._global_recovery_control_error(ValueError(secret_detail))
    ) == {"error": "invalid_recovery_control_request"}


def test_global_recovery_mcp_schemas_are_closed_and_epoch_fenced() -> None:
    preflight = server.okto_pulse_kg_global_discovery_recovery_preflight.parameters
    confirm = server.okto_pulse_kg_global_discovery_recovery_confirm.parameters
    run = server.okto_pulse_kg_global_discovery_recovery_run.parameters
    status = server.okto_pulse_kg_global_discovery_recovery_status.parameters
    cancel = server.okto_pulse_kg_global_discovery_recovery_cancel.parameters
    resume = server.okto_pulse_kg_global_discovery_recovery_resume.parameters

    assert preflight == {"type": "object", "properties": {}}
    assert set(confirm["properties"]) == {
        "run_id",
        "manifest_ref",
        "preflight_hash",
    }
    assert set(confirm["required"]) == {
        "run_id",
        "manifest_ref",
        "preflight_hash",
    }
    assert set(run["required"]) == {
        "confirmation_id",
        "manifest_ref",
        "preflight_hash",
        "reason",
    }
    assert run["properties"]["reason"]["minLength"] == 1
    assert run["properties"]["reason"]["maxLength"] == 512
    assert status["required"] == ["run_id"]

    for schema in (cancel, resume):
        assert set(schema["properties"]) == {
            "run_id",
            "expected_epoch",
            "reason",
        }
        assert set(schema["required"]) == {"run_id", "expected_epoch"}
        reason_schema = schema["properties"]["reason"]
        string_branch = next(
            branch
            for branch in reason_schema["anyOf"]
            if branch.get("type") == "string"
        )
        assert string_branch["maxLength"] == 512


def test_preflight_and_run_never_call_request_bound_inventory_helpers() -> None:
    preflight_source = inspect.getsource(
        server.okto_pulse_kg_global_discovery_recovery_preflight.fn
    )
    run_source = inspect.getsource(
        server.okto_pulse_kg_global_discovery_recovery_run.fn
    )

    for forbidden in (
        "_global_recovery_board_pairs",
        "_global_recovery_health_evidence",
        "_global_recovery_inventory",
        "_global_recovery_candidate_seeds",
    ):
        assert forbidden not in preflight_source
    assert "_global_recovery_prepare_start" not in run_source


class RecordingDispatcher:
    def __init__(self) -> None:
        self.dispatched: list[tuple[str, int, str, object]] = []

    def dispatch(self, *, run_id: str, epoch: int, attempt_id: str, kind) -> None:
        self.dispatched.append((run_id, epoch, attempt_id, kind))


def test_public_composition_facade_resolves_only_recovery_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import interfaces as kg_interfaces
    from okto_pulse.core.ports import global_discovery_recovery_control as facade

    recovery = object()
    artifact_store = object()
    registry = SimpleNamespace(
        require_global_discovery_recovery=lambda: recovery,
        require_rebuild_audit_artifact_store=lambda: artifact_store,
    )
    monkeypatch.setattr(kg_interfaces, "get_kg_registry", lambda: registry)

    assert facade.resolve_global_discovery_recovery_runtime_dependencies() == (
        recovery,
        artifact_store,
    )


def _preparation_command() -> RecoveryPreparationCommand:
    return RecoveryPreparationCommand(
        binding=RecoveryRunBinding(
            run_id="run-mcp-control",
            actor_id="agent-mcp",
        ),
        admitted_at=NOW,
        counts=RecoveryProgressCounts(sources_total=3, boards_total=2),
    )


def _command() -> RecoveryStartCommand:
    return RecoveryStartCommand(
        binding=RecoveryRunBinding(
            run_id="run-mcp-control",
            actor_id="agent-mcp",
            confirmation_fingerprint="confirmation-fingerprint",
            manifest_ref="global_discovery_manifest_test",
            preflight_hash="preflight-hash",
            reason="operator requested bounded recovery",
        ),
        started_at=NOW + timedelta(milliseconds=3),
        counts=RecoveryProgressCounts(
            sources_total=3,
            boards_total=2,
            boards_scanned=2,
        ),
    )


async def _authorized(*_args):
    return SimpleNamespace(agent_id="agent-mcp"), None


def _mcp_status(
    *,
    run_id: str = "run-mcp-control",
    phase: str = "prepared",
    preparation_state: str = "prepared",
) -> SimpleNamespace:
    payload = {
        "run_id": run_id,
        "attempt_id": f"{run_id}/attempt-1",
        "epoch": 1,
        "state": "pending",
        "phase": phase,
        "preparation_state": preparation_state,
        "progress_seq": 2,
        "terminal_outcome": None,
        "status_tool": "okto_pulse_kg_global_discovery_recovery_status",
    }
    return SimpleNamespace(
        run_id=run_id,
        actor_id="admitting-admin",
        phase=phase,
        to_dict=lambda: dict(payload),
    )


@pytest.mark.asyncio
async def test_preflight_only_reserves_and_dispatches_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    command = SimpleNamespace(
        binding=SimpleNamespace(run_id="run-preparation-candidate")
    )
    incumbent = _mcp_status(run_id="run-preparation-candidate")

    class Service:
        def new_preparation_command(self, *, actor_id: str):
            calls.append(("new_preparation_command", actor_id))
            return command

    class Control:
        def prepare(self, received):
            calls.append(("prepare", received))
            return incumbent

    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "_global_recovery_service", lambda: Service())
    monkeypatch.setattr(server, "_global_recovery_control_plane", lambda: Control())

    result = json.loads(
        await asyncio.wait_for(
            server.okto_pulse_kg_global_discovery_recovery_preflight.fn(),
            timeout=2.0,
        )
    )

    assert calls == [
        ("new_preparation_command", "agent-mcp"),
        ("prepare", command),
    ]
    assert result == {
        **incumbent.to_dict(),
        "outcome": "preparation_accepted",
        "idempotent_replay": False,
        "action_required": "call_okto_pulse_kg_global_discovery_recovery_confirm",
    }


@pytest.mark.asyncio
async def test_preflight_timeout_returns_pollable_run_without_draining_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = Event()
    command = SimpleNamespace(binding=SimpleNamespace(run_id="run-timeout"))

    class Service:
        def new_preparation_command(self, *, actor_id: str):
            assert actor_id == "agent-mcp"
            return command

    class SlowControl:
        def prepare(self, received):
            assert received is command
            release.wait(timeout=1.0)
            return _mcp_status(run_id="run-timeout", phase="queued")

    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "_global_recovery_service", lambda: Service())
    monkeypatch.setattr(
        server,
        "_global_recovery_control_plane",
        lambda: SlowControl(),
    )
    monkeypatch.setattr(server, "_GLOBAL_RECOVERY_PREFLIGHT_TIMEOUT_SECONDS", 0.01)

    started = monotonic()
    try:
        result = json.loads(
            await server.okto_pulse_kg_global_discovery_recovery_preflight.fn()
        )
        elapsed = monotonic() - started
    finally:
        release.set()

    assert elapsed < 0.2
    assert result == {
        "error": "global_discovery_recovery_preflight_timeout",
        "run_id": "run-timeout",
        "status_tool": "okto_pulse_kg_global_discovery_recovery_status",
        "action_required": (
            "call_okto_pulse_kg_global_discovery_recovery_status"
        ),
    }


@pytest.mark.asyncio
async def test_confirm_and_run_only_fence_snapshot_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_calls: list[tuple[str, dict[str, object]]] = []
    control_calls: list[tuple[str, object]] = []
    command = SimpleNamespace(binding=SimpleNamespace(run_id="run-prepared"))
    prepared = _mcp_status(run_id="run-prepared")
    accepted = _mcp_status(
        run_id="run-prepared",
        phase="confirmed",
        preparation_state="prepared",
    )

    class Service:
        def current_snapshot_fingerprint(self) -> str:
            service_calls.append(("current_snapshot_fingerprint", {}))
            return "snapshot-fingerprint"

        def confirm(self, **kwargs):
            service_calls.append(("confirm", kwargs))
            return {
                "confirmation_id": "confirmation-1",
                "run_id": kwargs["run_id"],
                "manifest_ref": kwargs["manifest_ref"],
                "preflight_hash": kwargs["preflight_hash"],
            }

        def prepare_durable_start(self, **kwargs):
            service_calls.append(("prepare_durable_start", kwargs))
            return command

    class Control:
        def status(self, run_id: str):
            control_calls.append(("status", run_id))
            return prepared

        def start(self, received):
            control_calls.append(("start", received))
            return accepted

    service = Service()
    control = Control()
    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "_global_recovery_service", lambda: service)
    monkeypatch.setattr(server, "_global_recovery_control_plane", lambda: control)

    confirmed = json.loads(
        await server.okto_pulse_kg_global_discovery_recovery_confirm.fn(
            run_id="run-prepared",
            manifest_ref="manifest-1",
            preflight_hash="hash-1",
        )
    )
    started = json.loads(
        await server.okto_pulse_kg_global_discovery_recovery_run.fn(
            confirmation_id="confirmation-1",
            manifest_ref="manifest-1",
            preflight_hash="hash-1",
            reason="operator-approved",
        )
    )

    assert confirmed["run_id"] == "run-prepared"
    assert service_calls == [
        ("current_snapshot_fingerprint", {}),
        (
            "confirm",
            {
                "actor_id": "agent-mcp",
                "run_id": "run-prepared",
                "manifest_ref": "manifest-1",
                "preflight_hash": "hash-1",
                "current_snapshot_fingerprint": "snapshot-fingerprint",
            },
        ),
        ("current_snapshot_fingerprint", {}),
        (
            "prepare_durable_start",
            {
                "actor_id": "agent-mcp",
                "confirmation_id": "confirmation-1",
                "manifest_ref": "manifest-1",
                "preflight_hash": "hash-1",
                "reason": "operator-approved",
                "current_snapshot_fingerprint": "snapshot-fingerprint",
            },
        ),
    ]
    assert control_calls == [("status", "run-prepared"), ("start", command)]
    assert started["attempt_id"] == "run-prepared/attempt-1"
    assert started["preparation_state"] == "prepared"
    assert started["terminal_outcome"] is None
    assert started["status_tool"] == "okto_pulse_kg_global_discovery_recovery_status"


@pytest.mark.asyncio
async def test_authorized_global_admin_controls_cross_actor_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    incumbent = _mcp_status(run_id="run-admitted-by-another-admin")

    class Control:
        def status(self, run_id: str):
            calls.append(("status", run_id))
            return incumbent

        def cancel(self, **kwargs):
            calls.append(("cancel", kwargs))
            return incumbent

        def resume(self, **kwargs):
            calls.append(("resume", kwargs))
            return incumbent

    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(server, "_global_recovery_control_plane", lambda: Control())

    observed = json.loads(
        await server.okto_pulse_kg_global_discovery_recovery_status.fn(
            run_id=incumbent.run_id
        )
    )
    cancelled = json.loads(
        await server.okto_pulse_kg_global_discovery_recovery_cancel.fn(
            run_id=incumbent.run_id,
            expected_epoch=1,
            reason="global admin cancellation",
        )
    )
    resumed = json.loads(
        await server.okto_pulse_kg_global_discovery_recovery_resume.fn(
            run_id=incumbent.run_id,
            expected_epoch=1,
            reason="global admin resume",
        )
    )

    assert observed["run_id"] == incumbent.run_id
    assert cancelled["run_id"] == incumbent.run_id
    assert resumed["run_id"] == incumbent.run_id
    assert [name for name, _payload in calls] == [
        "status",
        "status",
        "cancel",
        "status",
        "resume",
    ]
    assert calls[2][1]["requested_by_actor_id"] == "agent-mcp"
    assert calls[4][1]["requested_by_actor_id"] == "agent-mcp"


@pytest.mark.asyncio
async def test_actual_mcp_status_and_cancel_require_the_current_epoch(
    monkeypatch,
) -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)
    control.prepare(_preparation_command())
    preparing = store.mark_preparing(
        run_id="run-mcp-control",
        epoch=1,
        at=NOW + timedelta(milliseconds=1),
    )
    store.mark_prepared(
        run_id="run-mcp-control",
        epoch=1,
        expected_progress_seq=preparing.progress_seq,
        prepared=RecoveryPreparedResult(
            manifest_ref="global_discovery_manifest_test",
            preflight_hash="preflight-hash",
            snapshot_fingerprint="snapshot-fingerprint",
            prepared_at=NOW + timedelta(milliseconds=2),
            expires_at=NOW + timedelta(seconds=300, milliseconds=2),
            counts=RecoveryProgressCounts(
                sources_total=3,
                boards_total=2,
                boards_scanned=2,
            ),
        ),
    )
    control.start(_command())
    running = store.mark_running(
        run_id="run-mcp-control",
        epoch=1,
        at=NOW + timedelta(milliseconds=4),
    )
    register_recovery_control_plane(control)
    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)

    try:
        status = json.loads(
            await server.okto_pulse_kg_global_discovery_recovery_status.fn(
                run_id="run-mcp-control"
            )
        )
        stale = json.loads(
            await server.okto_pulse_kg_global_discovery_recovery_cancel.fn(
                run_id="run-mcp-control",
                expected_epoch=2,
            )
        )

        assert status["run_id"] == "run-mcp-control"
        assert status["state"] == "running"
        assert status["epoch"] == 1
        assert status["attempt_id"] == "run-mcp-control/attempt-1"
        assert status["preparation_state"] == "prepared"
        assert status["confirmation_state"] == "consumed"
        assert status["terminal_outcome"] is None
        assert status["status_tool"] == (
            "okto_pulse_kg_global_discovery_recovery_status"
        )
        assert stale == {
            "error": "recovery_epoch_conflict",
            "run_id": "run-mcp-control",
            "expected_epoch": 2,
            "actual_epoch": 1,
            "expected_progress_seq": running.progress_seq,
            "actual_progress_seq": running.progress_seq,
        }
        assert control.status("run-mcp-control") == running

        cancelled = json.loads(
            await server.okto_pulse_kg_global_discovery_recovery_cancel.fn(
                run_id="run-mcp-control",
                expected_epoch=1,
            )
        )
        assert cancelled["reason_code"] == "recovery_cancel_requested"
        assert cancelled["epoch"] == 1
        assert cancelled["cancel_requested_at"] is not None
        assert cancelled["cancel_requested_by_actor_id"] == "agent-mcp"
        assert cancelled["actor_id"] == "agent-mcp"
    finally:
        reset_recovery_control_plane()


@pytest.mark.asyncio
async def test_actual_mcp_resume_preserves_typed_denial_and_expected_epoch(
    monkeypatch,
) -> None:
    calls: list[tuple[str, int, str, str | None]] = []

    class RejectingControl:
        def status(self, run_id: str):
            return SimpleNamespace(actor_id="agent-mcp")

        def resume(
            self,
            *,
            run_id: str,
            expected_epoch: int,
            requested_at,
            requested_by_actor_id: str,
            reason: str | None,
        ):
            calls.append(
                (run_id, expected_epoch, requested_by_actor_id, reason)
            )
            raise RecoveryResumeRejected(
                code="worker_lease_active",
                run_id=run_id,
                epoch=expected_epoch,
            )

    monkeypatch.setattr(server, "_global_recovery_authorize", _authorized)
    monkeypatch.setattr(
        server,
        "_global_recovery_control_plane",
        lambda: RejectingControl(),
    )

    denied = json.loads(
        await server.okto_pulse_kg_global_discovery_recovery_resume.fn(
            run_id="run-mcp-control",
            expected_epoch=7,
            reason="retry after lease expiry",
        )
    )

    assert denied == {
        "error": "worker_lease_active",
        "run_id": "run-mcp-control",
        "epoch": 7,
    }
    assert calls == [
        ("run-mcp-control", 7, "agent-mcp", "retry after lease expiry")
    ]
