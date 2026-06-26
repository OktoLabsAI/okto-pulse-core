"""Spec #15 (card 72bcdfcf) — runtime ports definition.

Locks the four runtime Protocols + DTOs: structural conformance, the canonical
``kg_daily_tick`` job id, the no-shadow collision policy for RuntimeEventBusPort,
the sanitised ``kg.tick.reschedule_failed`` signal, and that the new ports layer
stays pure under the spec #12 ImportBoundaryGate (TYPE_CHECKING hints excluded).
The conformance-suite card (03829e48) owns the deeper signature checks.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from okto_pulse.core.ports import (
    KG_DAILY_TICK_JOB_ID,
    KG_TICK_RESCHEDULE_FAILED_SIGNAL,
    RESCHEDULE_FAILED_FORBIDDEN_FIELDS,
    RESCHEDULE_FAILED_REQUIRED_FIELDS,
    ActorContextLike,
    RuntimeCompositionLike,
    RuntimeControl,
    RuntimeEvent,
    RuntimeEventBusPort,
    RuntimeSettingsPort,
    RuntimeSettingsSnapshot,
    SchedulerControl,
    SchedulerResult,
    build_reschedule_failed_signal,
    sanitize_message,
)


class _Scheduler:
    def is_available(self) -> bool:
        return True

    async def reschedule_job(self, job_id: str, trigger: Mapping[str, Any]) -> SchedulerResult:
        return SchedulerResult(job_id=job_id, scheduled=True, audit_status="rescheduled")

    async def shutdown(self, wait: bool = False) -> None:
        return None


class _EventBus:
    async def publish_runtime_event(self, event: RuntimeEvent, *, session=None) -> None:
        return None

    async def flush(self) -> None:
        return None


class _Settings:
    async def load(self, scope: str) -> RuntimeSettingsSnapshot:
        return RuntimeSettingsSnapshot(scope=scope, version=1)

    async def persist(self, changes, *, actor) -> RuntimeSettingsSnapshot:
        return RuntimeSettingsSnapshot(scope="global", version=2, values=dict(changes))

    async def apply_runtime_effects(self, before, after) -> list:
        return []


class _Composition:
    def get_provider(self, key: str) -> Any:
        return object()


class _Control:
    async def startup(self, composition) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    def get_provider(self, key: str) -> Any:
        return object()


class _Actor:
    actor_id = "actor-1"
    source = "mcp"


def test_four_runtime_ports_are_structurally_conformant() -> None:
    assert isinstance(_Scheduler(), SchedulerControl)
    assert isinstance(_EventBus(), RuntimeEventBusPort)
    assert isinstance(_Settings(), RuntimeSettingsPort)
    assert isinstance(_Control(), RuntimeControl)
    assert isinstance(_Composition(), RuntimeCompositionLike)
    assert isinstance(_Actor(), ActorContextLike)


def test_real_actor_context_satisfies_actor_context_like() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext

    actor = ActorContext("u1", "mcp")
    assert isinstance(actor, ActorContextLike)


def test_scheduler_job_id_is_canonical_kg_daily_tick() -> None:
    assert KG_DAILY_TICK_JOB_ID == "kg_daily_tick"


def test_runtime_event_bus_port_does_not_shadow_existing_event_buses() -> None:
    from okto_pulse.core.events.bus import EventBus as DomainEventBus
    from okto_pulse.core.kg.interfaces.event_bus import EventBus as KgEventBus

    # The runtime port is a distinct contract with a distinct method name.
    assert RuntimeEventBusPort is not DomainEventBus
    assert RuntimeEventBusPort is not KgEventBus
    assert hasattr(RuntimeEventBusPort, "publish_runtime_event")
    # The ports package must not redefine a generic ``EventBus``.
    import okto_pulse.core.ports as ports_pkg

    assert not hasattr(ports_pkg, "EventBus")


def test_build_reschedule_failed_signal_is_complete_and_secret_free() -> None:
    err = RuntimeError("connect failed for postgres://user:s3cr3t@host/db token=ABC123")
    payload = build_reschedule_failed_signal(error=err, actor_id="actor-1", source="api")
    assert payload["signal"] == KG_TICK_RESCHEDULE_FAILED_SIGNAL
    assert payload["job_id"] == "kg_daily_tick"
    assert payload["error_class"] == "RuntimeError"
    assert payload["actor_id"] == "actor-1"
    assert payload["source"] == "api"
    # every required field present
    for fld in RESCHEDULE_FAILED_REQUIRED_FIELDS:
        assert fld in payload and payload[fld] not in (None, "")
    # no forbidden field, no leaked secret
    for fld in RESCHEDULE_FAILED_FORBIDDEN_FIELDS:
        assert fld not in payload
    assert "s3cr3t" not in payload["sanitized_message"]
    assert "ABC123" not in payload["sanitized_message"]
    assert "[REDACTED]" in payload["sanitized_message"]


def test_sanitize_message_redacts_credentials() -> None:
    assert "[REDACTED]" in sanitize_message("password=hunter2")
    assert "hunter2" not in sanitize_message("password=hunter2")
    assert "topsecret" not in sanitize_message("api_key: topsecret here")
    redacted = sanitize_message("db url mysql://u:p@host/db ok")
    assert "u:p@host" not in redacted


def test_runtime_event_is_immutable_snapshot() -> None:
    ev = RuntimeEvent(name="kg.tick.rescheduled", occurred_at=datetime(2026, 6, 24, tzinfo=timezone.utc), source="system")
    assert ev.payload == {}
    import dataclasses

    assert dataclasses.is_dataclass(ev)


def test_ports_layer_is_pure_under_import_boundary_gate() -> None:
    # The spec #12 ImportBoundaryGate (blocking mode) must find NO blocking
    # violation in core/ports/* — the TYPE_CHECKING AsyncSession hint is excluded.
    from okto_pulse.core.application.boundary import ImportBoundaryGate, ImportBoundaryGateInput

    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="blocking"))
    ports_blocking = [
        v
        for v in report.evidence["violations"]
        if v["status"] == "blocking" and v["file"].startswith("okto_pulse/core/ports/")
    ]
    assert ports_blocking == [], ports_blocking


def test_importing_ports_starts_no_runtime() -> None:
    # Importing the ports must not initialise a runtime (scheduler/worker thread),
    # even though the shared core package __init__ chain pulls libraries.
    from okto_pulse.core.application.boundary import (
        ImportSideEffectSmokeGate,
        ImportSideEffectSmokeInput,
    )

    report = ImportSideEffectSmokeGate().run(
        ImportSideEffectSmokeInput(
            modules=[
                "okto_pulse.core.ports.scheduler",
                "okto_pulse.core.ports.runtime_events",
                "okto_pulse.core.ports.runtime_settings",
                "okto_pulse.core.ports.runtime_control",
            ]
        )
    )
    assert report.status == "passed", report.evidence
