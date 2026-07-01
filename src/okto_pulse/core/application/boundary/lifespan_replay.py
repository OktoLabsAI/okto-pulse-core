"""CommunityLifespanReplay — deterministic golden replay of combined_lifespan.

Spec #03, card b4f07ad3, tr_b8b180cb / tr_8ef33b21 / contract api_18f9c6e6.

Replays a :class:`RuntimeComposition`'s lifecycle (startup + shutdown) and reports
the observed events against the closed Community golden baseline. The 15 required
events MUST all be observed; the ONLY accepted ``_default_lifespan``-only
exclusions are exactly ``kg_migration_sweep``, ``shutdown_kg_events_hub`` and
``close_all_connections``. Any other delta is ``unexpected_lifecycle_delta`` (or
``required_lifecycle_event_missing`` for an absent required event).

Pure conformance tooling: drives the composition's async hooks, no framework.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from okto_pulse.core.composition import (
    CompositionRuntime,
    RuntimeComposition,
    RuntimeProviderMissing,
    validate_required_providers,
)

#: Closed list of required Community ``combined_lifespan`` events (Remediacao #03 v2).
REQUIRED_LIFECYCLE_EVENTS: tuple[str, ...] = (
    "init_db",
    "seed_community_defaults",
    "backfill_qa_answered_at",
    "_afg_backfill_task",
    "_preload_embedding_model",
    "event_dispatcher",
    "consolidation_worker",
    "cleanup_worker",
    "outbox_worker",
    "settings_service",
    "scheduler",
    "mcp_session_factory",
    "frontend_served",
    "_metrics_beacon_loop",
    "close_db",
)

#: The ONLY accepted ``_default_lifespan``-only exclusions (closed allowlist).
DEFAULT_ONLY_EXCLUSIONS: tuple[str, ...] = (
    "kg_migration_sweep",
    "shutdown_kg_events_hub",
    "close_all_connections",
)

#: Events that count as activated background workers in the report.
WORKER_EVENTS: frozenset[str] = frozenset(
    {"consolidation_worker", "cleanup_worker", "outbox_worker", "event_dispatcher"}
)

ReplayStatus = Literal[
    "passed",
    "required_lifecycle_event_missing",
    "unexpected_lifecycle_delta",
    "lifecycle_event_order_mismatch",
    "community_replay_failed",
]


@dataclass(frozen=True)
class NamedLifecycleHook:
    """A lifecycle hook that names the startup/shutdown event it represents.

    Implements the :class:`~okto_pulse.core.composition.LifecycleHook` protocol so
    a :class:`RuntimeComposition` can drive it, while exposing the event names the
    replay observes.
    """

    startup_event: str | None = None
    shutdown_event: str | None = None
    recorder: list[tuple[str, str]] | None = None

    async def on_startup(self) -> None:
        if self.startup_event and self.recorder is not None:
            self.recorder.append(("startup", self.startup_event))

    async def on_shutdown(self) -> None:
        if self.shutdown_event and self.recorder is not None:
            self.recorder.append(("shutdown", self.shutdown_event))


@dataclass(frozen=True)
class LifecycleDelta:
    event: str
    kind: Literal["unexpected", "missing"]

    def as_dict(self) -> dict[str, str]:
        return {"event": self.event, "kind": self.kind}


@dataclass(frozen=True)
class LifecycleOrderMismatch:
    event: str
    expected_index: int
    actual_index: int | None
    actual_event: str | None

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "event": self.event,
            "expected_index": self.expected_index,
            "actual_index": self.actual_index,
            "actual_event": self.actual_event,
        }


@dataclass(frozen=True)
class CommunityLifespanReplayInput:
    composition: RuntimeComposition
    community_root: str | None = None
    include_frontend: bool = True
    strict_runtime: bool = False


@dataclass(frozen=True)
class LifecycleReplayReport:
    status: ReplayStatus
    startup_events: list[str]
    shutdown_events: list[str]
    workers: list[str]
    scheduler: str
    mcp_session_factory: str
    frontend: str
    default_only_exclusions: list[str]
    unexpected_deltas: list[LifecycleDelta] = field(default_factory=list)
    order_mismatches: list[LifecycleOrderMismatch] = field(default_factory=list)
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "status": self.status,
            "startup_events": self.startup_events,
            "shutdown_events": self.shutdown_events,
            "workers": self.workers,
            "scheduler": self.scheduler,
            "mcp_session_factory": self.mcp_session_factory,
            "frontend": self.frontend,
            "default_only_exclusions": self.default_only_exclusions,
            "unexpected_deltas": [d.as_dict() for d in self.unexpected_deltas],
            "order_mismatches": [m.as_dict() for m in self.order_mismatches],
            "error": self.error,
        }


class CommunityLifespanReplay:
    """Runs a composition's lifecycle and reports against the golden baseline."""

    async def run(self, gate_input: CommunityLifespanReplayInput) -> LifecycleReplayReport:
        composition = gate_input.composition
        if gate_input.strict_runtime:
            validate_required_providers(
                composition, require_lifecycle_hooks=True
            )  # may raise runtime_provider_missing

        recorder: list[tuple[str, str]] = []
        # Rebind the composition's named hooks onto the recorder so the actual
        # startup/shutdown run records the observed events.
        hooks = self._bind_recorder(composition.lifecycle_hooks, recorder)
        runtime = CompositionRuntime(
            _replace_hooks(composition, hooks), strict_runtime=gate_input.strict_runtime
        )
        try:
            async with runtime.app_lifespan():
                pass
        except RuntimeProviderMissing:
            raise
        except Exception as exc:  # noqa: BLE001 - replay reports failure, never crashes
            return self._failed(recorder, gate_input, str(exc))

        return self._build_report(recorder, gate_input)

    @staticmethod
    def _bind_recorder(
        hooks: object, recorder: list[tuple[str, str]]
    ) -> tuple[NamedLifecycleHook, ...]:
        bound: list[NamedLifecycleHook] = []
        for hook in hooks or ():
            if isinstance(hook, NamedLifecycleHook):
                bound.append(
                    NamedLifecycleHook(
                        startup_event=hook.startup_event,
                        shutdown_event=hook.shutdown_event,
                        recorder=recorder,
                    )
                )
            else:
                bound.append(hook)  # type: ignore[arg-type]
        return tuple(bound)

    def _build_report(
        self, recorder: list[tuple[str, str]], gate_input: CommunityLifespanReplayInput
    ) -> LifecycleReplayReport:
        startup_events = [e for phase, e in recorder if phase == "startup"]
        shutdown_events = [e for phase, e in recorder if phase == "shutdown"]
        observed = set(startup_events) | set(shutdown_events)
        required = set(REQUIRED_LIFECYCLE_EVENTS)
        exclusions = set(DEFAULT_ONLY_EXCLUSIONS)

        missing = [e for e in REQUIRED_LIFECYCLE_EVENTS if e not in observed]
        unexpected = sorted(observed - required - exclusions)
        actual_required_order = [e for e in [*startup_events, *shutdown_events] if e in required]
        order_mismatches = _order_mismatches(REQUIRED_LIFECYCLE_EVENTS, actual_required_order)
        deltas = [LifecycleDelta(e, "missing") for e in missing] + [
            LifecycleDelta(e, "unexpected") for e in unexpected
        ]
        if missing:
            status: ReplayStatus = "required_lifecycle_event_missing"
        elif unexpected:
            status = "unexpected_lifecycle_delta"
        elif order_mismatches:
            status = "lifecycle_event_order_mismatch"
        else:
            status = "passed"

        return LifecycleReplayReport(
            status=status,
            startup_events=startup_events,
            shutdown_events=shutdown_events,
            workers=sorted(observed & WORKER_EVENTS),
            scheduler="running" if "scheduler" in observed else "missing",
            mcp_session_factory="registered" if "mcp_session_factory" in observed else "missing",
            frontend=(
                "served"
                if gate_input.include_frontend and "frontend_served" in observed
                else "not_applicable"
            ),
            default_only_exclusions=sorted(observed & exclusions),
            unexpected_deltas=deltas,
            order_mismatches=order_mismatches,
        )

    @staticmethod
    def _failed(
        recorder: list[tuple[str, str]], gate_input: CommunityLifespanReplayInput, error: str
    ) -> LifecycleReplayReport:
        startup_events = [e for phase, e in recorder if phase == "startup"]
        shutdown_events = [e for phase, e in recorder if phase == "shutdown"]
        return LifecycleReplayReport(
            status="community_replay_failed",
            startup_events=startup_events,
            shutdown_events=shutdown_events,
            workers=[],
            scheduler="missing",
            mcp_session_factory="missing",
            frontend="not_applicable",
            default_only_exclusions=[],
            unexpected_deltas=[],
            order_mismatches=[],
            error=error,
        )


def _replace_hooks(
    composition: RuntimeComposition, hooks: tuple[NamedLifecycleHook, ...]
) -> RuntimeComposition:
    import dataclasses

    return dataclasses.replace(composition, lifecycle_hooks=hooks)


def _order_mismatches(
    expected: tuple[str, ...], actual: list[str]
) -> list[LifecycleOrderMismatch]:
    if actual == list(expected):
        return []
    mismatches: list[LifecycleOrderMismatch] = []
    for index, event in enumerate(expected):
        actual_event = actual[index] if index < len(actual) else None
        if actual_event == event:
            continue
        mismatches.append(
            LifecycleOrderMismatch(
                event=event,
                expected_index=index,
                actual_index=actual.index(event) if event in actual else None,
                actual_event=actual_event,
            )
        )
    return mismatches
