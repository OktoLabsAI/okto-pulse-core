"""Application-facing access to app-scoped runtime workers.

The helpers never create runners and never retain process-global state. They
resolve the registry owned by the active :class:`RuntimeComposition`; callers
outside an application scope receive an explicit inactive result.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from okto_pulse.core.composition import current_runtime_composition
from okto_pulse.core.ports.runtime_workers import RuntimeWorkerRegistry


def current_runtime_worker_registry() -> RuntimeWorkerRegistry | None:
    composition = current_runtime_composition()
    if composition is None:
        return None
    registry = composition.worker_registry
    return registry if isinstance(registry, RuntimeWorkerRegistry) else None


def signal_runtime_worker(family: str) -> bool:
    registry = current_runtime_worker_registry()
    return registry.notify(family) if registry is not None else False


def runtime_worker_is_running(family: str) -> bool:
    registry = current_runtime_worker_registry()
    return registry.is_running(family) if registry is not None else False


async def process_runtime_worker_once(family: str) -> int:
    registry = current_runtime_worker_registry()
    return await registry.process_once(family) if registry is not None else 0


def runtime_worker_snapshot(family: str, **context: Any) -> dict[str, Any]:
    registry = current_runtime_worker_registry()
    return registry.snapshot(family, **context) if registry is not None else {}


class RuntimeWorkerQuiesceError(RuntimeError):
    """A live worker family could not be stopped at a fail-closed boundary."""


@asynccontextmanager
async def temporarily_quiesce_runtime_worker(family: str):  # noqa: ANN201
    """Stop one active worker family and always restore it afterwards.

    Operational mutations occasionally need a short relational writer window
    that cannot safely race the worker owning the same queue.  This boundary
    reuses the edition-owned stop protocol, including its bounded native drain,
    and never manufactures a runner when the current runtime has none.
    """

    registry = current_runtime_worker_registry()
    was_active = registry is not None and registry.get_handle(family) is not None
    if not was_active:
        yield False
        return

    failures = await registry.stop_families((family,))
    if failures:
        failure = failures[0]
        raise RuntimeWorkerQuiesceError(
            "runtime_worker_quiesce_failed: "
            f"family={family} error_class={failure.error_class}"
        )
    try:
        yield True
    finally:
        await registry.start_family(family)


__all__ = [
    "current_runtime_worker_registry",
    "process_runtime_worker_once",
    "RuntimeWorkerQuiesceError",
    "runtime_worker_is_running",
    "runtime_worker_snapshot",
    "signal_runtime_worker",
    "temporarily_quiesce_runtime_worker",
]
