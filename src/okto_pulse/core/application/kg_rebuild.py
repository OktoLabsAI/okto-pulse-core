"""Transport-neutral KG rebuild admission and composition helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.repositories import PulseUnitOfWork


REBUILD_REJECT_STATES: frozenset[str] = frozenset({"quarantined"})
HealthProbe = Callable[..., Awaitable[dict[str, Any]]]


async def get_kg_health(
    board_id: str,
    uow: PulseUnitOfWork,
    *,
    scheduler_control: SchedulerControl | None = None,
) -> dict[str, Any]:
    """Default health probe through the edition-composed service catalog."""

    return await uow.services.kg.health(
        board_id,
        scheduler_control=scheduler_control,
    )


async def refuse_rebuild_if_quarantined(
    board_id: str,
    uow: PulseUnitOfWork,
    *,
    scheduler_control: SchedulerControl | None = None,
    health_probe: HealthProbe | None = None,
) -> dict[str, object] | None:
    """Refuse a rebuild only while the graph is explicitly quarantined."""

    probe = health_probe or get_kg_health
    health = await probe(
        board_id,
        uow,
        scheduler_control=scheduler_control,
    )
    graph_state = health.get("graph_state")
    if graph_state in REBUILD_REJECT_STATES:
        return {
            "error": "rebuild_refused_quarantined",
            "graph_state": graph_state,
            "board_id": board_id,
            "message": (
                f"KG for board {board_id} is {graph_state}. "
                "A rebuild cannot proceed while the graph is quarantined. "
                "Use the KG reset flow to exit quarantine first."
            ),
        }
    return None


def build_source_store() -> Any:
    """Resolve a fail-closed row supplier for rebuild enumeration."""

    from okto_pulse.core.kg.interfaces import (
        SourceUnavailableError,
        get_kg_registry,
    )

    reader = get_kg_registry().require_board_source_reader()

    def _fetch_complete_rows(board_id: str) -> list[dict[str, object]]:
        snapshot = reader.fetch(board_id)
        if not snapshot.complete:
            raise SourceUnavailableError(
                "board source snapshot is incomplete "
                f"(board_id={board_id}, cause={snapshot.cause})",
                cause_type=str(snapshot.cause or "unknown"),
            )
        return [dict(row) for row in snapshot.rows]

    return _fetch_complete_rows


def provider_missing_payload(exc: Exception) -> dict[str, object]:
    provider_key = getattr(exc, "provider_key", "rebuild_ingestion_port")
    missing = list(getattr(exc, "missing", [provider_key]))
    return {
        "error": "provider_missing",
        "provider": provider_key,
        "missing": missing,
        "message": str(exc),
    }


def build_rebuild_step_adapter(*, manifest_store_obj: Any) -> Any:
    """Resolve the edition-composed rebuild ingestion adapter."""

    from okto_pulse.core.kg.interfaces import get_kg_registry

    ingestion = get_kg_registry().require_rebuild_ingestion_port()

    def _step_source_resolver(request: Any) -> tuple[dict[str, object], ...]:
        manifest = manifest_store_obj.load(request.manifest_ref)
        if manifest is None:
            return ()
        return tuple(row.to_dict() for row in manifest.materializable_sources)

    return ingestion.build_step_adapter(source_resolver=_step_source_resolver)


def empty_source_store(_board_id: str) -> list[dict[str, object]]:
    """Legacy test seam; production must resolve ``build_source_store``."""

    return []


__all__ = [
    "REBUILD_REJECT_STATES",
    "HealthProbe",
    "build_rebuild_step_adapter",
    "build_source_store",
    "empty_source_store",
    "get_kg_health",
    "provider_missing_payload",
    "refuse_rebuild_if_quarantined",
]
