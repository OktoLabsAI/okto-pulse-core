"""Transport-neutral KG rebuild admission and composition helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
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
        rebaseline_rows = getattr(request, "rebaseline_source_rows", None)
        rebaseline_evidence_id = getattr(
            request,
            "rebaseline_evidence_id",
            None,
        )
        rebaseline_target_source_set_hash = getattr(
            request,
            "rebaseline_target_source_set_hash",
            None,
        )
        if any(
            value is not None
            for value in (
                rebaseline_rows,
                rebaseline_evidence_id,
                rebaseline_target_source_set_hash,
            )
        ):
            if (
                not isinstance(rebaseline_rows, tuple)
                or not isinstance(rebaseline_evidence_id, str)
                or not rebaseline_evidence_id
                or not isinstance(rebaseline_target_source_set_hash, str)
                or len(rebaseline_target_source_set_hash) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in rebaseline_target_source_set_hash
                )
            ):
                raise RuntimeError("rebuild_rebaseline_projection_binding_invalid")
            resolved: list[dict[str, object]] = []
            for raw in rebaseline_rows:
                if not isinstance(raw, Mapping):
                    raise RuntimeError("rebuild_rebaseline_projection_row_invalid")
                row = dict(raw)
                if row.get(
                    "_rebuild_rebaseline_evidence_id"
                ) != rebaseline_evidence_id or not row.get(
                    "_rebuild_manifest_created_at"
                ):
                    raise RuntimeError(
                        "rebuild_rebaseline_projection_evidence_mismatch"
                    )
                resolved.append(row)
            return tuple(resolved)
        manifest = manifest_store_obj.load(request.manifest_ref)
        if manifest is None:
            return ()
        rows: list[dict[str, object]] = []
        for row in manifest.materializable_sources:
            payload = row.to_dict()
            # Rebuild queue admission uses this immutable manifest cut to
            # distinguish an already represented live intent from a mutation
            # that raced the run after source validation.
            payload["_rebuild_manifest_created_at"] = manifest.created_at
            rows.append(payload)
        # Superseded evidence is normally expired working context and is not a
        # denominator source.  It may nevertheless be a structural endpoint
        # required by an active evidence supersedence chain.  Expose only the
        # manifest-bound candidates here; the Community adapter resolves the
        # exact recursive closure against current relational authority and
        # enqueues only predecessors actually referenced by materializable
        # evidence.
        for row in manifest.skipped_expired_working:
            if (
                row.artifact_type != "code_evidence"
                or row.source_artifact_status != "superseded"
            ):
                continue
            payload = row.to_dict()
            payload["_rebuild_manifest_created_at"] = manifest.created_at
            payload["_rebuild_dependency_closure_candidate"] = (
                "code_evidence_supersedence"
            )
            rows.append(payload)
        return tuple(rows)

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
