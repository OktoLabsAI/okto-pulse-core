"""Edition boundary for one bounded, read-only materialization snapshot."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Protocol

from okto_pulse.core.kg.materialization_health import (
    BoardHealthCensus,
    CensusStatus,
    HealthProbeDeadline,
    MaterializationEvidence,
    MaterializationEvidenceRequest,
)
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)


class MaterializationEvidencePort(Protocol):
    async def current_generation(self, board_id: str) -> str:
        """Return the durable generation used to key the next probe."""
        ...

    async def probe(
        self,
        request: MaterializationEvidenceRequest,
    ) -> MaterializationEvidence: ...


async def run_bounded_health_probe(
    *,
    name: str,
    board_id: str,
    generation_id: str | None,
    build: Callable[[], Any],
    fallback: Any,
    deadline_at: float,
    ttl_s: float | None = None,
) -> Any:
    """Run the Core-owned bounded probe through the public edition facade.

    The service import is intentionally lazy: the service resolves this port
    while composing KG health, so importing it at module load would introduce
    a cycle at the Core/edition boundary.
    """

    from okto_pulse.core.services.kg_health_service import (
        run_bounded_health_probe as _run_bounded_health_probe,
    )

    kwargs: dict[str, Any] = {
        "name": name,
        "board_id": board_id,
        "generation_id": generation_id,
        "build": build,
        "fallback": fallback,
        "deadline_at": deadline_at,
    }
    if ttl_s is not None:
        kwargs["ttl_s"] = ttl_s
    return await _run_bounded_health_probe(**kwargs)


def record_first_write_acknowledged(
    *,
    board_id: str,
    previous_generation: str,
    generation: str,
    correlation_id: str | None,
    is_first_write: bool,
    acknowledged_monotonic: float | None = None,
) -> None:
    """Publish the durable-write receipt through the public edition facade."""

    from okto_pulse.core.observability.materialization_health import (
        record_first_write_acknowledged as _record_first_write_acknowledged,
    )

    _record_first_write_acknowledged(
        board_id=board_id,
        previous_generation=previous_generation,
        generation=generation,
        correlation_id=correlation_id,
        is_first_write=is_first_write,
        acknowledged_monotonic=acknowledged_monotonic,
    )


def record_read_side_mutation_guard(
    *,
    board_id: str,
    outcome: str,
    snapshot_before_sha256: str | None,
    snapshot_after_sha256: str | None,
    changed_paths: Sequence[str],
) -> None:
    """Publish a redacted read-side guard receipt via the public facade."""

    from okto_pulse.core.observability.materialization_health import (
        record_read_side_mutation_guard as _record_read_side_mutation_guard,
    )

    _record_read_side_mutation_guard(
        board_id=board_id,
        outcome=outcome,
        snapshot_before_sha256=snapshot_before_sha256,
        snapshot_after_sha256=snapshot_after_sha256,
        changed_paths=changed_paths,
    )


_RUNTIME_KEY = "ports.materialization_health.evidence"


def register_materialization_evidence_port(
    port: MaterializationEvidencePort,
) -> None:
    """Register the edition-owned bounded evidence adapter for this runtime."""

    register_runtime_value(_RUNTIME_KEY, port)


def get_materialization_evidence_port() -> MaterializationEvidencePort | None:
    """Resolve the optional adapter; absence is a fail-closed health signal."""

    value = resolve_runtime_value(_RUNTIME_KEY)
    return value  # type: ignore[return-value]


def reset_materialization_evidence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "BoardHealthCensus",
    "CensusStatus",
    "HealthProbeDeadline",
    "MaterializationEvidence",
    "MaterializationEvidencePort",
    "MaterializationEvidenceRequest",
    "get_materialization_evidence_port",
    "record_first_write_acknowledged",
    "record_read_side_mutation_guard",
    "register_materialization_evidence_port",
    "reset_materialization_evidence_port_for_tests",
    "run_bounded_health_probe",
]
