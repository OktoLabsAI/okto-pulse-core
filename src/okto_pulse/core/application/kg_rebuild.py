"""Resolve complete relational sources for the composed health census."""

from typing import Any
from okto_pulse.core.application.rebuild_ports import SourceObservationBudget


def build_source_store(*, observation_budget: SourceObservationBudget | None = None) -> Any:
    """Resolve a fail-closed row supplier for rebuild enumeration."""

    from okto_pulse.core.kg.interfaces import (
        SourceUnavailableError,
        get_kg_registry,
    )

    reader = get_kg_registry().require_board_source_reader()

    def _fetch_complete_rows(board_id: str) -> list[dict[str, object]]:
        # An edition lacking bounded reads must fail closed. Never retry without
        # the budget after a refusal or an unsupported keyword argument.
        snapshot = (reader.fetch(board_id) if observation_budget is None else
                    reader.fetch(board_id, observation_budget=observation_budget))
        if not snapshot.complete:
            raise SourceUnavailableError(
                "board source snapshot is incomplete "
                f"(board_id={board_id}, cause={snapshot.cause})",
                cause_type=str(snapshot.cause or "unknown"),
            )
        return [dict(row) for row in snapshot.rows]

    return _fetch_complete_rows


__all__ = ["build_source_store"]
