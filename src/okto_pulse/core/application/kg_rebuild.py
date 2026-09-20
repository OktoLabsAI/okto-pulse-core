"""Resolve complete relational sources for the composed health census."""

from typing import Any


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


__all__ = ["build_source_store"]
