"""In-memory BoardSourceReader fake for tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshot


@dataclass
class InMemoryBoardSourceReader:
    rows_by_board: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    snapshots_by_board: dict[str, BoardSourceSnapshot] = field(default_factory=dict)

    def fetch(self, board_id: str) -> BoardSourceSnapshot:
        configured = self.snapshots_by_board.get(board_id)
        if configured is not None:
            return configured
        return BoardSourceSnapshot(
            rows=tuple(dict(row) for row in self.rows_by_board.get(board_id, [])),
        )


__all__ = ["InMemoryBoardSourceReader"]
