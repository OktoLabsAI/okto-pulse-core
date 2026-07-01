"""In-memory BoardSourceReader fake for tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class InMemoryBoardSourceReader:
    rows_by_board: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def fetch(self, board_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows_by_board.get(board_id, [])]


__all__ = ["InMemoryBoardSourceReader"]
