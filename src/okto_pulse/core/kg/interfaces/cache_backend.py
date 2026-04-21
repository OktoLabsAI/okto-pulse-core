"""CacheBackend Protocol — read-through cache contract."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CacheBackend(Protocol):
    def get(self, tool_name: str, board_id: str, params: dict) -> tuple[bool, Any]:
        """Return (hit, value). On miss or expired: (False, None)."""
        ...

    def put(self, tool_name: str, board_id: str, params: dict, value: Any) -> None:
        """Store a value. Evicts oldest entry when full."""
        ...

    def invalidate_board(self, board_id: str) -> int:
        """Remove all entries for a board. Returns count evicted."""
        ...

    def stats(self) -> dict:
        """Return cache statistics."""
        ...
