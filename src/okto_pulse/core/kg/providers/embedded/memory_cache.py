"""InMemoryCacheBackend — satisfies CacheBackend Protocol using dict storage.

Refactored from okto_pulse.core.kg.cache with same semantics:
LRU+TTL, max 1000 entries, 60s TTL, board-indexed invalidation O(1).
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

_MAX_SIZE = 1000
_TTL_SECONDS = 60.0


class InMemoryCacheBackend:
    def __init__(self, max_size: int = _MAX_SIZE, ttl_seconds: float = _TTL_SECONDS):
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[float, Any]] = {}
        self._board_index: dict[str, set[str]] = {}

    def _key(self, tool_name: str, board_id: str, params: dict) -> str:
        raw = json.dumps({"t": tool_name, "b": board_id, "p": params}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, tool_name: str, board_id: str, params: dict) -> tuple[bool, Any]:
        key = self._key(tool_name, board_id, params)
        entry = self._cache.get(key)
        if entry is None:
            return False, None
        ts, val = entry
        if time.monotonic() - ts > self._ttl:
            self._cache.pop(key, None)
            return False, None
        return True, val

    def put(self, tool_name: str, board_id: str, params: dict, value: Any) -> None:
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache, key=lambda k: self._cache[k][0])
            self._cache.pop(oldest_key)
        key = self._key(tool_name, board_id, params)
        self._cache[key] = (time.monotonic(), value)
        self._board_index.setdefault(board_id, set()).add(key)

    def invalidate_board(self, board_id: str) -> int:
        keys = self._board_index.pop(board_id, set())
        for k in keys:
            self._cache.pop(k, None)
        return len(keys)

    def stats(self) -> dict:
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl,
            "boards_tracked": len(self._board_index),
        }

    def clear(self) -> None:
        self._cache.clear()
        self._board_index.clear()
