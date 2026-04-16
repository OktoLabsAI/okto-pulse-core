"""Read-through LRU+TTL cache for KG query tools.

Delegates to the KG registry's cache_backend (CacheBackend Protocol).
These module-level functions are backward-compat wrappers — new code
should use ``get_kg_registry().cache_backend`` directly.

Metrics emitted per tool call via structured logger:
  event: "kg.tool.call"
  tool_name, board_id, cache_hit (bool), duration_ms, result_count, error_code
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("okto_pulse.kg.cache")


def _backend():
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry().cache_backend


def cache_get(tool_name: str, board_id: str, params: dict) -> tuple[bool, Any]:
    """Return (hit, value). On miss or expired: (False, None)."""
    return _backend().get(tool_name, board_id, params)


def cache_put(tool_name: str, board_id: str, params: dict, value: Any) -> None:
    """Store a value. Evicts oldest entry when full."""
    _backend().put(tool_name, board_id, params, value)


def invalidate_board(board_id: str) -> int:
    """Remove all cache entries for a board. Called by commit_consolidation."""
    return _backend().invalidate_board(board_id)


def cache_stats() -> dict:
    """Current cache statistics."""
    return _backend().stats()


def clear_cache() -> None:
    """Drop everything — tests only."""
    backend = _backend()
    if hasattr(backend, "clear"):
        backend.clear()


# ---------------------------------------------------------------------------
# Metrics helper
# ---------------------------------------------------------------------------


def emit_tool_metrics(
    *,
    tool_name: str,
    board_id: str,
    cache_hit: bool,
    duration_ms: float,
    result_count: int,
    error_code: str | None = None,
) -> None:
    """Structured log entry for per-tool observability (FR-7)."""
    level = logging.INFO if error_code is None else logging.WARNING
    logger.log(
        level,
        "kg.tool.call tool=%s board=%s cache=%s dur=%.1fms results=%d err=%s",
        tool_name, board_id, cache_hit, duration_ms, result_count,
        error_code or "none",
        extra={
            "event": "kg.tool.call",
            "tool_name": tool_name,
            "board_id": board_id,
            "cache_hit": cache_hit,
            "duration_ms": round(duration_ms, 1),
            "result_count": result_count,
            "error_code": error_code,
        },
    )
