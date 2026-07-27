"""Backend-neutral failures exposed by graph adapters."""

from __future__ import annotations

import math


GRAPH_MEMORY_PRESSURE_CODE = "graph_memory_pressure"
GRAPH_MEMORY_PRESSURE_DEFAULT_RETRY_AFTER_SECONDS = 60
GRAPH_MEMORY_PRESSURE_MAX_RETRY_AFTER_SECONDS = 300


class GraphError(RuntimeError):
    """Base error for a graph operation that crossed an adapter boundary."""

    code = "graph_error"
    retryable = False

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.details = dict(details or {})


class GraphUnavailable(GraphError):
    code = "graph_unavailable"
    retryable = True


class GraphCorruption(GraphError):
    code = "graph_corruption"


class GraphLockContention(GraphUnavailable):
    code = "graph_lock_contention"


class GraphCapabilityUnavailable(GraphError):
    code = "graph_capability_unavailable"


class GraphIndexUnavailable(GraphCapabilityUnavailable):
    code = "graph_index_unavailable"
    retryable = True


def graph_memory_pressure_retry_after_seconds(
    failure: BaseException | str,
) -> int | None:
    """Return a bounded retry delay for typed graph allocation pressure.

    Editions expose backend-specific failures through the Core ``GraphError``
    contract.  The worker policy therefore keys on the stable error code and
    optional ``retry_after_ms`` detail without importing an edition adapter.
    String support preserves the same policy for legacy paths that persisted
    the semantic error before the original exception reached the processor.
    """

    retry_after_ms: object | None = None
    matched = False
    if isinstance(failure, BaseException):
        seen: set[int] = set()
        current: BaseException | None = failure
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if (
                isinstance(current, GraphError)
                and current.retryable
                and current.code == GRAPH_MEMORY_PRESSURE_CODE
            ):
                retry_after_ms = current.details.get("retry_after_ms")
                matched = True
                break
            current = current.__cause__ or current.__context__
    else:
        matched = GRAPH_MEMORY_PRESSURE_CODE in str(failure).lower()
    if not matched:
        return None

    retry_after_seconds = GRAPH_MEMORY_PRESSURE_DEFAULT_RETRY_AFTER_SECONDS
    if (
        isinstance(retry_after_ms, (int, float))
        and not isinstance(retry_after_ms, bool)
        and math.isfinite(float(retry_after_ms))
        and float(retry_after_ms) > 0
    ):
        retry_after_seconds = math.ceil(float(retry_after_ms) / 1000.0)
    return max(
        1,
        min(
            retry_after_seconds,
            GRAPH_MEMORY_PRESSURE_MAX_RETRY_AFTER_SECONDS,
        ),
    )


__all__ = [
    "GRAPH_MEMORY_PRESSURE_CODE",
    "GRAPH_MEMORY_PRESSURE_DEFAULT_RETRY_AFTER_SECONDS",
    "GRAPH_MEMORY_PRESSURE_MAX_RETRY_AFTER_SECONDS",
    "GraphCapabilityUnavailable",
    "GraphCorruption",
    "GraphError",
    "GraphIndexUnavailable",
    "GraphLockContention",
    "GraphUnavailable",
    "graph_memory_pressure_retry_after_seconds",
]
