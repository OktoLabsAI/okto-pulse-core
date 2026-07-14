"""Backend-neutral failures exposed by graph adapters."""

from __future__ import annotations


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


__all__ = [
    "GraphCapabilityUnavailable",
    "GraphCorruption",
    "GraphError",
    "GraphIndexUnavailable",
    "GraphLockContention",
    "GraphUnavailable",
]
