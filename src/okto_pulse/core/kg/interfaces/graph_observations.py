"""Optional, bounded observations; no storage-specific IDs or implementations.

Commit tokens are opaque store-qualified observations, not authorization. History
is not an application restore or an atomic snapshot of relational data.
"""

from typing import Protocol


class GraphHistory(Protocol):
    def commits(
        self, board_id: str, *, after: str | None = None, limit: int = 100
    ) -> dict: ...
    def as_of(
        self,
        board_id: str,
        at: str,
        node_types: tuple[str, ...],
        relationship_types: tuple[str, ...],
        *,
        max_rows: int = 1000,
        max_bytes: int = 16777216,
    ) -> dict: ...
    def diff(
        self,
        board_id: str,
        before: str,
        after: str,
        node_types: tuple[str, ...],
        relationship_types: tuple[str, ...],
        *,
        max_rows: int = 1000,
        max_bytes: int = 16777216,
    ) -> dict: ...


class GraphAnalytics(Protocol):
    def analyze(
        self,
        board_id: str,
        *,
        node_types: tuple[str, ...],
        relationship_types: tuple[str, ...],
        algorithm: str,
        graph_layer: str = "canonical",
        include_code_traceability: bool = False,
        source_type: str | None = None,
        source_id: str | None = None,
        direction: str = "out",
        max_depth: int = 10,
        max_nodes: int = 1000,
        max_edges: int = 10000,
        timeout_seconds: float = 10.0,
    ) -> dict: ...
