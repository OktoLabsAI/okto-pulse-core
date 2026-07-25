"""Graph transaction port with materialized, backend-neutral results."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Protocol, runtime_checkable


SPEC_LINEAGE_RULE_PREFIXES: tuple[str, str] = (
    "belongs_to/spec_to_ideation@",
    "belongs_to/spec_to_refinement@",
)


def is_spec_lineage_rule_id(rule_id: str) -> bool:
    """Return whether ``rule_id`` belongs to the exclusive Spec-parent family."""

    normalized = str(rule_id or "")
    return any(normalized.startswith(prefix) for prefix in SPEC_LINEAGE_RULE_PREFIXES)


class SpecLineageParentIntent(str, Enum):
    """Internal deterministic-worker intent for the exclusive Spec parent."""

    PRESERVE = "preserve"
    CLEAR = "clear"


@dataclass(frozen=True)
class SpecLineageEdgeSnapshot:
    """Complete before-image for one deterministic Spec-parent relationship."""

    source_id: str
    target_id: str
    rule_id: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class SpecLineageReconciliationReceipt:
    """Receipt used to compensate an auto-committed exclusive-parent swap."""

    source_id: str
    target_id: str | None
    target_rule_id: str | None
    target_attrs: dict[str, Any]
    new_edge_created: bool
    removed_edges: tuple[SpecLineageEdgeSnapshot, ...] = ()
    ambiguous_legacy_edges: int = 0


class SpecLineageReconciliationError(RuntimeError):
    """Typed, fail-closed error for the bounded Spec-parent capability."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        receipt: SpecLineageReconciliationReceipt | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.receipt = receipt


@dataclass
class GraphStatementResult:
    """Materialized rows returned by a semantic graph statement."""

    rows: tuple[tuple[Any, ...], ...] = ()
    columns: tuple[str, ...] = ()
    affected_count: int | None = None

    @classmethod
    def from_rows(
        cls,
        rows: Iterable[Iterable[Any]],
        *,
        columns: Iterable[str] = (),
        affected_count: int | None = None,
    ) -> "GraphStatementResult":
        return cls(
            rows=tuple(tuple(row) for row in rows),
            columns=tuple(str(column) for column in columns),
            affected_count=affected_count,
        )

    def __iter__(self):
        return (list(row) for row in self.rows)


@runtime_checkable
class GraphTransactionScope(Protocol):
    def execute(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
    ) -> GraphStatementResult:
        """Run a statement and return materialized rows."""
        ...

    def create_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
        *,
        source_session_id: str,
    ) -> None: ...

    def update_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
    ) -> None: ...

    def mark_superseded(
        self,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None: ...

    def edge_exists(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool: ...

    def create_edge(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
        attrs: dict[str, Any],
    ) -> bool: ...

    def reconcile_spec_lineage_parent(
        self,
        source_id: str,
        target_id: str,
        attrs: dict[str, Any],
    ) -> SpecLineageReconciliationReceipt:
        """Create/confirm the new Spec parent before removing its old parent."""
        ...

    def compensate_spec_lineage_parent(
        self,
        receipt: SpecLineageReconciliationReceipt,
    ) -> None:
        """Restore old parents before removing a newly-created replacement."""
        ...

    def clear_spec_lineage_parent(
        self,
        source_id: str,
    ) -> SpecLineageReconciliationReceipt:
        """Remove only deterministic Spec-parent edges with a compensable receipt."""
        ...

    def find_node_types(self, node_id: str) -> tuple[str, ...]: ...

    def delete_edges_by_session(self, session_id: str) -> None: ...

    def delete_edges_by_session_preserving_spec_lineage(
        self,
        session_id: str,
        preserved_edges: tuple[SpecLineageEdgeSnapshot, ...],
    ) -> None:
        """Delete session-owned edges without deleting restored Spec parents."""
        ...

    def delete_nodes_by_session(
        self,
        session_id: str,
        node_types: tuple[str, ...],
    ) -> tuple[str, ...]: ...

    def increment_attestation(
        self,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ) -> None: ...

    async def commit(self) -> None:
        """Finalize the scope."""
        ...

    async def rollback(self) -> None:
        """Abort the scope (best-effort on the embedded auto-commit adapter)."""
        ...

    async def __aenter__(self) -> "GraphTransactionScope": ...

    async def __aexit__(self, *exc: Any) -> None: ...


@runtime_checkable
class GraphTransaction(Protocol):
    async def begin(self, board_id: str) -> GraphTransactionScope:
        """Open a staged-write scope for ``board_id``."""
        ...


__all__ = [
    "GraphStatementResult",
    "GraphTransaction",
    "GraphTransactionScope",
    "SPEC_LINEAGE_RULE_PREFIXES",
    "SpecLineageEdgeSnapshot",
    "SpecLineageParentIntent",
    "SpecLineageReconciliationError",
    "SpecLineageReconciliationReceipt",
    "is_spec_lineage_rule_id",
]
