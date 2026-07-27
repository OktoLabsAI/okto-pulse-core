"""Compensating transaction pattern for graph writes.

The application UnitOfWork owns relational commit and rollback. This
orchestrator records graph mutations so the use case can compensate them when
the UnitOfWork fails after graph writes were applied.

The orchestrator tracks every graph write keyed by session_id so a failure can
delete exactly those nodes/edges via `source_session_id` / `created_by_session_id`
filters. Edges use a per-rel-type DELETE loop because some graph backends lack a
`MATCH ()-[r]-() DELETE r` that works across rel tables.

"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageEdgeSnapshot,
    SpecLineageReconciliationError,
    SpecLineageReconciliationReceipt,
    is_spec_lineage_rule_id,
)
from okto_pulse.core.kg.schema_contract import resolve_relationship_endpoint_pair

logger = logging.getLogger("okto_pulse.kg.transaction")


class _StoreBackedGraphScope:
    """Semantic compatibility scope for callers carrying a read-only handle."""

    def __init__(self, board_id: str, store: Any, raw_scope: Any) -> None:
        self._board_id = board_id
        self._store = store
        self._raw_scope = raw_scope

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any:
        if params is None:
            return self._raw_scope.execute(statement)
        return self._raw_scope.execute(statement, params)

    def create_node(self, node_type, node_id, attrs, *, source_session_id):
        self._store.create_node(
            self._board_id,
            node_type,
            node_id,
            {**attrs, "source_session_id": source_session_id},
        )

    def update_node(self, node_type, node_id, attrs):
        self._store.update_node(self._board_id, node_type, node_id, attrs)

    def mark_superseded(self, node_type, node_id, **values):
        self._store.mark_superseded(
            self._board_id,
            node_type,
            node_id,
            **values,
        )

    def edge_exists(self, edge_type, from_type, to_type, from_id, to_id):
        return self._store.edge_exists(
            self._board_id,
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
        )

    def create_edge(self, edge_type, from_type, to_type, from_id, to_id, attrs):
        if not self.find_node_types(from_id) or not self.find_node_types(to_id):
            return False
        self._store.create_edge(
            self._board_id,
            edge_type,
            from_id,
            to_id,
            attrs,
            from_type=from_type,
            to_type=to_type,
        )
        return True

    def reconcile_spec_lineage_parent(self, source_id, target_id, attrs):
        raise SpecLineageReconciliationError(
            "spec_lineage_reconciliation_capability_unavailable",
            "The compatibility graph-store scope cannot reconcile exclusive "
            "Spec lineage; install a GraphTransaction adapter with the bounded "
            "Spec-lineage capability.",
        )

    def clear_spec_lineage_parent(self, source_id):
        del source_id
        raise SpecLineageReconciliationError(
            "spec_lineage_reconciliation_capability_unavailable",
            "The compatibility graph-store scope cannot clear exclusive Spec "
            "lineage; install a GraphTransaction adapter with the bounded "
            "Spec-lineage capability.",
        )

    def compensate_spec_lineage_parent(self, receipt):
        del receipt
        raise SpecLineageReconciliationError(
            "spec_lineage_compensation_capability_unavailable",
            "The compatibility graph-store scope cannot compensate exclusive "
            "Spec lineage.",
        )

    def find_node_types(self, node_id):
        return self._store.find_node_types(self._board_id, node_id)

    def delete_edges_by_session(self, session_id):
        self._store.delete_edges_by_session(self._board_id, session_id)

    def delete_edges_by_session_preserving_spec_lineage(
        self,
        session_id,
        preserved_edges,
    ):
        del session_id, preserved_edges
        raise SpecLineageReconciliationError(
            "spec_lineage_compensation_capability_unavailable",
            "The compatibility graph-store scope cannot preserve restored "
            "Spec lineage while compensating session edges.",
        )

    def delete_nodes_by_session(self, session_id, node_types):
        del node_types
        self._store.delete_nodes_by_session(self._board_id, session_id)
        return ()

    def increment_attestation(self, node_type, node_id, *, attested_at):
        self._store.increment_attestation(
            self._board_id,
            node_type,
            node_id,
            attested_at=attested_at,
        )


def _result_has_row(result: Any) -> bool:
    """Best-effort detection that a Cypher CREATE ... RETURN matched endpoints."""
    if result is None:
        return False
    rows = getattr(result, "rows", None)
    if rows is not None:
        return bool(rows)
    try:
        next(iter(result))
        return True
    except (StopIteration, TypeError):
        return False


@dataclass
class GraphWriteRecord:
    """One mutation applied to the graph adapter, used for compensating delete."""

    kind: str  # "node" | "edge"
    entity_type: str  # "Decision", "supersedes", etc.
    entity_id: str  # For nodes: the id. For edges: synthetic session-scoped key.
    # Edge-only: anchors for MATCH DELETE pattern.
    from_id: str | None = None
    to_id: str | None = None
    lineage_receipt: SpecLineageReconciliationReceipt | None = None


@dataclass
class CommitCounters:
    """Roll-up counts returned by commit for the audit row.

    Spec eca49df9 (FR6): counters are mutually exclusive per candidate —
    each processed candidate increments exactly one of nodes_added (CREATE),
    nodes_updated (in-place UPDATE), nodes_merged (NC-8 dedup-reuse),
    nodes_superseded (SUPERSEDE) or nodes_noop (NOOP), so
    ``processed_candidates`` closes as their sum. ``merge_audit_items`` carries
    the per-merge audit payload surfaced in the commit response (NOT a
    GraphWriteRecord — the reused node belongs to a prior session and must not
    enter compensation rollback).
    """

    nodes_added: int = 0
    nodes_updated: int = 0
    nodes_superseded: int = 0
    edges_added: int = 0
    nodes_merged: int = 0
    nodes_noop: int = 0
    merge_audit_items: list[dict[str, Any]] = field(default_factory=list)


class CompensationError(Exception):
    """Raised when compensating delete itself fails — operator intervention needed."""

    def __init__(self, message: str, original_exc: Exception | None = None,
                 failed_records: list[GraphWriteRecord] | None = None):
        super().__init__(message)
        self.original_exc = original_exc
        self.failed_records = failed_records or []


@dataclass(init=False)
class TransactionOrchestrator:
    """Coordinates graph writes and their compensation.

    The orchestrator is single-use. On a later UnitOfWork failure, call
    ``compensate`` to reverse graph writes.
    """

    graph_scope: Any
    session_id: str
    board_id: str
    records: list[GraphWriteRecord] = field(default_factory=list)
    counters: CommitCounters = field(default_factory=CommitCounters)
    _compensated: bool = False

    def __init__(
        self,
        graph_scope: Any | None = None,
        session_id: str = "",
        board_id: str = "",
    ) -> None:
        if graph_scope is None:
            raise TypeError("TransactionOrchestrator requires a graph_scope")
        if not callable(getattr(graph_scope, "create_node", None)):
            from okto_pulse.core.kg.interfaces import get_kg_registry

            store = get_kg_registry().graph_store
            if store is None:
                raise RuntimeError("semantic_graph_store_not_configured")
            graph_scope = _StoreBackedGraphScope(board_id, store, graph_scope)
        self.graph_scope = graph_scope
        self.session_id = session_id
        self.board_id = board_id
        self.records = []
        self.counters = CommitCounters()
        self._compensated = False

    def execute_graph(self, stmt: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a graph statement through the composed graph transaction port."""

        if params is None:
            return self.graph_scope.execute(stmt)
        return self.graph_scope.execute(stmt, params)

    # ------------------------------------------------------------------
    # Graph write phase
    # ------------------------------------------------------------------

    def create_node(self, node_type: str, node_id: str, attrs: dict[str, Any]) -> None:
        """Insert a new node into the graph and record it for compensation.

        `attrs` must NOT include `id`, `source_session_id`, or `board_id` —
        those are injected here so the caller can't forget them.
        """
        self._guard_fresh()
        self.graph_scope.create_node(
            node_type,
            node_id,
            dict(attrs),
            source_session_id=self.session_id,
        )

        self.records.append(
            GraphWriteRecord(kind="node", entity_type=node_type, entity_id=node_id)
        )
        self.counters.nodes_added += 1

    def update_node(self, node_type: str, node_id: str, attrs: dict[str, Any]) -> None:
        """Overwrite a node's mutable attrs in place and count it as an UPDATE.

        Spec eca49df9 (TR6) wires this as the production call site for
        op==UPDATE. It previously had NO call site, which masked the fact that
        the node schema (see schema.py) has no ``updated_by_session_id`` /
        ``updated_at`` columns — SETting them raised a graph backend Binder exception.
        Only the caller-supplied content attrs are SET (callers pass the
        _NODE_UPDATEABLE_ATTRS subset, never ``embedding`` which is HNSW-locked).

        Not reverted by compensate — updates are lossy and there's nothing to
        compensate to (compensating rollback only protects partial ADD writes,
        not UPDATE overwrites; documented in the spec)."""
        self._guard_fresh()
        values = {key: value for key, value in attrs.items() if key != "id"}
        if not values:
            return
        self.graph_scope.update_node(node_type, node_id, values)
        self.counters.nodes_updated += 1

    def supersede_node(
        self,
        node_type: str,
        new_node_id: str,
        superseded_node_id: str,
        new_attrs: dict[str, Any],
        revocation_reason: str | None = None,
    ) -> None:
        """Create new node and attach a :supersedes edge to the old node.

        Both writes are recorded so compensate rolls back both. The old node
        is marked via superseded_by/superseded_at attrs (append-only history
        for Decision/Criterion/Constraint/Learning/Bug per the spec)."""
        self._guard_fresh()
        # 1. Create the new node
        self.create_node(node_type, new_node_id, new_attrs)
        self.counters.nodes_added -= 1  # reclassify from added to superseded
        self.counters.nodes_superseded += 1

        # 2. Mark the old node as superseded
        self.graph_scope.mark_superseded(
            node_type,
            superseded_node_id,
            superseded_by=new_node_id,
            superseded_at=_now_iso(),
            revocation_reason=revocation_reason or "",
        )

        # 3. Create the walkable :supersedes edge — universal for ALL node
        # types (spec MKG-D-S1 FR4; was Decision-only until the rel became
        # multi-pair). from/to hints are mandatory for multi-pair rels.
        self.create_edge(
            "supersedes",
            new_node_id,
            superseded_node_id,
            attrs={"confidence": 1.0},
            from_type=node_type,
            to_type=node_type,
        )

    def create_edge(
        self,
        edge_type: str,
        from_id: str,
        to_id: str,
        attrs: dict[str, Any] | None = None,
        *,
        from_type: str | None = None,
        to_type: str | None = None,
    ) -> None:
        """Insert a relationship between two existing nodes.

        For single-pair rels the from/to node types are resolved automatically
        from the registry. For rel names with multiple endpoint pairs
        (including names present in both REL_TYPES and MULTI_REL_TYPES, such as
        ``implements``), the caller MUST pass ``from_type``/``to_type`` because
        the Cypher MATCH needs the concrete node labels.
        """
        self._guard_fresh()
        edge_attrs: dict[str, Any] = dict(attrs or {})
        edge_attrs.setdefault("confidence", 0.7)
        edge_attrs["created_by_session_id"] = self.session_id
        edge_attrs.setdefault("created_at", _now_iso())
        # v0.2.0 provenance metadata. Default to layer="cognitive" because the
        # TransactionOrchestrator is the cognitive-agent write path — the Layer 1
        # worker uses its own write API and overrides these explicitly.
        edge_attrs.setdefault("layer", "cognitive")
        edge_attrs.setdefault("rule_id", "")
        edge_attrs.setdefault("created_by", self.session_id)
        edge_attrs.setdefault("fallback_reason", "")

        from_type, to_type = resolve_relationship_endpoint_pair(
            edge_type,
            from_type=from_type,
            to_type=to_type,
        )

        rule_id = str(edge_attrs.get("rule_id") or "")
        if edge_type == "belongs_to" and is_spec_lineage_rule_id(rule_id):
            if (from_type, to_type) != ("Entity", "Entity"):
                raise SpecLineageReconciliationError(
                    "spec_lineage_endpoint_type_invalid",
                    "Spec lineage reconciliation requires Entity -> Entity "
                    f"endpoints, received {from_type} -> {to_type}.",
                )
            reconcile = getattr(
                self.graph_scope,
                "reconcile_spec_lineage_parent",
                None,
            )
            if not callable(reconcile):
                raise SpecLineageReconciliationError(
                    "spec_lineage_reconciliation_capability_unavailable",
                    "The configured GraphTransaction scope does not expose the "
                    "bounded Spec-lineage reconciliation capability.",
                )
            try:
                receipt = reconcile(from_id, to_id, edge_attrs)
            except SpecLineageReconciliationError as exc:
                partial_receipt = exc.receipt
                if partial_receipt is not None and (
                    partial_receipt.new_edge_created
                    or partial_receipt.removed_edges
                ):
                    self.records.append(
                        GraphWriteRecord(
                            kind="edge",
                            entity_type=edge_type,
                            entity_id=f"{from_id}->{to_id}",
                            from_id=from_id,
                            to_id=to_id,
                            lineage_receipt=partial_receipt,
                        )
                    )
                raise
            if receipt.ambiguous_legacy_edges:
                logger.warning(
                    "kg.transaction.spec_lineage_legacy_ambiguous "
                    "session=%s source=%s count=%d",
                    self.session_id,
                    from_id,
                    receipt.ambiguous_legacy_edges,
                    extra={
                        "event": "kg.transaction.spec_lineage_legacy_ambiguous",
                        "session_id": self.session_id,
                        "board_id": self.board_id,
                        "source_id": from_id,
                        "ambiguous_legacy_edges": receipt.ambiguous_legacy_edges,
                    },
                )
            if receipt.new_edge_created or receipt.removed_edges:
                self.records.append(
                    GraphWriteRecord(
                        kind="edge",
                        entity_type=edge_type,
                        entity_id=f"{from_id}->{to_id}",
                        from_id=from_id,
                        to_id=to_id,
                        lineage_receipt=receipt,
                    )
                )
            if receipt.new_edge_created:
                self.counters.edges_added += 1
            return

        if self._edge_exists(edge_type, from_type, to_type, from_id, to_id):
            logger.info(
                "kg.transaction.edge_exists session=%s edge=%s from=%s(%s) to=%s(%s)",
                self.session_id, edge_type, from_type, from_id, to_type, to_id,
                extra={
                    "event": "kg.transaction.edge_exists",
                    "session_id": self.session_id,
                    "edge_type": edge_type,
                    "from_type": from_type,
                    "from_id": from_id,
                    "to_type": to_type,
                    "to_id": to_id,
                },
            )
            return

        created = self.graph_scope.create_edge(
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
            edge_attrs,
        )
        if not created:
            actual_from = self._find_node_types(from_id)
            actual_to = self._find_node_types(to_id)
            raise ValueError(
                "edge was not created because the endpoint nodes were not "
                f"matched: {edge_type} {from_type}({from_id}) -> "
                f"{to_type}({to_id}); actual endpoint types: "
                f"from={actual_from or ['not_found']}, "
                f"to={actual_to or ['not_found']}"
            )

        self.records.append(
            GraphWriteRecord(
                kind="edge",
                entity_type=edge_type,
                entity_id=f"{from_id}->{to_id}",
                from_id=from_id,
                to_id=to_id,
            )
        )
        self.counters.edges_added += 1

    def clear_spec_lineage_parent(self, source_id: str) -> None:
        """Apply an explicit deterministic parent unlink with a before-image."""

        self._guard_fresh()
        clear = getattr(self.graph_scope, "clear_spec_lineage_parent", None)
        if not callable(clear):
            raise SpecLineageReconciliationError(
                "spec_lineage_reconciliation_capability_unavailable",
                "The configured GraphTransaction scope does not expose the "
                "bounded Spec-lineage clear capability.",
            )
        try:
            receipt = clear(source_id)
        except SpecLineageReconciliationError as exc:
            partial_receipt = exc.receipt
            if partial_receipt is not None and partial_receipt.removed_edges:
                self.records.append(
                    GraphWriteRecord(
                        kind="edge",
                        entity_type="belongs_to",
                        entity_id=f"{source_id}-><none>",
                        from_id=source_id,
                        lineage_receipt=partial_receipt,
                    )
                )
            raise
        if receipt.ambiguous_legacy_edges:
            logger.warning(
                "kg.transaction.spec_lineage_clear_legacy_ambiguous "
                "session=%s source=%s count=%d",
                self.session_id,
                source_id,
                receipt.ambiguous_legacy_edges,
                extra={
                    "event": (
                        "kg.transaction."
                        "spec_lineage_clear_legacy_ambiguous"
                    ),
                    "session_id": self.session_id,
                    "board_id": self.board_id,
                    "source_id": source_id,
                    "ambiguous_legacy_edges": receipt.ambiguous_legacy_edges,
                },
            )
        if receipt.removed_edges:
            self.records.append(
                GraphWriteRecord(
                    kind="edge",
                    entity_type="belongs_to",
                    entity_id=f"{source_id}-><none>",
                    from_id=source_id,
                    lineage_receipt=receipt,
                )
            )

    def _edge_exists(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool:
        """Return True when this semantic edge already exists.

        graph backend does not enforce a unique constraint for relationship tables, so
        repeated artifact consolidation can otherwise materialize the same
        edge many times. The KG treats `(edge_type, from_id, to_id)` as the
        natural identity for all current relationships.
        """
        return self.graph_scope.edge_exists(
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
        )

    def _find_node_types(self, node_id: str) -> list[str]:
        """Best-effort lookup used only to make failed edge errors actionable."""
        return list(self.graph_scope.find_node_types(node_id))

    async def compensate(self) -> None:
        """Reverse every graph write recorded so far.

        Strategy: iterate `records` in reverse insertion order. Delete edges
        first (they depend on nodes), then nodes. Uses session_id filters in
        WHERE clauses so partial state is safe — re-running compensate is
        idempotent.
        """
        if self._compensated:
            return
        if not self.records:
            self._compensated = True
            return

        failed: list[GraphWriteRecord] = []

        lineage_receipts = [
            record.lineage_receipt
            for record in reversed(self.records)
            if record.lineage_receipt is not None
        ]
        preserved_edges: list[SpecLineageEdgeSnapshot] = []
        for receipt in lineage_receipts:
            try:
                self.graph_scope.compensate_spec_lineage_parent(receipt)
            except Exception as exc:
                logger.error(
                    "kg.compensate.spec_lineage_restore_failed "
                    "session=%s source=%s target=%s err=%s",
                    self.session_id,
                    receipt.source_id,
                    receipt.target_id,
                    exc,
                )
                raise CompensationError(
                    "Spec-lineage compensation failed before generic session "
                    "cleanup; the replacement edge was preserved.",
                    original_exc=exc,
                    failed_records=[
                        record
                        for record in self.records
                        if record.lineage_receipt is receipt
                    ],
                ) from exc
            preserved_edges.extend(receipt.removed_edges)

        try:
            if preserved_edges:
                delete_preserving = getattr(
                    self.graph_scope,
                    "delete_edges_by_session_preserving_spec_lineage",
                    None,
                )
                if not callable(delete_preserving):
                    raise SpecLineageReconciliationError(
                        "spec_lineage_compensation_capability_unavailable",
                        "The configured GraphTransaction scope cannot preserve "
                        "restored Spec lineage during session cleanup.",
                    )
                delete_preserving(self.session_id, tuple(preserved_edges))
            else:
                self.graph_scope.delete_edges_by_session(self.session_id)
        except Exception as exc:
            logger.error(
                "kg.compensate.edge_delete_failed session=%s err=%s",
                self.session_id,
                exc,
            )
            if lineage_receipts:
                raise CompensationError(
                    "compensating edge delete failed",
                    original_exc=exc,
                    failed_records=list(self.records),
                ) from exc

        # 2. Delete nodes created by this session (group by type for efficiency)
        node_types = {
            r.entity_type for r in self.records if r.kind == "node"
        }
        failed_types = self.graph_scope.delete_nodes_by_session(
            self.session_id,
            tuple(sorted(node_types)),
        )
        for node_type in failed_types:
            failed.extend(
                record
                for record in self.records
                if record.kind == "node" and record.entity_type == node_type
            )

        self._compensated = True
        logger.info(
            "kg.compensate.done session=%s records=%d failed=%d",
            self.session_id, len(self.records), len(failed),
        )
        if failed:
            raise CompensationError(
                f"compensating delete failed for {len(failed)} records",
                failed_records=failed,
            )

    # ------------------------------------------------------------------
    # Guards
    # ------------------------------------------------------------------

    def _guard_fresh(self) -> None:
        if self._compensated:
            raise RuntimeError("orchestrator already compensated")


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
