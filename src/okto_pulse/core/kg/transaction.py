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

import inspect
import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from okto_pulse.core.kg.interfaces.graph_errors import GraphCapabilityUnavailable
from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphNodePropertyBeforeImage,
    ProjectionActiveSetIntent,
    ProjectionActiveSetReceipt,
    ProjectionActiveSetReconciliationError,
    ProjectionEdgeBeforeImage,
    SpecLineageEdgeSnapshot,
    SpecLineageReconciliationError,
    SpecLineageReconciliationReceipt,
    is_spec_lineage_rule_id,
)
from okto_pulse.core.kg.schema_contract import resolve_relationship_endpoint_pair

_SPEC_DEPENDENCY_RULE_PREFIX = "precedes/spec_dependency/"


def _projection_edge_identity(
    edge_type: str,
    from_type: str,
    to_type: str,
    from_id: str,
    to_id: str,
    rule_id: str,
) -> tuple[str, str, str, str, str, str]:
    """Name one relationship for netting, carrying the rule only where the rule identifies.

    A Spec dependency edge is named by its rule as well as its endpoints, because one
    prerequisite may precede one owner under more than one rule.  Leaving the rule out would
    let a dependency the session just wrote cancel a completely different one that merely
    shares endpoints -- and the one cancelled would be the one the before-image vouches for.
    Every other relationship is identified by its endpoints, and demanding a rule it may not
    declare would split identities that are actually the same.
    """

    dependency = (
        edge_type == "precedes"
        and from_type == "Entity"
        and to_type == "Entity"
        and rule_id.startswith(_SPEC_DEPENDENCY_RULE_PREFIX)
    )
    return (
        edge_type,
        from_type,
        to_type,
        from_id,
        to_id,
        rule_id if dependency else "",
    )


def _accepts_preserved_projection_edges(delete_preserving: Any) -> bool:
    """Whether a cleanup implementation can be told which projection edges to keep.

    Only an explicitly named parameter counts.  ``**kwargs`` would swallow the argument and
    report nothing, so an implementation written before this contract would pass the probe,
    run an ordinary sweep, delete the edges compensation had just restored, and return
    success -- the exact silent failure this probe exists to prevent.
    """

    try:
        parameters = inspect.signature(delete_preserving).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get("preserved_projection_edges")
    return parameter is not None and parameter.kind in {
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }

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

    def replace_node_payload(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
        *,
        source_session_id: str,
    ) -> bool:
        replace = getattr(self._store, "replace_node_payload", None)
        if not callable(replace):
            # A backend without the atomic replacement is refused by NAME rather than by a bare
            # RuntimeError: the caller's alternative is a read-modify-write that would publish a
            # half-replaced payload, so it has to be able to tell this apart from any other
            # failure and never fall back to one.
            raise GraphCapabilityUnavailable(
                "graph_node_payload_replacement_capability_unavailable"
            )
        return bool(
            replace(
                self._board_id,
                node_type,
                node_id,
                dict(attrs),
                source_session_id=source_session_id,
            )
        )

    def snapshot_node_properties(self, node_type, node_id, property_names):
        if any(
            not str(name).replace("_", "").isalnum()
            for name in property_names
        ):
            raise ValueError("invalid graph property name")
        projection = ", ".join(f"n.{name}" for name in property_names)
        result = self.execute(
            f"MATCH (n:{node_type} {{id: $node_id}}) "
            f"RETURN {projection} LIMIT 1",
            {"node_id": node_id},
        )
        rows = getattr(result, "rows", None)
        if rows is None:
            rows = tuple(tuple(row) for row in result)
        if not rows:
            return None
        return GraphNodePropertyBeforeImage(
            node_type=str(node_type),
            node_id=str(node_id),
            attrs={
                str(name): rows[0][index]
                for index, name in enumerate(property_names)
            },
        )

    def restore_node_properties(self, before_image):
        self._store.update_node(
            self._board_id,
            before_image.node_type,
            before_image.node_id,
            dict(before_image.attrs),
        )

    def mark_superseded(self, node_type, node_id, **values):
        self._store.mark_superseded(
            self._board_id,
            node_type,
            node_id,
            **values,
        )

    def edge_exists(
        self,
        edge_type,
        from_type,
        to_type,
        from_id,
        to_id,
        rule_id=None,
    ):
        return self._store.edge_exists(
            self._board_id,
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
            rule_id,
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

    def reconcile_projection_active_set(self, intent):
        del intent
        raise ProjectionActiveSetReconciliationError(
            "projection_active_set_capability_unavailable",
            "The compatibility graph-store scope cannot reconcile a relational "
            "projection active set.",
        )

    def compensate_projection_active_set(self, receipt):
        del receipt
        raise ProjectionActiveSetReconciliationError(
            "projection_active_set_compensation_capability_unavailable",
            "The compatibility graph-store scope cannot compensate a relational "
            "projection active set.",
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
        *,
        preserved_projection_edges=(),
    ):
        del session_id
        # Refusing is the same either way; naming the right one is not cosmetic, because a
        # caller preserving only projection edges would otherwise be told its Spec lineage
        # could not be kept, and go looking in the wrong place.
        if preserved_projection_edges and not preserved_edges:
            raise ProjectionActiveSetReconciliationError(
                "projection_active_set_compensation_capability_unavailable",
                "The compatibility graph-store scope cannot preserve restored "
                "relational projection edges while compensating session edges.",
            )
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

    kind: str  # "node" | "edge" | "property" | "projection"
    entity_type: str  # "Decision", "supersedes", etc.
    entity_id: str  # For nodes: the id. For edges: synthetic session-scoped key.
    # Edge-only: anchors for MATCH DELETE pattern.
    from_id: str | None = None
    to_id: str | None = None
    # Edge-only: enough of the written relationship to recognise it inside a projection
    # before-image.  Compensation has to tell "this edge was here when the session started"
    # apart from "this session put it there", and endpoints alone cannot say which.
    from_type: str | None = None
    to_type: str | None = None
    edge_rule_id: str | None = None
    lineage_receipt: SpecLineageReconciliationReceipt | None = None
    lineage_compensation_applied: bool = False
    lineage_progress_preserved: bool = False
    property_before_image: GraphNodePropertyBeforeImage | None = None
    projection_receipt: ProjectionActiveSetReceipt | None = None


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
        record = GraphWriteRecord(
            kind="node",
            entity_type=node_type,
            entity_id=node_id,
        )
        # Record before invoking the adapter: an embedded driver can apply a
        # statement and still raise while materializing/closing its result.
        self.records.append(record)
        self.graph_scope.create_node(
            node_type,
            node_id,
            dict(attrs),
            source_session_id=self.session_id,
        )
        self.counters.nodes_added += 1

    def update_node(
        self,
        node_type: str,
        node_id: str,
        attrs: dict[str, Any],
        *,
        count_candidate: bool = True,
    ) -> None:
        """Overwrite a node's mutable attrs in place and count it as an UPDATE.

        Spec eca49df9 (TR6) wires this as the production call site for
        op==UPDATE. It previously had NO call site, which masked the fact that
        the node schema (see schema.py) has no ``updated_by_session_id`` /
        ``updated_at`` columns — SETting them raised a graph backend Binder exception.
        Only the caller-supplied content attrs are SET (callers pass the
        _NODE_UPDATEABLE_ATTRS subset, never ``embedding`` which is HNSW-locked).

        Every changed property is snapshotted first so an adapter that
        auto-commits (or applies and then raises) remains compensable.

        ``count_candidate=False`` is reserved for property patches that are
        part of an NC-8 merge. The candidate is then counted exactly once as
        ``nodes_merged`` by the caller while this method still provides the
        same before-image and compensation guarantees."""
        self._guard_fresh()
        values = {key: value for key, value in attrs.items() if key != "id"}
        if not values:
            return
        self.protect_node_properties(
            node_type,
            node_id,
            tuple(sorted(values)),
        )
        self.graph_scope.update_node(node_type, node_id, values)
        if count_candidate:
            self.counters.nodes_updated += 1

    def protect_node_properties(
        self,
        node_type: str,
        node_id: str,
        property_names: tuple[str, ...],
    ) -> GraphNodePropertyBeforeImage:
        """Record a before-image for a bounded mutation performed by a helper."""

        self._guard_fresh()
        before_image = self._snapshot_node_properties(
            node_type,
            node_id,
            tuple(sorted(set(property_names))),
        )
        self.records.append(
            GraphWriteRecord(
                kind="property",
                entity_type=node_type,
                entity_id=node_id,
                property_before_image=before_image,
            )
        )
        return before_image

    def mark_superseded(
        self,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None:
        """Mark an existing node with an exact, compensable before-image."""

        self._guard_fresh()
        self.protect_node_properties(
            node_type,
            node_id,
            ("revocation_reason", "superseded_at", "superseded_by"),
        )
        self.graph_scope.mark_superseded(
            node_type,
            node_id,
            superseded_by=superseded_by,
            superseded_at=superseded_at,
            revocation_reason=revocation_reason,
        )

    def increment_attestation(
        self,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ) -> None:
        """Increment attestation metadata while retaining its before-image."""

        self._guard_fresh()
        self.protect_node_properties(
            node_type,
            node_id,
            ("attestation_count", "last_attested_at"),
        )
        self.graph_scope.increment_attestation(
            node_type,
            node_id,
            attested_at=attested_at,
        )

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
        self.mark_superseded(
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
                            from_type=from_type,
                            to_type=to_type,
                            edge_rule_id=rule_id,
                            lineage_receipt=partial_receipt,
                            lineage_compensation_applied=bool(
                                exc.compensation_applied
                            ),
                            lineage_progress_preserved=bool(
                                exc.preserve_progress
                            ),
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
                        from_type=from_type,
                        to_type=to_type,
                        edge_rule_id=rule_id,
                        lineage_receipt=receipt,
                    )
                )
            if receipt.new_edge_created:
                self.counters.edges_added += 1
            return

        dependency_rule_id = (
            rule_id
            if edge_type == "precedes"
            and rule_id.startswith("precedes/spec_dependency/")
            else None
        )
        if self._edge_exists(
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
            rule_id=dependency_rule_id,
        ):
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

        record = GraphWriteRecord(
            kind="edge",
            entity_type=edge_type,
            entity_id=f"{from_id}->{to_id}",
            from_id=from_id,
            to_id=to_id,
            from_type=from_type,
            to_type=to_type,
            edge_rule_id=rule_id,
        )
        # As with nodes, retain a provisional record across apply-then-raise.
        self.records.append(record)
        created = self.graph_scope.create_edge(
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
            edge_attrs,
        )
        if not created:
            # A materialized false result proves no relationship was applied;
            # only exceptions remain ambiguous apply-then-raise outcomes.
            self.records.remove(record)
            actual_from = self._find_node_types(from_id)
            actual_to = self._find_node_types(to_id)
            raise ValueError(
                "edge was not created because the endpoint nodes were not "
                f"matched: {edge_type} {from_type}({from_id}) -> "
                f"{to_type}({to_id}); actual endpoint types: "
                f"from={actual_from or ['not_found']}, "
                f"to={actual_to or ['not_found']}"
            )

        self.counters.edges_added += 1

    def reconcile_projection_active_set(
        self,
        intent: ProjectionActiveSetIntent,
    ) -> None:
        """Apply an exact relational projection active-set replacement."""

        self._guard_fresh()
        reconcile = getattr(
            self.graph_scope,
            "reconcile_projection_active_set",
            None,
        )
        if not callable(reconcile):
            raise ProjectionActiveSetReconciliationError(
                "projection_active_set_capability_unavailable",
                "The configured GraphTransaction scope does not expose the "
                "bounded relational projection active-set capability.",
            )
        try:
            receipt = reconcile(intent)
        except ProjectionActiveSetReconciliationError as exc:
            if exc.receipt is not None and (
                exc.receipt.before_images
                or exc.receipt.edge_before_images
            ):
                self.records.append(
                    GraphWriteRecord(
                        kind="projection",
                        entity_type=intent.namespace,
                        entity_id=f"{intent.owner_type}:{intent.owner_id}",
                        projection_receipt=exc.receipt,
                    )
                )
            raise
        if receipt.before_images or receipt.edge_before_images:
            self.records.append(
                GraphWriteRecord(
                    kind="projection",
                    entity_type=intent.namespace,
                    entity_id=f"{intent.owner_type}:{intent.owner_id}",
                    projection_receipt=receipt,
                )
            )

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
                        lineage_compensation_applied=bool(
                            exc.compensation_applied
                        ),
                        lineage_progress_preserved=bool(
                            exc.preserve_progress
                        ),
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
        *,
        rule_id: str | None = None,
    ) -> bool:
        """Return True when this semantic edge already exists.

        graph backend does not enforce a unique constraint for relationship tables, so
        repeated artifact consolidation can otherwise materialize the same
        edge many times. The KG treats `(edge_type, from_id, to_id)` as the
        natural identity for all current relationships.
        """
        if rule_id is None:
            return self.graph_scope.edge_exists(
                edge_type,
                from_type,
                to_type,
                from_id,
                to_id,
            )
        return self.graph_scope.edge_exists(
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
            rule_id,
        )

    def _find_node_types(self, node_id: str) -> list[str]:
        """Best-effort lookup used only to make failed edge errors actionable."""
        return list(self.graph_scope.find_node_types(node_id))

    def _snapshot_node_properties(
        self,
        node_type: str,
        node_id: str,
        property_names: tuple[str, ...],
    ) -> GraphNodePropertyBeforeImage:
        snapshot = getattr(self.graph_scope, "snapshot_node_properties", None)
        if not callable(snapshot):
            raise CompensationError(
                "graph property before-image capability unavailable"
            )
        before_image = snapshot(node_type, node_id, property_names)
        if before_image is None:
            raise LookupError(
                f"graph node not found before mutation: {node_type}({node_id})"
            )
        return before_image

    def _session_written_edge_identities(
        self,
    ) -> Counter[tuple[str, str, str, str, str, str]]:
        """How many edges of each identity THIS orchestrator wrote.

        A multiset rather than a set: writing the same relationship twice is two effects to
        undo, and collapsing them would leave one of them preserved.
        """

        return Counter(
            _projection_edge_identity(
                record.entity_type,
                str(record.from_type or ""),
                str(record.to_type or ""),
                str(record.from_id or ""),
                str(record.to_id or ""),
                str(record.edge_rule_id or ""),
            )
            for record in self.records
            if record.kind == "edge" and record.to_id is not None
        )

    def _net_preserved_projection_edges(
        self,
        restored: list[ProjectionEdgeBeforeImage],
    ) -> list[ProjectionEdgeBeforeImage]:
        """Everything the before-images vouch for, minus what this session wrote itself."""

        written = self._session_written_edge_identities()
        if not written:
            return list(restored)
        preserved: list[ProjectionEdgeBeforeImage] = []
        for edge in restored:
            identity = _projection_edge_identity(
                edge.edge_type,
                edge.from_type,
                edge.to_type,
                edge.from_id,
                edge.to_id,
                str(edge.attrs.get("rule_id") or ""),
            )
            if written[identity] > 0:
                # One written edge cancels one restored copy, so a relationship that
                # pre-dated the session still survives alongside one the session added.
                written[identity] -= 1
                continue
            preserved.append(edge)
        return preserved

    def _resolve_preserving_cleanup(
        self,
        preserved_edges: list[SpecLineageEdgeSnapshot],
        preserved_projection_edges: list[ProjectionEdgeBeforeImage],
    ) -> Any:
        """Resolve a cleanup that can keep what compensation restored, or refuse typed.

        Both questions are asked BEFORE the call rather than discovered by making it: the
        call IS the deletion, so a missing method or a signature that predates this contract
        would otherwise be indistinguishable from a failure raised after the restored edges
        had already been swept away.
        """

        delete_preserving = getattr(
            self.graph_scope,
            "delete_edges_by_session_preserving_spec_lineage",
            None,
        )
        if not callable(delete_preserving):
            # Naming the right one is not cosmetic: a caller preserving only projection
            # edges would otherwise be told its Spec lineage could not be kept.
            if preserved_projection_edges and not preserved_edges:
                raise ProjectionActiveSetReconciliationError(
                    "projection_active_set_compensation_capability_unavailable",
                    "The configured GraphTransaction scope cannot preserve restored "
                    "relational projection edges during session cleanup.",
                )
            raise SpecLineageReconciliationError(
                "spec_lineage_compensation_capability_unavailable",
                "The configured GraphTransaction scope cannot preserve "
                "restored Spec lineage during session cleanup.",
            )
        if preserved_projection_edges and not _accepts_preserved_projection_edges(
            delete_preserving
        ):
            raise ProjectionActiveSetReconciliationError(
                "projection_active_set_compensation_capability_unavailable",
                "The configured GraphTransaction scope cannot be told which restored "
                "relational projection edges to keep during session cleanup.",
            )
        return delete_preserving

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

        projection_receipts = [
            record.projection_receipt
            for record in reversed(self.records)
            if record.projection_receipt is not None
        ]
        # Every before-image contributes, because reverse compensation restores all of them
        # and any of those edges may be the state the session started from.  What must NOT be
        # preserved is what this session itself wrote: an intermediate edge the session
        # created has no older before-image vouching for it, and keeping it would let the
        # session's own effect survive its compensation.  Union first, subtract second --
        # picking one receipt cannot express either half.
        restored_projection_edges: list[ProjectionEdgeBeforeImage] = []
        for receipt in projection_receipts:
            try:
                self.graph_scope.compensate_projection_active_set(receipt)
            except Exception as exc:
                logger.error(
                    "kg.compensate.projection_restore_failed "
                    "session=%s owner=%s:%s namespace=%s err=%s",
                    self.session_id,
                    receipt.intent.owner_type,
                    receipt.intent.owner_id,
                    receipt.intent.namespace,
                    exc,
                )
                raise CompensationError(
                    "relational projection active-set compensation failed",
                    original_exc=exc,
                    failed_records=[
                        record
                        for record in self.records
                        if record.projection_receipt is receipt
                    ],
                ) from exc
            # Collected only once the restore for this receipt has actually succeeded: an
            # edge that was never put back is not one the sweep has to be told to keep.
            restored_projection_edges.extend(receipt.edge_before_images)
            for node_before_image in receipt.before_images:
                restored_projection_edges.extend(node_before_image.incident_edges)

        preserved_projection_edges = self._net_preserved_projection_edges(
            restored_projection_edges
        )

        lineage_records = [
            record
            for record in reversed(self.records)
            if record.lineage_receipt is not None
        ]
        preserved_edges: list[SpecLineageEdgeSnapshot] = []
        for record in lineage_records:
            receipt = record.lineage_receipt
            assert receipt is not None
            if not (
                record.lineage_compensation_applied
                or record.lineage_progress_preserved
            ):
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
                            candidate
                            for candidate in self.records
                            if candidate.lineage_receipt is receipt
                        ],
                    ) from exc
            preserved_edges.extend(receipt.removed_edges)
            if (
                record.lineage_progress_preserved
                and receipt.target_id is not None
                and receipt.target_rule_id is not None
            ):
                preserved_edges.append(
                    SpecLineageEdgeSnapshot(
                        source_id=receipt.source_id,
                        target_id=receipt.target_id,
                        rule_id=receipt.target_rule_id,
                        attrs=dict(receipt.target_attrs),
                    )
                )

        property_records = [
            record
            for record in reversed(self.records)
            if record.property_before_image is not None
        ]
        for record in property_records:
            try:
                self.graph_scope.restore_node_properties(
                    record.property_before_image
                )
            except Exception as exc:
                logger.error(
                    "kg.compensate.property_restore_failed "
                    "session=%s type=%s node=%s err=%s",
                    self.session_id,
                    record.entity_type,
                    record.entity_id,
                    exc,
                )
                raise CompensationError(
                    "graph property before-image compensation failed",
                    original_exc=exc,
                    failed_records=[record],
                ) from exc

        try:
            if preserved_edges or preserved_projection_edges:
                delete_preserving = self._resolve_preserving_cleanup(
                    preserved_edges,
                    preserved_projection_edges,
                )
                if preserved_projection_edges:
                    delete_preserving(
                        self.session_id,
                        tuple(preserved_edges),
                        preserved_projection_edges=tuple(preserved_projection_edges),
                    )
                else:
                    # Nothing to preserve beyond lineage: make exactly the call this code
                    # made before the argument existed, so an older implementation still
                    # sees the signature it was written against.
                    delete_preserving(self.session_id, tuple(preserved_edges))
            else:
                self.graph_scope.delete_edges_by_session(self.session_id)
        except Exception as exc:
            logger.error(
                "kg.compensate.edge_delete_failed session=%s err=%s",
                self.session_id,
                exc,
            )
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
