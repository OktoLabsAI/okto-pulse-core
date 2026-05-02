"""Compensating transaction pattern for Kùzu + SQLite commits.

Kùzu is an embedded graph DB without distributed transactions. When we commit
a consolidation session we need atomic semantics across two stores:

1. Kùzu writes (CREATE nodes + edges) happen first.
2. SQLite transaction (audit row + kuzu_node_refs + outbox event) happens second.
3. On any failure of step 2, we MUST reverse step 1 — this is the "compensate".

The orchestrator tracks every Kùzu write keyed by session_id so a failure can
delete exactly those nodes/edges via `source_session_id` / `created_by_session_id`
filters. Edges use a per-rel-type DELETE loop because Kùzu has no universal
`MATCH ()-[r]-() DELETE r` that works across rel tables.

Usage:

    orch = TransactionOrchestrator(kuzu_conn, sqlite_session, session_id,
                                   board_id=board_id)
    orch.create_node("Decision", node_id, attrs)
    orch.create_edge("supersedes", from_id, to_id, attrs)
    try:
        await orch.commit_sqlite(sqlite_mutations)
    except Exception:
        await orch.compensate()  # reverses Kùzu writes
        raise
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.schema import MULTI_REL_TYPES, NODE_TYPES, REL_TYPES

logger = logging.getLogger("okto_pulse.kg.transaction")


def _relationship_pairs() -> list[tuple[str, str, str]]:
    """Return every concrete relationship table/pair used by compensation."""
    pairs = list(REL_TYPES)
    for rel_name, endpoint_pairs in MULTI_REL_TYPES:
        pairs.extend((rel_name, from_type, to_type) for from_type, to_type in endpoint_pairs)
    return pairs


def _result_has_row(result: Any) -> bool:
    """Best-effort detection that a Cypher CREATE ... RETURN matched endpoints."""
    if result is None:
        return False
    try:
        if hasattr(result, "has_next"):
            return bool(result.has_next())
        if hasattr(result, "get_next"):
            try:
                result.get_next()
                return True
            except Exception:
                return False
        try:
            next(iter(result))
            return True
        except StopIteration:
            return False
        except TypeError:
            return False
    finally:
        close = getattr(result, "close", None)
        if callable(close):
            close()


def _close_result(result: Any) -> None:
    close = getattr(result, "close", None)
    if callable(close):
        close()


@dataclass
class KuzuWriteRecord:
    """One mutation applied to Kùzu — used for compensating delete."""

    kind: str  # "node" | "edge"
    entity_type: str  # "Decision", "supersedes", etc.
    entity_id: str  # For nodes: the id. For edges: synthetic session-scoped key.
    # Edge-only: anchors for MATCH DELETE pattern.
    from_id: str | None = None
    to_id: str | None = None


@dataclass
class CommitCounters:
    """Roll-up counts returned by commit for the audit row."""

    nodes_added: int = 0
    nodes_updated: int = 0
    nodes_superseded: int = 0
    edges_added: int = 0


class CompensationError(Exception):
    """Raised when compensating delete itself fails — operator intervention needed."""

    def __init__(self, message: str, original_exc: Exception | None = None,
                 failed_records: list[KuzuWriteRecord] | None = None):
        super().__init__(message)
        self.original_exc = original_exc
        self.failed_records = failed_records or []


@dataclass
class TransactionOrchestrator:
    """Coordinates Kùzu writes with SQLite commits using the compensating
    transaction pattern.

    The orchestrator is single-use: instantiate once per commit, call
    `create_node` / `create_edge` zero or more times, then `commit_sqlite`.
    On any SQLite failure, call `compensate` to reverse the Kùzu writes.
    """

    kuzu_conn: Any  # kuzu.Connection
    sqlite_session: AsyncSession
    session_id: str
    board_id: str
    records: list[KuzuWriteRecord] = field(default_factory=list)
    counters: CommitCounters = field(default_factory=CommitCounters)
    _committed: bool = False
    _compensated: bool = False

    # ------------------------------------------------------------------
    # Kùzu write phase
    # ------------------------------------------------------------------

    def create_node(self, node_type: str, node_id: str, attrs: dict[str, Any]) -> None:
        """Insert a new node into Kùzu and record it for compensation.

        `attrs` must NOT include `id`, `source_session_id`, or `board_id` —
        those are injected here so the caller can't forget them.
        """
        self._guard_fresh()
        params: dict[str, Any] = dict(attrs)
        params["id"] = node_id
        params["source_session_id"] = self.session_id

        columns = ", ".join(f"{k}: ${k}" for k in params)
        stmt = f"CREATE (n:{node_type} {{{columns}}})"
        self.kuzu_conn.execute(stmt, params)

        self.records.append(
            KuzuWriteRecord(kind="node", entity_type=node_type, entity_id=node_id)
        )
        self.counters.nodes_added += 1

    def update_node(self, node_type: str, node_id: str, attrs: dict[str, Any]) -> None:
        """Overwrite a node's mutable attrs. Does NOT get reverted by compensate
        — updates are already lossy and there's nothing to compensate to. This
        limitation is documented in the spec: compensating rollback only
        protects against partial ADD writes, not against UPDATE overwrites."""
        self._guard_fresh()
        set_clauses = ", ".join(f"n.{k} = ${k}" for k in attrs if k != "id")
        params = dict(attrs)
        params["id"] = node_id
        stmt = (
            f"MATCH (n:{node_type} {{id: $id}}) "
            f"SET {set_clauses}, n.updated_by_session_id = $session_id, "
            f"n.updated_at = timestamp($ts)"
        )
        params["session_id"] = self.session_id
        params["ts"] = attrs.get("ts") or _now_iso()
        self.kuzu_conn.execute(stmt, params)
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
        self.kuzu_conn.execute(
            f"MATCH (old:{node_type} {{id: $old_id}}) "
            f"SET old.superseded_by = $new_id, "
            f"old.superseded_at = timestamp($ts), "
            f"old.revocation_reason = $reason",
            {
                "old_id": superseded_node_id,
                "new_id": new_node_id,
                "ts": _now_iso(),
                "reason": revocation_reason or "",
            },
        )

        # 3. Create the :supersedes edge
        if node_type == "Decision":
            self.create_edge(
                "supersedes",
                new_node_id,
                superseded_node_id,
                attrs={"confidence": 1.0},
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

        For single-pair rels (REL_TYPES) the from/to node types are resolved
        automatically from the registry. For multi-pair rels (MULTI_REL_TYPES,
        e.g. ``belongs_to``) the caller MUST pass ``from_type``/``to_type``
        because the same rel name accepts many endpoint combinations and the
        Cypher MATCH needs the concrete node labels.
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

        # Resolve from/to types: REL_TYPES single-pair → auto; otherwise honour
        # the caller-supplied hints (multi-pair rels like `belongs_to`).
        rel_row = next((r for r in REL_TYPES if r[0] == edge_type), None)
        if rel_row is not None:
            _, resolved_from, resolved_to = rel_row
        else:
            from okto_pulse.core.kg.schema import MULTI_REL_TYPES
            multi = next((m for m in MULTI_REL_TYPES if m[0] == edge_type), None)
            if multi is None:
                raise ValueError(f"unknown edge_type: {edge_type}")
            if not from_type or not to_type:
                raise ValueError(
                    f"multi-pair edge_type '{edge_type}' requires explicit "
                    f"from_type/to_type hints; got {from_type!r}/{to_type!r}"
                )
            valid_pairs = multi[1]
            if (from_type, to_type) not in valid_pairs:
                raise ValueError(
                    f"edge_type '{edge_type}' does not accept pair "
                    f"({from_type}, {to_type}); valid pairs: {valid_pairs}"
                )
            resolved_from, resolved_to = from_type, to_type
        from_type, to_type = resolved_from, resolved_to

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

        attr_cols = ", ".join(f"{k}: ${k}" for k in edge_attrs)
        stmt = (
            f"MATCH (a:{from_type} {{id: $from_id}}), "
            f"(b:{to_type} {{id: $to_id}}) "
            f"CREATE (a)-[r:{edge_type} {{{attr_cols}}}]->(b) "
            "RETURN r.created_by_session_id"
        )
        params = dict(edge_attrs)
        params["from_id"] = from_id
        params["to_id"] = to_id

        # created_at is a TIMESTAMP column — wrap with timestamp() function.
        # Replace the literal param binding with the function call.
        stmt = stmt.replace("created_at: $created_at",
                            "created_at: timestamp($created_at)")

        result = self.kuzu_conn.execute(stmt, params)
        if not _result_has_row(result):
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
            KuzuWriteRecord(
                kind="edge",
                entity_type=edge_type,
                entity_id=f"{from_id}->{to_id}",
                from_id=from_id,
                to_id=to_id,
            )
        )
        self.counters.edges_added += 1

    def _edge_exists(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
    ) -> bool:
        """Return True when this semantic edge already exists.

        Kùzu does not enforce a unique constraint for relationship tables, so
        repeated artifact consolidation can otherwise materialize the same
        edge many times. The KG treats `(edge_type, from_id, to_id)` as the
        natural identity for all current relationships.
        """
        stmt = (
            f"MATCH (a:{from_type} {{id: $from_id}})-[r:{edge_type}]->"
            f"(b:{to_type} {{id: $to_id}}) "
            "RETURN r.created_by_session_id LIMIT 1"
        )
        result = self.kuzu_conn.execute(stmt, {
            "from_id": from_id,
            "to_id": to_id,
        })
        try:
            if hasattr(result, "has_next"):
                return bool(result.has_next())
            try:
                next(iter(result))
                return True
            except StopIteration:
                return False
            except TypeError:
                return False
        finally:
            _close_result(result)

    def _find_node_types(self, node_id: str) -> list[str]:
        """Best-effort lookup used only to make failed edge errors actionable."""
        found: list[str] = []
        for node_type in NODE_TYPES:
            try:
                result = self.kuzu_conn.execute(
                    f"MATCH (n:{node_type} {{id: $node_id}}) "
                    "RETURN n.id LIMIT 1",
                    {"node_id": node_id},
                )
                try:
                    if hasattr(result, "has_next") and result.has_next():
                        found.append(node_type)
                finally:
                    _close_result(result)
            except Exception:
                continue
        return found

    # ------------------------------------------------------------------
    # SQLite commit phase
    # ------------------------------------------------------------------

    async def commit_sqlite(
        self,
        mutations: Callable[[AsyncSession], Any],
    ) -> CommitCounters:
        """Apply the SQLite-side mutations inside a transaction and commit.

        `mutations` is a callable that takes the AsyncSession and stages all
        ORM objects (audit row, kuzu_node_refs, outbox event, etc.). The
        orchestrator owns flush/commit semantics and triggers compensating
        delete on failure.
        """
        self._guard_fresh()
        try:
            result = mutations(self.sqlite_session)
            if hasattr(result, "__await__"):
                await result
            await self.sqlite_session.commit()
            self._committed = True
            logger.info(
                "kg.transaction.commit session=%s nodes=%d updated=%d "
                "superseded=%d edges=%d",
                self.session_id,
                self.counters.nodes_added,
                self.counters.nodes_updated,
                self.counters.nodes_superseded,
                self.counters.edges_added,
            )
            return self.counters
        except Exception as exc:
            logger.warning(
                "kg.transaction.sqlite_commit_failed session=%s err=%s — "
                "triggering compensating delete",
                self.session_id,
                exc,
            )
            await self.sqlite_session.rollback()
            await self.compensate()
            raise

    async def compensate(self) -> None:
        """Reverse every Kùzu write recorded so far.

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

        failed: list[KuzuWriteRecord] = []

        # 1. Delete edges created by this session (any rel type)
        for rel_name, from_type, to_type in _relationship_pairs():
            try:
                self.kuzu_conn.execute(
                    f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
                    f"WHERE r.created_by_session_id = $sid DELETE r",
                    {"sid": self.session_id},
                )
            except Exception as exc:
                logger.error(
                    "kg.compensate.edge_delete_failed rel=%s session=%s err=%s",
                    rel_name, self.session_id, exc,
                )
                # Continue deleting other rel types — partial compensation is
                # better than none.

        # 2. Delete nodes created by this session (group by type for efficiency)
        node_types = {
            r.entity_type for r in self.records if r.kind == "node"
        }
        for node_type in node_types:
            try:
                self.kuzu_conn.execute(
                    f"MATCH (n:{node_type}) "
                    f"WHERE n.source_session_id = $sid DETACH DELETE n",
                    {"sid": self.session_id},
                )
            except Exception as exc:
                logger.error(
                    "kg.compensate.node_delete_failed type=%s session=%s err=%s",
                    node_type, self.session_id, exc,
                )
                failed.extend(
                    r for r in self.records
                    if r.kind == "node" and r.entity_type == node_type
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
        if self._committed:
            raise RuntimeError("orchestrator already committed")
        if self._compensated:
            raise RuntimeError("orchestrator already compensated")


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
