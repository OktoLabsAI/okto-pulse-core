"""Spec eca49df9 (Cognitive Dedup Granularity, SUPERSEDE Wiring & Counted
Merge) — IMPL-2 tests: op==SUPERSEDE wiring via TransactionOrchestrator.

AC5: a SUPERSEDE candidate creates the new node, marks the old node
superseded_by + increments nodes_superseded, and creates the :supersedes
edge ONLY for node types the schema supports (Decision). For cognitive types
(Alternative/Assumption) the same path runs WITHOUT the edge and never falls
through to a lone CREATE. Also exercises the created_at -> timestamp() fix on
create_node (TR6) that makes supersede_node usable.

Uses an injected fake Kùzu connection (store) — hermetic, no real graph.
"""

from __future__ import annotations

from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphNodePropertyBeforeImage,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator


class _FakeResult:
    def __init__(self, has_row: bool):
        self.has_row = has_row
        self.closed = False

    def has_next(self) -> bool:
        return self.has_row

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, *results: _FakeResult):
        self.results = list(results)
        self.statements: list[tuple[str, dict]] = []

    def execute(self, statement: str, params: dict):
        self.statements.append((statement, params))
        if self.results:
            return self.results.pop(0)
        return _FakeResult(has_row=False)

    def create_node(self, node_type, node_id, attrs, *, source_session_id):
        self.statements.append(("create_node", {
            "node_type": node_type,
            "node_id": node_id,
            "attrs": attrs,
            "source_session_id": source_session_id,
        }))

    def mark_superseded(self, node_type, node_id, **values):
        self.statements.append(("mark_superseded", {
            "node_type": node_type,
            "node_id": node_id,
            **values,
        }))

    def snapshot_node_properties(
        self,
        node_type,
        node_id,
        property_names,
    ):
        self.statements.append(("snapshot_node_properties", {
            "node_type": node_type,
            "node_id": node_id,
            "property_names": property_names,
        }))
        return GraphNodePropertyBeforeImage(
            node_type=node_type,
            node_id=node_id,
            attrs={name: "" for name in property_names},
        )

    def restore_node_properties(self, before_image):
        self.statements.append(("restore_node_properties", {
            "before_image": before_image,
        }))

    def edge_exists(self, *args):
        self.statements.append(("edge_exists", {"args": args}))
        result = self.results.pop(0) if self.results else _FakeResult(False)
        return result.has_next()

    def create_edge(self, *args):
        self.statements.append(("create_edge", {"args": args}))
        result = self.results.pop(0) if self.results else _FakeResult(True)
        return result.has_next()


def _orch(conn: _FakeConnection) -> TransactionOrchestrator:
    return TransactionOrchestrator(
        conn,
          # type: ignore[arg-type]
        session_id="sess-sup",
        board_id="board-sup",
    )


_NEW_ATTRS = {
    "title": "Nova decisao",
    "content": "corpo",
    "created_at": "2026-05-29T00:00:00",
}


def test_supersede_decision_creates_node_marks_old_and_makes_edge():
    # create_node CREATE, mark-old SET, edge exists-probe (False), edge create (True)
    conn = _FakeConnection(_FakeResult(False), _FakeResult(True))
    orch = _orch(conn)

    orch.supersede_node("Decision", "decision_new", "decision_old", dict(_NEW_ATTRS))

    operations = [name for name, _ in conn.statements]
    assert operations == [
        "create_node",
        "snapshot_node_properties",
        "mark_superseded",
        "edge_exists",
        "create_edge",
    ]
    # Counters: reclassified from added to superseded; edge counted.
    assert orch.counters.nodes_superseded == 1
    assert orch.counters.nodes_added == 0  # create_node +1 then reclassify -1
    assert orch.counters.edges_added == 1


def test_supersede_cognitive_type_creates_universal_supersedes_edge():
    # Spec MKG-D-S1 (FR4): the :supersedes edge is universal — cognitive
    # types now get the walkable trail too (was Decision-only; the previous
    # assertion of NO edge is deliberately inverted by MKG-D).
    conn = _FakeConnection(_FakeResult(False), _FakeResult(True))
    orch = _orch(conn)

    orch.supersede_node("Alternative", "alternative_new", "alternative_old", dict(_NEW_ATTRS))

    operations = [name for name, _ in conn.statements]
    assert operations == [
        "create_node",
        "snapshot_node_properties",
        "mark_superseded",
        "edge_exists",
        "create_edge",
    ]
    assert orch.counters.nodes_superseded == 1
    assert orch.counters.nodes_added == 0
    assert orch.counters.edges_added == 1
