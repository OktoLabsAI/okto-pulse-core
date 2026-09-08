"""Provenance preparation indexes only local edges, not mutable graph authority."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import primitives
from okto_pulse.core.kg.schemas import KGEdgeType, KGNodeType, NodeCandidate


class CountedEdges(dict):
    visits = 0

    def values(self):
        for value in super().values():
            self.visits += 1
            yield value


def candidate(identity, ref=None):
    return NodeCandidate(candidate_id=identity, node_type=KGNodeType.ALTERNATIVE,
                         title=identity, source_artifact_ref=ref or f"spec:{identity}")


def edge(source, kind="belongs_to"):
    return SimpleNamespace(from_candidate_id=source, edge_type=kind)


def test_auto_provenance_visits_edge_batch_once_and_keeps_every_root_lookup(monkeypatch):
    count = 250
    nodes = {f"a{i}": candidate(f"a{i}") for i in range(count)}
    edges = CountedEdges({f"e{i}": edge(f"a{i}", "relates_to") for i in range(count)})
    roots = []

    def resolve(scope, ref):
        roots.append(ref)
        return "entity-root", "Entity"

    monkeypatch.setattr(primitives, "_resolve_provenance_root", resolve)
    monkeypatch.setattr(primitives, "_lookup_existing_node", lambda *_: None)
    primitives._auto_attach_provenance_edges(
        graph_scope=object(), node_candidates=nodes, edge_candidates=edges)
    assert edges.visits == count
    assert roots == [n.source_artifact_ref for n in nodes.values()]
    assert len(edges) == count * 2
    assert all(edges[f"a{i}__auto_belongs_to_source_root"].to_candidate_id == "kg:entity-root"
               for i in range(count))

    # The next phase must see edges just created above and avoid inheritance.
    monkeypatch.setattr(primitives, "_resolve_op", lambda *_: pytest.fail("already attached"))
    primitives._inherit_supersede_provenance_edges(
        graph_scope=object(), board_id="board", node_candidates=nodes,
        edge_candidates=edges, effective_hints={}, explicit_override_candidate_ids=frozenset())
    assert edges.visits == count * 3  # 250 first phase + 500 second phase.


@pytest.mark.parametrize("root", [None, ("root", "Entity"), ("root", "Bug")])
def test_auto_provenance_matches_repeated_scan_oracle(monkeypatch, root):
    nodes = {name: candidate(name) for name in ("existing", "first", "second")}
    initial = {"manual": edge("existing", KGEdgeType.BELONGS_TO),
               "unrelated": edge("first", "relates_to")}
    monkeypatch.setattr(primitives, "_resolve_provenance_root", lambda *_: root)
    monkeypatch.setattr(primitives, "_lookup_existing_node", lambda *_: None)
    optimized = dict(initial)
    primitives._auto_attach_provenance_edges(
        graph_scope=object(), node_candidates=nodes, edge_candidates=optimized)

    class RepeatedScan:
        def __init__(self, edges, kind):
            self.edges, self.kind = edges, kind

        def __contains__(self, identity):
            return any(str(getattr(e, "from_candidate_id", "")) == identity
                       and primitives._enum_value(getattr(e, "edge_type", "")) == self.kind
                       for e in self.edges.values())

        def add(self, identity):
            pass  # The original scan sees mutations directly on the next call.

    monkeypatch.setattr(primitives, "_outgoing_edge_sources", RepeatedScan)
    baseline = dict(initial)
    primitives._auto_attach_provenance_edges(
        graph_scope=object(), node_candidates=nodes, edge_candidates=baseline)
    assert optimized == baseline


def test_root_refusal_after_first_candidate_is_not_cached_away(monkeypatch):
    calls = []

    def resolve(*_):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("root authority lost")
        return "root", "Entity"

    monkeypatch.setattr(primitives, "_resolve_provenance_root", resolve)
    monkeypatch.setattr(primitives, "_lookup_existing_node", lambda *_: None)
    nodes = {name: candidate(name, "spec:shared") for name in ("a", "b")}
    with pytest.raises(RuntimeError, match="root authority lost"):
        primitives._auto_attach_provenance_edges(
            graph_scope=object(), node_candidates=nodes, edge_candidates={})
    assert len(calls) == 2


def test_empty_batch_does_not_inspect_edges():
    class RefusedEdges(dict):
        def values(self):
            pytest.fail("empty node batch must not inspect edges")

    primitives._auto_attach_provenance_edges(
        graph_scope=object(), node_candidates={}, edge_candidates=RefusedEdges())
    primitives._inherit_supersede_provenance_edges(
        graph_scope=object(), board_id="board", node_candidates={},
        edge_candidates=RefusedEdges(), effective_hints={},
        explicit_override_candidate_ids=frozenset())


def test_inheritance_collision_withdraws_displaced_provenance_membership(monkeypatch):
    nodes = {name: candidate(name, "spec:shared") for name in ("first", "second")}
    edges = {"first__inherit_supersede_belongs_to": edge("second")}
    monkeypatch.setattr(primitives, "_resolve_op",
                        lambda *_: primitives.ReconciliationOperation.SUPERSEDE)
    monkeypatch.setattr(primitives, "_lookup_node_type_by_id", lambda *_: "Alternative")
    monkeypatch.setattr(primitives, "_node_is_human_curated", lambda *_: False)
    monkeypatch.setattr(primitives, "_lookup_node_source_ref_by_id", lambda *_: "spec:shared")
    monkeypatch.setattr(primitives, "_node_generation", lambda *_: 0)
    monkeypatch.setattr(primitives, "_lookup_existing_node_identity_by_id", lambda *_: None)
    monkeypatch.setattr(primitives, "_find_existing_connectivity_match",
                        lambda **_: ("root", "Entity", "outgoing", "canonical"))
    primitives._inherit_supersede_provenance_edges(
        graph_scope=object(), board_id="board", node_candidates=nodes,
        edge_candidates=edges,
        effective_hints={key: SimpleNamespace(target_node_id=f"old-{key}") for key in nodes},
        explicit_override_candidate_ids=frozenset())
    assert len(edges) == 2
    assert edges["first__inherit_supersede_belongs_to"].from_candidate_id == "first"
    assert edges["second__inherit_supersede_belongs_to"].from_candidate_id == "second"
