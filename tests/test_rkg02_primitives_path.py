"""RKG-02 — REAL commit-path coverage (validation finding, codex 2026-06-25).

The first guard integration test injected an artificial ``source_artifact_ref``
on the existing Bug ref. The REAL path in ``primitives`` builds existing-endpoint
refs with source_artifact_ref=None unless it is loaded from the graph. These
tests use the actual primitives helpers against a fake Kùzu connection so the
Bug's source_ref is *loaded* (not injected), proving the fix closes the bug for
existing endpoints — and that the bug-derived decision no longer uses a divergent
local parser (BR3).
"""

from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.kg.connectivity_guard import (
    KGNodeConnectivityGuard,
    KGNodeRef,
)
from okto_pulse.core.kg.primitives import (
    _candidate_has_known_bug_source,
    _existing_refs_for_edge_endpoints,
    _graph_canonical_bug_probe,
    _lookup_node_source_ref_by_id,
)

U = "11111111-1111-1111-1111-111111111111"


class _FakeRes:
    def __init__(self, rows):
        self._rows = list(rows)
        self._i = 0

    def has_next(self):
        return self._i < len(self._rows)

    def get_next(self):
        row = self._rows[self._i]
        self._i += 1
        return row

    def close(self):
        pass


class _FakeKConn:
    """A graph with one canonical Bug ``bug-node`` whose source_artifact_ref is
    ``bug:<U>`` (None simulates the pre-fix unloaded format)."""

    def __init__(self, *, bug_id="bug-node", source_ref=None, layer="canonical"):
        self.bug_id = bug_id
        self.source_ref = source_ref
        self.layer = layer

    def execute(self, cypher, params=None):
        params = params or {}
        if "RETURN b.id, b.source_artifact_ref" in cypher:
            return _FakeRes([[self.bug_id, self.source_ref]])
        if "(n:Bug)" in cypher and "RETURN n.id" in cypher:
            return _FakeRes([[self.bug_id]] if params.get("id") == self.bug_id else [])
        if "RETURN n.id LIMIT 1" in cypher:  # type probe for a non-Bug label
            return _FakeRes([])
        if "RETURN n.graph_layer" in cypher:
            return _FakeRes([[self.layer]])
        if "RETURN n.source_artifact_ref" in cypher:
            return _FakeRes([[self.source_ref]])
        return _FakeRes([])


def _node(candidate_id, node_type, source_ref=""):
    return SimpleNamespace(
        candidate_id=candidate_id, node_type=node_type, source_artifact_ref=source_ref)


def _edge(candidate_id, edge_type, source, target):
    return SimpleNamespace(
        candidate_id=candidate_id, edge_type=edge_type,
        from_candidate_id=source, to_candidate_id=target)


# ---------------------------------------------------------------------------
# 1. primitives LOADS the Bug's real source_artifact_ref into endpoint refs
# ---------------------------------------------------------------------------


def test_existing_endpoint_refs_now_carry_loaded_source_artifact_ref():
    fake = _FakeKConn(source_ref=f"bug:{U}")
    refs = _existing_refs_for_edge_endpoints(
        kconn=fake,
        edge_candidates={"e1": _edge("e1", "validates", "learn-1", "kg:bug-node")},
        node_candidates={"learn-1": _node("learn-1", "Learning", f"card:{U}")},
        candidate_to_existing_id={},
    )
    bug_refs = [r for r in refs if r.node_type == "Bug"]
    assert bug_refs, "edge endpoint should resolve to the existing Bug"
    # THE FIX: source_artifact_ref is loaded from the graph (was None before).
    assert all(r.source_artifact_ref == f"bug:{U}" for r in bug_refs)


def test_lookup_node_source_ref_by_id_reads_graph():
    fake = _FakeKConn(source_ref=f"bug:{U}")
    assert _lookup_node_source_ref_by_id(fake, "Bug", "bug-node") == f"bug:{U}"


def test_graph_canonical_bug_probe_matches_card_uuid():
    probe = _graph_canonical_bug_probe(_FakeKConn(source_ref=f"bug:{U}"))
    assert probe(U) is True
    assert probe("99999999-9999-9999-9999-999999999999") is False


# ---------------------------------------------------------------------------
# 2. guard boundary: with the LOADED source_ref the card:<uuid> Learning passes;
#    without it (the real pre-fix format) it fails. This is the real-format
#    fail-before / pass-after codex asked for.
# ---------------------------------------------------------------------------


def _guard_validate(existing_refs):
    guard = KGNodeConnectivityGuard()
    return guard.validate(
        board_id="b1", writer_path="commit_consolidation", kg_health_state="healthy",
        nodes=[_node("learn-1", "Learning", f"card:{U}")],
        edges=[_edge("e1", "validates", "learn-1", "kg:bug-node")],
        existing_node_refs=existing_refs,
    )


def test_real_format_fails_without_loaded_source_ref():
    # Pre-fix: primitives left source_artifact_ref=None on the Bug endpoint.
    refs = [KGNodeRef(ref_id="kg:bug-node", node_type="Bug",
                      graph_layer="canonical", source_artifact_ref=None)]
    assert _guard_validate(refs).passed is False


def test_real_format_passes_with_loaded_source_ref():
    # Post-fix: primitives loads source_artifact_ref=bug:<U> (the value the
    # _existing_refs_for_edge_endpoints test above proves it now produces).
    refs = [KGNodeRef(ref_id="kg:bug-node", node_type="Bug",
                      graph_layer="canonical", source_artifact_ref=f"bug:{U}")]
    assert _guard_validate(refs).passed is True


# ---------------------------------------------------------------------------
# 3. the divergent local parser is gone — bug-derived detection is type-aware
#    via the shared resolver (BR3).
# ---------------------------------------------------------------------------


def test_candidate_bug_source_is_type_aware_via_resolver():
    cand = _node("learn-1", "Learning", f"card:{U}")
    # card:<uuid> with NO probe / non-bug card → NOT bug-derived (no blind alias).
    assert _candidate_has_known_bug_source(cand, lambda uuid: False) is False
    # card:<uuid> confirmed canonical bug by the probe → bug-derived.
    assert _candidate_has_known_bug_source(cand, lambda uuid: uuid == U) is True
    # explicit bug form stays bug-derived without a probe.
    assert _candidate_has_known_bug_source(_node("l2", "Learning", f"bug:{U}")) is True
