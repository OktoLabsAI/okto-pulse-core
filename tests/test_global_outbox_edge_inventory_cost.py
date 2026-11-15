"""Global delivery counts one edge inventory without losing integrity predicates."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor


BOARD = "board-alpha"


def _edge(oid, count=1, **changes):
    return dict(source_board_id=BOARD, digest_board_id=BOARD,
                digest_id=f"dd_{BOARD[:8]}_{oid}", original_node_id=oid,
                edge_count=count) | changes


def _former_count(rows, digest_id, oid):
    return sum(int(row["edge_count"]) for row in rows
               if row["source_board_id"] == BOARD and row["digest_board_id"] == BOARD
               and row["digest_id"] == digest_id and row["original_node_id"] == oid)


class CountingRows(list):
    def __init__(self, rows):
        super().__init__(rows)
        self.visits = 0

    def __iter__(self):
        for row in super().__iter__():
            self.visits += 1
            yield row


def test_counts_equal_former_predicate_with_duplicate_and_foreign_identities():
    rows = [_edge("a", 2), _edge("a", 3), _edge("b", 0),
            _edge("a", 41, source_board_id="foreign"),
            _edge("a", 42, digest_board_id="foreign"),
            _edge("a", 7, digest_id="wrong"), _edge("b", 4),
            _edge("a", 8, original_node_id="other"),
            _edge("a", 9, digest_id=1)]
    before = [dict(row) for row in rows]
    counts = GlobalOutboxProcessor._correct_board_digest_edge_counts(rows, board_id=BOARD)
    for oid in ("a", "b", "other", "absent"):
        for digest in (f"dd_{BOARD[:8]}_a", f"dd_{BOARD[:8]}_b", "wrong", "1", 1):
            assert counts.get((digest, oid), 0) == _former_count(rows, digest, oid)
    assert rows == before
    assert counts[(f"dd_{BOARD[:8]}_a", "a")] == 5  # Not a set/deduplication.


def _fixture(monkeypatch, size):
    processor = GlobalOutboxProcessor(lambda: None)
    sources = {f"source-{i}": "Alternative" for i in range(size)}
    rows = [_edge(oid) for oid in sources]
    digests = [dict(digest_id=r["digest_id"], original_node_id=r["original_node_id"],
                    node_type="Alternative", current_layer="canonical", source_revoked=False)
               for r in rows]
    observed = []

    def read_edges(_runtime, _board):
        batch = CountingRows(dict(row) for row in rows)
        observed.append(batch)
        return batch

    monkeypatch.setattr(processor, "_read_global_digest_rows", lambda *_: digests)
    monkeypatch.setattr(processor, "_read_board_digest_edge_rows", read_edges)
    monkeypatch.setattr(processor, "_read_digest_inbound_edge_counts",
                        lambda *_: {r["digest_id"]: int(r["edge_count"]) for r in rows})
    return processor, sources, rows, observed


@pytest.mark.parametrize("size", [256, 1024, 1025])
@pytest.mark.asyncio
async def test_reconcile_and_fresh_verification_traverse_edge_rows_linearly(monkeypatch, size):
    processor, sources, _rows, observed = _fixture(monkeypatch, size)

    async def inline(operation):
        return operation()

    monkeypatch.setattr(processor, "_run_graph_io", inline)
    monkeypatch.setattr(processor, "_read_board_layer_meta",
                        lambda *_: {oid: {"node_type": kind, "graph_layer": "canonical"}
                                    for oid, kind in sources.items()})
    monkeypatch.setattr(processor, "_assert_source_inventory_unchanged", lambda *_: None)
    monkeypatch.setattr(processor, "_now", lambda: datetime(2026, 9, 8, tzinfo=timezone.utc))
    runtime = SimpleNamespace(delete_invalid_board_digest_links=lambda **_: 0)
    expected = {}
    assert await processor._reconcile_board_digest_layers(
        runtime, BOARD, None, source_types_by_id=sources, expected_state=expected,
    ) == 0
    assert expected == {oid: ("canonical", kind) for oid, kind in sources.items()}
    assert len(observed) == 2  # Fresh verification reads again, never trusts the first map.
    assert [batch.visits for batch in observed] == [size, 2 * size]


@pytest.mark.parametrize("change,reason", [
    ({"edge_count": 2}, "correct_contains_edges=2"),
    ({"edge_count": 0}, "correct_contains_edges=0"),
    ({"source_board_id": "foreign"}, "invalid_contains_edge"),
    ({"digest_board_id": "foreign"}, "invalid_contains_edge"),
    ({"original_node_id": "wrong"}, "invalid_contains_edge"),
    ({"digest_id": "wrong"}, "invalid_contains_edge"),
])
def test_reverification_observes_changed_multiplicity_and_identity(monkeypatch, change, reason):
    processor, sources, rows, _observed = _fixture(monkeypatch, 2)
    expected = {oid: ("canonical", kind) for oid, kind in sources.items()}
    processor._verify_reconciled_digest_layers(None, BOARD, expected)
    rows[0].update(change)
    with pytest.raises(RuntimeError, match=reason):
        processor._verify_reconciled_digest_layers(None, BOARD, expected)


def test_inbound_links_from_another_board_still_refuse_ack(monkeypatch):
    processor, sources, rows, _observed = _fixture(monkeypatch, 1)
    monkeypatch.setattr(processor, "_read_digest_inbound_edge_counts",
                        lambda *_: {rows[0]["digest_id"]: 2})
    with pytest.raises(RuntimeError, match="total_inbound_contains_edges=2"):
        processor._verify_reconciled_digest_layers(
            None, BOARD, {oid: ("canonical", kind) for oid, kind in sources.items()},
        )


def _read_inbound(rows, identities):
    return GlobalOutboxProcessor._read_digest_inbound_edge_counts(
        SimpleNamespace(execute=lambda *_: SimpleNamespace(rows=rows)), BOARD, identities,
    )


@pytest.mark.parametrize("size", [0, 1, 1024, 1025, 2500])
def test_inbound_inventory_keeps_every_identity_and_multiplicity(size):
    identities = tuple(f"digest-{i}" for i in range(size))
    # Independent fixture oracle includes parallel and cross-owner edges. The
    # reader receives grouped scalar rows, not a collection of digest identities.
    edges = [(BOARD, identity) for identity in identities[1:]]
    if size > 1:
        edges += [(BOARD, identities[1]), ("foreign-board", identities[1])]
    expected = {identity: sum(target == identity for _, target in edges)
                for identity in identities}
    rows = tuple(reversed(tuple(expected.items())))
    assert _read_inbound(rows, identities) == {key: value for key, value in expected.items() if value}


@pytest.mark.parametrize("rows,reason", [
    ([], "population_changed"),
    ([("other", 1)], "population_changed"),
    ([("digest", 1), ("other", 0)], "population_changed"),
    ([("digest", 1), ("digest", 2)], "duplicate_row"),
    ([("digest",)], "malformed_row"),
    ([("digest", 1, 2)], "malformed_row"),
    ([(None, 1)], "malformed_row"),
    ([("", 1)], "malformed_row"),
    ([(1, 1)], "malformed_row"),
    ([("digest", None)], "malformed_row"),
    ([("digest", "1")], "malformed_row"),
    ([("digest", 1.0)], "malformed_row"),
    ([("digest", True)], "malformed_row"),
    ([("digest", -1)], "malformed_row"),
])
def test_inbound_inventory_refuses_partial_or_malformed_observations(rows, reason):
    with pytest.raises(RuntimeError, match=reason):
        _read_inbound(rows, ("digest",))


@pytest.mark.parametrize("identities,reason", [
    (("digest", "digest"), "duplicate_identity"),
    (("",), "invalid_identity"),
    ((None,), "invalid_identity"),
    ((1,), "invalid_identity"),
])
def test_inbound_inventory_refuses_invalid_source_population(identities, reason):
    with pytest.raises(RuntimeError, match=reason):
        _read_inbound([], identities)


def test_inbound_inventory_never_reuses_a_previous_observation():
    observations = iter([[('digest', 1)], [('digest', 3)], [('digest', 0)]])
    runtime = SimpleNamespace(execute=lambda *_: SimpleNamespace(rows=next(observations)))
    read = GlobalOutboxProcessor._read_digest_inbound_edge_counts
    assert read(runtime, BOARD, ('digest',)) == {'digest': 1}
    assert read(runtime, BOARD, ('digest',)) == {'digest': 3}
    assert read(runtime, BOARD, ('digest',)) == {}


def test_inbound_inventory_refuses_silent_truncation_at_native_collection_boundary():
    identities = tuple(f'digest-{i}' for i in range(1025))
    with pytest.raises(RuntimeError, match='population_changed'):
        _read_inbound([(identity, 1) for identity in identities[:1024]], identities)


def test_inbound_inventory_propagates_late_iteration_failure():
    def rows():
        yield ('digest', 1)
        raise RuntimeError('native read interrupted')

    with pytest.raises(RuntimeError, match='native read interrupted'):
        _read_inbound(rows(), ('digest',))


@pytest.mark.parametrize('change,reason', [
    ({'node_type': 'Learning'}, 'node_type=Learning'),
    ({'current_layer': 'working'}, 'layer=working'),
    ({'source_revoked': True}, 'unexpected_source_revoked'),
    ({'digest_id': 'wrong'}, 'unstable_digest_id'),
    ({'original_node_id': ''}, 'missing_original_node_id_rows=1'),
    ({'original_node_id': 'foreign'}, 'unexpected_global_identity'),
])
def test_inbound_optimization_preserves_full_post_effect_identity_checks(monkeypatch, change, reason):
    processor, sources, _, _ = _fixture(monkeypatch, 1)
    processor._read_global_digest_rows(None, BOARD)[0].update(change)
    with pytest.raises(RuntimeError, match=reason):
        processor._verify_reconciled_digest_layers(
            None, BOARD, {oid: ('canonical', kind) for oid, kind in sources.items()},
        )


def test_revoked_digest_without_inbound_edges_is_allowed_but_a_new_live_identity_is_not(monkeypatch):
    processor, sources, _, _ = _fixture(monkeypatch, 1)
    retired = dict(digest_id='retired', original_node_id='retired-source',
                   node_type='Alternative', current_layer='canonical', source_revoked=True)
    processor._read_global_digest_rows(None, BOARD).append(retired)
    expected = {oid: ('canonical', kind) for oid, kind in sources.items()}
    processor._verify_reconciled_digest_layers(None, BOARD, expected)
    retired['source_revoked'] = False
    with pytest.raises(RuntimeError, match='unexpected_global_identity'):
        processor._verify_reconciled_digest_layers(None, BOARD, expected)
