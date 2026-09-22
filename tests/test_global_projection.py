"""Complete source identity and existing publication policy share one seed path."""

from dataclasses import replace

import pytest

from okto_pulse.core.kg.canonical_learning_partition import HISTORICAL_DEBT_REASON
from okto_pulse.core.ports.global_discovery_recovery_control import GlobalDiscoveryRecoveryBoardSeedInput
from okto_pulse.core.ports.global_projection import GlobalProjectionSource, build_global_projection_seed


SOURCE = GlobalProjectionSource('a', 'Learning', 'Sealed learning', (0.0, -0.0),
    'bug:ticket', 'canonical', canonical_bug_count=1)
INPUT = GlobalDiscoveryRecoveryBoardSeedInput('board', 'Board', 'Summary', ())


def build(sources=(SOURCE,), expected=(('a', 'Learning'),), source_input=INPUT):
    return build_global_projection_seed(source_input=source_input, expected_sources=expected,
        sources=sources, summary_embedding=(0.25, 0.75))


def test_seed_keeps_the_existing_content_shape_and_publication_rule():
    seed = build()
    assert (seed.board_id, seed.board_name, seed.summary, seed.summary_embedding) == ('board', 'Board', 'Summary', (0.25, 0.75))
    digest, = seed.digests
    assert (digest.original_node_id, digest.title, digest.summary, digest.node_type,
        digest.graph_layer, digest.source_artifact_ref) == ('a', 'Sealed learning', 'Sealed learning', 'Learning', 'canonical', 'bug:ticket')
    assert tuple(value.hex() for value in digest.embedding) == ('0x0.0p+0', '-0x0.0p+0')
    assert build((replace(SOURCE, title='x' * 300),)).digests[0].summary == 'x' * 280


def test_captured_debt_and_missing_canonical_evidence_demote_publication_only():
    held = replace(INPUT, overlay_exclusions=(('bug:ticket', HISTORICAL_DEBT_REASON),))
    assert build(source_input=held).digests[0].graph_layer == 'working'
    assert build((replace(SOURCE, canonical_bug_count=0),)).digests[0].graph_layer == 'working'
    assert SOURCE.graph_layer == 'canonical'
    entity = replace(SOURCE, node_type='Entity')
    assert build((entity,), (('a', 'Entity'),), held).digests[0].graph_layer == 'canonical'


def test_source_order_does_not_change_content_or_fingerprint():
    second = replace(SOURCE, node_id='b', node_type='Entity')
    assert build((SOURCE, second), (('a', 'Learning'), ('b', 'Entity'))) == build(
        (second, SOURCE), (('b', 'Entity'), ('a', 'Learning')))


@pytest.mark.parametrize('sources,expected', [
    ((SOURCE,), (('a', 'Learning'), ('b', 'Entity'))),
    ((), (('a', 'Learning'),)),
    ((SOURCE, SOURCE), (('a', 'Learning'),)),
    ((SOURCE,), (('a', 'Learning'), ('a', 'Entity'))),
    ((SOURCE,), (('a', 'Entity'),)),
    ((replace(SOURCE, node_id='foreign'),), (('a', 'Learning'),)),
], ids=['missing_row', 'empty_read', 'duplicate_read', 'ambiguous_id', 'wrong_type', 'unexpected_id'])
def test_partial_duplicate_or_ambiguous_source_inventory_is_not_a_seed(sources, expected):
    with pytest.raises(ValueError, match='global_projection_source_'):
        build(sources, expected)


@pytest.mark.parametrize('values', [(), (float('nan'),), (float('inf'),), (True,), (1,)])
def test_vector_representation_is_not_coerced(values):
    with pytest.raises(ValueError, match='vector_invalid'):
        build((replace(SOURCE, embedding=values),))


def test_empty_complete_inventory_retains_the_existing_zero_seed_fingerprint():
    seed = build((), ())
    assert seed.digests == ()
    assert seed.source_inventory_hash == '7aac42da6b62b1e98e7ab7bae9b96876628169d31d3efbc693f4d3dcf4a8d145'


@pytest.mark.asyncio
async def test_live_recovery_path_refuses_omitted_source_after_shared_materialization(monkeypatch):
    from types import SimpleNamespace
    from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor

    class Processor(GlobalOutboxProcessor):
        async def _run_graph_io(self, action):
            return action()

        @staticmethod
        def _read_board_digestable_node_types(_board):
            return {'a': 'Learning', 'b': 'Entity'}

        @staticmethod
        def _read_board_nodes_for_refs(_board, _refs):
            return [{'id': 'a', 'title': SOURCE.title, 'embedding': SOURCE.embedding}]

        @staticmethod
        def _read_board_layer_meta(_board, sources):
            return {key: {'node_type': kind, 'graph_layer': 'canonical', 'source_artifact_ref': 'bug:ticket',
                'canonical_bug_count': 1} for key, kind in sources.items()}

    monkeypatch.setattr('okto_pulse.core.kg.embedding.get_embedding_provider',
        lambda: SimpleNamespace(encode=lambda _text: [0.25, 0.75]))
    with pytest.raises(ValueError, match='source_inventory_incomplete'):
        await Processor().build_recovery_board_seed(board_id='board', board_name='Board', board_summary='Summary',
            captured_overlay_exclusions={})
