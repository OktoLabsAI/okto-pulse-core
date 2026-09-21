"""The private migration seam prepares the same proposals as the live worker."""

from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.processors import consolidation as live
from okto_pulse.core.ports.consolidation import ConsolidationProjectionInputs
from okto_pulse.core.ports.deterministic_projection import (
    DeterministicProjectionSource, make_deterministic_projection_planner, require_board_projection_cleanup,
)


def spec(**values):
    return SimpleNamespace(**({'id': 'spec-one', 'board_id': 'board', 'title': 'Expected Spec',
        'description': 'Retain structured requirements.', 'context': '', 'status': 'done',
        'functional_requirements': [{'id': 'fr-one', 'title': 'Retain the source'}],
        'technical_requirements': [], 'acceptance_criteria': [], 'business_rules': [],
        'test_scenarios': [], 'api_contracts': [], 'decisions': [], 'architecture_designs': []} | values))


def persistence(artifact):
    return SimpleNamespace(load_artifact=AsyncMock(return_value=artifact),
        load_projection_inputs=AsyncMock(return_value=ConsolidationProjectionInputs()),
        list_artifacts=AsyncMock(return_value=()))


@pytest.mark.asyncio
async def test_preparation_matches_live_proposals_without_starting_a_graph_session(monkeypatch):
    port = persistence(spec())
    source = DeterministicProjectionSource('board', 'spec', 'spec-one')
    planner = make_deterministic_projection_planner(port)
    monkeypatch.setattr(live, 'get_consolidation_persistence_port', lambda: pytest.fail('planner used ambient provider'))
    plan = await planner.prepare(None, source)
    assert (await planner.prepare(None, source)).document == plan.document
    expected = json.loads(plan.document)['projection']
    assert {'spec:spec-one', 'spec:spec-one:fr:fr-one'} <= {
        node['source_artifact_ref'] for node in expected['nodes']}
    class Captured(Exception):
        pass
    async def begin(request, **kwargs):
        assert [node.model_dump(mode='json') for node in request.deterministic_candidates] == expected['nodes']
        assert request.raw_content == expected['raw_content']
        raise Captured
    monkeypatch.setattr(live, 'get_consolidation_persistence_port', lambda: port)
    monkeypatch.setattr(live, 'begin_consolidation', begin)
    with pytest.raises(Captured):
        await live._process_queue_entry(None, SimpleNamespace(
            board_id=source.board_id, artifact_type=source.artifact_type, artifact_id=source.artifact_id,
            work_kind='consolidate'))


@pytest.mark.asyncio
@pytest.mark.parametrize('artifact,reason', [(None, 'unavailable'), (spec(board_id='other'), 'scope_mismatch'),
    (spec(id='another-spec'), 'identity_mismatch')])
async def test_missing_or_foreign_source_does_not_become_an_empty_projection(artifact, reason):
    with pytest.raises(ValueError, match=reason):
        await make_deterministic_projection_planner(persistence(artifact)).prepare(None,
            DeterministicProjectionSource('board', 'spec', 'spec-one'))


@pytest.mark.asyncio
async def test_foreign_lineage_endpoint_fails_before_any_proposal():
    port = persistence(spec(refinement_id='foreign'))
    port.load_artifact.side_effect = [spec(refinement_id='foreign'), SimpleNamespace(board_id='other')]
    with pytest.raises(ValueError, match='scope_mismatch'):
        await make_deterministic_projection_planner(port).prepare(None,
            DeterministicProjectionSource('board', 'spec', 'spec-one'))


@pytest.mark.asyncio
async def test_cancelled_source_remains_explicitly_skipped():
    plan = await make_deterministic_projection_planner(persistence(spec(status='cancelled'))).prepare(None,
        DeterministicProjectionSource('board', 'spec', 'spec-one'))
    assert json.loads(plan.document)['disposition'] == 'skipped_cancelled'


def terminal_row(status='cancelled'):
    return {'artifact_type': 'refinement', 'id': 'refinement-one', 'status': status,
        'source_ref': 'refinement:refinement-one', 'source_version': 'v1', 'content_hash': 'a' * 64,
        'created_at': '2026-09-21T00:00:00Z'}


async def terminal_board(*, status='cancelled', port=None):
    port = port or persistence(spec(id='refinement-one', status=status))
    return await make_deterministic_projection_planner(port).prepare_board(None, board_id='board',
        source_rows=(terminal_row(status),), cognitive_rows=(), captured_at=datetime(2026, 9, 21, tzinfo=timezone.utc))


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['cancelled', 'archived'])
async def test_terminal_refinement_cleanup_matches_live_without_reviving_root_or_census(status, monkeypatch):
    port = persistence(spec(id='refinement-one', status=status))
    encoded = await terminal_board(status=status, port=port)
    board = json.loads(encoded)
    assert board['census']['skipped_cancelled_count'] == 1
    assert board['census']['eligible_count'] == 0
    assert len(board['plans']) == 1 and board['dependency_closure'] == []
    plan = board['plans'][0]
    assert plan['disposition'] == 'prepared'
    assert plan['projection']['nodes'] == plan['projection']['edges'] == []
    assert plan['projection']['relational_projection_active_set_intent'] == {
        'owner_type': 'refinement', 'owner_id': 'refinement-one', 'namespace': 'rdl',
        'active_refs': [], 'active_edges': [],
    }
    require_board_projection_cleanup(encoded)
    port.load_projection_inputs.assert_not_awaited()
    class Captured(Exception):
        pass
    async def begin(request, **kwargs):
        assert request.raw_content == plan['projection']['raw_content']
        assert request.deterministic_candidates == []
        raise Captured
    monkeypatch.setattr(live, 'get_consolidation_persistence_port', lambda: port)
    monkeypatch.setattr(live, 'begin_consolidation', begin)
    with pytest.raises(Captured):
        await live._process_queue_entry(None, SimpleNamespace(board_id='board', artifact_type='refinement',
            artifact_id='refinement-one', work_kind='consolidate'))


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['missing', 'wrong_owner', 'retained_child', 'duplicate', 'foreign_board'])
async def test_retained_terminal_cleanup_is_complete_and_owner_scoped(damage):
    board = json.loads(await terminal_board())
    if damage == 'missing':
        board['plans'] = []  # A pre-fix v2 document: never silently augment it.
    elif damage == 'wrong_owner':
        board['plans'][0]['projection']['relational_projection_active_set_intent']['owner_id'] = 'another'
    elif damage == 'retained_child':
        board['plans'][0]['projection']['relational_projection_active_set_intent']['active_refs'] = ['refinement:stale']
    elif damage == 'duplicate':
        board['plans'].append(board['plans'][0])
    else:
        board['plans'][0]['source']['board_id'] = 'another'
    encoded = json.dumps(board, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    with pytest.raises(ValueError, match='cleanup_|scope_or_duplicate'):
        require_board_projection_cleanup(encoded)
    assert json.loads(encoded) == board


@pytest.mark.asyncio
async def test_reopened_refinement_cannot_use_earlier_terminal_census():
    port = persistence(spec(id='refinement-one', status='done', ideation_id=None,
        in_scope=[], out_of_scope=[], analysis='Reopened source', labels=[]))
    with pytest.raises(ValueError, match='cleanup_source_changed'):
        await terminal_board(port=port)


@pytest.mark.asyncio
async def test_terminal_cleanup_is_included_in_aggregate_byte_limit(monkeypatch):
    from okto_pulse.core.application import deterministic_projection as module
    raw = json.dumps({'sources': (terminal_row(),), 'cognitive': ()}, ensure_ascii=False,
        sort_keys=True, separators=(',', ':')).encode()
    monkeypatch.setattr(module, '_BOARD_PLAN_LIMIT', len(raw) + 1)
    with pytest.raises(ValueError, match='census_limit'):
        await terminal_board()


@pytest.mark.asyncio
async def test_empty_complete_census_and_retired_source_are_distinct():
    planner = make_deterministic_projection_planner(persistence(None))
    kwargs = dict(board_id='board', cognitive_rows=(), captured_at=datetime(2026, 9, 21, tzinfo=timezone.utc))
    empty = json.loads(await planner.prepare_board(None, source_rows=(), **kwargs))
    assert empty['plans'] == [] and empty['census']['eligible_count'] == 0
    with pytest.raises(ValueError, match='retired_source'):
        await planner.prepare_board(None, source_rows=({'artifact_type': 'sprint', 'id': 'old'},), **kwargs)


@pytest.mark.parametrize('kind', ['sprint', 'unknown', None, []])
def test_source_kind_is_closed(kind):
    with pytest.raises(ValueError, match='source_invalid'):
        DeterministicProjectionSource('board', kind, 'source')


@pytest.mark.asyncio
async def test_aggregate_projection_limit_stops_before_reading_further_sources(monkeypatch):
    from okto_pulse.core.application import deterministic_projection as module
    rows = tuple({'artifact_type': 'spec', 'id': f'spec-{i}', 'status': 'done',
        'source_ref': f'spec:spec-{i}', 'source_version': 'v1', 'content_hash': 'a' * 64,
        'created_at': '2026-09-21T00:00:00Z'} for i in range(3))
    raw = json.dumps({'sources': rows, 'cognitive': ()}, ensure_ascii=False,
        sort_keys=True, separators=(',', ':')).encode()
    plan = SimpleNamespace(document=json.dumps({'padding': 'x' * 1_000}).encode())
    planner = make_deterministic_projection_planner(persistence(None))
    planner.prepare = AsyncMock(return_value=plan)
    monkeypatch.setattr(module, '_BOARD_PLAN_LIMIT', len(raw) + len(plan.document) + 1)
    with pytest.raises(ValueError, match='census_limit'):
        await planner.prepare_board(None, board_id='board', source_rows=rows, cognitive_rows=(),
            captured_at=datetime(2026, 9, 21, tzinfo=timezone.utc))
    assert planner.prepare.await_count == 2


@pytest.mark.parametrize('mutation', ['omit', 'change', 'duplicate', 'invent'])
def test_dependency_provider_cannot_rewrite_the_captured_source_set(mutation):
    row = {'artifact_type': 'code_evidence', 'id': 'current', 'content_hash': 'a' * 64}
    census = SimpleNamespace(materializable_sources=(SimpleNamespace(to_dict=lambda: dict(row)),),
        skipped_expired_working=())
    def resolve(*, board_id, sources):
        if mutation == 'omit':
            return ()
        if mutation == 'change':
            sources[0]['content_hash'] = 'b' * 64
            return sources
        if mutation == 'duplicate':
            return sources + sources
        return sources + ({'artifact_type': 'code_evidence', 'id': 'invented'},)
    planner = make_deterministic_projection_planner(persistence(None),
        dependencies=SimpleNamespace(resolve=resolve))
    with pytest.raises(ValueError, match='dependency_selection_invalid'):
        planner._dependency_sources(census, 'board', datetime(2026, 9, 21, tzinfo=timezone.utc))


def test_evidence_projection_requires_an_explicit_dependency_reader():
    row = {'artifact_type': 'code_evidence', 'id': 'current'}
    census = SimpleNamespace(materializable_sources=(SimpleNamespace(to_dict=lambda: row),),
        skipped_expired_working=())
    with pytest.raises(ValueError, match='dependency_resolver_required'):
        make_deterministic_projection_planner(persistence(None))._dependency_sources(
            census, 'board', datetime(2026, 9, 21, tzinfo=timezone.utc))
