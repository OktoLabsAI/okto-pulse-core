"""F5 keeps contribution calculations and authority while retiring Sprint scope."""
import json
from pathlib import Path

import pytest

from delivery_contribution_cases import NOW, cards, cases
from okto_pulse.core.application.use_cases.delivery_intelligence import DeliveryIntelligenceCommand
from okto_pulse.core.ports.analytics_foundation import AnalyticsFilterClause
from okto_pulse.core.services import analytics_service

BASELINE = json.loads((Path(__file__).parent / 'fixtures/delivery_contribution_baseline.json').read_text(encoding='utf-8'))
CASES = list(cases())


@pytest.mark.asyncio
@pytest.mark.parametrize(('key', 'kwargs'), CASES, ids=[key for key, _ in CASES])
async def test_contributions_match_published_same_population_baseline(monkeypatch, key, kwargs):
    async def read(_db, entity, *, filters):
        assert entity == 'card'
        assert [(item.field, item.operator) for item in filters] == [
            ('board_id', 'eq'), ('archived', 'is_false'), ('created_at', 'gte'), ('created_at', 'lt'),
        ]
        assert filters[0].value == 'board-1'
        assert filters[2].value == kwargs['query'].window.from_inclusive
        assert filters[3].value == NOW
        population = cards()
        for card in population:
            del card.sprint_id  # no surviving dependency on legacy membership
        return population

    monkeypatch.setattr(analytics_service, '_analytics_list', read)
    payload = await analytics_service.compute_delivery_intelligence(object(), **kwargs)
    assert {key: payload[key] for key in ('contributions', 'exclusions')} == BASELINE['cases'][key]
    assert payload['contract_version'] == '2'
    assert 'sprints' not in payload and 'summary' not in payload
    assert payload['population_scope']['accessible_count'] == 14
    assert payload['query_fingerprint'] == kwargs['query'].fingerprint
    assert payload['provenance']['sources'][0]['timestamp_field'] == 'cards.created_at'


@pytest.mark.parametrize('field', ['sprint_id', 'lane'])
@pytest.mark.parametrize('operator,value', [('eq','legacy'), ('ne','legacy'), ('in',('legacy',)), ('not_in',('legacy',))])
def test_retired_filters_fail_closed(field, operator, value):
    query = CASES[0][1]['query']
    with pytest.raises(ValueError, match='filter_field_unsupported'):
        DeliveryIntelligenceCommand(board_id=query.board_id, window=query.window, as_of=NOW,
            filters=(AnalyticsFilterClause(field, operator, value),))


def test_sprint_cursor_cannot_be_reinterpreted_as_contribution_cursor():
    query = CASES[0][1]['query']
    with pytest.raises(ValueError, match='cursor_invalid'):
        DeliveryIntelligenceCommand(board_id=query.board_id, window=query.window, as_of=NOW, cursor='offset:1')


@pytest.mark.asyncio
async def test_paging_preserves_contributions_and_privacy_metadata(monkeypatch):
    async def read(*args, **kwargs):
        return cards()
    monkeypatch.setattr(analytics_service, '_analytics_list', read)
    kwargs = next(kwargs for key, kwargs in CASES if key == 'owner/True/operator/all')
    complete = await analytics_service.compute_delivery_intelligence(object(), **kwargs)
    offset = 0
    rows = []
    while True:
        page = await analytics_service.compute_delivery_intelligence(object(), **kwargs, cursor_offset=offset, limit=1)
        rows.extend(page['contributions'])
        assert page['exclusions'] == complete['exclusions']
        assert page['population_scope'] == complete['population_scope']
        if page['next_cursor'] is None:
            break
        assert page['next_cursor'].startswith('contributions-v2:offset:')
        offset = int(page['next_cursor'].rsplit(':', 1)[1])
        assert offset < 10
    assert rows == complete['contributions']
