"""Recovery seeds must capture the complete canonical debt exclusion set."""

from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import canonical_partition_integrity as partition
from okto_pulse.core.kg.canonical_learning_partition import HISTORICAL_DEBT_REASON
from okto_pulse.core.ports.global_discovery_recovery_control import GlobalDiscoveryRecoveryBoardSeedInputService


def row(number):
    return {'id': str(number), 'board_id': 'board', 'source_ref': f'spec:source-{number}',
        'canonical_state': 'pending', 'failure_reason': HISTORICAL_DEBT_REASON}


async def capture():
    return await GlobalDiscoveryRecoveryBoardSeedInputService().capture_board_seed_input(
        object(), board_id='board', board_name='Board', board_summary='',
        captured_cognitive_pending_exclusions={})


@pytest.mark.asyncio
async def test_recovery_captures_debt_after_first_page(monkeypatch):
    rows = [row(index) for index in range(201)]
    calls = []

    async def read(_db, *, board_id, limit, offset=0):
        assert board_id == 'board'
        calls.append(offset)
        return SimpleNamespace(total=len(rows), items=rows[offset:offset + limit])

    monkeypatch.setattr(partition, 'list_canonical_debt', read)
    result = await capture()
    assert len(result.overlay_exclusions) == 201
    assert dict(result.overlay_exclusions)['spec:source-200'] == HISTORICAL_DEBT_REASON
    assert calls == [0, 200]


@pytest.mark.asyncio
async def test_recovery_never_treats_unavailable_debt_as_empty(monkeypatch):
    async def unavailable(*_args, **_kwargs):
        raise RuntimeError('debt storage unavailable')

    monkeypatch.setattr(partition, 'list_canonical_debt', unavailable)
    with pytest.raises(RuntimeError, match='debt storage unavailable'):
        await capture()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['total_changes', 'truncated', 'duplicate', 'foreign', 'missing_source', 'bool_total', 'too_many'])
async def test_recovery_refuses_incomplete_or_ambiguous_inventory(monkeypatch, fault):
    rows = [row(index) for index in range(201)]
    if fault == 'duplicate':
        rows[-1] = rows[0]
    elif fault == 'foreign':
        rows[-1]['board_id'] = 'other'
    elif fault == 'missing_source':
        rows[-1]['source_ref'] = None

    async def read(_db, *, board_id, limit, offset):
        total = 202 if offset and fault == 'total_changes' else 201
        if fault == 'bool_total':
            total = True
        elif fault == 'too_many':
            total = 100_001
        items = rows[offset:offset + limit]
        if offset and fault == 'truncated':
            items = []
        return SimpleNamespace(total=total, items=items)

    monkeypatch.setattr(partition, 'list_canonical_debt', read)
    with pytest.raises(ValueError, match='canonical_debt_capture_'):
        await capture()


@pytest.mark.asyncio
async def test_empty_inventory_requires_successful_complete_read(monkeypatch):
    async def read(*_args, **_kwargs):
        return SimpleNamespace(total=0, items=[])

    monkeypatch.setattr(partition, 'list_canonical_debt', read)
    assert (await capture()).overlay_exclusions == ()


@pytest.mark.asyncio
async def test_existing_reason_state_and_debt_precedence_are_preserved(monkeypatch):
    from okto_pulse.core.kg.connectivity_guard import CANONICAL_LEARNING_WORKING_ONLY_REASON
    rows = [row(index) for index in range(3)]
    rows[1]['canonical_state'] = 'resolved'
    rows[2]['failure_reason'] = 'unrelated'

    async def read(*_args, **_kwargs):
        return SimpleNamespace(total=3, items=rows)

    monkeypatch.setattr(partition, 'list_canonical_debt', read)
    result = await GlobalDiscoveryRecoveryBoardSeedInputService().capture_board_seed_input(
        object(), board_id='board', board_name='Board', board_summary='',
        captured_cognitive_pending_exclusions={'spec:source-0': CANONICAL_LEARNING_WORKING_ONLY_REASON,
            'spec:pending': CANONICAL_LEARNING_WORKING_ONLY_REASON})
    assert dict(result.overlay_exclusions) == {'spec:source-0': HISTORICAL_DEBT_REASON,
        'spec:pending': CANONICAL_LEARNING_WORKING_ONLY_REASON}
