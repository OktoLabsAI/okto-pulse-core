"""Write admission must not silently choose an ambiguous structured reference."""

import pytest

from okto_pulse.core.services.analytics_service import (
    resolve_linked_criteria_to_ids,
    resolve_linked_requirement_tokens_to_fr_or_tr_ids,
    resolve_linked_requirements_to_ids,
)


@pytest.fixture(params=[('ac', resolve_linked_criteria_to_ids), ('fr', resolve_linked_requirements_to_ids), ('tr', resolve_linked_requirements_to_ids)])
def resolver(request):
    return request.param


def test_id_precedes_another_rows_text_and_survives_reorder(resolver):
    prefix, resolve = resolver
    token = f'{prefix}_target'
    rows = [{'id': f'{prefix}_other', 'text': token}, {'id': token, 'text': 'Target'}]
    assert resolve([token], rows) == ([token], [])
    assert resolve([token], list(reversed(rows))) == ([token], [])


def test_duplicate_text_is_unresolved(resolver):
    prefix, resolve = resolver
    rows = [{'id': f'{prefix}_one', 'text': 'Repeated'}, {'id': f'{prefix}_two', 'text': 'Repeated'}]
    assert resolve(['Repeated'], rows) == ([], ['Repeated'])


def test_unknown_canonical_id_does_not_bind_to_text(resolver):
    prefix, resolve = resolver
    token = f'{prefix}_missing'
    assert resolve([token], [{'id': f'{prefix}_other', 'text': token}]) == ([], [token])


def test_duplicate_id_is_unresolved(resolver):
    prefix, resolve = resolver
    token = f'{prefix}_duplicate'
    rows = [{'id': token, 'text': 'First'}, {'id': token, 'text': 'Second'}]
    assert resolve([token], rows) == ([], [token])


def test_unique_legacy_text_and_index_remain_supported(resolver):
    prefix, resolve = resolver
    rows = [{'id': f'{prefix}_one', 'text': 'First'}, {'id': f'{prefix}_two', 'text': 'Second'}]
    assert resolve(['Second', 0, '1'], rows) == ([f'{prefix}_two', f'{prefix}_one'], [])
    assert resolve(['Legacy'], ['Legacy']) == (['Legacy'], [])


def test_duplicate_legacy_text_is_unresolved(resolver):
    _, resolve = resolver
    assert resolve(['Legacy'], ['Legacy', 'Legacy']) == ([], ['Legacy'])


def test_canonical_tr_reference_is_not_consumed_as_fr_text():
    assert resolve_linked_requirement_tokens_to_fr_or_tr_ids(
        ['tr_target'], [{'id': 'fr_other', 'text': 'tr_target'}],
        [{'id': 'tr_target', 'text': 'Technical condition'}],
    ) == (['tr_target'], [])
