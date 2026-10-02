"""One exact-ID contract for requirement reads and writes."""
import copy
import pytest
from okto_pulse.core.services.analytics_service import (
    resolve_linked_criteria_to_ids, resolve_linked_requirements_to_ids,
    resolve_linked_criteria_to_indices, resolve_linked_fr_indices,
    resolve_linked_requirement_tokens_to_fr_or_tr_ids,
)


@pytest.mark.parametrize('resolver', [resolve_linked_criteria_to_ids, resolve_linked_requirements_to_ids])
@pytest.mark.parametrize('reference', [0, '0', True, 'Current text', 'Current', ' Current id '])
def test_old_reference_shapes_never_receive_identity(resolver, reference):
    rows = [{'id': 'Current id', 'text': 'Current text'}]
    before = copy.deepcopy(rows)
    assert resolver([reference], rows) == ([], [str(reference)])
    assert rows == before


@pytest.mark.parametrize('resolver', [resolve_linked_criteria_to_ids, resolve_linked_requirements_to_ids])
def test_exact_id_duplicate_text_and_duplicate_identity(resolver):
    rows = [{'id': 'first', 'text': 'Same'}, {'id': 'second', 'text': 'Same'}]
    assert resolver(['second', 'first', 'second'], rows) == (['second', 'first'], [])
    assert resolver(['first'], rows + [rows[0]]) == ([], ['first'])
    assert resolver(['Old text'], ['Old text']) == ([], ['Old text'])


@pytest.mark.parametrize('resolver', [resolve_linked_criteria_to_indices, resolve_linked_fr_indices])
def test_output_positions_follow_identity_without_accepting_position_inputs(resolver):
    rows = [{'id': 'b', 'text': 'B'}, {'id': 'a', 'text': 'A'}, {'id': '0', 'text': 'Numeric string ID'}]
    assert resolver(['a', '0', 0, 'B'], rows) == {1, 2}
    assert resolver(['a'], list(reversed(rows))) == {1}
    assert resolver(['a'], rows + [{'id': 'a', 'text': 'Duplicate identity'}]) == set()


def test_fr_tr_resolution_does_not_coerce_unresolved_tokens_between_namespaces():
    frs = [{'id': 'fr', 'text': 'FR'}]
    trs = [{'id': '1', 'text': 'TR'}, {'id': 'True', 'text': 'Other TR'}]
    assert resolve_linked_requirement_tokens_to_fr_or_tr_ids([1, True, '1', 'fr'], frs, trs) == (['1', 'fr'], ['1', 'True'])
    assert resolve_linked_requirement_tokens_to_fr_or_tr_ids(['fr'], frs, [{'id': 'fr', 'text': 'Conflicting TR'}]) == ([], ['fr'])
