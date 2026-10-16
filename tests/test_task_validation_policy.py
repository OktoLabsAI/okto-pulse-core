"""Current per-field policy, without Card migration or a second hierarchy."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.task_validation_policy import resolve_task_validation_config
from okto_pulse.core.models.schemas import CardCreate, CardResponse, CardUpdate
from okto_pulse.core.services import CardService


@pytest.mark.parametrize('mapping', [False, True])
def test_each_field_resolves_from_current_spec_then_board(mapping):
    raw = dict(require_task_validation=False, validation_min_confidence=0,
               validation_min_completeness=None, validation_max_drift=0)
    spec = raw if mapping else SimpleNamespace(**raw)
    board = dict(require_task_validation=True, min_confidence=90, min_completeness=92, max_drift=50)
    before = deepcopy((raw, board))
    result = resolve_task_validation_config(spec, board)
    assert result == dict(required=False, min_confidence=0, min_completeness=92, max_drift=0,
                         resolved_from='spec', resolved_sources=dict(required='spec',
                         min_confidence='spec', min_completeness='board', max_drift='spec'))
    assert (raw, board) == before


@pytest.mark.parametrize('board,expected,source', [({}, True, 'board'),
    ({'require_task_validation': None}, False, 'default'),
    ({'require_task_validation': False}, False, 'board')])
def test_required_default_and_explicit_null_keep_the_current_semantics(board, expected, source):
    result = resolve_task_validation_config(None, board)
    assert result['required'] is expected
    assert result['resolved_from'] == source
    assert (result['min_confidence'], result['min_completeness'], result['max_drift']) == (70, 80, 50)


def test_cards_of_the_same_spec_follow_live_policy_changes():
    spec = SimpleNamespace(validation_min_confidence=90)
    cards = [SimpleNamespace(id='a'), SimpleNamespace(id='b')]
    assert [CardService._resolve_validation_config(None, card, spec, {})['min_confidence']
            for card in cards] == [90, 90]
    spec.validation_min_confidence = None
    assert [CardService._resolve_validation_config(None, card, spec, {'min_confidence': 60})['min_confidence']
            for card in cards] == [60, 60]


@pytest.mark.parametrize('model', [CardCreate, CardUpdate])
@pytest.mark.parametrize('value', [None, {}, {'overrides': {'required': False}}])
def test_removed_policy_input_is_rejected_including_null(model, value):
    with pytest.raises(ValidationError, match='extra_forbidden'):
        model.model_validate({'title': 'Task', 'migrated_validation_policy': value})
    assert 'migrated_validation_policy' not in model.model_json_schema()['properties']
    assert 'migrated_validation_policy' not in CardResponse.model_json_schema()['properties']
