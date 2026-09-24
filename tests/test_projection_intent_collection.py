from dataclasses import replace

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    RelationalProjectionActiveSetIntent,
    RelationalProjectionActiveRef,
)
from okto_pulse.core.kg.primitives import KGPrimitiveError, _validate_projection_intent_collection


def intent(namespace):
    return RelationalProjectionActiveSetIntent('spec', 'owner', namespace, ())


def test_distinct_namespaces_are_preserved_in_order():
    intents = (intent('dependencies'), intent('scenario_criteria'))
    _validate_projection_intent_collection(intents, session_id='test')
    assert [item.namespace for item in intents] == ['dependencies', 'scenario_criteria']


@pytest.mark.parametrize('intents', [None, [], [intent('dependencies')], (intent('a'),) * 17])
def test_unbounded_or_mutable_collection_is_rejected(intents):
    with pytest.raises(KGPrimitiveError, match='bounded tuple'):
        _validate_projection_intent_collection(intents, session_id='test')


def test_duplicate_namespace_is_rejected_even_when_members_are_empty():
    with pytest.raises(KGPrimitiveError, match='distinct'):
        _validate_projection_intent_collection((intent('dependencies'), intent('dependencies')), session_id='test')


def test_member_cannot_be_owned_by_two_namespaces():
    ref = RelationalProjectionActiveRef('Criterion', 'candidate', 'spec:owner:ac:one')
    intents = tuple(replace(intent(namespace), active_refs=(ref,)) for namespace in ('a', 'b'))
    with pytest.raises(KGPrimitiveError, match='exactly one owner'):
        _validate_projection_intent_collection(intents, session_id='test')
