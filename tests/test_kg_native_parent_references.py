"""Deferred parent references use only the native artifact grammar."""

import pytest

from okto_pulse.core.kg.primitives import (
    _cross_session_entity_source_prefix,
    _is_cross_session_entity_ref,
)


@pytest.mark.parametrize("artifact_type", ["story", "ideation", "refinement", "spec", "card"])
def test_current_parent_reference_uses_the_same_live_and_recovery_grammar(artifact_type):
    endpoint = f"{artifact_type}_12345678_entity"
    assert _is_cross_session_entity_ref(endpoint) is True
    assert _cross_session_entity_source_prefix(endpoint) == f"{artifact_type}:12345678"


@pytest.mark.parametrize("endpoint", ["sprint_12345678_entity", "sprint__entity"])
def test_removed_parent_reference_is_not_deferred_or_resolved(endpoint):
    assert _is_cross_session_entity_ref(endpoint) is False
    assert _cross_session_entity_source_prefix(endpoint) is None
