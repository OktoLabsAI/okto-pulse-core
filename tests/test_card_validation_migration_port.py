from copy import deepcopy

import pytest

from okto_pulse.core.ports.card_validation_migration import plan_card_validation_migration, verify_card_validation_migration


def facts():
    return {"card": {"id": "c", "board_id": "b", "spec_id": "sp", "sprint_id": "s"},
        "spec": {"id": "sp", "board_id": "b", "validation_min_completeness": 85},
        "sprint": {"id": "s", "board_id": "b", "validation_min_confidence": 90}, "board_settings": {}}


def test_public_plan_preserves_exact_values_and_independent_inheritance():
    source = facts()
    before = deepcopy(source)
    plan = plan_card_validation_migration(**source, migration_id="migration")
    assert source == before
    assert plan.policy.overrides.model_dump(exclude_none=True) == {"min_confidence": 90}
    assert plan.before["resolved_sources"]["min_confidence"] == "sprint"
    assert plan.after["resolved_sources"]["min_confidence"] == "card_compatibility"
    assert plan.after["min_completeness"] == 85
    detached = {**source["card"], "sprint_id": None, "migrated_validation_policy": plan.policy}
    verify_card_validation_migration(card=detached, spec=source["spec"], board_settings={}, expected=plan.after)
    with pytest.raises(ValueError, match="parity_changed"):
        verify_card_validation_migration(card={**detached, "migrated_validation_policy": None},
            spec=source["spec"], board_settings={}, expected=plan.after)


@pytest.mark.parametrize("migration_id", [None, "", 1, "x" * 129])
def test_identity_validation_applies_even_when_no_override_is_needed(migration_id):
    source = facts()
    source["sprint"]["validation_min_confidence"] = 70
    with pytest.raises(ValueError):
        plan_card_validation_migration(**source, migration_id=migration_id)
