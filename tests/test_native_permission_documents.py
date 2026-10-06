"""Current permission documents never acquire grants through old fingerprints."""

from copy import deepcopy

import pytest

from okto_pulse.core.ports.permission_policy import (
    PermissionPresetLineageNode,
    SKA_PERMISSION_INTRODUCTION_V1,
    builtin_permission_presets,
    direct_permission_review,
    registered_permission_flags,
    resolve_agent_permission_facts,
    set_permission_flag,
)


def test_current_full_control_document_and_trusted_sentinel_are_preserved():
    full = registered_permission_flags()
    assert direct_permission_review(full, preset_id=None) == (False, None)
    for flags in (None, full):
        resolved = resolve_agent_permission_facts(
            agent_flags=flags, preset_id=None,
            presets=(), board_overrides=None,
        )
        assert not resolved.owner_review_required
        assert resolved.has("board.read")
        assert resolved.has("guidelines.assessments.record")
    assert full == registered_permission_flags()


@pytest.mark.parametrize("variant", ["missing_generation", "false_generation", "sprint", "runtime", "extension", "malformed"])
def test_incomplete_or_retired_snapshot_is_not_normalized_to_full_control(variant):
    flags = registered_permission_flags()
    if variant == "missing_generation":
        del flags["guidelines"]["assessments"]
    elif variant == "false_generation":
        for leaf in SKA_PERMISSION_INTRODUCTION_V1.leaves:
            set_permission_flag(flags, leaf, False)
    elif variant == "sprint":
        flags["sprint"] = {"entity": {"read": True}}
    elif variant == "runtime":
        flags["runtime"] = {"settings": {"read": True, "write": True}}
    elif variant == "extension":
        flags["vendor"] = {"read": True}
    else:
        flags["board"]["read"] = 1
    before = deepcopy(flags)
    resolved = resolve_agent_permission_facts(
        agent_flags=flags, preset_id=None,
        presets=(), board_overrides=None,
    )
    assert resolved.owner_review_required
    assert not resolved.has("board.read")
    assert not resolved.has("guidelines.assessments.record")
    assert flags == before


def test_native_preset_delta_preserves_explicit_denial_and_board_ceiling():
    full = next(item["flags"] for item in builtin_permission_presets() if item["name"] == "Full Control")
    delta = {"guidelines": {"assessments": {"record": False}}}
    before = deepcopy(delta)
    resolved = resolve_agent_permission_facts(
        agent_flags=delta, preset_id="native",
        presets=(PermissionPresetLineageNode("native", full, None),),
        board_overrides=registered_permission_flags(),
    )
    assert not resolved.owner_review_required
    assert not resolved.has("guidelines.assessments.record")
    assert resolved.has("guidelines.revisions.read")
    assert resolved.has("board.read")
    assert delta == before
