from copy import deepcopy
import ast
import inspect

import pytest

from okto_pulse.core.domain.permission_migration_review import PermissionMigrationReview
from okto_pulse.core.ports.permission_policy import PermissionPresetLineageNode, registered_permission_flags, resolve_agent_permission_facts
from okto_pulse.core.ports.permission_retirement import capture_permission_migration_review


def marker(layer, reason):
    return PermissionMigrationReview(layer, reason, "a" * 64, "b" * 64).document()


def resolve(*, flags=None, preset=None, presets=(), overrides=None, agent_review=None, board_review=None):
    return resolve_agent_permission_facts(agent_flags=flags, legacy_permissions=None, preset_id=preset,
        presets=presets, board_overrides=overrides, agent_migration_review=agent_review,
        board_migration_review=board_review)


def test_old_partial_snapshot_cannot_lose_review_by_matching_the_smaller_registry():
    flags = registered_permission_flags()
    retained = capture_permission_migration_review(layer="agent", flags=flags,
        source_sha256="a" * 64, checkpoint_sha256="b" * 64)
    assert retained.review_reason == "unrecognized_direct_permissions"
    current = resolve(flags=flags, agent_review=retained.document())
    assert current.owner_review_required and not current.has("board.read")
    assert current.review_reason == retained.review_reason
    assert not resolve(flags=flags).owner_review_required


def test_review_belongs_to_the_layer_and_preset_descendants_cannot_clear_the_base():
    base = PermissionPresetLineageNode("base", {}, migration_review=marker("preset", "invalid_preset_flags"))
    child = PermissionPresetLineageNode("child", registered_permission_flags(), "base")
    result = resolve(preset="child", presets=(base, child))
    assert result.owner_review_required and result.review_reason == "invalid_preset_flags"
    assert not result.has("board.read")
    independent = PermissionPresetLineageNode("other", {})
    assert not resolve(preset="other", presets=(base, child, independent)).owner_review_required


@pytest.mark.parametrize("agent_reason,board_review,preset_review,expected", [
    ("invalid_agent_flags", True, True, "invalid_agent_flags"),
    (None, True, True, "invalid_board_overrides"),
    (None, False, True, "invalid_preset_flags"),
    ("unrecognized_direct_permissions", True, False, "invalid_board_overrides"),
    ("unrecognized_direct_permissions", False, False, "unrecognized_direct_permissions"),
])
def test_original_review_reason_priority_survives_cleanup(agent_reason, board_review, preset_review, expected):
    node = PermissionPresetLineageNode("base", {}, migration_review=marker("preset", "invalid_preset_flags") if preset_review else None)
    result = resolve(preset="base" if preset_review else None, presets=(node,),
        agent_review=marker("agent", agent_reason) if agent_reason else None,
        board_review=marker("board", "invalid_board_overrides") if board_review else None)
    assert result.owner_review_required and result.review_reason == expected
    assert not result.has("any.unregistered.extension")


@pytest.mark.parametrize("value", [{}, [], 1, "true", {"format": "unknown"}, marker("board", "invalid_board_overrides")])
def test_damaged_or_wrong_layer_marker_never_means_full_control(value):
    result = resolve(agent_review=value)
    assert result.owner_review_required and result.review_reason == "invalid_permission_migration_review"
    assert not result.has("board.read")


def test_classifier_does_not_flatten_a_parent_review_into_an_agent_marker():
    assert capture_permission_migration_review(layer="agent", flags={}, preset_id="some-parent",
        source_sha256="a" * 64, checkpoint_sha256="b" * 64) is None
    assert capture_permission_migration_review(layer="board", flags={},
        source_sha256="a" * 64, checkpoint_sha256="b" * 64) is None


def test_preset_marker_input_is_detached():
    source = marker("preset", "invalid_preset_flags")
    before = deepcopy(source)
    node = PermissionPresetLineageNode("base", {}, migration_review=source)
    source.clear()
    assert node.migration_review == before
    assert resolve(preset="base", presets=(node,)).owner_review_required


def test_malformed_extension_in_retiring_preset_branch_is_classified_even_without_consumers():
    review = capture_permission_migration_review(layer="preset", flags={"sprint": {"extension": "true"}},
        source_sha256="a" * 64, checkpoint_sha256="b" * 64)
    assert review.review_reason == "invalid_preset_flags"


def test_migration_review_value_object_depends_only_on_the_standard_library():
    from okto_pulse.core.domain import permission_migration_review
    tree = ast.parse(inspect.getsource(permission_migration_review))
    assert [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)] == ["dataclasses"]
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))


def test_persisted_review_preserves_extension_projection_and_does_not_mutate_input():
    flags = {"vendor_extension": {"grant": True, "audit": False}}
    original = deepcopy(flags)
    result = resolve(flags=flags, preset="base", presets=(PermissionPresetLineageNode("base", {}),),
        agent_review=marker("agent", "invalid_agent_flags"))
    assert result.flags["vendor_extension"] == {"grant": False, "audit": False}
    assert flags == original
