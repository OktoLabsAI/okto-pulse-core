"""A current permission never delegates to an old token or another operation."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement, decide_authorization,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.permissions import (
    PermissionContext, PermissionSet, check_permission, evaluate_permission, has_permission,
)
from okto_pulse.core.ports.permission_policy import registered_permission_flags


@pytest.mark.parametrize("token", ["board:read", "cards:update", "specs:update", "self:update", "vendor.unknown"])
@pytest.mark.parametrize("permissions", [None, [], ["*"]])
def test_unregistered_operations_are_never_permissions(token, permissions):
    assert not has_permission(permissions, token)
    assert check_permission(permissions, token) is not None
    assert not PermissionSet(registered_permission_flags()).has(token)
    assert not evaluate_permission(PermissionContext(token, permissions)).allowed


@pytest.mark.parametrize("operation,substitute", [
    ("board.read", "board:read"),
    ("agent.entity.read", "board.read"),
    ("card.move.not_started_to_started", "cards:move"),
    ("profile.update", "self:update"),
    ("kg.operations.health.read", "kg.admin.settings_read"),
])
def test_only_exact_canonical_capability_passes(operation, substitute):
    actor = ActorContext("agent", "mcp", permissions=(substitute,))
    assert not decide_authorization(actor, PermissionRequirement(operation)).allowed
    actor = ActorContext("agent", "mcp", permissions=(operation,))
    assert decide_authorization(actor, PermissionRequirement(operation)).allowed


def test_removed_alternative_argument_is_rejected():
    with pytest.raises(TypeError, match="legacy_operation"):
        PermissionRequirement("board.read", legacy_operation="board:read")
    with pytest.raises(TypeError, match="legacy_operation"):
        PermissionContext("board.read", legacy_operation="board:read")


def test_mcp_kg_and_story_helpers_refuse_old_tokens():
    from okto_pulse.core.mcp.kg_authorization import kg_permission_error
    from okto_pulse.core.mcp.server import _mcp_check_permission
    from okto_pulse.core.services.permission_policy import check_story_state_permission

    assert _mcp_check_permission(["self:update"], "profile.update") is not None
    assert _mcp_check_permission(["profile.update"], "profile.update") is None
    assert kg_permission_error(SimpleNamespace(permissions=["board:read"]), "board.read") is not None
    assert kg_permission_error(SimpleNamespace(permissions=["board.read"]), "board.read") is None
    assert check_story_state_permission(
        ["specs:update"], "story.entity.edit_fields", SimpleNamespace(),
        story_state="draft",
    ) is not None
    assert check_story_state_permission(
        ["story.entity.edit_fields"], "story.entity.edit_fields", SimpleNamespace(),
        story_state="draft",
    ) is None
