"""Pure and async contracts for the central Core authorization foundation."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
    normalize_permission_input,
    require_all,
    require_any_authority,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.permissions import PermissionDecision, PermissionSet


class FakeServices:
    def __init__(self, permissions):
        self.permissions = permissions
        self.calls: list[tuple[str, str]] = []

    async def resolve_user_permissions(self, actor_id: str, board_id: str):
        self.calls.append((actor_id, board_id))
        return self.permissions


def _actor(permissions=None, *, source="mcp", roles=(), board_id="board-1"):
    return ActorContext(
        "actor-1",
        source,
        board_id=board_id,
        permissions=permissions,
        roles=roles,
    )


def test_mapping_normalizes_to_permission_set_and_none_stays_trusted() -> None:
    normalized = normalize_permission_input({"board": {"read": False}})
    assert isinstance(normalized, PermissionSet)
    assert normalized.has("board.read") is False
    assert normalize_permission_input(None) is None


def test_permission_set_uses_granular_permission_without_legacy_fallback() -> None:
    decision = decide_authorization(
        _actor(PermissionSet({"board": {"read": False}})),
        PermissionRequirement("board.read", legacy_operation="board:read"),
    )
    assert decision.allowed is False


def test_flat_permissions_allow_granular_and_explicit_legacy_tokens() -> None:
    requirement = PermissionRequirement("board.read", legacy_operation="board:read")
    assert decide_authorization(_actor(["board.read"]), requirement).allowed is True
    assert decide_authorization(_actor(("board:read",)), requirement).allowed is True


def test_introduced_leaf_accepts_bounded_flat_token_for_historical_authority() -> None:
    requirement = PermissionRequirement(
        "agent.entity.read",
        legacy_operation="board.read",
    )
    assert decide_authorization(_actor(["agent.entity.read"]), requirement).allowed
    assert decide_authorization(_actor(["board.read"]), requirement).allowed
    assert decide_authorization(_actor(["board:read"]), requirement).allowed
    assert not decide_authorization(_actor(["cards:update"]), requirement).allowed


def test_none_is_trusted_full_access_but_unknown_flag_fails_closed() -> None:
    actor = _actor(None)
    assert decide_authorization(actor, PermissionRequirement("board.read")).allowed
    denied = decide_authorization(actor, PermissionRequirement("not.registered"))
    assert denied.allowed is False
    assert json.loads(denied.reason or "{}") == {
        "error": "Permission denied",
        "reason": "unknown_permission",
        "required_permission": "not.registered",
        "detail": (
            "The permission 'not.registered' is not registered by the Core "
            "permission policy."
        ),
    }


@pytest.mark.parametrize("source", ["mcp", "system"])
def test_internal_sources_preserve_legacy_trusted_wildcard(source: str) -> None:
    decision = decide_authorization(
        _actor(["*"], source=source),
        PermissionRequirement("card.entity.create", legacy_operation="cards:create"),
    )
    assert decision.allowed is True


def test_rest_does_not_treat_legacy_wildcard_as_trusted() -> None:
    decision = decide_authorization(
        _actor(["*"], source="rest"),
        PermissionRequirement("card.entity.create", legacy_operation="cards:create"),
    )
    assert decision.allowed is False


def test_permission_set_decision_is_state_aware() -> None:
    actor = _actor(
        PermissionSet(
            {
                "card": {
                    "tests": {"update_status": True},
                    "interact_in": {"done": False},
                }
            }
        )
    )
    decision = decide_authorization(
        actor,
        PermissionRequirement(
            "card.tests.update_status",
            entity="card",
            state="done",
        ),
    )
    assert decision.allowed is False
    assert json.loads(decision.reason or "{}")["reason"] == "interact_in_blocked"


@pytest.mark.asyncio
async def test_rest_permissions_are_resolved_once_for_require_all() -> None:
    services = FakeServices(
        PermissionSet({"board": {"read": True, "activity_read": True}})
    )
    uow = SimpleNamespace(services=services)
    decisions = await require_all(
        _actor(None, source="rest"),
        PermissionRequirement("board.read"),
        PermissionRequirement("board.activity_read"),
        uow=uow,
    )
    assert all(decision.allowed for decision in decisions)
    assert services.calls == [("actor-1", "board-1")]


@pytest.mark.asyncio
async def test_non_rest_missing_permissions_do_not_call_resolver() -> None:
    services = FakeServices(PermissionSet({"board": {"read": False}}))
    decision = await require_authorization(
        _actor(None, source="mcp"),
        PermissionRequirement("board.read"),
        uow=SimpleNamespace(services=services),
    )
    assert decision.allowed is True
    assert services.calls == []


@pytest.mark.asyncio
async def test_denial_raises_permission_denied_with_structured_reason() -> None:
    with pytest.raises(PermissionDeniedError) as exc_info:
        await require_authorization(
            _actor([]),
            PermissionRequirement("board.read"),
        )
    detail = json.loads(str(exc_info.value))
    assert detail["reason"] == "permission_missing"
    assert detail["required_permission"] == "board.read"


@pytest.mark.asyncio
async def test_role_bypass_only_happens_for_explicit_accepted_roles() -> None:
    actor = _actor([], roles=("ADMIN",))
    with pytest.raises(PermissionDeniedError):
        await require_any_authority(actor, PermissionRequirement("board.read"))

    decision = await require_any_authority(
        actor,
        PermissionRequirement("board.read"),
        roles=("admin",),
    )
    assert decision == decision.allow("role:admin")


@pytest.mark.asyncio
async def test_explicit_permission_denial_precedes_accepted_role() -> None:
    actor = _actor(
        PermissionSet({"board": {"read": False}}),
        roles=("admin",),
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        await require_any_authority(
            actor,
            PermissionRequirement("board.read"),
            roles=("admin",),
        )

    detail = json.loads(str(exc_info.value))
    assert detail["required_permission"] == "board.read"


@pytest.mark.asyncio
async def test_explicit_mapping_override_denial_precedes_accepted_role() -> None:
    actor = _actor([], roles=("admin",))

    with pytest.raises(PermissionDeniedError):
        await require_any_authority(
            actor,
            PermissionRequirement("board.read"),
            roles=("admin",),
            permissions={"board": {"read": False}},
        )


@pytest.mark.asyncio
async def test_explicit_legacy_denial_precedes_role_for_introduced_flag() -> None:
    actor = _actor(
        PermissionSet({"kg": {"admin": {"settings_read": False}}}),
        roles=("operator",),
    )

    with pytest.raises(PermissionDeniedError):
        await require_any_authority(
            actor,
            PermissionRequirement(
                "runtime.settings.read",
                legacy_operation="kg.admin.settings_read",
            ),
            roles=("operator",),
        )


@pytest.mark.asyncio
async def test_role_does_not_bypass_missing_historical_authority() -> None:
    actor = _actor(
        PermissionSet({"runtime": {"settings": {"read": True}}}),
        roles=("operator",),
    )

    with pytest.raises(PermissionDeniedError):
        await require_any_authority(
            actor,
            PermissionRequirement(
                "runtime.settings.read",
                legacy_operation="kg.admin.settings_read",
            ),
            roles=("operator",),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requirement",
    (
        PermissionRequirement("not.registered"),
        PermissionRequirement("board.read", entity="board"),
    ),
    ids=("unknown-permission", "invalid-context"),
)
@pytest.mark.parametrize(
    "permissions",
    (PermissionSet({}), [], None, ["board:read"]),
    ids=("permission-set", "empty-legacy", "trusted-legacy", "flat-legacy"),
)
async def test_role_never_bypasses_invalid_permission_requirement(
    requirement: PermissionRequirement,
    permissions,
) -> None:
    actor = _actor(permissions, roles=("admin",))

    with pytest.raises(PermissionDeniedError):
        await require_any_authority(
            actor,
            requirement,
            roles=("admin",),
        )


@pytest.mark.asyncio
async def test_require_any_resolves_once_and_accepts_one_permission() -> None:
    services = FakeServices(PermissionSet({"board": {"read": True}}))
    decision = await require_any_authority(
        _actor(None, source="rest"),
        PermissionRequirement("agent.list"),
        PermissionRequirement("board.read"),
        uow=SimpleNamespace(services=services),
    )
    assert decision.allowed is True
    assert decision.required_permission == "board.read"
    assert services.calls == [("actor-1", "board-1")]


def test_explicit_none_override_cannot_elevate_restricted_actor() -> None:
    actor = _actor([])
    requirement = PermissionRequirement("board.read")
    assert decide_authorization(actor, requirement).allowed is False
    assert decide_authorization(actor, requirement, permissions=None).allowed is False


def test_explicit_non_null_override_differs_from_actor_permissions() -> None:
    actor = _actor(PermissionSet({"board": {"read": True}}))
    requirement = PermissionRequirement("board.read")
    assert decide_authorization(actor, requirement).allowed is True
    assert decide_authorization(actor, requirement, permissions=[]).allowed is False


def test_rest_none_is_never_trusted_even_as_explicit_override() -> None:
    decision = decide_authorization(
        _actor(None, source="rest"),
        PermissionRequirement("board.read"),
        permissions=None,
    )
    assert decision.allowed is False


@pytest.mark.parametrize(
    ("entity", "state"),
    (("card", None), (None, "done"), ("card", "not-a-state")),
)
def test_invalid_state_context_fails_closed(entity, state) -> None:
    decision = decide_authorization(
        _actor(PermissionSet({})),
        PermissionRequirement(
            "card.tests.update_status",
            entity=entity,
            state=state,
        ),
    )
    assert decision.allowed is False
    assert json.loads(decision.reason or "{}")["reason"] == (
        "invalid_permission_context"
    )


def test_custom_policy_cannot_allow_an_unknown_operation() -> None:
    class AllowEverythingPolicy:
        def evaluate(self, context):
            return PermissionDecision.allow(context.operation)

    decision = decide_authorization(
        _actor([]),
        PermissionRequirement("not.registered"),
        policy=AllowEverythingPolicy(),
    )
    assert decision.allowed is False
    assert json.loads(decision.reason or "{}")["reason"] == "unknown_permission"


def test_state_context_cannot_gate_an_operation_from_another_entity() -> None:
    decision = decide_authorization(
        _actor(None),
        PermissionRequirement("board.read", entity="card", state="done"),
    )
    assert decision.allowed is False
    assert json.loads(decision.reason or "{}")["reason"] == (
        "invalid_permission_context"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uow",
    (
        None,
        SimpleNamespace(services=SimpleNamespace()),
        SimpleNamespace(),
    ),
)
async def test_rest_unresolved_permissions_fail_closed(uow) -> None:
    with pytest.raises(PermissionDeniedError):
        await require_authorization(
            _actor(None, source="rest"),
            PermissionRequirement("board.read"),
            uow=uow,
        )


@pytest.mark.asyncio
async def test_rest_resolver_returning_none_fails_closed() -> None:
    services = FakeServices(None)
    with pytest.raises(PermissionDeniedError):
        await require_authorization(
            _actor(None, source="rest"),
            PermissionRequirement("board.read"),
            uow=SimpleNamespace(services=services),
        )
    assert services.calls == [("actor-1", "board-1")]


@pytest.mark.asyncio
async def test_rest_without_board_fails_closed_without_resolver_call() -> None:
    services = FakeServices(PermissionSet({"board": {"read": True}}))
    with pytest.raises(PermissionDeniedError):
        await require_authorization(
            _actor(None, source="rest", board_id=None),
            PermissionRequirement("board.read"),
            uow=SimpleNamespace(services=services),
        )
    assert services.calls == []


@pytest.mark.asyncio
async def test_rest_target_board_does_not_reuse_actor_board_permissions() -> None:
    services = FakeServices(PermissionSet({"board": {"read": False}}))
    with pytest.raises(PermissionDeniedError):
        await require_authorization(
            _actor(
                PermissionSet({"board": {"read": True}}),
                source="rest",
                board_id="board-1",
            ),
            PermissionRequirement("board.read"),
            board_id="board-2",
            uow=SimpleNamespace(services=services),
        )
    assert services.calls == [("actor-1", "board-2")]


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_board_id", ("board-1", None))
async def test_non_rest_target_board_scope_mismatch_fails_closed(
    actor_board_id,
) -> None:
    with pytest.raises(PermissionDeniedError):
        await require_authorization(
            _actor(
                PermissionSet({"board": {"read": True}}),
                source="mcp",
                board_id=actor_board_id,
            ),
            PermissionRequirement("board.read"),
            board_id="board-2",
        )


@pytest.mark.asyncio
async def test_explicit_role_does_not_bypass_non_rest_board_scope() -> None:
    with pytest.raises(PermissionDeniedError) as exc_info:
        await require_any_authority(
            _actor([], source="mcp", roles=("admin",), board_id="board-1"),
            PermissionRequirement("board.read"),
            roles=("admin",),
            board_id="board-2",
        )
    assert json.loads(str(exc_info.value))["reason"] == "board_scope_mismatch"


@pytest.mark.asyncio
async def test_explicit_none_does_not_bypass_authorization_board_scope() -> None:
    with pytest.raises(PermissionDeniedError) as exc_info:
        await require_authorization(
            _actor(None, source="mcp", board_id="board-1"),
            PermissionRequirement("board.read"),
            permissions=None,
            board_id="board-2",
        )
    assert json.loads(str(exc_info.value))["reason"] == "board_scope_mismatch"


@pytest.mark.asyncio
async def test_explicit_none_does_not_bypass_require_all_board_scope() -> None:
    with pytest.raises(PermissionDeniedError) as exc_info:
        await require_all(
            _actor(None, source="system", board_id="board-1"),
            PermissionRequirement("board.read"),
            permissions=None,
            board_id="board-2",
        )
    assert json.loads(str(exc_info.value))["reason"] == "board_scope_mismatch"
