"""BG-01.3 — Full-context guard foundation."""

from __future__ import annotations

import uuid

import pytest


pytestmark = pytest.mark.asyncio

USER_ID = "bg-guard-user"


async def _create_board(db, *, settings: dict | None = None):
    from okto_pulse.core.models.db import Board

    board = Board(
        id=f"bg-guard-board-{uuid.uuid4().hex[:8]}",
        name="BG Guard Board",
        owner_id=USER_ID,
        settings=settings or {},
    )
    db.add(board)
    await db.flush()
    return board


class RecordingResolver:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def resolve_full_context(self, **kwargs):
        self.calls.append(kwargs)
        return self.payload


class RaisingResolver:
    async def resolve_full_context(self, **kwargs):
        raise RuntimeError("resolver exploded")


async def test_disabled_guard_preserves_existing_behavior_without_resolver_call(db_factory):
    from okto_pulse.core.services.critical_context_guard import (
        CriticalAction,
        FullContextCriticalActionGuard,
    )

    resolver = RecordingResolver({"card": {"id": "card-1"}})

    async with db_factory() as db:
        board = await _create_board(
            db,
            settings={"require_full_context_for_critical_actions": False},
        )
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        decision = await FullContextCriticalActionGuard(
            db,
            resolvers={"card": resolver},
        ).authorize_and_resolve(
            board_id=board_id,
            actor_id=USER_ID,
            entity_type="card",
            entity_id="card-1",
            critical_action=CriticalAction.CARD_MOVE_STATUS,
            surface="service",
        )

    assert resolver.calls == []
    assert decision.outcome == "allow"
    assert decision.reason == "guard_disabled"
    assert decision.context_profile == "not_required"
    assert decision.context_fingerprint is None


async def test_enabled_guard_resolves_full_context_and_returns_safe_fingerprint(db_factory):
    from okto_pulse.core.services.critical_context_guard import (
        CONTEXT_FINGERPRINT_ALG,
        CriticalAction,
        FullContextCriticalActionGuard,
    )

    payload_a = {
        "card": {"id": "card-1", "title": "A"},
        "spec": {"id": "spec-1", "requirements": [{"id": "fr-1", "text": "x"}]},
    }
    payload_b_same_content_different_order = {
        "spec": {"requirements": [{"text": "x", "id": "fr-1"}], "id": "spec-1"},
        "card": {"title": "A", "id": "card-1"},
    }

    async with db_factory() as db:
        board = await _create_board(
            db,
            settings={"require_full_context_for_critical_actions": True},
        )
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        guard_a = FullContextCriticalActionGuard(
            db,
            resolvers={"card": RecordingResolver(payload_a)},
        )
        guard_b = FullContextCriticalActionGuard(
            db,
            resolvers={"card": RecordingResolver(payload_b_same_content_different_order)},
        )
        decision_a = await guard_a.authorize_and_resolve(
            board_id=board_id,
            actor_id=USER_ID,
            entity_type="card",
            entity_id="card-1",
            critical_action="card.move_status",
            surface="mcp",
        )
        decision_b = await guard_b.authorize_and_resolve(
            board_id=board_id,
            actor_id=USER_ID,
            entity_type="card",
            entity_id="card-1",
            critical_action=CriticalAction.CARD_MOVE_STATUS,
            surface="mcp",
        )

    assert decision_a.outcome == "allow"
    assert decision_a.reason == "full_context_resolved"
    assert decision_a.context_profile == "full"
    assert decision_a.context_fingerprint is not None
    assert decision_a.context_fingerprint.startswith(f"{CONTEXT_FINGERPRINT_ALG}:")
    assert decision_a.context_fingerprint == decision_b.context_fingerprint
    assert "A" not in decision_a.context_fingerprint
    assert "fr-1" not in decision_a.context_fingerprint

    audit = decision_a.audit_details()
    assert set(audit) == {
        "metric_name",
        "board_id",
        "actor_id",
        "entity_type",
        "critical_action",
        "surface",
        "outcome",
        "reason",
        "context_profile",
        "entity_id",
        "context_fingerprint",
        "context_resolved_at",
    }
    assert audit["metric_name"] == "critical_context_guard_decision_total"


async def test_enabled_guard_fails_closed_when_no_resolver_is_registered(db_factory):
    from okto_pulse.core.services.critical_context_guard import (
        CriticalAction,
        FullContextCriticalActionGuard,
        FullContextRequiredError,
    )

    async with db_factory() as db:
        board = await _create_board(
            db,
            settings={"require_full_context_for_critical_actions": True},
        )
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(FullContextRequiredError) as exc_info:
            await FullContextCriticalActionGuard(db).authorize_and_resolve(
                board_id=board_id,
                actor_id=USER_ID,
                entity_type="card",
                entity_id="card-1",
                critical_action=CriticalAction.CARD_MOVE_STATUS,
                surface="service",
            )

    decision = exc_info.value.decision
    assert decision.outcome == "deny"
    assert decision.reason == "full_context_required"
    assert decision.context_profile == "missing"
    assert decision.context_fingerprint is None


async def test_enabled_guard_fails_closed_on_resolver_or_fingerprint_failure(db_factory):
    from okto_pulse.core.services.critical_context_guard import (
        CriticalAction,
        FullContextCriticalActionGuard,
        FullContextUnavailableError,
    )

    async with db_factory() as db:
        board = await _create_board(
            db,
            settings={"require_full_context_for_critical_actions": True},
        )
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(FullContextUnavailableError) as exc_info:
            await FullContextCriticalActionGuard(
                db,
                resolvers={"card": RaisingResolver()},
            ).authorize_and_resolve(
                board_id=board_id,
                actor_id=USER_ID,
                entity_type="card",
                entity_id="card-1",
                critical_action=CriticalAction.CARD_MOVE_STATUS,
                surface="rest",
            )
    assert exc_info.value.decision.outcome == "deny"
    assert exc_info.value.decision.reason == "full_context_unavailable"
    assert exc_info.value.decision.context_profile == "unavailable"

    async with db_factory() as db:
        with pytest.raises(FullContextUnavailableError) as empty_exc:
            await FullContextCriticalActionGuard(
                db,
                resolvers={"card": RecordingResolver({})},
            ).authorize_and_resolve(
                board_id=board_id,
                actor_id=USER_ID,
                entity_type="card",
                entity_id="card-1",
                critical_action=CriticalAction.CARD_MOVE_STATUS,
                surface="rest",
            )
    assert empty_exc.value.decision.reason == "full_context_unavailable"


async def test_critical_action_registry_covers_required_categories():
    from okto_pulse.core.services.critical_context_guard import (
        CRITICAL_ACTION_REGISTRY,
        CriticalAction,
        critical_actions_for_entity,
        get_critical_action_definition,
    )

    categories = {item.category for item in CRITICAL_ACTION_REGISTRY}
    assert {
        "status_move",
        "validation",
        "evaluation",
        "approval",
        "implementation_start",
        "closeout",
        "cancellation",
        "archive",
        "gate_sensitive_write",
    }.issubset(categories)

    for entity_type in ("card", "spec", "sprint", "ideation", "refinement"):
        assert critical_actions_for_entity(entity_type)

    assert (
        get_critical_action_definition(CriticalAction.SPEC_SUBMIT_EVALUATION)
        is not None
    )
    assert (
        get_critical_action_definition("card.start_implementation").category
        == "implementation_start"
    )

