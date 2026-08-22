"""BG-01.4 — Critical-context guard wiring."""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select


pytestmark = pytest.mark.asyncio

USER_ID = "bg-wiring-user"


async def _seed_board_spec_card(
    db,
    *,
    board_settings: dict | None = None,
    spec_status=None,
):
    from sqlalchemy_test_models import (
        Board,
        Card,
        CardStatus,
        Spec,
        SpecStatus,
    )

    board_id = f"bg-wiring-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bg-wiring-spec-{uuid.uuid4().hex[:8]}"
    card_id = f"bg-wiring-card-{uuid.uuid4().hex[:8]}"

    board = Board(
        id=board_id,
        name="BG Wiring Board",
        owner_id=USER_ID,
        settings=board_settings if board_settings is not None else {},
    )
    spec = Spec(
        id=spec_id,
        board_id=board_id,
        title="Guarded Spec",
        description="Spec used by critical context wiring tests.",
        status=spec_status or SpecStatus.IN_PROGRESS,
        created_by=USER_ID,
        skip_decisions_coverage=True,
        evaluations=[],
    )
    card = Card(
        id=card_id,
        board_id=board_id,
        spec_id=spec_id,
        title="Guarded Card",
        description="Card used by critical context wiring tests.",
        status=CardStatus.NOT_STARTED,
        created_by=USER_ID,
    )
    db.add_all([board, spec, card])
    await db.flush()
    return board, spec, card


async def _critical_logs(db, *, board_id: str, entity_id: str | None = None):
    from sqlalchemy_test_models import ActivityLog
    from okto_pulse.core.services.critical_context_guard import (
        CRITICAL_CONTEXT_DECISION_ACTION,
    )

    query = select(ActivityLog).where(
        ActivityLog.board_id == board_id,
        ActivityLog.action == CRITICAL_CONTEXT_DECISION_ACTION,
    )
    if entity_id:
        query = query.where(ActivityLog.details["entity_id"].as_string() == entity_id)
    result = await db.execute(query.order_by(ActivityLog.created_at.asc()))
    return list(result.scalars().all())


async def test_card_move_resolves_full_context_before_status_mutation(db_factory):
    from sqlalchemy_test_models import Card, CardStatus
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService

    async with db_factory() as db:
        board, _spec, card = await _seed_board_spec_card(db)
        board_id = board.id
        card_id = card.id
        await db.commit()

    async with db_factory() as db:
        moved = await CardService(db).move_card(
            card_id,
            USER_ID,
            CardMove(status=CardStatus.STARTED),
            actor_name="BG Wiring User",
        )
        assert moved is not None
        await db.commit()

    async with db_factory() as db:
        stored = await db.get(Card, card_id)
        assert stored is not None
        assert stored.status == CardStatus.STARTED

        logs = await _critical_logs(db, board_id=board_id, entity_id=card_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["entity_type"] == "card"
        assert details["critical_action"] == "card.move_status"
        assert details["outcome"] == "allow"
        assert details["reason"] == "full_context_resolved"
        assert details["context_profile"] == "full"
        assert details["context_fingerprint"].startswith("ctx_sha256_v1:")
        assert "Guarded Card" not in details["context_fingerprint"]
        assert "description" not in details


async def test_create_card_resolves_parent_spec_context_before_insert(db_factory):
    from sqlalchemy_test_models import Card
    from okto_pulse.core.models.schemas import CardCreate
    from okto_pulse.core.services.main import CardService

    async with db_factory() as db:
        board, spec, _card = await _seed_board_spec_card(db)
        board_id = board.id
        spec_id = spec.id
        await db.commit()

    async with db_factory() as db:
        created = await CardService(db).create_card(
            board_id,
            USER_ID,
            CardCreate(
                spec_id=spec_id,
                title="Guarded New Card",
                description="Created only after full parent spec context is resolved.",
            ),
        )
        assert created is not None
        created_id = created.id
        await db.commit()

    async with db_factory() as db:
        stored = await db.get(Card, created_id)
        assert stored is not None
        assert stored.title == "Guarded New Card"

        logs = await _critical_logs(db, board_id=board_id, entity_id=spec_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["entity_type"] == "spec"
        assert details["entity_id"] == spec_id
        assert details["critical_action"] == "card.create"
        assert details["outcome"] == "allow"
        assert details["reason"] == "full_context_resolved"
        assert details["context_profile"] == "full"
        assert details["context_fingerprint"].startswith("ctx_sha256_v1:")
        assert logs[0].card_id is None


async def test_update_card_resolves_card_context_before_write(db_factory):
    from sqlalchemy_test_models import Card
    from okto_pulse.core.models.schemas import CardUpdate
    from okto_pulse.core.services.main import CardService

    async with db_factory() as db:
        board, _spec, card = await _seed_board_spec_card(db)
        board_id = board.id
        card_id = card.id
        await db.commit()

    async with db_factory() as db:
        updated = await CardService(db).update_card(
            card_id,
            USER_ID,
            CardUpdate(title="Guarded Card Updated"),
        )
        assert updated is not None
        await db.commit()

    async with db_factory() as db:
        stored = await db.get(Card, card_id)
        assert stored is not None
        assert stored.title == "Guarded Card Updated"

        logs = await _critical_logs(db, board_id=board_id, entity_id=card_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["entity_type"] == "card"
        assert details["entity_id"] == card_id
        assert details["critical_action"] == "card.update"
        assert details["outcome"] == "allow"
        assert details["reason"] == "full_context_resolved"
        assert details["context_profile"] == "full"
        assert details["context_fingerprint"].startswith("ctx_sha256_v1:")
        assert "Guarded Card Updated" not in details["context_fingerprint"]


async def test_guard_disabled_records_decision_without_context_fingerprint(db_factory):
    from sqlalchemy_test_models import CardStatus
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService

    async with db_factory() as db:
        board, _spec, card = await _seed_board_spec_card(
            db,
            board_settings={"require_full_context_for_critical_actions": False},
        )
        board_id = board.id
        card_id = card.id
        await db.commit()

    async with db_factory() as db:
        await CardService(db).move_card(
            card_id,
            USER_ID,
            CardMove(status=CardStatus.STARTED),
            actor_name="BG Wiring User",
        )
        await db.commit()

    async with db_factory() as db:
        logs = await _critical_logs(db, board_id=board_id, entity_id=card_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["outcome"] == "allow"
        assert details["reason"] == "guard_disabled"
        assert details["context_profile"] == "not_required"
        assert "context_fingerprint" not in details


async def test_full_context_failure_blocks_card_move_before_mutation(monkeypatch, db_factory):
    from sqlalchemy_test_models import Card, CardStatus
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.critical_context_guard import FullContextUnavailableError
    from okto_pulse.core.services.main import CardService

    class EmptyResolver:
        async def resolve_full_context(self, **_kwargs):
            return {}

    def failing_resolvers(_db):
        return {"card": EmptyResolver()}

    monkeypatch.setattr(
        "okto_pulse.core.services.main.build_default_full_context_resolvers",
        failing_resolvers,
    )

    async with db_factory() as db:
        board, _spec, card = await _seed_board_spec_card(db)
        board_id = board.id
        card_id = card.id
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(FullContextUnavailableError):
            await CardService(db).move_card(
                card_id,
                USER_ID,
                CardMove(status=CardStatus.STARTED),
                actor_name="BG Wiring User",
            )
        await db.commit()

    async with db_factory() as db:
        stored = await db.get(Card, card_id)
        assert stored is not None
        assert stored.status == CardStatus.NOT_STARTED

        logs = await _critical_logs(db, board_id=board_id, entity_id=card_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["outcome"] == "deny"
        assert details["reason"] == "full_context_unavailable"
        assert details["context_profile"] == "unavailable"
        assert "context_fingerprint" not in details


async def test_mcp_spec_evaluation_resolves_full_context_and_appends_evaluation(
    monkeypatch,
    db_factory,
):
    """TC-BG01-05 / ts_d98f3528 — MCP spec eval is guarded behaviorally."""
    from okto_pulse.core.mcp import server as mcp_server
    from sqlalchemy_test_models import Spec, SpecStatus
    from okto_pulse.core.services.critical_context_guard import CriticalAction

    class SpecSpyResolver:
        def __init__(self):
            self.calls: list[dict] = []

        async def resolve_full_context(self, **kwargs):
            self.calls.append(dict(kwargs))
            return {
                "spec": {
                    "id": kwargs["entity_id"],
                    "title": "Guarded Spec",
                    "status": "validated",
                }
            }

    resolver = SpecSpyResolver()

    def spy_resolvers(_db):
        return {"spec": resolver}

    monkeypatch.setattr(
        "okto_pulse.core.services.main.build_default_full_context_resolvers",
        spy_resolvers,
    )

    async with db_factory() as db:
        board, spec, _card = await _seed_board_spec_card(
            db,
            spec_status=SpecStatus.VALIDATED,
        )
        board_id = board.id
        spec_id = spec.id
        await db.commit()

    register_mcp_test_runtime(db_factory)
    ctx = mcp_server.AgentContext(
        USER_ID,
        "BG Wiring Agent",
        board_id,
        permissions=[],
    )

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=ctx),
    ), patch.object(mcp_server, "check_permission", return_value=None):
        raw = await mcp_server.okto_pulse_submit_spec_evaluation.fn(
            board_id=board_id,
            spec_id=spec_id,
            breakdown_completeness=95,
            breakdown_justification="complete enough",
            granularity=95,
            granularity_justification="well split",
            dependency_coherence=95,
            dependency_justification="coherent",
            test_coverage_quality=95,
            test_coverage_justification="covered",
            overall_score=95,
            overall_justification="approved after guarded context resolution",
            recommendation="approve",
        )

    payload = json.loads(raw)
    assert payload["success"] is True
    assert payload["evaluation"]["recommendation"] == "approve"

    assert len(resolver.calls) == 1
    call = resolver.calls[0]
    assert call["board_id"] == board_id
    assert call["entity_type"] == "spec"
    assert call["entity_id"] == spec_id
    assert call["critical_action"] == CriticalAction.SPEC_SUBMIT_EVALUATION

    async with db_factory() as db:
        stored = await db.get(Spec, spec_id)
        assert stored is not None
        assert stored.evaluations is not None
        assert len(stored.evaluations) == 1
        assert stored.evaluations[0]["evaluator_id"] == USER_ID
        assert stored.evaluations[0]["recommendation"] == "approve"

        logs = await _critical_logs(db, board_id=board_id, entity_id=spec_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["entity_type"] == "spec"
        assert details["critical_action"] == "spec.submit_evaluation"
        assert details["surface"] == "mcp"
        assert details["outcome"] == "allow"
        assert details["reason"] == "full_context_resolved"
        assert details["context_profile"] == "full"
        assert details["context_fingerprint"].startswith("ctx_sha256_v1:")


async def test_mcp_spec_evaluation_failure_does_not_append_evaluation(
    monkeypatch,
    db_factory,
):
    """TC-BG01-05 / ts_d98f3528 — guarded failure is pre-mutation."""
    from okto_pulse.core.mcp import server as mcp_server
    from sqlalchemy_test_models import Spec, SpecStatus

    class EmptySpecResolver:
        async def resolve_full_context(self, **_kwargs):
            return {}

    def empty_resolvers(_db):
        return {"spec": EmptySpecResolver()}

    monkeypatch.setattr(
        "okto_pulse.core.services.main.build_default_full_context_resolvers",
        empty_resolvers,
    )

    async with db_factory() as db:
        board, spec, _card = await _seed_board_spec_card(
            db,
            spec_status=SpecStatus.VALIDATED,
        )
        board_id = board.id
        spec_id = spec.id
        await db.commit()

    register_mcp_test_runtime(db_factory)
    ctx = mcp_server.AgentContext(
        USER_ID,
        "BG Wiring Agent",
        board_id,
        permissions=[],
    )

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=ctx),
    ), patch.object(mcp_server, "check_permission", return_value=None):
        raw = await mcp_server.okto_pulse_submit_spec_evaluation.fn(
            board_id=board_id,
            spec_id=spec_id,
            breakdown_completeness=95,
            breakdown_justification="would be complete",
            granularity=95,
            granularity_justification="would be granular",
            dependency_coherence=95,
            dependency_justification="would be coherent",
            test_coverage_quality=95,
            test_coverage_justification="would be covered",
            overall_score=95,
            overall_justification="must not persist on guard denial",
            recommendation="approve",
        )

    payload = json.loads(raw)
    assert payload["reason"] == "full_context_unavailable"
    assert payload["decision"]["outcome"] == "deny"

    async with db_factory() as db:
        stored = await db.get(Spec, spec_id)
        assert stored is not None
        assert stored.evaluations == []

        logs = await _critical_logs(db, board_id=board_id, entity_id=spec_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["critical_action"] == "spec.submit_evaluation"
        assert details["surface"] == "mcp"
        assert details["outcome"] == "deny"
        assert details["reason"] == "full_context_unavailable"
        assert details["context_profile"] == "unavailable"
        assert "context_fingerprint" not in details


async def test_read_only_mcp_paths_do_not_invoke_critical_context_guard(
    monkeypatch,
    db_factory,
):
    """TC-BG01-05 / ts_c13363bb — read-only MCP surfaces stay exempt."""
    from okto_pulse.core.mcp import server as mcp_server

    async with db_factory() as db:
        board, spec, _card = await _seed_board_spec_card(
            db,
            board_settings={"require_full_context_for_critical_actions": True},
        )
        board_id = board.id
        spec_id = spec.id
        await db.commit()

    guard_calls: list[dict] = []

    async def forbidden_guard(*_args, **kwargs):
        guard_calls.append(dict(kwargs))
        raise AssertionError("read-only MCP path unexpectedly invoked critical-context guard")

    monkeypatch.setattr(
        "okto_pulse.core.services.main._authorize_critical_context_or_raise",
        forbidden_guard,
    )

    register_mcp_test_runtime(db_factory)
    ctx = mcp_server.AgentContext(
        USER_ID,
        "BG Wiring Agent",
        board_id,
        permissions=[
            "code_traceability.investigation.read",
            "code_traceability.evidence.read",
            "code_traceability.target.read",
            "code_traceability.overlap.read",
        ],
    )

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=ctx),
    ), patch.object(mcp_server, "check_permission", return_value=None):
        spec_context = json.loads(
            await mcp_server.okto_pulse_get_spec_context.fn(
                board_id=board_id,
                spec_id=spec_id,
                profile="summary",
            )
        )
        listed = json.loads(
            await mcp_server.okto_pulse_list_by_board.fn(
                board_id=board_id,
                entity_type="spec",
            )
        )
        analytics = json.loads(
            await mcp_server.okto_pulse_get_analytics.fn(
                board_id=board_id,
                metric_type="overview",
            )
        )

    assert guard_calls == []
    assert spec_context.get("id") == spec_id, spec_context
    assert spec_context["projection"]["profile"] == "summary"
    assert listed["entity_type"] == "spec"
    assert any(item["id"] == spec_id for item in listed["items"])
    assert analytics["board_id"] == board_id


async def test_story_mutations_are_explicit_non_critical_exclusions():
    from okto_pulse.core.services.critical_context_guard import (
        NON_CRITICAL_MUTATION_EXCLUSIONS,
        critical_actions_for_entity,
    )

    excluded = {(item.service, item.method, item.entity_type) for item in NON_CRITICAL_MUTATION_EXCLUSIONS}
    assert ("StoryService", "move_story", "story") in excluded
    assert ("StoryService", "archive_story", "story") in excluded
    assert ("ArchiveService", "archive_tree", "tree") in excluded
    assert critical_actions_for_entity("story") == ()


async def test_bg01_4_inventory_has_no_silent_service_layer_gaps():
    from okto_pulse.core.services.critical_context_guard import (
        CRITICAL_MUTATION_GUARD_COVERAGE,
        NON_CRITICAL_MUTATION_EXCLUSIONS,
    )

    expected_inventory = {
        ("CardService", "create_card"),
        ("CardService", "update_card"),
        ("CardService", "submit_task_validation"),
        ("CardService", "confirm_amendment_coverage"),
        ("CardService", "move_card"),
        ("SpecService", "move_spec"),
        ("SpecService", "submit_spec_validation"),
        ("IdeationService", "move_ideation"),
        ("RefinementService", "move_refinement"),
        ("SprintService", "move_sprint"),
        ("SprintService", "submit_evaluation"),
        ("StoryService", "move_story"),
        ("StoryService", "archive_story"),
        ("ArchiveService", "archive_tree"),
    }
    guarded = {(item.service, item.method) for item in CRITICAL_MUTATION_GUARD_COVERAGE}
    excluded = {(item.service, item.method) for item in NON_CRITICAL_MUTATION_EXCLUSIONS}

    assert guarded.isdisjoint(excluded)
    assert guarded | excluded == expected_inventory
    assert len(guarded) == len(CRITICAL_MUTATION_GUARD_COVERAGE)
    assert len(excluded) == len(NON_CRITICAL_MUTATION_EXCLUSIONS)
    assert all(item.critical_action.value for item in CRITICAL_MUTATION_GUARD_COVERAGE)
    assert all(item.context_entity_type for item in CRITICAL_MUTATION_GUARD_COVERAGE)
    assert all(item.reason for item in CRITICAL_MUTATION_GUARD_COVERAGE)
    assert all(item.reason for item in NON_CRITICAL_MUTATION_EXCLUSIONS)
