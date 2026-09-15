"""Positive semantic oracles for Discovery's relational cards.

These tests exercise the real dispatcher against the neutral read port. Scope
assertions verify the reader requests; SQL adapter filtering is a separate layer.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.discovery_intent_catalog import DEFAULT_DISCOVERY_INTENTS
from okto_pulse.core.domain.enums import CardPriority, CardStatus, SprintStatus
from okto_pulse.core.ports.discovery_execution import (
    DiscoveryActivityFact,
    DiscoveryCardFact,
    DiscoveryDependencyFact,
    DiscoveryDependentCardFact,
    DiscoveryExecutionReadPort,
    DiscoveryMentionFact,
    DiscoverySpecFact,
    DiscoverySprintFact,
)
from okto_pulse.core.services import discovery_executor


BOARD = "semantic-board"
USER = "semantic-user"
NOW = datetime.now(timezone.utc)


def _card(card_id, *, status=CardStatus.IN_PROGRESS, sprint_id=None, **overrides):
    values = dict(
        id=card_id, board_id=BOARD, title=f"Title {card_id}", status=status,
        priority=CardPriority.HIGH, spec_id=None, sprint_id=sprint_id,
        archived=False, updated_at=NOW,
    )
    values.update(overrides)
    return DiscoveryCardFact(**values)


def _spec(spec_id, scenarios):
    return DiscoverySpecFact(
        id=spec_id, board_id=BOARD, title=f"Title {spec_id}", status="approved",
        version=1, functional_requirements=(), business_rules=(),
        technical_requirements=(), decisions=(), acceptance_criteria=(),
        api_contracts=(), integration_requirements=(), observability_requirements=(),
        test_scenarios=tuple(scenarios), skip_rules_coverage=False,
        skip_test_coverage=False, skip_trs_coverage=False, skip_contract_coverage=False,
        skip_ir_coverage=False, skip_or_coverage=False, skip_decisions_coverage=False,
        skip_code_evidence_coverage=False,
    )


@pytest.fixture
def reader(monkeypatch):
    port = AsyncMock(spec=DiscoveryExecutionReadPort)
    monkeypatch.setattr(discovery_executor, "get_discovery_execution_read_port", lambda: port)
    return port


async def _execute(name, *, params=None, user_id=USER, board_id=BOARD):
    seed = next(row for row in DEFAULT_DISCOVERY_INTENTS if row["name"] == name)
    return await discovery_executor.execute_intent(
        db=None, user_id=user_id, board_id=board_id,
        intent=SimpleNamespace(**seed), params=params or {},
    )


@pytest.mark.asyncio
async def test_recent_activity_preserves_order_and_resolves_entity_navigation(reader):
    reader.list_recent_activity.return_value = (
        DiscoveryActivityFact(
            "activity-spec", "spec_moved",
            {"spec_id": "spec-1", "from_status": "draft", "to_status": "review"},
            "incidental-card", USER, "human", "Ada", NOW,
        ),
        DiscoveryActivityFact(
            "activity-card", "card_updated", {}, "card-1", USER, "human", None,
            NOW - timedelta(hours=1),
        ),
        DiscoveryActivityFact(
            "activity-board", "board_created", {}, None, USER, "human", "Ada", None,
        ),
    )
    reader.resolve_entity_titles.return_value = {
        ("spec", "spec-1"): "Checkout specification",
        ("card", "card-1"): "Build checkout",
    }

    result = await _execute("recent_activity")

    assert [row["id"] for row in result["rows"]] == [
        "activity-spec", "activity-card", "activity-board",
    ]
    spec, card, board = result["rows"]
    assert spec["title"] == "Checkout specification"
    assert spec["meta"]["entity_type"] == "spec"
    assert spec["meta"]["entity_id"] == "spec-1"
    assert "draft → review" in spec["summary"]
    assert card["title"] == "Build checkout"
    assert card["meta"]["entity_id"] == "card-1"
    assert board["meta"]["entity_id"] is None
    assert board["meta"]["entity_type"] is None
    assert result["total"] == 3
    reader.list_recent_activity.assert_awaited_once_with(None, board_id=BOARD, limit=50)
    reader.resolve_entity_titles.assert_awaited_once_with(
        None, refs=[("spec", "spec-1"), ("card", "card-1")],
    )


@pytest.mark.asyncio
async def test_recent_activity_redacts_task_validation_details(reader):
    reader.list_recent_activity.return_value = (
        DiscoveryActivityFact(
            "validation", "validation_submitted", {"private_evidence": "secret"},
            "card-1", USER, "human", "Ada", NOW,
        ),
    )
    reader.resolve_entity_titles.return_value = {("card", "card-1"): "Build checkout"}

    result = await _execute("recent_activity")

    row = result["rows"][0]
    assert row["id"] == "validation"
    assert row["meta"]["entity_id"] == "card-1"
    assert row["meta"]["details"] == {"redacted": True}
    assert "secret" not in repr(result)


@pytest.mark.asyncio
async def test_my_mentions_uses_authenticated_user_and_preserves_comment_identity(reader):
    comment = f"@{USER} please review " + "x" * 240
    reader.list_mentions.return_value = (
        DiscoveryMentionFact("comment-2", comment, "author-2", NOW, _card("card-2")),
        DiscoveryMentionFact("comment-1", f"Thanks @{USER}", "author-1", None, _card("card-1")),
    )

    result = await _execute("my_mentions")

    assert [row["id"] for row in result["rows"]] == ["comment-2", "comment-1"]
    assert result["total"] == 2
    first = result["rows"][0]
    assert first["title"] == "Title card-2"
    assert first["summary"] == comment[:200]
    assert first["meta"]["comment_id"] == "comment-2"
    assert first["meta"]["entity_id"] == "card-2"
    assert first["meta"]["author_id"] == "author-2"
    reader.list_mentions.assert_awaited_once_with(
        None, board_id=BOARD, mention_token=f"@{USER}", limit=50,
    )


@pytest.mark.asyncio
async def test_dependencies_returns_cards_waiting_on_selected_card(reader):
    reader.get_card_by_id.return_value = _card("foundation")
    reader.list_card_dependents.return_value = tuple(
        DiscoveryDependentCardFact(
            DiscoveryDependencyFact(card_id, "foundation", NOW), _card(card_id),
        )
        for card_id in ("consumer-a", "consumer-b")
    )

    result = await _execute(
        "dependencies_of_card", params={"card_id": {"entity_type": "card", "id": "foundation"}},
    )

    assert [row["id"] for row in result["rows"]] == ["consumer-a", "consumer-b"]
    assert result["params_echo"] == {"card_id": "foundation"}
    assert result["total"] == 2
    assert [row["meta"]["entity_id"] for row in result["rows"]] == ["consumer-a", "consumer-b"]
    assert all(row["meta"]["card_status"] == "in_progress" for row in result["rows"])
    reader.get_card_by_id.assert_awaited_once_with(None, card_id="foundation")
    reader.list_card_dependents.assert_awaited_once_with(None, card_id="foundation")
    reader.list_dependencies_for_cards.assert_not_awaited()


@pytest.mark.asyncio
async def test_dependency_selector_cannot_read_another_boards_card(reader):
    reader.get_card_by_id.return_value = _card("private-card", board_id="other-board")

    with pytest.raises(discovery_executor.DiscoverySelectorExecutionError) as caught:
        await _execute("dependencies_of_card", params={"card_id": "private-card"})

    assert caught.value.code == "selector_reference_forbidden"
    assert caught.value.status_code == 403
    reader.list_card_dependents.assert_not_awaited()


@pytest.mark.asyncio
async def test_scenarios_without_tasks_excludes_linked_scenarios_across_specs(reader):
    reader.list_specs.return_value = (
        _spec("spec-a", [
            {"id": "unlinked-a", "title": "Missing implementation", "linked_task_ids": []},
            {"id": "linked-a", "title": "Has implementation", "linked_task_ids": ["card-a"]},
            "legacy malformed scenario",
        ]),
        _spec("spec-b", [
            {"id": "unlinked-b", "title": "No links field", "status": "ready"},
            {"id": "linked-b", "title": "Has task", "linked_task_ids": ["card-b"]},
        ]),
    )

    result = await _execute("scenarios_without_tasks")

    assert [row["id"] for row in result["rows"]] == ["unlinked-a", "unlinked-b"]
    assert result["total"] == 2
    assert [row["meta"]["spec_id"] for row in result["rows"]] == ["spec-a", "spec-b"]
    assert all(row["meta"]["linked_task_ids"] == [] for row in result["rows"])
    assert all(row["meta"]["entity_type"] == "spec" for row in result["rows"])
    reader.list_specs.assert_awaited_once_with(None, board_id=BOARD)


@pytest.mark.asyncio
async def test_current_sprint_blockers_distinguish_unresolved_hold_rejected_and_stale(reader):
    reader.list_sprints.return_value = (
        DiscoverySprintFact("sprint-active", BOARD, "Current delivery", SprintStatus.ACTIVE),
        DiscoverySprintFact("sprint-closed", BOARD, "Previous delivery", SprintStatus.CLOSED),
        DiscoverySprintFact("sprint-draft", BOARD, "Future delivery", SprintStatus.DRAFT),
    )
    cards = (
        _card("blocked", sprint_id="sprint-active"),
        _card("hold", status=CardStatus.ON_HOLD, sprint_id="sprint-active"),
        _card("rejected", status=CardStatus.REJECTED, sprint_id="sprint-active"),
        _card("stale", sprint_id="sprint-active", updated_at=NOW - timedelta(days=10)),
        _card("healthy", sprint_id="sprint-active"),
        _card("done", status=CardStatus.DONE, sprint_id="sprint-active"),
        _card("cancelled", status=CardStatus.CANCELLED, sprint_id="sprint-active"),
    )
    reader.list_cards_for_sprints.return_value = cards
    reader.list_dependencies_for_cards.return_value = (
        DiscoveryDependencyFact("blocked", "external-pending", NOW),
        DiscoveryDependencyFact("blocked", "external-done", NOW),
        DiscoveryDependencyFact("blocked", "missing-target", NOW),
        DiscoveryDependencyFact("healthy", "external-done", NOW),
        DiscoveryDependencyFact("done", "external-pending", NOW),
        DiscoveryDependencyFact("cancelled", "external-pending", NOW),
    )
    reader.list_cards_by_ids.return_value = (
        _card("external-pending"), _card("external-done", status=CardStatus.DONE),
    )

    result = await _execute("blockers_current_sprint")

    assert [(row["id"], row["type"]) for row in result["rows"]] == [
        ("blocked", "blocked_card"), ("hold", "on_hold_card"),
        ("rejected", "rejected_card"), ("stale", "stale_card"),
    ]
    assert result["total"] == 4
    assert result["active_sprint_ids"] == ["sprint-active"]
    assert result["summary"] == {
        "blocked_card": 1, "on_hold_card": 1, "rejected_card": 1, "stale_card": 1,
    }
    assert result["rows"][0]["meta"]["blocking_cards"] == [
        {"id": "external-pending", "title": "Title external-pending", "status": "in_progress"},
        {"id": "missing-target", "title": None, "status": None},
    ]
    assert all(row["meta"]["sprint_id"] == "sprint-active" for row in result["rows"])
    assert all(row["meta"]["entity_id"] == row["id"] for row in result["rows"])
    assert result["rows"][-1]["meta"]["age_hours"] >= 240
    reader.list_sprints.assert_awaited_once_with(None, board_id=BOARD)
    reader.list_cards_for_sprints.assert_awaited_once_with(
        None, board_id=BOARD, sprint_ids=["sprint-active"],
    )
    reader.list_dependencies_for_cards.assert_awaited_once_with(
        None, card_ids=[card.id for card in cards],
    )
    assert set(reader.list_cards_by_ids.await_args.kwargs["card_ids"]) == {
        "external-pending", "external-done", "missing-target",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [
    "recent_activity", "my_mentions", "dependencies_of_card",
    "scenarios_without_tasks", "blockers_current_sprint",
])
async def test_relational_cards_meaningful_empty_results(reader, name):
    reader.list_recent_activity.return_value = ()
    reader.resolve_entity_titles.return_value = {}
    reader.list_mentions.return_value = ()
    reader.get_card_by_id.return_value = _card("independent-card")
    reader.list_card_dependents.return_value = ()
    reader.list_specs.return_value = (
        _spec("fully-linked", [{"id": "covered", "linked_task_ids": ["card-1"]}]),
    )
    reader.list_sprints.return_value = (
        DiscoverySprintFact("closed", BOARD, "Closed sprint", SprintStatus.CLOSED),
    )
    params = {"card_id": "independent-card"} if name == "dependencies_of_card" else {}

    result = await _execute(name, params=params)

    assert result["rows"] == []
    assert result["total"] == 0
    assert result["execution"] == "real_tool"
    if name == "blockers_current_sprint":
        assert "No active sprint" in result["message"]
        reader.list_cards_for_sprints.assert_not_awaited()
        reader.list_dependencies_for_cards.assert_not_awaited()
