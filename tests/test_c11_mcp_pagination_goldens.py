"""C11 MCP pagination goldens (FR13 / AC14 / api_f7b0501b).

The two MCP list tools intentionally keep different wire contracts.  These
tests freeze those contracts while proving that their windows are executed by
``EntityPageService`` instead of the legacy collection-loading services.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mcp_runtime_testing import register_mcp_test_runtime
from okto_pulse.core.application.use_cases.entity_pagination import EntityPageService
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.services.main import (
    BoardService,
    IdeationService,
    RefinementService,
    SpecService,
    SprintService,
    StoryService,
)
from sqlalchemy_test_models import (
    Board,
    Card,
    CardPriority,
    CardStatus,
    CardType,
    Ideation,
    IdeationComplexity,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
    Sprint,
    SprintLaneType,
    SprintStatus,
    Story,
    StoryStatus,
    Topic,
)


ACTOR_ID = "c11-mcp-agent"
STAMP = datetime(2026, 7, 20, 12, 0, 0)


def _id(prefix: str, suffix: str) -> str:
    return f"c11-{prefix}-{suffix}"


def _normalize_timestamps(value):
    """Replace volatile DB timestamp rendering, preserving every wire key."""
    if isinstance(value, list):
        return [_normalize_timestamps(item) for item in value]
    if isinstance(value, dict):
        return {
            key: (
                "<timestamp>"
                if key in {"created_at", "updated_at"}
                else _normalize_timestamps(item)
            )
            for key, item in value.items()
        }
    return value


@pytest.fixture
async def c11_graph(db_factory):
    suffix = uuid.uuid4().hex[:8]
    ids = {
        name: _id(name, suffix)
        for name in (
            "board",
            "topic",
            "topic_zulu",
            "topic_archived",
            "story",
            "story_ready",
            "story_archived",
            "ideation",
            "ideation_done",
            "refinement",
            "refinement_done",
            "spec",
            "spec_done",
            "sprint",
            "sprint_done",
            "card_cancelled",
            "card_done",
            "card_progress",
            "card_ns0",
            "card_ns1",
            "card_archived",
        )
    }

    async with db_factory() as db:
        db.add(
            Board(
                id=ids["board"],
                name="C11 golden board",
                owner_id=ACTOR_ID,
                created_at=STAMP,
                updated_at=STAMP,
            )
        )
        await db.flush()

        db.add_all(
            [
                Topic(
                    id=ids["topic"],
                    board_id=ids["board"],
                    name="Alpha topic",
                    description="Primary topic",
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Topic(
                    id=ids["topic_zulu"],
                    board_id=ids["board"],
                    name="Zulu topic",
                    description="Second topic",
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Topic(
                    id=ids["topic_archived"],
                    board_id=ids["board"],
                    name="Archived topic",
                    description="Hidden by default",
                    archived=True,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Ideation(
                    id=ids["ideation"],
                    board_id=ids["board"],
                    title="Draft ideation",
                    description="Ideation body",
                    problem_statement="A precise problem",
                    complexity=IdeationComplexity.SMALL,
                    status=IdeationStatus.DRAFT,
                    version=2,
                    assignee_id="ideation-owner",
                    labels=["golden"],
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Ideation(
                    id=ids["ideation_done"],
                    board_id=ids["board"],
                    title="Done ideation",
                    status=IdeationStatus.DONE,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Story(
                    id=ids["story"],
                    board_id=ids["board"],
                    topic_id=ids["topic"],
                    title="Draft story",
                    description="Story body",
                    actor="operator",
                    goal="page safely",
                    benefit="bounded payload",
                    labels=["mcp"],
                    status=StoryStatus.DRAFT,
                    assignee_id="story-owner",
                    screen_mockups=[{"id": "sm-c11"}],
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Story(
                    id=ids["story_ready"],
                    board_id=ids["board"],
                    topic_id=ids["topic"],
                    title="Ready story",
                    description="Second active story",
                    status=StoryStatus.READY,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Story(
                    id=ids["story_archived"],
                    board_id=ids["board"],
                    topic_id=ids["topic"],
                    title="Archived story",
                    description="Counted by topic aggregation",
                    status=StoryStatus.DRAFT,
                    archived=True,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Refinement(
                    id=ids["refinement"],
                    ideation_id=ids["ideation"],
                    board_id=ids["board"],
                    title="Draft refinement",
                    description="Refinement body",
                    in_scope=["pagination"],
                    out_of_scope=["unrelated"],
                    status=RefinementStatus.DRAFT,
                    version=3,
                    assignee_id="refinement-owner",
                    labels=["golden"],
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Refinement(
                    id=ids["refinement_done"],
                    ideation_id=ids["ideation"],
                    board_id=ids["board"],
                    title="Done refinement",
                    status=RefinementStatus.DONE,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Spec(
                    id=ids["spec"],
                    board_id=ids["board"],
                    ideation_id=ids["ideation"],
                    refinement_id=ids["refinement"],
                    title="Draft spec",
                    description="Spec body",
                    status=SpecStatus.DRAFT,
                    version=4,
                    assignee_id="spec-owner",
                    labels=["golden"],
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Spec(
                    id=ids["spec_done"],
                    board_id=ids["board"],
                    title="Done spec",
                    status=SpecStatus.DONE,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Sprint(
                    id=ids["sprint"],
                    board_id=ids["board"],
                    spec_id=ids["spec"],
                    title="Draft sprint",
                    description="Sprint body",
                    status=SprintStatus.DRAFT,
                    lane_type=SprintLaneType.NORMAL,
                    spec_version=4,
                    test_scenario_ids=["ts-c11"],
                    business_rule_ids=["br-c11"],
                    labels=["golden"],
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
                Sprint(
                    id=ids["sprint_done"],
                    board_id=ids["board"],
                    spec_id=ids["spec"],
                    title="Done sprint",
                    status=SprintStatus.CLOSED,
                    lane_type=SprintLaneType.NORMAL,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                ),
            ]
        )
        await db.flush()

        card_rows = (
            ("card_cancelled", CardStatus.CANCELLED, 0, False),
            ("card_done", CardStatus.DONE, 0, False),
            ("card_progress", CardStatus.IN_PROGRESS, 0, False),
            ("card_ns0", CardStatus.NOT_STARTED, 0, False),
            ("card_ns1", CardStatus.NOT_STARTED, 1, False),
            ("card_archived", CardStatus.STARTED, 0, True),
        )
        for index, (key, status, position, archived) in enumerate(card_rows):
            db.add(
                Card(
                    id=ids[key],
                    board_id=ids["board"],
                    spec_id=ids["spec"],
                    title=f"Card {key.removeprefix('card_')}",
                    description=f"Description {key}",
                    status=status,
                    priority=CardPriority.HIGH,
                    card_type=CardType.NORMAL,
                    position=position,
                    assignee_id="card-owner",
                    due_date=datetime(2026, 8, index + 1, 9, 30),
                    labels=["golden"],
                    test_scenario_ids=["ts-c11"],
                    archived=archived,
                    created_by=ACTOR_ID,
                    created_at=STAMP,
                    updated_at=STAMP,
                )
            )
        await db.commit()

    return ids


@pytest.fixture
def mcp_call(db_factory, c11_graph):
    register_mcp_test_runtime(db_factory)
    ctx = SimpleNamespace(
        agent_id=ACTOR_ID,
        agent_name="C11 Agent",
        board_id=c11_graph["board"],
        realm_id="local",
        permissions=["*"],
    )

    async def call(tool_name: str, **kwargs):
        with (
            patch.object(
                mcp_server,
                "_get_agent_ctx",
                return_value=ctx,
            ),
            patch.object(mcp_server, "check_permission", return_value=None),
        ):
            tool = await mcp_server.mcp.get_tool(tool_name)
            return json.loads(await tool.fn(**kwargs))

    return call


def _card_golden(ids: dict[str, str], key: str, status: str, position: int) -> dict:
    index = {
        "card_cancelled": 0,
        "card_done": 1,
        "card_progress": 2,
        "card_ns0": 3,
        "card_ns1": 4,
        "card_archived": 5,
    }[key]
    return {
        "id": ids[key],
        "title": f"Card {key.removeprefix('card_')}",
        "description": f"Description {key}",
        "status": status,
        "priority": "high",
        "position": position,
        "assignee_id": "card-owner",
        "spec_id": ids["spec"],
        "test_scenario_ids": ["ts-c11"],
        "due_date": datetime(2026, 8, index + 1, 9, 30).isoformat(),
        "labels": ["golden"],
    }


@pytest.mark.asyncio
async def test_list_cards_by_status_empty_mode_exact_golden_and_archived_total(
    c11_graph,
    mcp_call,
):
    payload = await mcp_call(
        "okto_pulse_list_cards_by_status",
        board_id=c11_graph["board"],
    )

    ordered = (
        ("card_cancelled", "cancelled", 0),
        ("card_done", "done", 0),
        ("card_progress", "in_progress", 0),
        ("card_ns0", "not_started", 0),
        ("card_ns1", "not_started", 1),
        ("card_archived", "started", 0),
    )
    assert payload == {
        "total_all": 6,
        "filtered_count": 6,
        "offset": 0,
        "limit": 50,
        "cards": [
            _card_golden(c11_graph, key, status, position)
            for key, status, position in ordered
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected_keys"),
    [
        (
            "open",
            ["card_progress", "card_ns0", "card_ns1", "card_archived"],
        ),
        ("not_started", ["card_ns0", "card_ns1"]),
        # Archived rows participate in status filtering exactly as before.
        ("started", ["card_archived"]),
    ],
)
async def test_list_cards_by_status_modes_preserve_semantics_and_total_all(
    c11_graph,
    mcp_call,
    status,
    expected_keys,
):
    payload = await mcp_call(
        "okto_pulse_list_cards_by_status",
        board_id=c11_graph["board"],
        status=status,
    )

    assert payload["total_all"] == 6
    assert payload["filtered_count"] == len(expected_keys)
    assert [item["id"] for item in payload["cards"]] == [
        c11_graph[key] for key in expected_keys
    ]


@pytest.mark.asyncio
async def test_list_cards_by_status_window_default_cap_offset_and_order(
    c11_graph,
    mcp_call,
):
    page = await mcp_call(
        "okto_pulse_list_cards_by_status",
        board_id=c11_graph["board"],
        offset=1,
        limit=2,
    )
    capped = await mcp_call(
        "okto_pulse_list_cards_by_status",
        board_id=c11_graph["board"],
        limit=999,
    )

    assert page["offset"] == 1
    assert page["limit"] == 2
    assert page["filtered_count"] == 6
    assert [item["id"] for item in page["cards"]] == [
        c11_graph["card_done"],
        c11_graph["card_progress"],
    ]
    assert capped["limit"] == 200
    assert capped["filtered_count"] == 6


@pytest.mark.asyncio
async def test_list_cards_by_status_optional_filters_are_server_side(
    c11_graph,
    mcp_call,
):
    payload = await mcp_call(
        "okto_pulse_list_cards_by_status",
        board_id=c11_graph["board"],
        status="open",
        spec_id=c11_graph["spec"],
        priority="high",
        assignee_id="card-owner",
    )
    miss = await mcp_call(
        "okto_pulse_list_cards_by_status",
        board_id=c11_graph["board"],
        status="open",
        assignee_id="somebody-else",
    )

    assert payload["total_all"] == 6
    assert payload["filtered_count"] == 4
    assert miss == {
        "total_all": 6,
        "filtered_count": 0,
        "offset": 0,
        "limit": 50,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entity_type", "filters", "expected_total", "item_golden"),
    [
        (
            "spec",
            {"status": "draft"},
            1,
            lambda ids: {
                "id": ids["spec"],
                "title": "Draft spec",
                "description": "Spec body",
                "status": "draft",
                "version": 4,
                "assignee_id": "spec-owner",
                "labels": ["golden"],
                "created_at": "<timestamp>",
                "updated_at": "<timestamp>",
            },
        ),
        (
            "ideation",
            {"status": "draft"},
            1,
            lambda ids: {
                "id": ids["ideation"],
                "title": "Draft ideation",
                "description": "Ideation body",
                "problem_statement": "A precise problem",
                "complexity": "small",
                "status": "draft",
                "active_refinement_count": 2,
                "active_spec_count": 0,
                "derivation_pending": False,
                "version": 2,
                "assignee_id": "ideation-owner",
                "labels": ["golden"],
                "created_at": "<timestamp>",
                "updated_at": "<timestamp>",
            },
        ),
        (
            "refinement",
            {"ideation_id": "__IDEATION__", "status": "draft"},
            1,
            lambda ids: {
                "id": ids["refinement"],
                "title": "Draft refinement",
                "description": "Refinement body",
                "in_scope": ["pagination"],
                "out_of_scope": ["unrelated"],
                "status": "draft",
                "active_spec_count": 1,
                "derivation_pending": False,
                "version": 3,
                "assignee_id": "refinement-owner",
                "labels": ["golden"],
                "created_at": "<timestamp>",
                "updated_at": "<timestamp>",
            },
        ),
        (
            "sprint",
            {"spec_id": "__SPEC__", "status": "draft"},
            1,
            lambda ids: {
                "id": ids["sprint"],
                "title": "Draft sprint",
                "status": "draft",
                "lane_type": "normal",
                "origin_sprint_id": None,
                "origin_bug_id": None,
                "normal_sprint_created": True,
                "spec_version": 4,
                "test_scenario_ids": ["ts-c11"],
                "business_rule_ids": ["br-c11"],
                "labels": ["golden"],
            },
        ),
        (
            "story",
            {"status": "draft"},
            1,
            lambda ids: {
                "id": ids["story"],
                "board_id": ids["board"],
                "topic_id": ids["topic"],
                "title": "Draft story",
                "description": "Story body",
                "actor": "operator",
                "goal": "page safely",
                "benefit": "bounded payload",
                "labels": ["mcp"],
                "status": "draft",
                "assignee_id": "story-owner",
                "screen_mockups": [{"id": "sm-c11"}],
                "archived": False,
                "ideation_links": [],
                "created_at": "<timestamp>",
                "updated_at": "<timestamp>",
            },
        ),
        (
            "topic",
            {},
            2,
            lambda ids: {
                "id": ids["topic"],
                "board_id": ids["board"],
                "name": "Alpha topic",
                "description": "Primary topic",
                "archived": False,
                "story_count": 2,
                "active_count": 2,
                "archived_count": 1,
                "total_associated_count": 3,
                "created_by": ACTOR_ID,
                "created_at": "<timestamp>",
                "updated_at": "<timestamp>",
            },
        ),
    ],
    ids=("spec", "ideation", "refinement", "sprint", "story", "topic"),
)
async def test_list_by_board_per_entity_exact_golden_with_additive_total_overall(
    c11_graph,
    mcp_call,
    entity_type,
    filters,
    expected_total,
    item_golden,
):
    filters = {
        key: (
            c11_graph["ideation"]
            if value == "__IDEATION__"
            else c11_graph["spec"]
            if value == "__SPEC__"
            else value
        )
        for key, value in filters.items()
    }
    payload = _normalize_timestamps(
        await mcp_call(
            "okto_pulse_list_by_board",
            board_id=c11_graph["board"],
            entity_type=entity_type,
            filters=filters,
            limit=1,
        )
    )
    expected = {
        "board_id": c11_graph["board"],
        "entity_type": entity_type,
        "total": expected_total,
        "total_overall": 2,
        "offset": 0,
        "limit": 1,
        "items": [item_golden(c11_graph)],
    }
    if entity_type == "refinement":
        expected["ideation_id"] = c11_graph["ideation"]
    if entity_type == "sprint":
        expected["spec_id"] = c11_graph["spec"]
    assert payload == expected


@pytest.mark.asyncio
async def test_list_by_board_window_default_cap_offset_and_topic_order(
    c11_graph,
    mcp_call,
):
    default = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="spec",
        filters={"status": "draft"},
    )
    capped = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="spec",
        filters={"status": "draft"},
        limit=999,
    )
    second_topic = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="topic",
        filters={},
        offset=1,
        limit=1,
    )

    assert default["limit"] == 100
    assert capped["limit"] == 200
    assert second_topic["offset"] == 1
    assert second_topic["total"] == 2
    assert second_topic["total_overall"] == 2
    assert [item["id"] for item in second_topic["items"]] == [c11_graph["topic_zulu"]]


@pytest.mark.asyncio
async def test_list_by_board_include_archived_expands_filtered_and_overall_totals(
    c11_graph,
    mcp_call,
):
    stories = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="story",
        filters={"include_archived": True},
    )
    topics = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="topic",
        filters={"include_archived": True},
    )

    assert (stories["total"], stories["total_overall"]) == (3, 3)
    assert any(item["id"] == c11_graph["story_archived"] for item in stories["items"])
    assert (topics["total"], topics["total_overall"]) == (3, 3)
    assert any(item["id"] == c11_graph["topic_archived"] for item in topics["items"])


@pytest.mark.asyncio
async def test_ideation_derivation_false_includes_done_with_null_complexity(
    c11_graph,
    mcp_call,
):
    not_pending = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="ideation",
        filters={"derivation_pending": False},
    )
    pending = await mcp_call(
        "okto_pulse_list_by_board",
        board_id=c11_graph["board"],
        entity_type="ideation",
        filters={"derivation_pending": True},
    )

    assert c11_graph["ideation_done"] in {
        item["id"] for item in not_pending["items"]
    }
    assert pending["total"] == 0
    assert pending["items"] == []


@pytest.mark.asyncio
async def test_both_mcp_tools_execute_through_entity_page_service_without_full_fetch(
    c11_graph,
    mcp_call,
):
    requests = []
    aggregate_requests = []
    original_list = EntityPageService.list
    original_group_count = EntityPageService.group_count

    async def recording_list(self, request):
        requests.append(request)
        return await original_list(self, request)

    async def recording_group_count(self, request):
        aggregate_requests.append(request)
        return await original_group_count(self, request)

    async def legacy_full_fetch_forbidden(*args, **kwargs):
        raise AssertionError("legacy MCP full-fetch path was called")

    with (
        patch.object(EntityPageService, "list", recording_list),
        patch.object(EntityPageService, "group_count", recording_group_count),
        patch.object(
            BoardService,
            "get_board",
            legacy_full_fetch_forbidden,
        ),
        patch.object(
            SpecService,
            "list_specs",
            legacy_full_fetch_forbidden,
        ),
        patch.object(
            IdeationService,
            "list_ideations",
            legacy_full_fetch_forbidden,
        ),
        patch.object(
            RefinementService,
            "list_refinements",
            legacy_full_fetch_forbidden,
        ),
        patch.object(
            SprintService,
            "list_sprints",
            legacy_full_fetch_forbidden,
        ),
        patch.object(
            StoryService,
            "list_stories",
            legacy_full_fetch_forbidden,
        ),
        patch.object(
            StoryService,
            "list_topics",
            legacy_full_fetch_forbidden,
        ),
    ):
        cards = await mcp_call(
            "okto_pulse_list_cards_by_status",
            board_id=c11_graph["board"],
            status="open",
            offset=1,
            limit=2,
        )
        for entity_type, filters in (
            ("spec", {"status": "draft"}),
            ("ideation", {"status": "draft"}),
            (
                "refinement",
                {"ideation_id": c11_graph["ideation"], "status": "draft"},
            ),
            ("sprint", {"spec_id": c11_graph["spec"], "status": "draft"}),
            ("story", {"status": "draft"}),
            ("topic", {}),
        ):
            await mcp_call(
                "okto_pulse_list_by_board",
                board_id=c11_graph["board"],
                entity_type=entity_type,
                filters=filters,
                offset=1,
                limit=2,
            )

    assert cards["filtered_count"] == 4
    assert len(requests) == 7
    assert requests[0].surface == "mcp_card_status_list"
    assert requests[0].offset == 1
    assert requests[0].limit == 2
    assert {request.offset for request in requests[1:]} == {1}
    assert {request.limit for request in requests[1:]} == {2}
    assert all(request.surface.startswith("mcp_") for request in requests)
    # The page contains one Topic and still uses exactly one board-wide count,
    # never one Story query per returned Topic.
    assert [request.surface for request in aggregate_requests] == ["topic_story_counts"]
