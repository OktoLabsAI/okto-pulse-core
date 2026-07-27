"""C10 contract tests for topic Story counts through the GROUP BY port."""

from __future__ import annotations

from typing import Any

import pytest

from okto_pulse.core.application.use_cases.entity_pagination import (
    EntityPageService,
)
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationGroupCount,
    ApplicationGroupCountQuery,
    GroupCountRequest,
    get_application_persistence_port,
    register_application_persistence_port,
    reset_application_persistence_port_for_tests,
)
from okto_pulse.core.services.main import StoryService


class _GroupCountOnlyPort:
    """Port double that fails if count code tries to hydrate Story rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ApplicationGroupCountQuery]] = []

    async def group_count(
        self, context: Any, query: ApplicationGroupCountQuery
    ) -> tuple[ApplicationGroupCount, ...]:
        self.calls.append((context, query))
        if query.group_by == ("topic_id", "archived"):
            return (
                ApplicationGroupCount(("topic-a", False), 2),
                ApplicationGroupCount(("topic-a", True), 1),
                ApplicationGroupCount(("topic-b", True), 3),
            )
        if query.group_by == ("archived",):
            return (
                ApplicationGroupCount((False,), 2),
                ApplicationGroupCount((True,), 1),
            )
        raise AssertionError(f"unexpected group_by: {query.group_by!r}")

    async def list(self, _context: Any, _query: Any) -> tuple[Any, ...]:
        raise AssertionError("topic Story counts must not hydrate Story rows")


@pytest.fixture
def group_count_port() -> _GroupCountOnlyPort:
    try:
        previous = get_application_persistence_port()
    except Exception:  # noqa: BLE001 - the process port may be unset
        previous = None
    port = _GroupCountOnlyPort()
    register_application_persistence_port(port)
    try:
        yield port
    finally:
        if previous is None:
            reset_application_persistence_port_for_tests()
        else:
            register_application_persistence_port(previous)


async def test_bulk_topic_counts_use_one_catalogued_group_count_query(
    group_count_port: _GroupCountOnlyPort,
) -> None:
    marker = object()
    rows = await EntityPageService(marker).group_count(
        GroupCountRequest(
            surface="topic_story_counts",
            scope=(ApplicationFilter("board_id", "eq", "board-1"),),
            group_by=("topic_id", "archived"),
        )
    )

    assert rows == (
        ApplicationGroupCount(("topic-a", False), 2),
        ApplicationGroupCount(("topic-a", True), 1),
        ApplicationGroupCount(("topic-b", True), 3),
    )
    assert group_count_port.calls == [
        (
            marker,
            ApplicationGroupCountQuery(
                entity="story",
                group_by=("topic_id", "archived"),
                filters=(ApplicationFilter("board_id", "eq", "board-1"),),
            ),
        )
    ]


async def test_individual_topic_counts_use_group_count_without_hydration(
    group_count_port: _GroupCountOnlyPort,
) -> None:
    marker = object()
    counts = await StoryService(marker)._topic_story_counts(
        "topic-a", board_id="board-1"
    )

    assert counts == {
        "active_count": 2,
        "archived_count": 1,
        "total_associated_count": 3,
    }
    assert group_count_port.calls == [
        (
            marker,
            ApplicationGroupCountQuery(
                entity="story",
                group_by=("archived",),
                filters=(
                    ApplicationFilter("board_id", "eq", "board-1"),
                    ApplicationFilter("topic_id", "eq", "topic-a"),
                ),
            ),
        )
    ]


@pytest.mark.parametrize(
    ("scope", "group_by", "error"),
    (
        (
            (ApplicationFilter("topic_id", "eq", "topic-a"),),
            ("archived",),
            "group_count_request_scope_anchor_required",
        ),
        (
            (ApplicationFilter("board_id", "eq", "board-1"),),
            ("topic_id",),
            "group_count_request_group_by_not_allowed",
        ),
    ),
)
async def test_topic_count_surface_rejects_unscoped_or_partial_shapes(
    group_count_port: _GroupCountOnlyPort,
    scope: tuple[ApplicationFilter, ...],
    group_by: tuple[str, ...],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        await EntityPageService(None).group_count(
            GroupCountRequest(
                surface="topic_story_counts",
                scope=scope,
                group_by=group_by,
            )
        )
    assert group_count_port.calls == []
