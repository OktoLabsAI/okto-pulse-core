"""Focused Core contract tests for the typed GROUP BY count primitive."""

from __future__ import annotations

from typing import Any

import pytest

from okto_pulse.core.application.use_cases.entity_pagination import (
    EntityPageService,
    group_count_entities,
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


class _CapturingPort:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ApplicationGroupCountQuery]] = []

    async def group_count(
        self, context: Any, query: ApplicationGroupCountQuery
    ) -> tuple[ApplicationGroupCount, ...]:
        self.calls.append((context, query))
        return (ApplicationGroupCount(("started", "normal"), 3),)


@pytest.fixture
def capturing_port() -> _CapturingPort:
    try:
        previous = get_application_persistence_port()
    except Exception:  # noqa: BLE001 - the process port may be unset
        previous = None
    port = _CapturingPort()
    register_application_persistence_port(port)
    try:
        yield port
    finally:
        if previous is None:
            reset_application_persistence_port_for_tests()
        else:
            register_application_persistence_port(previous)


def _request(**overrides: Any) -> GroupCountRequest:
    base: dict[str, Any] = {
        "surface": "kanban_facets",
        "scope": (
            ApplicationFilter("board_id", "eq", "b1"),
            ApplicationFilter("archived", "is_false", None),
        ),
        "group_by": ("status", "card_type"),
    }
    base.update(overrides)
    return GroupCountRequest(**base)


async def test_group_count_preserves_independent_disjunction_dimensions(
    capturing_port: _CapturingPort,
) -> None:
    marker = object()
    disjunctions = (
        (
            (ApplicationFilter("spec_id", "eq", "s1"),),
            (ApplicationFilter("spec_id", "is_none", None),),
        ),
        (
            (ApplicationFilter("title", "ilike", "%needle%"),),
            (ApplicationFilter("description", "ilike", "%needle%"),),
        ),
        (
            (
                ApplicationFilter("status", "eq", "started"),
                ApplicationFilter("card_type", "in", ("normal", "test")),
            ),
            (ApplicationFilter("status", "in", ("done", "cancelled")),),
        ),
    )
    rows = await EntityPageService(marker).group_count(
        _request(
            filters=(ApplicationFilter("assignee_id", "eq", "agent-1"),),
            disjunctions=disjunctions,
        )
    )

    assert rows == (ApplicationGroupCount(("started", "normal"), 3),)
    assert len(capturing_port.calls) == 1
    context, query = capturing_port.calls[0]
    assert context is marker
    assert query == ApplicationGroupCountQuery(
        entity="card",
        group_by=("status", "card_type"),
        filters=(
            ApplicationFilter("board_id", "eq", "b1"),
            ApplicationFilter("archived", "is_false", None),
            ApplicationFilter("assignee_id", "eq", "agent-1"),
        ),
        disjunctions=disjunctions,
    )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"group_by": ()}, "group_count_request_group_by_not_allowed"),
        (
            {"group_by": ("card_type", "status")},
            "group_count_request_group_by_not_allowed",
        ),
        ({"scope": ()}, "group_count_request_scope_required"),
        (
            {"scope": (ApplicationFilter("status", "eq", "started"),)},
            "group_count_request_scope_anchor_required",
        ),
        (
            {"disjunctions": ((),)},
            "group_count_request_disjunction_empty",
        ),
        (
            {"disjunctions": (((),),)},
            "group_count_request_branch_empty",
        ),
        (
            {
                "disjunctions": (
                    ((ApplicationFilter("status", "eq", "warp"),),),
                )
            },
            "group_count_request_filter_operator_not_allowed",
        ),
        (
            {"filters": (ApplicationFilter("owner_id", "eq", "u"),)},
            "group_count_request_filter_field_not_allowed",
        ),
    ],
)
async def test_group_count_contract_rejects_fail_open_shapes(
    capturing_port: _CapturingPort,
    overrides: dict[str, Any],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        await group_count_entities(None, _request(**overrides))
    assert capturing_port.calls == []


async def test_group_count_normalizes_one_shot_containers(
    capturing_port: _CapturingPort,
) -> None:
    dimension = (
        branch
        for branch in (
            (ApplicationFilter("spec_id", "eq", "s1"),),
            (ApplicationFilter("spec_id", "is_none", None),),
        )
    )
    await group_count_entities(
        None,
        _request(
            group_by=(field for field in ("assignee_id",)),
            filters=(
                item
                for item in (ApplicationFilter("card_type", "eq", "normal"),)
            ),
            disjunctions=(dimension for dimension in (dimension,)),
        ),
    )
    query = capturing_port.calls[0][1]
    assert query.group_by == ("assignee_id",)
    assert query.filters[-1] == ApplicationFilter("card_type", "eq", "normal")
    assert len(query.disjunctions[0]) == 2
