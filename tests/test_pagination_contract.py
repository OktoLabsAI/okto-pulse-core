"""Unit tests for the Core pagination contract (card C1 — SURFACE model, FR0/FR3).

Exercises ``list_entities_page`` and ``ListEntityUseCase`` over the SURFACE
catalog: each surface pins one entity and ONE canonical order applied BY the
executor (callers cannot choose an ordering — the field no longer exists on
``PageRequest``). Covers the round-6 verdict (val_7e9b2577): per-surface
discrimination (card_list vs kanban_column vs lookups) and the fail-open
repros — ``status eq ''``/``'warp'``, ``status IN ('',)``, ``spec_id IN
('',)`` and ``any_groups=((),)`` are all rejected fail-closed.
"""

from __future__ import annotations

from typing import Any

import pytest

from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationQuery,
    ApplicationRecord,
    PageRequest,
    PageResult,
    get_application_persistence_port,
    register_application_persistence_port,
    reset_application_persistence_port_for_tests,
)
from okto_pulse.core.services.main import list_entities_page


class _InMemoryPort:
    """Minimal ApplicationPersistencePort over a list of dicts."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    @staticmethod
    def _one(row: dict[str, Any], item: ApplicationFilter) -> bool:
        if item.operator == "eq":
            return row.get(item.field) == item.value
        if item.operator == "in":
            return row.get(item.field) in tuple(item.value)
        return False

    def _match(self, row: dict[str, Any], query: ApplicationQuery) -> bool:
        if not all(self._one(row, item) for item in query.filters):
            return False
        if query.any_filters and not any(
            self._one(row, item) for item in query.any_filters
        ):
            return False
        if query.any_groups and not any(
            all(self._one(row, item) for item in group)
            for group in query.any_groups
        ):
            return False
        return True

    def _select(self, query: ApplicationQuery) -> list[dict[str, Any]]:
        matched = [r for r in self._rows if self._match(r, query)]
        for field_name, descending in reversed(query.order_by):
            matched.sort(key=lambda r: r.get(field_name), reverse=descending)
        return matched

    async def list(
        self, context: Any, query: ApplicationQuery
    ) -> tuple[ApplicationRecord, ...]:
        matched = self._select(query)
        start = query.offset or 0
        end = start + query.limit if query.limit is not None else None
        return tuple(
            ApplicationRecord(entity=query.entity, values=dict(r))
            for r in matched[start:end]
        )

    async def count(self, context: Any, query: ApplicationQuery) -> int:
        return len(self._select(query))

    # --- unused Protocol members (stubs) -----------------------------------
    async def get(self, context: Any, *, entity: str, record_id: str, includes: tuple[str, ...] = ()):  # noqa: ANN201
        raise NotImplementedError

    async def add(self, context: Any, record: ApplicationRecord):  # noqa: ANN201
        raise NotImplementedError

    async def delete(self, context: Any, record: ApplicationRecord) -> None:
        raise NotImplementedError

    async def flush(self, context: Any) -> None:
        raise NotImplementedError

    async def refresh(self, context: Any, record: ApplicationRecord):  # noqa: ANN201
        raise NotImplementedError

    async def commit(self, context: Any) -> None:
        raise NotImplementedError

    async def rollback(self, context: Any) -> None:
        raise NotImplementedError

    async def backfill_qa_answered_at(self, context: Any) -> dict[str, int]:
        raise NotImplementedError


B1_SCOPE = (ApplicationFilter("board_id", "eq", "b1"),)


def _request(**overrides: Any) -> PageRequest:
    base: dict[str, Any] = {
        "surface": "card_list",
        "scope": B1_SCOPE,
        "offset": 0,
        "limit": 25,
    }
    base.update(overrides)
    return PageRequest(**base)


@pytest.fixture
def in_memory_port() -> Any:
    rows: list[dict[str, Any]] = [
        {
            "id": f"c{i}",
            "board_id": "b1",
            "status": "started" if i % 2 == 0 else "done",
            "priority": "high" if i in (2, 3) else "none",
            # Positions DELIBERATELY reversed vs ids so the surface order is
            # observable: kanban (position ASC) != card_list (updated_at/id);
            # titles fully collide so the LOOKUP order exposes its id ASC
            # tie-break (vs the list surfaces' id DESC).
            "position": 9 - i,
            "title": "T",
            "updated_at": "2026-07-20T00:00:00",
        }
        for i in range(10)
    ]
    rows.append(
        {
            "id": "x0",
            "board_id": "b2",
            "status": "started",
            "priority": "none",
            "position": 0,
            "updated_at": "2026-07-20T00:00:00",
        }
    )

    try:
        previous = get_application_persistence_port()
    except Exception:  # noqa: BLE001 — port may be unset before registration
        previous = None

    register_application_persistence_port(_InMemoryPort(rows))
    try:
        yield rows
    finally:
        if previous is not None:
            register_application_persistence_port(previous)
        else:
            reset_application_persistence_port_for_tests()


async def test_scope_drives_overall_and_filters_drive_page(in_memory_port: Any) -> None:
    result = await list_entities_page(
        None,
        _request(
            filters=(ApplicationFilter("status", "eq", "started"),),
            offset=1,
            limit=2,
        ),
    )
    assert isinstance(result, PageResult)
    # card_list surface: equal updated_at -> id DESC. started = {c0,c2,c4,
    # c6,c8}; id DESC -> c8,c6,c4,c2,c0; window offset=1 limit=2 -> c6,c4.
    assert result.total_filtered == 5
    assert result.total_overall == 10  # scope only — independent of filters
    assert [r.id for r in result.items] == ["c6", "c4"]


async def test_surfaces_apply_their_own_canonical_order(in_memory_port: Any) -> None:
    # Round-6 REJECT core (val_7e9b2577): the SURFACE — not the caller —
    # decides the order. Same entity, same dataset, different surfaces:
    listed = await list_entities_page(None, _request(limit=3))
    assert [r.id for r in listed.items] == ["c9", "c8", "c7"]  # updated_at/id DESC
    kanban = await list_entities_page(
        None,
        _request(
            surface="kanban_column",
            scope=(
                ApplicationFilter("board_id", "eq", "b1"),
                ApplicationFilter("status", "eq", "started"),
            ),
            limit=3,
        ),
    )
    # kanban_column: position ASC — c8(pos 1), c6(pos 3), c4(pos 5).
    assert [r.id for r in kanban.items] == ["c8", "c6", "c4"]
    # PageRequest no longer carries order_by at all — a caller cannot even
    # EXPRESS a non-canonical order.
    with pytest.raises(TypeError):
        PageRequest(
            surface="card_list",
            scope=B1_SCOPE,
            offset=0,
            limit=25,
            order_by=(("id", True),),
        )


async def test_offset_beyond_total_is_empty_with_real_totals(
    in_memory_port: Any,
) -> None:
    result = await list_entities_page(None, _request(offset=100, limit=25))
    assert result.items == ()
    assert result.total_filtered == 10
    assert result.total_overall == 10
    assert result.offset == 100
    assert result.limit == 25


async def test_contract_enforcement_rejects_bad_requests(in_memory_port: Any) -> None:
    for surface in ("", "card", "warp_list"):
        with pytest.raises(ValueError, match="page_request_unknown_surface"):
            await list_entities_page(None, _request(surface=surface))
    with pytest.raises(ValueError, match="page_request_scope_required"):
        await list_entities_page(None, _request(scope=()))
    with pytest.raises(ValueError, match="page_request_scope_field_not_allowed"):
        await list_entities_page(
            None, _request(scope=(ApplicationFilter("title", "eq", "x"),))
        )
    with pytest.raises(ValueError, match="page_request_invalid_window"):
        await list_entities_page(None, _request(offset=-1))
    for bad_limit in (None, 0, 201, True):
        with pytest.raises(ValueError, match="page_request_invalid_window"):
            await list_entities_page(None, _request(limit=bad_limit))
    for bad_offset in (True, 1.5):
        with pytest.raises(ValueError, match="page_request_invalid_window"):
            await list_entities_page(None, _request(offset=bad_offset))
    with pytest.raises(ValueError, match="page_request_filter_field_not_allowed"):
        await list_entities_page(
            None, _request(filters=(ApplicationFilter("owner_id", "eq", "u"),))
        )
    with pytest.raises(ValueError, match="page_request_include_not_allowed"):
        await list_entities_page(None, _request(includes=("qa_items",)))


async def test_scope_isolation_and_per_field_kinds(in_memory_port: Any) -> None:
    # Anchor required: archived-only, status-only (kanban) and spec-id-less
    # scopes are rejected; eq null/blank never counts as an anchor.
    for surface, scope in (
        ("card_list", (ApplicationFilter("archived", "eq", False),)),
        ("story_list", (ApplicationFilter("archived", "is_false", None),)),
        (
            "kanban_column",
            (ApplicationFilter("status", "eq", "started"),),
        ),
    ):
        with pytest.raises(ValueError, match="page_request_scope_anchor_required"):
            await list_entities_page(
                None, _request(surface=surface, scope=scope)
            )
    with pytest.raises(ValueError, match="page_request_scope_"):
        await list_entities_page(
            None, _request(scope=(ApplicationFilter("board_id", "eq", None),))
        )
    with pytest.raises(ValueError, match="page_request_scope_"):
        await list_entities_page(
            None, _request(scope=(ApplicationFilter("board_id", "eq", "  "),))
        )
    for operator in ("ne", "not_none", "contains"):
        with pytest.raises(
            ValueError, match="page_request_scope_operator_not_allowed"
        ):
            await list_entities_page(
                None,
                _request(scope=(ApplicationFilter("board_id", operator, "b1"),)),
            )
    # Kind mismatches: bool op on id field, contains on enum, bool op on text.
    with pytest.raises(ValueError, match="page_request_scope_operator_not_allowed"):
        await list_entities_page(
            None,
            _request(
                scope=(
                    ApplicationFilter("board_id", "eq", "b1"),
                    ApplicationFilter("board_id", "is_true", None),
                )
            ),
        )
    with pytest.raises(ValueError, match="page_request_filter_operator_not_allowed"):
        await list_entities_page(
            None, _request(filters=(ApplicationFilter("status", "contains", "sta"),))
        )
    with pytest.raises(ValueError, match="page_request_filter_operator_not_allowed"):
        await list_entities_page(
            None, _request(filters=(ApplicationFilter("title", "is_true", None),))
        )
    with pytest.raises(ValueError, match="page_request_filter_operator_not_allowed"):
        await list_entities_page(
            None, _request(filters=(ApplicationFilter("status", "ne", "done"),))
        )


async def test_round6_fail_open_repros_are_closed(in_memory_port: Any) -> None:
    # status eq '' and status eq 'warp': enum values come from the entity's
    # catalogue — the empty string and unknown values are fail-closed.
    for value in ("", "warp"):
        with pytest.raises(
            ValueError, match="page_request_filter_operator_not_allowed"
        ):
            await list_entities_page(
                None, _request(filters=(ApplicationFilter("status", "eq", value),))
            )
    # status IN ('',) and spec_id IN ('',): blank entries never match SQL
    # rows silently — rejected instead.
    with pytest.raises(ValueError, match="page_request_filter_operator_not_allowed"):
        await list_entities_page(
            None, _request(filters=(ApplicationFilter("status", "in", ("",)),))
        )
    with pytest.raises(ValueError, match="page_request_filter_operator_not_allowed"):
        await list_entities_page(
            None, _request(filters=(ApplicationFilter("spec_id", "in", ("",)),))
        )
    # any_groups=((),): an empty AND-group vanishes in SQL and stops
    # restricting the OR — rejected.
    with pytest.raises(ValueError, match="page_request_filter_group_empty"):
        await list_entities_page(None, _request(any_groups=((),)))


async def test_round7_kanban_scope_lookup_order_and_generators(
    in_memory_port: Any,
) -> None:
    # 1) kanban_column REQUIRES status in the scope: board-only would mix a
    # column total_filtered with a board total_overall.
    with pytest.raises(ValueError, match="page_request_scope_field_required"):
        await list_entities_page(None, _request(surface="kanban_column"))
    # ... and status is NOT a discretionary filter of that surface anymore.
    with pytest.raises(ValueError, match="page_request_filter_field_not_allowed"):
        await list_entities_page(
            None,
            _request(
                surface="kanban_column",
                scope=(
                    ApplicationFilter("board_id", "eq", "b1"),
                    ApplicationFilter("status", "eq", "started"),
                ),
                filters=(ApplicationFilter("status", "eq", "done"),),
            ),
        )
    # 2) Lookup tie-break is id ASC (approved C4 DDL/queries) — with fully
    # colliding titles the lookup pages ASC while the list pages DESC.
    lookup = await list_entities_page(
        None, _request(surface="spec_lookup", limit=3)
    )
    assert [r.id for r in lookup.items] == ["c0", "c1", "c2"]
    # 3) Generators are normalized ONCE to tuples before validation, so they
    # reach the port intact (a consumed generator would arrive empty).
    generator_filters = (
        item for item in [ApplicationFilter("status", "eq", "started")]
    )
    result = await list_entities_page(None, _request(filters=generator_filters))
    assert result.total_filtered == 5  # not silently unfiltered/empty
    inner_generator = (
        (item for item in [ApplicationFilter("status", "eq", "started")]),
    )
    grouped = await list_entities_page(None, _request(any_groups=inner_generator))
    assert grouped.total_filtered == 5


async def test_real_collision_resolved_by_id_desc_across_pages(
    in_memory_port: Any,
) -> None:
    # Every b1 row shares the SAME updated_at — ordering is total only via
    # the surface's id DESC tie-break. Adjacent pages must reassemble the
    # full sequence without duplicates or gaps (KG dec-s05-01).
    pages: list[str] = []
    for offset in (0, 4, 8):
        result = await list_entities_page(None, _request(offset=offset, limit=4))
        assert result.total_filtered == 10
        pages.extend(r.id for r in result.items)

    expected = sorted((f"c{i}" for i in range(10)), reverse=True)
    assert pages == expected
    assert len(set(pages)) == len(pages)


async def test_composite_filters_any_filters_and_any_groups(
    in_memory_port: Any,
) -> None:
    result = await list_entities_page(
        None,
        _request(
            filters=(ApplicationFilter("status", "eq", "started"),),
            any_filters=(
                ApplicationFilter("priority", "eq", "high"),
                ApplicationFilter("priority", "eq", "none"),
            ),
            any_groups=(
                (
                    ApplicationFilter("status", "eq", "started"),
                    ApplicationFilter("priority", "eq", "high"),
                ),
                (
                    ApplicationFilter("status", "eq", "done"),
                    ApplicationFilter("priority", "eq", "none"),
                ),
            ),
        ),
    )
    # started ∩ any_filters(all) ∩ groups((started AND high) -> {c2} |
    # (done AND none) -> {c1,c5,c7,c9}) = {c2}.
    assert [r.id for r in result.items] == ["c2"]
    assert result.total_filtered == 1
    assert result.total_overall == 10


async def test_use_case_is_uow_shaped_and_hints_resolve(in_memory_port: Any) -> None:
    from types import SimpleNamespace
    from typing import get_type_hints

    from okto_pulse.core.application.use_cases.entity_pagination import (
        EntityPageService,
        ListEntityPageCommand,
        ListEntityUseCase,
    )
    from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
    from okto_pulse.core.services import main as services_main

    uow = SimpleNamespace(
        services=SimpleNamespace(entity_pages=EntityPageService(None))
    )
    outcome = await ListEntityUseCase().execute(
        ListEntityPageCommand(_request(limit=3)), uow=uow
    )
    assert isinstance(outcome.page, PageResult)
    assert outcome.page.total_overall == 10
    assert [r.id for r in outcome.page.items] == ["c9", "c8", "c7"]

    facade_hints = get_type_hints(services_main.list_entities_page)
    assert facade_hints["request"] is PageRequest
    execute_hints = get_type_hints(ListEntityUseCase.execute)
    assert execute_hints["uow"] is PulseUnitOfWork
