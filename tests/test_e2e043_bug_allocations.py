"""Native SQL regression for Bug lineage and owned legacy allocation repair."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from test_e2e043_source_context import native_source_factory as _native_source_factory
from okto_pulse.community.adapters.sqlalchemy_models import (
    ActivityLog,
    Board,
    Card,
    Spec,
    SpecHistory,
)
from okto_pulse.community.adapters.sqlalchemy_policy_subject_versioning import (
    bind_semantic_subject_actor,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.bug_allocation_repair import (
    BugAllocationRepairCommand,
    BugAllocationRepairUseCase,
)
from okto_pulse.core.models.schemas import CardCreate, CardMove
from okto_pulse.core.ports.application_persistence import (
    get_application_persistence_port,
)
from okto_pulse.core.services.bug_allocation_repair import (
    ACTION,
    INHERITED_FIELDS,
    SCOPE_FIELDS,
    BugAllocationRepairError,
    BugAllocationRepairService,
)
from okto_pulse.core.services.main import CardService

native_source_factory = _native_source_factory
ACTOR = ActorContext(
    "owner", "mcp", actor_kind="agent", board_id="board", permissions=["*"]
)
NOW = datetime(2026, 11, 4, 14, tzinfo=timezone.utc)


def _bind(db):
    bind_semantic_subject_actor(db, ACTOR)


def _item(field, *, plan=True):
    item = {
        "id": f"{field}-1",
        "text": "Preserved obligation",
        "linked_task_ids": ["origin"],
        "status": "active",
    }
    if plan:
        item["implementation_plan"] = {
            "contract_version": "implementation-plan/v1",
            "contributions": [
                {"card_id": "origin", "scope": "whole_requirement", "criterion_ids": []}
            ],
        }
    return item


async def _seed(factory, *, legacy=True, plan=True, mutation=None):
    from okto_pulse.community.adapters.sqlalchemy_critical_context import (
        CommunitySqlAlchemyCriticalContextReader,
    )
    from okto_pulse.core.ports.critical_context import (
        register_critical_context_read_port,
    )

    register_critical_context_read_port(CommunitySqlAlchemyCriticalContextReader())
    before = {field: [_item(field, plan=plan)] for field in SCOPE_FIELDS}
    current = deepcopy(before)
    if legacy:
        for field in INHERITED_FIELDS:
            current[field][0]["linked_task_ids"].append("bug")
    async with factory() as db:
        _bind(db)
        db.add(
            Board(
                id="board",
                realm_id="local",
                name="Repair",
                owner_id="owner",
                settings={},
            )
        )
        db.add(
            Spec(
                id="spec",
                board_id="board",
                title="Done scope",
                created_by="owner",
                status="done",
                architecture_adoption={},
                **current,
            )
        )
        db.add(
            Card(
                id="origin",
                board_id="board",
                spec_id="spec",
                title="Origin",
                created_by="owner",
                status="done",
                card_type="normal",
                created_at=NOW - timedelta(minutes=10),
            )
        )
        db.add(
            SpecHistory(
                id="before",
                spec_id="spec",
                action="structured_update",
                actor_id="owner",
                actor_name="Owner",
                actor_type="user",
                version=1,
                created_at=NOW - timedelta(minutes=2),
                changes=[{"field": f, "old": [], "new": v} for f, v in before.items()],
            )
        )
        if legacy:
            db.add(
                Card(
                    id="bug",
                    board_id="board",
                    spec_id="spec",
                    title="Bug",
                    created_by="owner",
                    card_type="bug",
                    origin_task_id="origin",
                    status="cancelled",
                    created_at=NOW,
                )
            )
            details = {
                "traceability": {"requested": [], "changed": [], "idempotent": True}
            }
            if mutation == "explicit_creation":
                details["traceability"]["requested"] = [
                    {"target_type": "rule", "target_id": "business_rules-1"}
                ]
            if mutation == "fixed_creation":
                details["lineage_policy"] = "explicit_scope_v1"
            db.add(
                ActivityLog(
                    id="creation",
                    board_id="board",
                    card_id="bug",
                    action="card_created",
                    actor_id="owner",
                    actor_type="agent",
                    actor_name="Owner",
                    created_at=NOW,
                    details=details,
                )
            )
            if mutation == "explicit_later":
                db.add(
                    ActivityLog(
                        id="manual",
                        board_id="board",
                        card_id="bug",
                        action="card_traceability_linked",
                        actor_id="owner",
                        actor_type="agent",
                        actor_name="Owner",
                        created_at=NOW + timedelta(seconds=2),
                        details={
                            "changed": [],
                            "requested": [
                                {"target_type": "rule", "target_id": "business_rules-1"}
                            ],
                        },
                    )
                )
            if mutation == "later_history":
                db.add(
                    SpecHistory(
                        id="manual-history",
                        spec_id="spec",
                        action="traceability_linked",
                        actor_id="owner",
                        actor_name="Owner",
                        actor_type="user",
                        version=1,
                        created_at=NOW + timedelta(seconds=2),
                        changes=[
                            {
                                "field": "business_rules.linked_task_ids",
                                "new": ["origin", "bug"],
                            }
                        ],
                    )
                )
        await db.commit()
    return before, current


async def _preview(factory):
    async with factory() as db:
        return await BugAllocationRepairService(db).preview(
            board_id="board", spec_id="spec", bug_id="bug"
        )


def _request(preview, selected=None, key="repair-1"):
    return dict(
        board_id="board",
        spec_id="spec",
        bug_id="bug",
        actor=ACTOR,
        expected_spec_version=preview["spec_version"],
        expected_scope_digest=preview["scope_digest"],
        selected_link_ids=selected
        if selected is not None
        else [r["link_id"] for r in preview["links"] if r["state"] == "eligible"],
        idempotency_key=key,
    )


@pytest.mark.asyncio
async def test_create_bug_preserves_all_normative_fields_version_and_regression_lineage(
    native_source_factory,
):
    before, _ = await _seed(native_source_factory, legacy=False)
    async with native_source_factory() as db:
        _bind(db)
        version = (await db.get(Spec, "spec")).version
        bug = await CardService(db).create_card(
            "board",
            "owner",
            CardCreate(
                title="New Bug",
                spec_id="spec",
                card_type="bug",
                origin_task_id="origin",
                severity="major",
                expected_behavior="Works",
                observed_behavior="Fails",
            ),
            target_id="new-bug",
        )
        await get_application_persistence_port().commit(db)
        assert bug.origin_task_id == "origin"
        assert (
            not bug.test_scenario_ids
        )  # association is resolved through origin, not copied as scope
    async with native_source_factory() as db:
        _bind(db)
        cancelled = await CardService(db).move_card(
            "new-bug",
            "owner",
            CardMove(status="cancelled", cancellation_reason="Fixture probe finished"),
        )
        await get_application_persistence_port().commit(db)
        assert cancelled.status.value == "cancelled"
    async with native_source_factory() as db:
        spec = await db.get(Spec, "spec")
        assert spec.version == version
        assert {f: getattr(spec, f) for f in SCOPE_FIELDS} == before
        creation = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.card_id == "new-bug",
                    ActivityLog.action == "card_created",
                )
            )
        ).scalar_one()
        assert creation.details["lineage_policy"] == "explicit_scope_v1"


@pytest.mark.asyncio
@pytest.mark.parametrize("plan", [True, False])
async def test_native_repair_restores_exact_scope_and_retains_history_replay(
    native_source_factory, plan
):
    before, _ = await _seed(native_source_factory, plan=plan)
    preview = await _preview(native_source_factory)
    assert len(preview["links"]) == len(INHERITED_FIELDS)
    assert all(
        r["state"] == "eligible" and r["proof"]["spec_history_id"] == "before"
        for r in preview["links"]
    )
    async with native_source_factory() as db:
        _bind(db)
        result = await BugAllocationRepairService(db).apply(**_request(preview))
        await get_application_persistence_port().commit(db)
    async with native_source_factory() as db:
        _bind(db)
        spec = await db.get(Spec, "spec")
        assert {f: getattr(spec, f) for f in SCOPE_FIELDS} == before
        assert spec.status.value == "done" and spec.edition == preview["spec_edition"]
        assert result["spec_version"] == spec.version
        assert spec.version > preview["spec_version"]
        original = await db.get(SpecHistory, "before")
        assert original.changes == [
            {"field": f, "old": [], "new": v} for f, v in before.items()
        ]
        again = await BugAllocationRepairService(db).apply(**_request(preview))
        assert again == {**result, "replayed": True}
        await get_application_persistence_port().commit(db)
    async with native_source_factory() as db:
        audits = (
            (await db.execute(select(SpecHistory).where(SpecHistory.action == ACTION)))
            .scalars()
            .all()
        )
        assert len(audits) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["explicit_creation", "fixed_creation", "explicit_later", "later_history"],
)
async def test_manual_or_ambiguous_links_are_not_repaired(
    native_source_factory, mutation
):
    _, current = await _seed(native_source_factory, mutation=mutation)
    preview = await _preview(native_source_factory)
    blocked = next(r for r in preview["links"] if r["field"] == "business_rules")
    assert blocked["state"] == "requires_scope_review"
    async with native_source_factory() as db:
        _bind(db)
        with pytest.raises(BugAllocationRepairError, match="lack affirmative"):
            await BugAllocationRepairService(db).apply(
                **_request(preview, [blocked["link_id"]])
            )
        await db.rollback()
    async with native_source_factory() as db:
        assert {
            f: getattr(await db.get(Spec, "spec"), f) for f in SCOPE_FIELDS
        } == current


@pytest.mark.asyncio
async def test_mixed_scope_only_changes_selected_eligible_links(native_source_factory):
    before, current = await _seed(native_source_factory, mutation="later_history")
    preview = await _preview(native_source_factory)
    selected = ["technical_requirements:technical_requirements-1"]
    async with native_source_factory() as db:
        _bind(db)
        await BugAllocationRepairService(db).apply(**_request(preview, selected))
        await get_application_persistence_port().commit(db)
    async with native_source_factory() as db:
        spec = await db.get(Spec, "spec")
        assert spec.technical_requirements == before["technical_requirements"]
        assert spec.business_rules == current["business_rules"]
        assert spec.test_scenarios == current["test_scenarios"]


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["version", "digest", "history", "key"])
async def test_repair_fences_stale_scope_provenance_and_reused_key(
    native_source_factory, conflict
):
    await _seed(native_source_factory)
    preview = await _preview(native_source_factory)
    request = _request(preview)
    if conflict == "version":
        request["expected_spec_version"] += 1
    elif conflict == "digest":
        request["expected_scope_digest"] = "0" * 64
    elif conflict == "history":
        async with native_source_factory() as db:
            db.add(
                ActivityLog(
                    id="extra",
                    board_id="board",
                    card_id="bug",
                    action="card_traceability_linked",
                    actor_id="owner",
                    actor_type="user",
                    actor_name="Owner",
                    details={},
                )
            )
            await db.commit()
    else:
        async with native_source_factory() as db:
            _bind(db)
            await BugAllocationRepairService(db).apply(**request)
            await get_application_persistence_port().commit(db)
        request["selected_link_ids"] = request["selected_link_ids"][:1]
    async with native_source_factory() as db:
        _bind(db)
        with pytest.raises(BugAllocationRepairError) as error:
            await BugAllocationRepairService(db).apply(**request)
        assert error.value.code in {"repair_conflict", "repair_idempotency_conflict"}
        await db.rollback()


@pytest.mark.asyncio
async def test_late_publication_failure_rolls_back_links_and_replay_record(
    native_source_factory, monkeypatch
):
    _, current = await _seed(native_source_factory)
    preview = await _preview(native_source_factory)
    monkeypatch.setattr(
        "okto_pulse.core.events.publish",
        AsyncMock(side_effect=RuntimeError("publication failed")),
    )
    async with native_source_factory() as db:
        _bind(db)
        with pytest.raises(RuntimeError, match="publication failed"):
            await BugAllocationRepairService(db).apply(**_request(preview))
        await get_application_persistence_port().rollback(db)
    async with native_source_factory() as db:
        spec = await db.get(Spec, "spec")
        assert {f: getattr(spec, f) for f in SCOPE_FIELDS} == current
        assert spec.version == preview["spec_version"]
        assert (
            not (
                await db.execute(
                    select(SpecHistory).where(SpecHistory.action == ACTION)
                )
            )
            .scalars()
            .all()
        )


@pytest.mark.asyncio
async def test_two_native_sessions_cannot_apply_same_stale_preview_twice(
    native_source_factory,
):
    await _seed(native_source_factory)
    async with native_source_factory() as first, native_source_factory() as second:
        _bind(first)
        _bind(second)
        a, b = BugAllocationRepairService(first), BugAllocationRepairService(second)
        p1 = await a.preview(board_id="board", spec_id="spec", bug_id="bug")
        p2 = await b.preview(board_id="board", spec_id="spec", bug_id="bug")
        assert p1 == p2
        await a.apply(**_request(p1))
        await get_application_persistence_port().commit(first)
        with pytest.raises(BugAllocationRepairError) as error:
            await b.apply(**_request(p2, key="competing-repair"))
        assert error.value.code == "repair_conflict"
        await second.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["foreign_board", "read_only"])
async def test_use_case_denies_unauthorized_repair_before_owner(case):
    specs = SimpleNamespace(apply_bug_allocation_repair=AsyncMock())
    uow = SimpleNamespace(
        boards=SimpleNamespace(
            get=AsyncMock(
                return_value=SimpleNamespace(
                    id="board", owner_id="owner", realm_id="local"
                )
            )
        ),
        services=SimpleNamespace(specs=specs),
    )
    actor = ActorContext(
        "agent",
        "mcp",
        actor_kind="agent",
        board_id="other" if case == "foreign_board" else "board",
        permissions=["spec.entity.read"],
    )
    with pytest.raises(Exception) as error:
        await BugAllocationRepairUseCase().execute(
            BugAllocationRepairCommand(
                "board", "spec", "bug", 1, "digest", ("link",), "key"
            ),
            actor=actor,
            uow=uow,
            apply=True,
        )
    assert type(error.value).__name__ in {
        "EntityNotFoundError",
        "PermissionDeniedError",
    }
    specs.apply_bug_allocation_repair.assert_not_called()


@pytest.mark.asyncio
async def test_bounded_history_fails_closed_without_partial_provenance(
    native_source_factory,
):
    await _seed(native_source_factory)
    async with native_source_factory() as db:
        for i in range(200):
            db.add(
                SpecHistory(
                    id=f"history-{i}",
                    spec_id="spec",
                    action="status_changed",
                    actor_id="owner",
                    actor_name="Owner",
                    actor_type="user",
                    version=1,
                    changes=[],
                )
            )
        await db.commit()
    with pytest.raises(BugAllocationRepairError) as error:
        await _preview(native_source_factory)
    assert error.value.code == "repair_provenance_unavailable"


@pytest.mark.asyncio
async def test_native_rest_mcp_preview_apply_conflict_and_replay(
    native_source_factory, monkeypatch
):
    import httpx
    from fastapi import FastAPI
    from okto_pulse.community.adapters.sqlalchemy_unit_of_work import (
        CommunityUnitOfWorkFactory,
    )
    from okto_pulse.community.api.auth_deps import require_user
    from okto_pulse.community.api.deps import get_unit_of_work
    from okto_pulse.community.api.specs import router
    from okto_pulse.core.mcp import server

    await _seed(native_source_factory)
    factory = CommunityUnitOfWorkFactory(native_source_factory)
    human = ActorContext(
        "owner", "rest", actor_kind="human", board_id="board", realm_id="local"
    )

    async def dependency():
        async with factory(actor=human) as uow:
            yield uow

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_user] = lambda: "owner"
    app.dependency_overrides[get_unit_of_work] = dependency
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: factory)
    ctx = SimpleNamespace(agent_id="owner", agent_name="Owner", permissions=["*"])
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=ctx))
    preview_tool = await server.mcp.get_tool("okto_pulse_preview_bug_allocation_repair")
    apply_tool = await server.mcp.get_tool("okto_pulse_apply_bug_allocation_repair")
    path = "/boards/board/specs/spec/bugs/bug/allocation-repair"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        rest = await client.get(path)
        assert rest.status_code == 200, rest.text
        mcp = json.loads(
            await preview_tool.fn(board_id="board", spec_id="spec", bug_id="bug")
        )
        assert mcp == rest.json()
        body = {
            k: v
            for k, v in _request(mcp).items()
            if k not in {"actor", "board_id", "spec_id", "bug_id"}
        }
        bad = {**body, "expected_scope_digest": "0" * 64}
        rejected = await client.post(path, json=bad)
        assert rejected.status_code == 409
        mcp_rejected = json.loads(
            await apply_tool.fn(board_id="board", spec_id="spec", bug_id="bug", **bad)
        )
        assert (
            mcp_rejected["code"]
            == rejected.json()["detail"]["code"]
            == "repair_conflict"
        )
        applied = await client.post(path, json=body)
        assert applied.status_code == 200, applied.text
        replay = json.loads(
            await apply_tool.fn(board_id="board", spec_id="spec", bug_id="bug", **body)
        )
        assert replay == {**applied.json(), "replayed": True}
        assert (
            await client.get(path.replace("/board/", "/elsewhere/"))
        ).status_code == 404
        # Same resolved actor with read-only MCP permissions must not mutate.
        ctx.permissions = ["spec.entity.read"]
        denied = json.loads(
            await apply_tool.fn(board_id="board", spec_id="spec", bug_id="bug", **body)
        )
        assert "error" in denied and "audit_id" not in denied
