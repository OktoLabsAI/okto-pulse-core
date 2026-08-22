"""Authorization and child-containment matrix for the REST card surface."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.cards import router as cards_router
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.core.domain.enums import BugSeverity, CardType
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    BoardShare,
    Card,
    CardDependency,
    Spec,
    SpecStatus,
)


PREFIX = "/api/v1/cards"
ATTACKER = "card-rest-scope-attacker"
OTHER_OWNER = "card-rest-scope-owner"

VALID_VALIDATION = {
    "expected_subject_version": 1,
    "idempotency_key": "card-rest-scope-validation",
    "confidence": 90,
    "confidence_justification": "The reviewer inspected the delivered behavior.",
    "estimated_completeness": 100,
    "completeness_justification": "All acceptance criteria are implemented.",
    "estimated_drift": 0,
    "drift_justification": "No implementation drift was identified.",
    "general_justification": "The implementation satisfies the reviewed task contract.",
    "recommendation": "approve",
}


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(cards_router, prefix=PREFIX)
    app.include_router(specs_router, prefix="/api/v1")
    app.dependency_overrides[require_user] = lambda: ATTACKER
    return TestClient(app)


def _missing(kind: str) -> str:
    return f"missing-{kind}-{uuid.uuid4().hex}"


@pytest.fixture
async def rest_graph(db_factory) -> dict[str, str]:
    suffix = uuid.uuid4().hex[:10]
    ids = {
        "owned_board": f"card-rest-owned-board-{suffix}",
        "foreign_board": f"card-rest-foreign-board-{suffix}",
        "realm_board": f"card-rest-realm-board-{suffix}",
        "viewer_board": f"card-rest-viewer-board-{suffix}",
        "editor_board": f"card-rest-editor-board-{suffix}",
        "admin_board": f"card-rest-admin-board-{suffix}",
    }
    for name in list(ids):
        if name.endswith("_board"):
            ids[name.replace("_board", "_spec")] = ids[name].replace("board", "spec")

    ids.update(
        {
            "owned_card": f"card-rest-owned-{suffix}",
            "owned_target": f"card-rest-owned-target-{suffix}",
            "owned_bug": f"card-rest-owned-bug-{suffix}",
            "owned_test": f"card-rest-owned-test-{suffix}",
            "foreign_card": f"card-rest-foreign-{suffix}",
            "foreign_target": f"card-rest-foreign-target-{suffix}",
            "foreign_test": f"card-rest-foreign-test-{suffix}",
            "realm_card": f"card-rest-realm-{suffix}",
            "viewer_card": f"card-rest-viewer-{suffix}",
            "viewer_target": f"card-rest-viewer-target-{suffix}",
            "editor_card": f"card-rest-editor-{suffix}",
            "admin_card": f"card-rest-admin-{suffix}",
            "owned_validation": f"card-rest-owned-validation-{suffix}",
            "foreign_validation": f"card-rest-foreign-validation-{suffix}",
            "owned_kb": f"card-rest-owned-kb-{suffix}",
            "foreign_kb": f"card-rest-foreign-kb-{suffix}",
            "corrupt_edge": f"card-rest-corrupt-edge-{suffix}",
        }
    )
    for scope in ("owned", "foreign", "realm", "viewer", "editor", "admin"):
        ids[f"{scope}_scenario"] = f"card-rest-{scope}-scenario-{suffix}"
        ids[f"{scope}_ir"] = f"card-rest-{scope}-ir-{suffix}"
        ids[f"{scope}_or"] = f"card-rest-{scope}-or-{suffix}"

    now = datetime.now(timezone.utc)
    async with db_factory() as db:
        db.add_all(
            [
                Board(
                    id=ids["owned_board"],
                    name="Owned",
                    owner_id=ATTACKER,
                    realm_id="local",
                ),
                Board(
                    id=ids["foreign_board"],
                    name="Foreign",
                    owner_id=OTHER_OWNER,
                    realm_id="local",
                ),
                # Same owner id is deliberately insufficient across realms.
                Board(
                    id=ids["realm_board"],
                    name="Foreign realm",
                    owner_id=ATTACKER,
                    realm_id="tenant-b",
                ),
                Board(
                    id=ids["viewer_board"],
                    name="Viewer",
                    owner_id=OTHER_OWNER,
                    realm_id="local",
                ),
                Board(
                    id=ids["editor_board"],
                    name="Editor",
                    owner_id=OTHER_OWNER,
                    realm_id="local",
                ),
                Board(
                    id=ids["admin_board"],
                    name="Admin",
                    owner_id=OTHER_OWNER,
                    realm_id="local",
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                BoardShare(
                    board_id=ids[f"{permission}_board"],
                    user_id=ATTACKER,
                    realm_id="local",
                    permission=permission,
                    shared_by=OTHER_OWNER,
                )
                for permission in ("viewer", "editor", "admin")
            ]
        )
        db.add_all(
            [
                Spec(
                    id=ids[f"{scope}_spec"],
                    board_id=ids[f"{scope}_board"],
                    title=f"{scope} spec",
                    status=SpecStatus.DRAFT,
                    created_by=OTHER_OWNER if scope != "owned" else ATTACKER,
                    test_scenarios=[
                        {
                            "id": ids[f"{scope}_scenario"],
                            "title": f"{scope} scenario",
                            "linked_task_ids": [],
                        }
                    ],
                    integration_requirements=[
                        {
                            "id": ids[f"{scope}_ir"],
                            "title": f"{scope} integration requirement",
                            "linked_task_ids": [],
                        }
                    ],
                    observability_requirements=[
                        {
                            "id": ids[f"{scope}_or"],
                            "title": f"{scope} observability requirement",
                            "linked_task_ids": [],
                        }
                    ],
                )
                for scope in ("owned", "foreign", "realm", "viewer", "editor", "admin")
            ]
        )
        await db.flush()

        def card(
            key: str,
            scope: str,
            *,
            card_type: CardType = CardType.NORMAL,
            created_at: datetime | None = None,
            **kwargs: Any,
        ) -> Card:
            return Card(
                id=ids[key],
                board_id=ids[f"{scope}_board"],
                spec_id=ids[f"{scope}_spec"],
                title=f"{key} title",
                created_by=ATTACKER if scope in {"owned", "realm"} else OTHER_OWNER,
                card_type=card_type,
                created_at=created_at,
                **kwargs,
            )

        db.add_all(
            [
                card(
                    "owned_card",
                    "owned",
                    validations=[
                        {
                            "id": ids["owned_validation"],
                            "recommendation": "approve",
                        }
                    ],
                    knowledge_bases=[
                        {
                            "id": ids["owned_kb"],
                            "title": "Owned knowledge",
                            "content": "owned",
                        }
                    ],
                ),
                card("owned_target", "owned"),
                card(
                    "owned_bug",
                    "owned",
                    card_type=CardType.BUG,
                    created_at=now,
                    severity=BugSeverity.MAJOR,
                    expected_behavior="expected",
                    observed_behavior="observed",
                    linked_test_task_ids=[ids["foreign_test"]],
                ),
                card(
                    "owned_test",
                    "owned",
                    card_type=CardType.TEST,
                    created_at=now + timedelta(seconds=1),
                ),
                card(
                    "foreign_card",
                    "foreign",
                    card_type=CardType.BUG,
                    created_at=now,
                    severity=BugSeverity.MAJOR,
                    expected_behavior="foreign expected",
                    observed_behavior="foreign observed",
                    linked_test_task_ids=[ids["foreign_test"]],
                    validations=[
                        {
                            "id": ids["foreign_validation"],
                            "recommendation": "approve",
                        }
                    ],
                    knowledge_bases=[
                        {
                            "id": ids["foreign_kb"],
                            "title": "Foreign knowledge",
                            "content": "secret",
                        }
                    ],
                ),
                card("foreign_target", "foreign"),
                card(
                    "foreign_test",
                    "foreign",
                    card_type=CardType.TEST,
                    created_at=now + timedelta(seconds=1),
                ),
                card("realm_card", "realm"),
                card("viewer_card", "viewer"),
                card("viewer_target", "viewer"),
                card("editor_card", "editor"),
                card("admin_card", "admin"),
            ]
        )
        await db.flush()
        # A legacy-corrupt cross-board edge must neither leak nor be removable
        # through a globally addressable target id.
        db.add(
            CardDependency(
                id=ids["corrupt_edge"],
                card_id=ids["owned_card"],
                depends_on_id=ids["foreign_target"],
            )
        )
        await db.commit()
    return ids


async def _snapshot(db_factory, ids: dict[str, str]) -> dict[str, Any]:
    async with db_factory() as db:
        cards = {}
        for key in (
            "owned_card",
            "owned_bug",
            "foreign_card",
            "foreign_target",
            "foreign_test",
            "realm_card",
            "viewer_card",
        ):
            value = await db.get(Card, ids[key])
            cards[key] = None if value is None else {
                "title": value.title,
                "status": value.status.value,
                "position": value.position,
                "validations": value.validations,
                "knowledge_bases": value.knowledge_bases,
                "linked_test_task_ids": value.linked_test_task_ids,
            }
        edges = list(
            (
                await db.execute(
                    select(
                        CardDependency.id,
                        CardDependency.card_id,
                        CardDependency.depends_on_id,
                    ).order_by(CardDependency.id)
                )
            ).all()
        )
        activity = list(
            (
                await db.execute(select(ActivityLog.id).order_by(ActivityLog.id))
            ).scalars()
        )
        specs = {}
        for scope in ("owned", "foreign", "realm", "viewer"):
            value = await db.get(Spec, ids[f"{scope}_spec"])
            specs[scope] = None if value is None else {
                "test_scenarios": value.test_scenarios,
                "integration_requirements": value.integration_requirements,
                "observability_requirements": value.observability_requirements,
            }
    return {
        "cards": cards,
        "edges": edges,
        "activity": activity,
        "specs": specs,
    }


def _request(
    client: TestClient,
    method: str,
    path: str,
    **kwargs: Any,
):
    return client.request(method, f"{PREFIX}/{path.lstrip('/')}", **kwargs)


@pytest.mark.asyncio
async def test_every_foreign_main_card_route_matches_missing_and_has_zero_effects(
    client: TestClient,
    db_factory,
    rest_graph: dict[str, str],
) -> None:
    ids = rest_graph
    before = await _snapshot(db_factory, ids)
    cases = [
        ("GET", "{card}", {}),
        ("PATCH", "{card}", {"json": {"title": "denied"}}),
        (
            "POST",
            "{card}/move",
            {"json": {"status": "not_started", "position": 3}},
        ),
        ("DELETE", "{card}", {}),
        ("GET", "{card}/dependencies", {}),
        ("GET", "{card}/dependents", {}),
        ("POST", f"{{card}}/dependencies/{ids['foreign_target']}", {}),
        ("DELETE", f"{{card}}/dependencies/{ids['foreign_target']}", {}),
        ("POST", "{card}/validate", {"json": VALID_VALIDATION}),
        ("GET", "{card}/validations", {}),
        ("GET", f"{{card}}/validations/{ids['foreign_validation']}", {}),
        ("DELETE", f"{{card}}/validations/{ids['foreign_validation']}", {}),
        (
            "GET",
            "{card}/regression-scenario-candidates",
            {"params": {"board_id": ids["foreign_board"]}},
        ),
        ("POST", "{card}/test-tasks", {"json": {"test_task_id": ids["foreign_test"]}}),
        ("DELETE", f"{{card}}/test-tasks/{ids['foreign_test']}", {}),
        ("GET", "{card}/activity", {}),
        ("GET", "{card}/seen", {}),
        ("GET", "{card}/knowledge", {}),
        ("GET", f"{{card}}/knowledge/{ids['foreign_kb']}", {}),
        ("GET", f"{{card}}/knowledge/{ids['foreign_kb']}/download", {}),
        (
            "POST",
            "{card}/knowledge",
            {"json": {"title": "denied", "content": "denied"}},
        ),
        (
            "PATCH",
            f"{{card}}/knowledge/{ids['foreign_kb']}",
            {"json": {"title": "denied"}},
        ),
        ("DELETE", f"{{card}}/knowledge/{ids['foreign_kb']}", {}),
    ]

    for method, template, kwargs in cases:
        denied = _request(
            client,
            method,
            template.format(card=ids["foreign_card"]),
            **kwargs,
        )
        missing = _request(
            client,
            method,
            template.format(card=_missing("card")),
            **kwargs,
        )
        assert denied.status_code == missing.status_code == 404, (
            method,
            template,
            denied.text,
            missing.text,
        )
        assert denied.content == missing.content, (method, template)

    assert await _snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_foreign_child_ids_match_missing_and_cannot_mutate_parent(
    client: TestClient,
    db_factory,
    rest_graph: dict[str, str],
) -> None:
    ids = rest_graph
    before = await _snapshot(db_factory, ids)
    child_cases = [
        (
            "POST",
            f"{ids['owned_card']}/dependencies/{{child}}",
            ids["foreign_target"],
            _missing("target"),
            {},
        ),
        (
            "DELETE",
            f"{ids['owned_card']}/dependencies/{{child}}",
            ids["foreign_target"],
            _missing("target"),
            {},
        ),
        (
            "GET",
            f"{ids['owned_card']}/validations/{{child}}",
            ids["foreign_validation"],
            _missing("validation"),
            {},
        ),
        (
            "DELETE",
            f"{ids['owned_card']}/validations/{{child}}",
            ids["foreign_validation"],
            _missing("validation"),
            {},
        ),
        (
            "POST",
            f"{ids['owned_bug']}/test-tasks",
            ids["foreign_test"],
            _missing("test-task"),
            {"body_child": True},
        ),
        (
            "DELETE",
            f"{ids['owned_bug']}/test-tasks/{{child}}",
            ids["foreign_test"],
            _missing("test-task"),
            {},
        ),
        (
            "GET",
            f"{ids['owned_card']}/knowledge/{{child}}",
            ids["foreign_kb"],
            _missing("kb"),
            {},
        ),
        (
            "GET",
            f"{ids['owned_card']}/knowledge/{{child}}/download",
            ids["foreign_kb"],
            _missing("kb"),
            {},
        ),
        (
            "PATCH",
            f"{ids['owned_card']}/knowledge/{{child}}",
            ids["foreign_kb"],
            _missing("kb"),
            {"json": {"title": "denied"}},
        ),
        (
            "DELETE",
            f"{ids['owned_card']}/knowledge/{{child}}",
            ids["foreign_kb"],
            _missing("kb"),
            {},
        ),
    ]

    for method, template, foreign_id, missing_id, kwargs in child_cases:
        kwargs = dict(kwargs)
        body_child = kwargs.pop("body_child", False)
        if body_child:
            denied = _request(
                client,
                method,
                template,
                json={"test_task_id": foreign_id},
            )
            missing = _request(
                client,
                method,
                template,
                json={"test_task_id": missing_id},
            )
        else:
            denied = _request(client, method, template.format(child=foreign_id), **kwargs)
            missing = _request(client, method, template.format(child=missing_id), **kwargs)
        assert denied.status_code == missing.status_code == 404, (
            method,
            template,
            denied.text,
            missing.text,
        )
        assert denied.content == missing.content, (method, template)

    assert await _snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_viewer_reads_but_all_representative_writes_are_hidden(
    client: TestClient,
    db_factory,
    rest_graph: dict[str, str],
) -> None:
    ids = rest_graph
    assert _request(client, "GET", ids["viewer_card"]).status_code == 200
    before = await _snapshot(db_factory, ids)
    denied_writes = [
        ("PATCH", ids["viewer_card"], {"json": {"title": "denied"}}),
        (
            "POST",
            f"{ids['viewer_card']}/move",
            {"json": {"status": "not_started", "position": 9}},
        ),
        ("DELETE", ids["viewer_card"], {}),
        (
            "POST",
            f"{ids['viewer_card']}/dependencies/{ids['viewer_target']}",
            {},
        ),
        ("POST", f"{ids['viewer_card']}/validate", {"json": VALID_VALIDATION}),
        (
            "POST",
            f"{ids['viewer_card']}/knowledge",
            {"json": {"title": "denied", "content": "denied"}},
        ),
    ]
    for method, path, kwargs in denied_writes:
        response = _request(client, method, path, **kwargs)
        assert response.status_code == 404, (method, path, response.text)
        assert response.json() == {"detail": "Card not found"}
    assert await _snapshot(db_factory, ids) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["editor", "admin"])
async def test_editor_and_admin_can_update_and_delete(
    client: TestClient,
    db_factory,
    rest_graph: dict[str, str],
    role: str,
) -> None:
    card_id = rest_graph[f"{role}_card"]
    fetched = _request(client, "GET", card_id)
    updated = _request(
        client,
        "PATCH",
        card_id,
        json={"title": f"updated by {role}"},
    )
    deleted = _request(client, "DELETE", card_id)

    assert fetched.status_code == 200
    assert updated.status_code == 200, updated.text
    assert updated.json()["title"] == f"updated by {role}"
    assert deleted.status_code == 204, deleted.text
    async with db_factory() as db:
        assert await db.get(Card, card_id) is None


@pytest.mark.asyncio
async def test_same_owner_id_in_another_realm_is_not_authorized(
    client: TestClient,
    db_factory,
    rest_graph: dict[str, str],
) -> None:
    ids = rest_graph
    before = await _snapshot(db_factory, ids)
    for method, kwargs in (
        ("GET", {}),
        ("PATCH", {"json": {"title": "cross-realm write"}}),
    ):
        denied = _request(client, method, ids["realm_card"], **kwargs)
        missing = _request(client, method, _missing("card"), **kwargs)
        assert denied.status_code == missing.status_code == 404
        assert denied.content == missing.content == b'{"detail":"Card not found"}'
    assert await _snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_rest_scenario_link_and_unlink_are_board_writer_scoped(
    client: TestClient,
    db_factory,
    rest_graph: dict[str, str],
) -> None:
    ids = rest_graph
    before = await _snapshot(db_factory, ids)

    for action in ("link-task", "unlink-task"):
        denied = client.post(
            f"/api/v1/specs/{ids['foreign_spec']}/scenarios/"
            f"{ids['foreign_scenario']}/{action}/{ids['foreign_card']}"
        )
        missing = client.post(
            f"/api/v1/specs/{_missing('spec')}/scenarios/"
            f"{ids['foreign_scenario']}/{action}/{ids['foreign_card']}"
        )
        viewer = client.post(
            f"/api/v1/specs/{ids['viewer_spec']}/scenarios/"
            f"{ids['viewer_scenario']}/{action}/{ids['viewer_card']}"
        )
        other_realm = client.post(
            f"/api/v1/specs/{ids['realm_spec']}/scenarios/"
            f"{ids['realm_scenario']}/{action}/{ids['realm_card']}"
        )

        assert (
            denied.status_code
            == missing.status_code
            == viewer.status_code
            == other_realm.status_code
            == 404
        )
        assert denied.content == missing.content == viewer.content == other_realm.content == (
            b'{"detail":"Spec not found"}'
        )

    assert await _snapshot(db_factory, ids) == before

    for resource, id_key in (
        ("integration-requirements", "ir"),
        ("observability-requirements", "or"),
    ):
        denied = client.post(
            f"/api/v1/specs/{ids['foreign_spec']}/{resource}/"
            f"{ids[f'foreign_{id_key}']}/link-task/{ids['foreign_card']}"
        )
        missing = client.post(
            f"/api/v1/specs/{_missing('spec')}/{resource}/"
            f"{ids[f'foreign_{id_key}']}/link-task/{ids['foreign_card']}"
        )
        viewer = client.post(
            f"/api/v1/specs/{ids['viewer_spec']}/{resource}/"
            f"{ids[f'viewer_{id_key}']}/link-task/{ids['viewer_card']}"
        )
        other_realm = client.post(
            f"/api/v1/specs/{ids['realm_spec']}/{resource}/"
            f"{ids[f'realm_{id_key}']}/link-task/{ids['realm_card']}"
        )
        assert (
            denied.status_code
            == missing.status_code
            == viewer.status_code
            == other_realm.status_code
            == 404
        )
        assert denied.content == missing.content == viewer.content == other_realm.content == (
            b'{"detail":"Spec not found"}'
        )

    assert await _snapshot(db_factory, ids) == before

    def normalized_detail(response, *values: str) -> str:
        detail = response.json()["detail"]
        for value in values:
            detail = detail.replace(value, "<opaque-id>")
        return detail

    for action in ("link-task", "unlink-task"):
        missing_scenario = _missing("scenario")
        foreign_child = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/scenarios/"
            f"{ids['foreign_scenario']}/{action}/{ids['owned_card']}"
        )
        missing_child = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/scenarios/"
            f"{missing_scenario}/{action}/{ids['owned_card']}"
        )
        assert foreign_child.status_code == missing_child.status_code == 404
        assert normalized_detail(
            foreign_child, ids["foreign_scenario"]
        ) == normalized_detail(missing_child, missing_scenario)

        missing_card = _missing("card")
        foreign_card = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/scenarios/"
            f"{ids['owned_scenario']}/{action}/{ids['foreign_card']}"
        )
        absent_card = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/scenarios/"
            f"{ids['owned_scenario']}/{action}/{missing_card}"
        )
        assert foreign_card.status_code == absent_card.status_code == 404
        assert normalized_detail(
            foreign_card, ids["foreign_card"]
        ) == normalized_detail(absent_card, missing_card)

    for resource, id_key in (
        ("integration-requirements", "ir"),
        ("observability-requirements", "or"),
    ):
        missing_requirement = _missing(id_key)
        foreign_child = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/{resource}/"
            f"{ids[f'foreign_{id_key}']}/link-task/{ids['owned_card']}"
        )
        missing_child = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/{resource}/"
            f"{missing_requirement}/link-task/{ids['owned_card']}"
        )
        assert foreign_child.status_code == missing_child.status_code == 404
        assert normalized_detail(
            foreign_child, ids[f"foreign_{id_key}"]
        ) == normalized_detail(missing_child, missing_requirement)

        missing_card = _missing("card")
        foreign_card = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/{resource}/"
            f"{ids[f'owned_{id_key}']}/link-task/{ids['foreign_card']}"
        )
        absent_card = client.post(
            f"/api/v1/specs/{ids['owned_spec']}/{resource}/"
            f"{ids[f'owned_{id_key}']}/link-task/{missing_card}"
        )
        assert foreign_card.status_code == absent_card.status_code == 404
        assert normalized_detail(
            foreign_card, ids["foreign_card"]
        ) == normalized_detail(absent_card, missing_card)

    assert await _snapshot(db_factory, ids) == before

    linked = client.post(
        f"/api/v1/specs/{ids['editor_spec']}/scenarios/"
        f"{ids['editor_scenario']}/link-task/{ids['editor_card']}"
    )
    unlinked = client.post(
        f"/api/v1/specs/{ids['editor_spec']}/scenarios/"
        f"{ids['editor_scenario']}/unlink-task/{ids['editor_card']}"
    )
    assert linked.status_code == 200, linked.text
    assert unlinked.status_code == 200, unlinked.text

    linked_ir = client.post(
        f"/api/v1/specs/{ids['editor_spec']}/integration-requirements/"
        f"{ids['editor_ir']}/link-task/{ids['editor_card']}"
    )
    linked_or = client.post(
        f"/api/v1/specs/{ids['editor_spec']}/observability-requirements/"
        f"{ids['editor_or']}/link-task/{ids['editor_card']}"
    )
    assert linked_ir.status_code == 200, linked_ir.text
    assert linked_or.status_code == 200, linked_or.text
    async with db_factory() as db:
        spec = await db.get(Spec, ids["editor_spec"])
        card = await db.get(Card, ids["editor_card"])
        assert spec.test_scenarios[0]["linked_task_ids"] == []
        assert spec.integration_requirements[0]["linked_task_ids"] == [
            ids["editor_card"]
        ]
        assert spec.observability_requirements[0]["linked_task_ids"] == [
            ids["editor_card"]
        ]
        assert card.test_scenario_ids == []
