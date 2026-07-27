"""Authorization, realm, and child-containment matrix for REST Specs."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.spec_crud import (
    AnswerSpecQuestionCommand,
    AnswerSpecQuestionUseCase,
    CreateSpecCommand,
    CreateSpecUseCase,
    DeleteSpecKnowledgeCommand,
    DeleteSpecKnowledgeUseCase,
    ExecuteTestScenarioEvidenceCommand,
    ExecuteTestScenarioEvidenceUseCase,
    RunStructuredSpecEntityCommand,
    RunStructuredSpecEntityUseCase,
    UpdateSpecCommand,
    UpdateSpecUseCase,
    _require_actor_board_spec,
)
from okto_pulse.core.application.use_cases.submit_spec_validation import (
    SubmitSpecValidationCommand,
    SubmitSpecValidationUseCase,
)
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.models.schemas import SpecUpdate
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    BoardShare,
    Card,
    DomainEventRow,
    Spec,
    SpecHistory,
    SpecKnowledgeBase,
    SpecQAItem,
    SpecStatus,
)


PREFIX = "/api/v1"
ATTACKER = "spec-rest-scope-attacker"
OTHER_OWNER = "spec-rest-scope-owner"

VALIDATION = {
    "completeness": 90,
    "completeness_justification": "Complete enough for the authorization probe.",
    "assertiveness": 90,
    "assertiveness_justification": "Assertive enough for the authorization probe.",
    "ambiguity": 10,
    "ambiguity_justification": "Ambiguity is explicitly bounded in this probe.",
    "general_justification": "This is a complete authorization-boundary validation probe.",
    "recommendation": "approve",
}

EVALUATION = {
    "breakdown_completeness": 90,
    "breakdown_justification": "The breakdown is complete.",
    "granularity": 85,
    "granularity_justification": "The granularity is appropriate.",
    "dependency_coherence": 88,
    "dependency_justification": "The dependencies are coherent.",
    "test_coverage_quality": 91,
    "test_coverage_justification": "The test coverage is sufficient.",
    "overall_score": 89,
    "overall_justification": "The specification is ready for execution.",
    "recommendation": "approve",
}


async def _allow_permissions(db, user_id, board_id):
    from okto_pulse.core.infra.permissions import resolve_permissions

    return resolve_permissions(None, None, None)


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setattr(
        "okto_pulse.core.services.main.resolve_user_permissions",
        _allow_permissions,
    )
    app = FastAPI()
    app.include_router(specs_router, prefix=PREFIX)
    app.dependency_overrides[require_user] = lambda: ATTACKER
    return TestClient(app)


def _missing(kind: str) -> str:
    return f"missing-{kind}-{uuid.uuid4().hex}"


def _business_rule(rule_id: str) -> dict[str, Any]:
    return {
        "id": rule_id,
        "title": "Scope rule",
        "rule": "Only authorized board writers can mutate this rule.",
        "when": "A request crosses the REST boundary.",
        "then": "Board scope is resolved before the child.",
        "linked_requirements": [],
        "linked_task_ids": [],
    }


@pytest.fixture
async def spec_graph(db_factory) -> dict[str, str]:
    suffix = uuid.uuid4().hex[:10]
    scopes = ("owned", "foreign", "realm", "viewer", "editor", "admin")
    ids: dict[str, str] = {}
    for scope in scopes:
        ids[f"{scope}_board"] = f"spec-rest-{scope}-board-{suffix}"
        ids[f"{scope}_spec"] = f"spec-rest-{scope}-spec-{suffix}"
        ids[f"{scope}_card"] = f"spec-rest-{scope}-card-{suffix}"
        ids[f"{scope}_scenario"] = f"spec-rest-{scope}-scenario-{suffix}"
        ids[f"{scope}_fr"] = f"fr-{scope}-{suffix}"
        ids[f"{scope}_rule"] = f"spec-rest-{scope}-rule-{suffix}"
        ids[f"{scope}_ir"] = f"spec-rest-{scope}-ir-{suffix}"
        ids[f"{scope}_or"] = f"spec-rest-{scope}-or-{suffix}"
        ids[f"{scope}_kb"] = f"spec-rest-{scope}-kb-{suffix}"
        ids[f"{scope}_qa"] = f"spec-rest-{scope}-qa-{suffix}"

    async with db_factory() as db:
        db.add_all(
            [
                Board(
                    id=ids["owned_board"],
                    name="Owned Spec board",
                    owner_id=ATTACKER,
                    realm_id="local",
                ),
                Board(
                    id=ids["foreign_board"],
                    name="Foreign Spec board",
                    owner_id=OTHER_OWNER,
                    realm_id="local",
                ),
                Board(
                    id=ids["realm_board"],
                    name="Other realm Spec board",
                    owner_id=ATTACKER,
                    realm_id="tenant-b",
                ),
                *[
                    Board(
                        id=ids[f"{permission}_board"],
                        name=f"{permission.title()} Spec board",
                        owner_id=OTHER_OWNER,
                        realm_id="local",
                    )
                    for permission in ("viewer", "editor", "admin")
                ],
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

        for scope in scopes:
            creator = ATTACKER if scope in {"owned", "realm"} else OTHER_OWNER
            db.add(
                Spec(
                    id=ids[f"{scope}_spec"],
                    board_id=ids[f"{scope}_board"],
                    title=f"{scope} scoped spec",
                    description="REST scope fixture",
                    status=SpecStatus.DRAFT,
                    created_by=creator,
                    functional_requirements=[
                        {
                            "id": ids[f"{scope}_fr"],
                            "text": "Scope the request.",
                            "linked_task_ids": [],
                        }
                    ],
                    acceptance_criteria=[
                        {"id": f"ac-{scope}", "text": "No cross-board access."}
                    ],
                    test_scenarios=[
                        {
                            "id": ids[f"{scope}_scenario"],
                            "title": f"{scope} scenario",
                            "scenario_type": "e2e",
                            "given": "A scoped actor",
                            "when": "The actor invokes the REST endpoint",
                            "then": "The board boundary is enforced",
                            "status": "draft",
                            "linked_task_ids": [],
                        }
                    ],
                    business_rules=[_business_rule(ids[f"{scope}_rule"])],
                    integration_requirements=[
                        {
                            "id": ids[f"{scope}_ir"],
                            "title": f"{scope} integration",
                            "description": "Scope-safe integration",
                            "linked_task_ids": [],
                        }
                    ],
                    observability_requirements=[
                        {
                            "id": ids[f"{scope}_or"],
                            "title": f"{scope} observability",
                            "description": "Scope-safe observability",
                            "linked_task_ids": [],
                        }
                    ],
                    evaluations=[],
                    validations=[],
                )
            )
        await db.flush()

        for scope in scopes:
            creator = ATTACKER if scope in {"owned", "realm"} else OTHER_OWNER
            db.add_all(
                [
                    Card(
                        id=ids[f"{scope}_card"],
                        board_id=ids[f"{scope}_board"],
                        spec_id=ids[f"{scope}_spec"],
                        title=f"{scope} card",
                        created_by=creator,
                        card_type=CardType.NORMAL,
                        test_scenario_ids=[],
                    ),
                    SpecKnowledgeBase(
                        id=ids[f"{scope}_kb"],
                        spec_id=ids[f"{scope}_spec"],
                        title=f"{scope} knowledge",
                        content=f"{scope} secret content",
                        mime_type="text/markdown",
                        created_by=creator,
                    ),
                    SpecQAItem(
                        id=ids[f"{scope}_qa"],
                        spec_id=ids[f"{scope}_spec"],
                        question=f"Is {scope} scoped?",
                        question_type="text",
                        asked_by=OTHER_OWNER,
                    ),
                ]
            )
        await db.commit()
    return ids


async def _snapshot(db_factory, ids: dict[str, str]) -> dict[str, Any]:
    async with db_factory() as db:
        specs = list(
            (
                await db.execute(
                    select(Spec).where(
                        Spec.id.in_(
                            [value for key, value in ids.items() if key.endswith("_spec")]
                        )
                    ).order_by(Spec.id)
                )
            ).scalars()
        )
        cards = list(
            (
                await db.execute(
                    select(Card).where(
                        Card.id.in_(
                            [value for key, value in ids.items() if key.endswith("_card")]
                        )
                    ).order_by(Card.id)
                )
            ).scalars()
        )
        knowledge = list(
            (await db.execute(select(SpecKnowledgeBase).order_by(SpecKnowledgeBase.id))).scalars()
        )
        questions = list(
            (await db.execute(select(SpecQAItem).order_by(SpecQAItem.id))).scalars()
        )
        activity_ids = list(
            (await db.execute(select(ActivityLog.id).order_by(ActivityLog.id))).scalars()
        )
        history_ids = list(
            (await db.execute(select(SpecHistory.id).order_by(SpecHistory.id))).scalars()
        )
        event_ids = list(
            (await db.execute(select(DomainEventRow.id).order_by(DomainEventRow.id))).scalars()
        )
        return {
            "specs": [
                (
                    item.id,
                    item.title,
                    item.status.value,
                    item.version,
                    deepcopy(item.functional_requirements),
                    deepcopy(item.business_rules),
                    deepcopy(item.test_scenarios),
                    deepcopy(item.integration_requirements),
                    deepcopy(item.observability_requirements),
                    deepcopy(item.validations),
                    deepcopy(item.evaluations),
                )
                for item in specs
            ],
            "cards": [
                (item.id, item.spec_id, deepcopy(item.test_scenario_ids))
                for item in cards
            ],
            "knowledge": [
                (item.id, item.spec_id, item.title, item.content) for item in knowledge
            ],
            "questions": [
                (item.id, item.spec_id, item.answer, item.answered_by)
                for item in questions
            ],
            "activity": activity_ids,
            "history": history_ids,
            "events": event_ids,
        }


def _request(
    client: TestClient,
    method: str,
    path: str,
    **kwargs: Any,
):
    return client.request(method, f"{PREFIX}/{path.lstrip('/')}", **kwargs)


@pytest.mark.asyncio
async def test_every_foreign_main_spec_route_matches_missing_without_effects(
    client: TestClient,
    db_factory,
    spec_graph: dict[str, str],
) -> None:
    ids = spec_graph
    before = await _snapshot(db_factory, ids)
    cases = [
        ("GET", "specs/{spec}", {}),
        ("PATCH", "specs/{spec}", {"json": {"title": "denied"}}),
        ("POST", "specs/{spec}/move", {"json": {"status": "review"}}),
        ("GET", "specs/{spec}/history", {}),
        (
            "POST",
            "specs/{spec}/structured-entities/business_rule",
            {"json": {"payload": _business_rule("br-denied")}},
        ),
        (
            "PATCH",
            f"specs/{{spec}}/structured-entities/business_rule/{ids['foreign_rule']}",
            {"json": {"payload": {"title": "denied"}}},
        ),
        (
            "POST",
            f"specs/{{spec}}/structured-entities/business_rule/{ids['foreign_rule']}",
            {"json": {"operation": "link_task", "task_id": ids["foreign_card"]}},
        ),
        (
            "POST",
            f"specs/{{spec}}/structured-entities/functional_requirement/{ids['foreign_fr']}",
            {"json": {"operation": "link_task", "task_id": ids["foreign_card"]}},
        ),
        (
            "POST",
            f"specs/{{spec}}/structured-entities/business_rule/{ids['foreign_rule']}/impact-preview",
            {"json": {"operation": "revoke"}},
        ),
        ("POST", "specs/{spec}/validation", {"json": VALIDATION}),
        ("GET", "specs/{spec}/validations", {}),
        ("POST", f"specs/{{spec}}/link-card/{ids['foreign_card']}", {}),
        ("POST", f"specs/{{spec}}/unlink-card/{ids['foreign_card']}", {}),
        (
            "POST",
            f"specs/{{spec}}/scenarios/{ids['foreign_scenario']}/link-task/{ids['foreign_card']}",
            {},
        ),
        (
            "POST",
            f"specs/{{spec}}/scenarios/{ids['foreign_scenario']}/unlink-task/{ids['foreign_card']}",
            {},
        ),
        (
            "POST",
            f"specs/{{spec}}/scenarios/{ids['foreign_scenario']}/evidence/execute",
            {"json": {"status": "passed", "manifest_ref": "pytest:scope"}},
        ),
        (
            "PATCH",
            f"specs/{{spec}}/scenarios/{ids['foreign_scenario']}/status",
            {"json": {"status": "ready"}},
        ),
        (
            "POST",
            f"specs/{{spec}}/integration-requirements/{ids['foreign_ir']}/link-task/{ids['foreign_card']}",
            {},
        ),
        (
            "POST",
            f"specs/{{spec}}/observability-requirements/{ids['foreign_or']}/link-task/{ids['foreign_card']}",
            {},
        ),
        ("GET", "specs/{spec}/knowledge", {}),
        ("GET", f"specs/{{spec}}/knowledge/{ids['foreign_kb']}", {}),
        (
            "POST",
            "specs/{spec}/knowledge",
            {"json": {"title": "denied", "content": "denied"}},
        ),
        ("DELETE", f"specs/{{spec}}/knowledge/{ids['foreign_kb']}", {}),
        ("GET", "specs/{spec}/qa", {}),
        ("POST", "specs/{spec}/qa", {"json": {"question": "denied?"}}),
        (
            "POST",
            f"specs/{{spec}}/qa/{ids['foreign_qa']}/answer",
            {"json": {"answer": "denied"}},
        ),
        ("DELETE", f"specs/{{spec}}/qa/{ids['foreign_qa']}", {}),
        ("POST", "specs/{spec}/evaluations", {"json": EVALUATION}),
        ("GET", "specs/{spec}/evaluations", {}),
        ("DELETE", "specs/{spec}", {}),
    ]

    for method, template, kwargs in cases:
        denied = _request(
            client,
            method,
            template.format(spec=ids["foreign_spec"]),
            **kwargs,
        )
        missing = _request(
            client,
            method,
            template.format(spec=_missing("spec")),
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
async def test_board_create_and_list_role_realm_matrix(
    client: TestClient,
    spec_graph: dict[str, str],
) -> None:
    ids = spec_graph
    missing_board = _missing("board")
    for scope in ("foreign", "realm"):
        denied_list = _request(client, "GET", f"boards/{ids[f'{scope}_board']}/specs")
        missing_list = _request(client, "GET", f"boards/{missing_board}/specs")
        assert denied_list.status_code == missing_list.status_code == 404
        assert denied_list.content == missing_list.content

        denied_create = _request(
            client,
            "POST",
            f"boards/{ids[f'{scope}_board']}/specs",
            json={"title": "denied"},
        )
        missing_create = _request(
            client,
            "POST",
            f"boards/{missing_board}/specs",
            json={"title": "missing"},
        )
        assert denied_create.status_code == missing_create.status_code == 404
        assert denied_create.content == missing_create.content

    viewer_list = _request(client, "GET", f"boards/{ids['viewer_board']}/specs")
    viewer_create = _request(
        client,
        "POST",
        f"boards/{ids['viewer_board']}/specs",
        json={"title": "viewer denied"},
    )
    assert viewer_list.status_code == 200, viewer_list.text
    assert ids["viewer_spec"] in {row["id"] for row in viewer_list.json()}
    assert viewer_create.status_code == 404

    for scope in ("owned", "editor", "admin"):
        listed = _request(client, "GET", f"boards/{ids[f'{scope}_board']}/specs")
        created = _request(
            client,
            "POST",
            f"boards/{ids[f'{scope}_board']}/specs",
            json={"title": f"created by {scope}"},
        )
        assert listed.status_code == 200, (scope, listed.text)
        assert ids[f"{scope}_spec"] in {row["id"] for row in listed.json()}
        assert created.status_code == 201, (scope, created.text)
        assert created.json()["board_id"] == ids[f"{scope}_board"]


@pytest.mark.asyncio
async def test_viewer_reads_but_representative_writes_are_hidden(
    client: TestClient,
    db_factory,
    spec_graph: dict[str, str],
) -> None:
    ids = spec_graph
    reads = [
        f"specs/{ids['viewer_spec']}",
        f"specs/{ids['viewer_spec']}/history",
        f"specs/{ids['viewer_spec']}/validations",
        f"specs/{ids['viewer_spec']}/knowledge",
        f"specs/{ids['viewer_spec']}/knowledge/{ids['viewer_kb']}",
        f"specs/{ids['viewer_spec']}/qa",
        f"specs/{ids['viewer_spec']}/evaluations",
    ]
    for path in reads:
        response = _request(client, "GET", path)
        assert response.status_code == 200, (path, response.text)

    before = await _snapshot(db_factory, ids)
    writes = [
        ("PATCH", f"specs/{ids['viewer_spec']}", {"json": {"title": "denied"}}),
        (
            "POST",
            f"specs/{ids['viewer_spec']}/structured-entities/business_rule",
            {"json": {"payload": _business_rule("viewer-denied")}},
        ),
        ("POST", f"specs/{ids['viewer_spec']}/validation", {"json": VALIDATION}),
        (
            "POST",
            f"specs/{ids['viewer_spec']}/link-card/{ids['viewer_card']}",
            {},
        ),
        (
            "POST",
            f"specs/{ids['viewer_spec']}/scenarios/{ids['viewer_scenario']}/evidence/execute",
            {"json": {"status": "passed", "manifest_ref": "pytest:viewer"}},
        ),
        (
            "PATCH",
            f"specs/{ids['viewer_spec']}/scenarios/{ids['viewer_scenario']}/status",
            {"json": {"status": "ready"}},
        ),
        (
            "POST",
            f"specs/{ids['viewer_spec']}/knowledge",
            {"json": {"title": "denied", "content": "denied"}},
        ),
        (
            "POST",
            f"specs/{ids['viewer_spec']}/qa",
            {"json": {"question": "denied?"}},
        ),
        (
            "POST",
            f"specs/{ids['viewer_spec']}/evaluations",
            {"json": EVALUATION},
        ),
        ("DELETE", f"specs/{ids['viewer_spec']}", {}),
    ]
    for method, path, kwargs in writes:
        response = _request(client, method, path, **kwargs)
        assert response.status_code == 404, (method, path, response.text)
    assert await _snapshot(db_factory, ids) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["editor", "admin"])
async def test_editor_and_admin_can_update_and_delete(
    client: TestClient,
    db_factory,
    spec_graph: dict[str, str],
    role: str,
) -> None:
    spec_id = spec_graph[f"{role}_spec"]
    for entity_type, entity_key in (
        ("functional_requirement", "fr"),
        ("business_rule", "rule"),
    ):
        linked = _request(
            client,
            "POST",
            f"specs/{spec_id}/structured-entities/{entity_type}/"
            f"{spec_graph[f'{role}_{entity_key}']}",
            json={"operation": "link_task", "task_id": spec_graph[f"{role}_card"]},
        )
        assert linked.status_code == 200, (role, entity_type, linked.text)
    updated = _request(
        client,
        "PATCH",
        f"specs/{spec_id}",
        json={"title": f"updated by {role}"},
    )
    deleted = _request(client, "DELETE", f"specs/{spec_id}")
    assert updated.status_code == 200, updated.text
    assert updated.json()["title"] == f"updated by {role}"
    assert deleted.status_code == 204, deleted.text
    async with db_factory() as db:
        assert await db.get(Spec, spec_id) is None


def _normalize_response(response, *opaque_ids: str) -> str:
    rendered = response.text
    for value in opaque_ids:
        rendered = rendered.replace(value, "<opaque-id>")
    return rendered


@pytest.mark.asyncio
async def test_foreign_child_ids_match_missing_and_never_mutate_owned_spec(
    client: TestClient,
    db_factory,
    spec_graph: dict[str, str],
) -> None:
    ids = spec_graph
    before = await _snapshot(db_factory, ids)
    cases = [
        (
            "POST",
            f"specs/{ids['owned_spec']}/link-card/{{child}}",
            ids["foreign_card"],
            _missing("card"),
            {},
        ),
        (
            "GET",
            f"specs/{ids['owned_spec']}/knowledge/{{child}}",
            ids["foreign_kb"],
            _missing("knowledge"),
            {},
        ),
        (
            "DELETE",
            f"specs/{ids['owned_spec']}/knowledge/{{child}}",
            ids["foreign_kb"],
            _missing("knowledge"),
            {},
        ),
        (
            "POST",
            f"specs/{ids['owned_spec']}/qa/{{child}}/answer",
            ids["foreign_qa"],
            _missing("qa"),
            {"json": {"answer": "must not persist"}},
        ),
        (
            "DELETE",
            f"specs/{ids['owned_spec']}/qa/{{child}}",
            ids["foreign_qa"],
            _missing("qa"),
            {},
        ),
        (
            "POST",
            f"specs/{ids['owned_spec']}/scenarios/{{child}}/evidence/execute",
            ids["foreign_scenario"],
            _missing("scenario"),
            {"json": {"status": "passed", "manifest_ref": "pytest:child"}},
        ),
        (
            "PATCH",
            f"specs/{ids['owned_spec']}/scenarios/{{child}}/status",
            ids["foreign_scenario"],
            _missing("scenario"),
            {"json": {"status": "ready"}},
        ),
        (
            "POST",
            f"specs/{ids['owned_spec']}/structured-entities/business_rule/{{child}}",
            ids["foreign_rule"],
            _missing("rule"),
            {"json": {"operation": "revoke"}},
        ),
    ]
    for method, template, foreign_id, missing_id, kwargs in cases:
        foreign = _request(
            client,
            method,
            template.format(child=foreign_id),
            **kwargs,
        )
        missing = _request(
            client,
            method,
            template.format(child=missing_id),
            **kwargs,
        )
        assert foreign.status_code == missing.status_code == 404, (
            method,
            template,
            foreign.text,
            missing.text,
        )
        assert _normalize_response(foreign, foreign_id) == _normalize_response(
            missing, missing_id
        )
    assert await _snapshot(db_factory, ids) == before


def _denied_uow() -> tuple[Any, Any, Any]:
    board = SimpleNamespace(
        id="foreign-board",
        owner_id=OTHER_OWNER,
        realm_id="local",
    )
    spec = SimpleNamespace(
        id="foreign-spec",
        board_id=board.id,
        title="Foreign",
        test_scenarios=[{"id": "scenario", "title": "Scenario"}],
        acceptance_criteria=[],
    )
    services = SimpleNamespace(
        specs=SimpleNamespace(
            get_spec=AsyncMock(return_value=spec),
            create_spec=AsyncMock(),
            update_spec=AsyncMock(),
            submit_spec_validation=AsyncMock(),
        ),
        shares=SimpleNamespace(get_user_permission=AsyncMock(return_value=None)),
        structured_specs=SimpleNamespace(apply=AsyncMock()),
        spec_knowledge=SimpleNamespace(
            get_knowledge=AsyncMock(),
            delete_knowledge=AsyncMock(),
        ),
        spec_qa=SimpleNamespace(
            get_question=AsyncMock(),
            answer_question=AsyncMock(),
        ),
        resolve_user_permissions=AsyncMock(),
        resolve_actor_name=AsyncMock(),
        resolve_effective_spec_parent_lineage=AsyncMock(),
    )
    uow = SimpleNamespace(
        services=services,
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        commit=AsyncMock(),
        rollback=AsyncMock(),
        synchronize=AsyncMock(),
    )
    actor = ActorContext(ATTACKER, "rest", realm_id="local")
    return uow, services, actor


@pytest.mark.asyncio
async def test_denied_writes_stop_before_resolvers_children_writers_and_commit(
    monkeypatch,
) -> None:
    from okto_pulse.core.ports import test_evidence

    uow, services, actor = _denied_uow()
    issuer_resolver = Mock(side_effect=AssertionError("issuer reached"))
    verifier_resolver = Mock(side_effect=AssertionError("verifier reached"))
    monkeypatch.setattr(
        test_evidence,
        "resolve_test_evidence_execution_issuer",
        issuer_resolver,
    )
    monkeypatch.setattr(
        test_evidence,
        "resolve_test_evidence_write_verifier",
        verifier_resolver,
    )

    commands = [
        (
            CreateSpecUseCase(),
            CreateSpecCommand(
                "foreign-board",
                SimpleNamespace(ideation_id="parent", refinement_id=None),
            ),
        ),
        (
            UpdateSpecUseCase(),
            UpdateSpecCommand("foreign-spec", SpecUpdate(title="denied")),
        ),
        (
            RunStructuredSpecEntityUseCase(),
            RunStructuredSpecEntityCommand(
                "foreign-spec",
                "business_rule",
                "create",
                payload=_business_rule("denied"),
            ),
        ),
        (
            ExecuteTestScenarioEvidenceUseCase(),
            ExecuteTestScenarioEvidenceCommand(
                "foreign-spec",
                "scenario",
                "passed",
                "pytest:denied",
            ),
        ),
        (
            DeleteSpecKnowledgeUseCase(),
            DeleteSpecKnowledgeCommand("foreign-spec", "knowledge"),
        ),
        (
            AnswerSpecQuestionUseCase(),
            AnswerSpecQuestionCommand(
                "qa",
                SimpleNamespace(answer="denied", selected=None),
                spec_id="foreign-spec",
            ),
        ),
        (
            SubmitSpecValidationUseCase(),
            SubmitSpecValidationCommand("foreign-spec", {}),
        ),
    ]
    for use_case, command in commands:
        with pytest.raises(EntityNotFoundError):
            await use_case.execute(command, actor=actor, uow=uow)

    services.resolve_effective_spec_parent_lineage.assert_not_awaited()
    services.resolve_user_permissions.assert_not_awaited()
    services.resolve_actor_name.assert_not_awaited()
    services.specs.create_spec.assert_not_awaited()
    services.specs.update_spec.assert_not_awaited()
    services.specs.submit_spec_validation.assert_not_awaited()
    services.structured_specs.apply.assert_not_awaited()
    services.spec_knowledge.get_knowledge.assert_not_awaited()
    services.spec_knowledge.delete_knowledge.assert_not_awaited()
    services.spec_qa.get_question.assert_not_awaited()
    services.spec_qa.answer_question.assert_not_awaited()
    issuer_resolver.assert_not_called()
    verifier_resolver.assert_not_called()
    uow.commit.assert_not_awaited()
    uow.rollback.assert_not_awaited()
    uow.synchronize.assert_not_awaited()


@pytest.mark.asyncio
async def test_services_only_spec_preflight_trusts_only_bound_mcp_actor() -> None:
    spec = SimpleNamespace(id="spec", board_id="board")
    services = SimpleNamespace(
        specs=SimpleNamespace(get_spec=AsyncMock(return_value=spec))
    )

    with pytest.raises(EntityNotFoundError):
        await _require_actor_board_spec(
            services,
            spec.id,
            ActorContext(ATTACKER, "rest", board_id=spec.board_id, realm_id="local"),
        )
    with pytest.raises(EntityNotFoundError):
        await _require_actor_board_spec(
            services,
            spec.id,
            ActorContext(ATTACKER, "mcp", board_id="other", realm_id="local"),
        )

    resolved = await _require_actor_board_spec(
        services,
        spec.id,
        ActorContext(ATTACKER, "mcp", board_id=spec.board_id, realm_id="local"),
    )
    assert resolved is spec
