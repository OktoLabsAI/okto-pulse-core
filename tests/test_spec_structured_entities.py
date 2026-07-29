from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import copy
import json
import logging
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import func, select

from okto_pulse.core.infra.permissions import get_builtin_presets, resolve_permissions
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    ConsolidationQueue,
    DomainEventHandlerExecution,
    DomainEventRow,
    Spec,
    SpecHistory,
    SpecStatus,
)
from okto_pulse.core.events.handlers.consolidation_enqueuer import ConsolidationEnqueuer
from okto_pulse.core.events.handlers.discovery_selector_cache import (
    DiscoverySelectorCacheInvalidationHandler,
)
from okto_pulse.core.events.types import StructuredSpecEntityUpdated
from okto_pulse.core.services.discovery_selector_catalog import (
    AllowAllSelectorAccessPolicy,
    DiscoverySelectorCatalog,
    SelectorOptionsResult,
    get_default_discovery_selector_cache,
    normalize_selector_cache_key,
)
from okto_pulse.core.services.spec_structured_entities import (
    InMemoryStructuredSpecEntityMetricsSink,
    STRUCTURED_SPEC_ENTITY_FIELDS,
    STRUCTURED_SPEC_ENTITY_OPERATIONS,
    StructuredSpecEntityCommand,
    StructuredSpecEntityErrorCode,
    StructuredSpecEntityService,
    filter_active_spec_children,
)
from okto_pulse.core.services.spec_entity_canonicalization import (
    canonicalize_spec_children,
)


@pytest_asyncio.fixture
async def structured_rest_client(db_factory, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.community.api import specs as specs_api
    from okto_pulse.community.api.auth_deps import require_user
    from okto_pulse.community.api.deps import get_unit_of_work
    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

    actor_id = "actor-structured-rest"
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    card_id = f"card-rest-{uuid.uuid4()}"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="REST linked task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        await db.commit()

    async def _override_uow():
        async with db_factory() as session:
            try:
                yield resolve_unit_of_work_factory().wrap(session)
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    permission_preset = {"name": "Spec"}

    async def _allow_permissions(db, user_id, board_id):
        return _permission_set(permission_preset["name"])

    app = FastAPI()
    app.include_router(specs_api.router, prefix="/api/v1")
    app.dependency_overrides[get_unit_of_work] = _override_uow
    app.dependency_overrides[require_user] = lambda: actor_id
    # Spec R01A REST-FU3b-S1: the structured-entity flow now resolves permissions
    # inside RunStructuredSpecEntityUseCase via services.main.resolve_user_permissions
    # (Clean Core — the use case no longer routes through the api alias), so the
    # canonical service function must be patched too for the preset switch to bite.
    monkeypatch.setattr(
        "okto_pulse.core.services.main.resolve_user_permissions", _allow_permissions
    )
    client = TestClient(app)
    client.permission_preset = permission_preset
    return client, board_id, spec_id, card_id


def _preset(name: str):
    return next(p for p in get_builtin_presets() if p["name"] == name)


def _permission_set(name: str):
    return resolve_permissions(None, _preset(name)["flags"], None)


def _payload_for(entity_type: str) -> dict:
    payloads = {
        "functional_requirement": {"text": "User can edit FRs structurally"},
        "acceptance_criterion": {"text": "Given a payload, when saved, then only one AC changes"},
        "technical_requirement": {"id": "tr_struct", "text": "Use the structured entity service"},
        "business_rule": {
            "id": "br_struct",
            "title": "Single mutation boundary",
            "rule": "All structured spec child edits must use the service",
            "when": "A caller edits a spec child entity",
            "then": "The service validates and persists the child entity",
        },
        "api_contract": {
            "id": "api_struct",
            "method": "COMPONENT",
            "path": "StructuredSpecEntityService.mutate",
            "description": "Structured mutation boundary",
        },
        "integration_requirement": {
            "id": "ir_struct",
            "title": "Discovery invalidation hook",
            "integration_type": "event",
        },
        "observability_requirement": {
            "id": "or_struct",
            "title": "Mutation metric",
            "signal_type": "metric",
            "metric_name": "spec_structured_entity_operation_total",
        },
        "decision": {
            "id": "dec_struct",
            "title": "Use structured child editing",
            "rationale": "Whole-list JSON replacement is error-prone",
        },
    }
    return copy.deepcopy(payloads[entity_type])


SPEC_CHILD_FIELDS = [
    "functional_requirements",
    "acceptance_criteria",
    "technical_requirements",
    "business_rules",
    "api_contracts",
    "integration_requirements",
    "observability_requirements",
    "decisions",
]


def _existing_payload_for(entity_type: str) -> dict:
    payload = _payload_for(entity_type)
    if entity_type == "functional_requirement":
        payload["id"] = "fr_existing"
    if entity_type == "acceptance_criterion":
        payload["id"] = "ac_existing"
    return payload


def _update_payload_for(entity_type: str) -> dict:
    return {
        "functional_requirement": {"text": "Updated FR through structured service"},
        "acceptance_criterion": {"text": "Updated AC through structured service"},
        "technical_requirement": {"text": "Updated TR through structured service"},
        "business_rule": {"then": "Updated BR through structured service"},
        "api_contract": {"description": "Updated API contract through structured service"},
        "integration_requirement": {"description": "Updated IR through structured service"},
        "observability_requirement": {"description": "Updated OR through structured service"},
        "decision": {"rationale": "Updated decision through structured service"},
    }[entity_type]


def _updated_value_for(entity_type: str) -> tuple[str, str]:
    field_by_type = {
        "functional_requirement": ("text", "Updated FR through structured service"),
        "acceptance_criterion": ("text", "Updated AC through structured service"),
        "technical_requirement": ("text", "Updated TR through structured service"),
        "business_rule": ("then", "Updated BR through structured service"),
        "api_contract": ("description", "Updated API contract through structured service"),
        "integration_requirement": ("description", "Updated IR through structured service"),
        "observability_requirement": ("description", "Updated OR through structured service"),
        "decision": ("rationale", "Updated decision through structured service"),
    }
    return field_by_type[entity_type]


async def _spec_json_snapshot(db, spec_id: str) -> str:
    spec = await db.get(Spec, spec_id)
    payload = {
        "edition": spec.edition,
        "version": spec.version,
        **{field_name: copy.deepcopy(getattr(spec, field_name)) for field_name in SPEC_CHILD_FIELDS},
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


async def _side_effect_counts(db, *, board_id: str, spec_id: str) -> dict[str, int]:
    history_count = await db.scalar(
        select(func.count()).select_from(SpecHistory).where(SpecHistory.spec_id == spec_id)
    )
    event_count = await db.scalar(
        select(func.count()).select_from(DomainEventRow).where(DomainEventRow.board_id == board_id)
    )
    return {
        "history": int(history_count or 0),
        "events": int(event_count or 0),
    }


SENSITIVE_TEST_FRAGMENTS = (
    "secret-decision-body",
    "super-secret@example.com",
    "hunter2",
    "<secret-xml>",
    "{\"secret_json\"",
)


def _assert_no_sensitive_fragments(value) -> None:
    serialized = json.dumps(value, default=str, sort_keys=True)
    for fragment in SENSITIVE_TEST_FRAGMENTS:
        assert fragment not in serialized


def _assert_structured_metric_labels_are_safe(events) -> None:
    allowed = {
        "board_id",
        "spec_id",
        "entity_type",
        "operation",
        "outcome",
        "reason",
        "event_type",
        "child_ref",
    }
    for event in events:
        labels = getattr(event, "labels", event)
        assert set(labels).issubset(allowed)
        assert "request_body" not in labels
        _assert_no_sensitive_fragments(labels)


async def _seed_spec(db, *, board_id: str, spec_id: str, actor_id: str) -> None:
    db.add(Board(id=board_id, name="Structured Board", owner_id=actor_id))
    db.add(
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Structured Spec",
            status=SpecStatus.DRAFT,
            edition=6,
            created_by=actor_id,
            # Generic structured-writer tests start from the canonical
            # post-SK-A shape. Dedicated legacy-materialization tests below
            # explicitly replace the collection they exercise with strings.
            functional_requirements=[
                {
                    "id": "fr_existing",
                    "text": "Existing FR",
                    "status": "active",
                }
            ],
            acceptance_criteria=[
                {
                    "id": "ac_existing",
                    "text": "Existing AC",
                    "status": "active",
                }
            ],
            technical_requirements=[],
            business_rules=[],
            api_contracts=[],
            integration_requirements=[],
            observability_requirements=[],
            decisions=[],
        )
    )
    await db.flush()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entity_type", "field_name", "expected_id"),
    [
        ("functional_requirement", "functional_requirements", "fr_"),
        ("acceptance_criterion", "acceptance_criteria", "ac_"),
        ("technical_requirement", "technical_requirements", "tr_struct"),
        ("business_rule", "business_rules", "br_struct"),
        ("api_contract", "api_contracts", "api_struct"),
        ("integration_requirement", "integration_requirements", "ir_struct"),
        ("observability_requirement", "observability_requirements", "or_struct"),
        ("decision", "decisions", "dec_struct"),
    ],
)
async def test_structured_create_supports_all_spec_entity_types(
    db_factory,
    entity_type,
    field_name,
    expected_id,
):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        service = StructuredSpecEntityService(db)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type=entity_type,
                operation="create",
                payload=_payload_for(entity_type),
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        if expected_id.endswith("_"):
            assert result.entity_id.startswith(expected_id)
        else:
            assert result.entity_id == expected_id
        assert result.spec_version == 2
        assert result.changed_fields == [field_name]
        spec = await db.get(Spec, spec_id)
        values = getattr(spec, field_name)
        expected_len = 2 if entity_type in {"functional_requirement", "acceptance_criterion"} else 1
        assert len(values) == expected_len
        if entity_type in {"functional_requirement", "acceptance_criterion"}:
            assert isinstance(values[-1], dict)
            assert values[-1]["id"] == result.entity_id
        assert spec.version == 2
        assert spec.edition == 6


@pytest.mark.asyncio
async def test_structured_decision_accepts_structured_tr_link(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.technical_requirements = [
            {"id": "tr_struct", "text": "Use the structured entity service"}
        ]
        await db.flush()

        service = StructuredSpecEntityService(db)
        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="create",
                payload={
                    **_payload_for("decision"),
                    "linked_requirements": ["tr_struct"],
                },
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        spec = await db.get(Spec, spec_id)
        assert spec.decisions[0]["linked_requirements"] == ["tr_struct"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entity_type", "field_name", "entity_id"),
    [
        ("functional_requirement", "functional_requirements", "fr_existing"),
        ("acceptance_criterion", "acceptance_criteria", "ac_existing"),
        ("technical_requirement", "technical_requirements", "tr_struct"),
        ("business_rule", "business_rules", "br_struct"),
        ("api_contract", "api_contracts", "api_struct"),
        ("integration_requirement", "integration_requirements", "ir_struct"),
        ("observability_requirement", "observability_requirements", "or_struct"),
        ("decision", "decisions", "dec_struct"),
    ],
)
async def test_structured_update_supports_all_spec_entity_types_and_persists_side_effects(
    db_factory,
    entity_type,
    field_name,
    entity_id,
):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        setattr(spec, field_name, [_existing_payload_for(entity_type)])
        await db.flush()

        before_snapshot = await _spec_json_snapshot(db, spec_id)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        service = StructuredSpecEntityService(db, metrics_sink=sink)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type=entity_type,
                entity_id=entity_id,
                operation="update",
                payload=_update_payload_for(entity_type),
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        assert result.entity_id == entity_id
        assert result.child_ref == f"spec:{spec_id}:{entity_type}:{entity_id}"
        assert result.spec_version == 2
        assert result.changed_fields == [field_name]
        after_snapshot = await _spec_json_snapshot(db, spec_id)
        assert after_snapshot != before_snapshot
        spec = await db.get(Spec, spec_id)
        updated_key, updated_value = _updated_value_for(entity_type)
        assert getattr(spec, field_name)[0][updated_key] == updated_value
        after_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        assert after_side_effects["history"] == before_side_effects["history"] + 1
        expected_event_delta = 3 if field_name in {
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
        } else 2
        assert after_side_effects["events"] == before_side_effects["events"] + expected_event_delta
        structured_event = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "structured_entity.updated",
                )
            )
        ).scalar_one()
        assert structured_event.payload_json["child_ref"] == f"spec:{spec_id}:{entity_type}:{entity_id}"
        assert structured_event.payload_json["changed_fields"]
        assert sink.events[-1].name == "spec_structured_entity_mutation_total"
        assert sink.events[-1].labels["outcome"] == "success"


@pytest.mark.asyncio
async def test_structured_update_mutates_only_target_collection_and_records_metric(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.business_rules = [_payload_for("business_rule")]
        await db.flush()

        service = StructuredSpecEntityService(db, metrics_sink=sink)
        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="business_rule",
                entity_id="br_struct",
                operation="update",
                payload={"then": "Only the selected business rule is changed"},
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        spec = await db.get(Spec, spec_id)
        assert spec.business_rules[0]["then"] == "Only the selected business rule is changed"
        assert spec.functional_requirements == [
            {
                "id": "fr_existing",
                "text": "Existing FR",
                "status": "active",
            }
        ]
        assert spec.acceptance_criteria == [
            {
                "id": "ac_existing",
                "text": "Existing AC",
                "status": "active",
            }
        ]
        assert spec.version == 2
        assert sink.events[-1].labels["outcome"] == "success"
        assert sink.events[-1].labels["entity_type"] == "business_rule"


@pytest.mark.asyncio
async def test_successful_structured_mutation_publishes_safe_event_and_handlers(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        service = StructuredSpecEntityService(db)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="create",
                payload=_payload_for("decision"),
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        row = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "structured_entity.created",
                )
            )
        ).scalar_one()
        assert row.actor_id == actor_id
        assert row.payload_json == {
            "spec_id": spec_id,
            "entity_type": "decision",
            "entity_id": "dec_struct",
            "child_ref": f"spec:{spec_id}:decision:dec_struct",
            "operation": "create",
            "changed_fields": [
                "decisions.dec_struct.id",
                "decisions.dec_struct.rationale",
                "decisions.dec_struct.title",
            ],
            "spec_version": 2,
        }
        assert "Exercise impact preview" not in json.dumps(row.payload_json)

        handlers = {
            execution.handler_name
            for execution in (
                await db.execute(
                    select(DomainEventHandlerExecution).where(
                        DomainEventHandlerExecution.event_id == row.id
                    )
                )
            ).scalars()
        }
        assert ConsolidationEnqueuer.__name__ in handlers
        assert DiscoverySelectorCacheInvalidationHandler.__name__ in handlers


@pytest.mark.asyncio
async def test_structured_update_event_uses_dotted_changed_fields_without_payload_dump(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.api_contracts = [_payload_for("api_contract")]
        await db.flush()

        service = StructuredSpecEntityService(db)
        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="api_contract",
                entity_id="api_struct",
                operation="update",
                payload={"request_body": {"password": "must-not-leak"}},
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        row = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "structured_entity.updated",
                )
            )
        ).scalar_one()
        assert row.payload_json["changed_fields"] == [
            "api_contracts.api_struct.request_body"
        ]
        serialized = json.dumps(row.payload_json)
        assert "must-not-leak" not in serialized
        assert "password" not in serialized


@pytest.mark.asyncio
async def test_structured_events_are_durable_after_commit_and_failed_mutations_emit_none(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.api_contracts = [_payload_for("api_contract")]
        spec.business_rules = [_payload_for("business_rule")]
        await db.flush()

        service = StructuredSpecEntityService(db)
        created = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="create",
                payload={
                    "id": "dec_secret",
                    "title": "Durable event decision",
                    "rationale": "secret-decision-body",
                    "notes": "super-secret@example.com",
                },
                permission_set=_permission_set("Spec"),
            )
        )
        updated = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="api_contract",
                entity_id="api_struct",
                operation="update",
                payload={
                    "request_body": {
                        "password": "hunter2",
                        "xml": "<secret-xml>",
                        "json": "{\"secret_json\": true}",
                    }
                },
                permission_set=_permission_set("Spec"),
            )
        )
        revoked = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="business_rule",
                entity_id="br_struct",
                operation="revoke",
                permission_set=_permission_set("Spec"),
            )
        )
        failed = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="business_rule",
                operation="create",
                payload={"id": "br_invalid", "rule": "secret-decision-body"},
                permission_set=_permission_set("Spec"),
            )
        )
        assert [created.success, updated.success, revoked.success, failed.success] == [True, True, True, False]
        await db.commit()

    async with db_factory() as db:
        rows = (
            await db.execute(
                select(DomainEventRow)
                .where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type.in_(
                        [
                            "structured_entity.created",
                            "structured_entity.updated",
                            "structured_entity.revoked",
                        ]
                    ),
                )
                .order_by(DomainEventRow.occurred_at.asc())
            )
        ).scalars().all()

    assert [row.event_type for row in rows] == [
        "structured_entity.created",
        "structured_entity.updated",
        "structured_entity.revoked",
    ]
    assert all(row.actor_id == actor_id for row in rows)
    assert rows[0].payload_json["child_ref"] == f"spec:{spec_id}:decision:dec_secret"
    assert rows[1].payload_json["child_ref"] == f"spec:{spec_id}:api_contract:api_struct"
    assert rows[2].payload_json["child_ref"] == f"spec:{spec_id}:business_rule:br_struct"
    for row in rows:
        _assert_no_sensitive_fragments(row.payload_json)


@pytest.mark.asyncio
async def test_structured_metric_labels_are_bounded_and_payload_free(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.api_contracts = [_payload_for("api_contract")]
        await db.flush()

        service = StructuredSpecEntityService(db, metrics_sink=sink)
        success = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id="super-secret@example.com",
                entity_type="api_contract",
                entity_id="api_struct",
                operation="update",
                payload={
                    "request_body": {
                        "password": "hunter2",
                        "xml": "<secret-xml>",
                        "json": "{\"secret_json\": true}",
                    }
                },
                permission_set=_permission_set("Spec"),
            )
        )
        failure = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id="super-secret@example.com",
                entity_type="business_rule",
                operation="create",
                payload={"id": "br_invalid", "rule": "secret-decision-body"},
                permission_set=_permission_set("Spec"),
            )
        )

    assert success.success is True
    assert failure.success is False
    assert {event.name for event in sink.events} >= {
        "spec_structured_entity_mutation_total",
        "spec_structured_entity_event_emitted_total",
        "spec_structured_entity_validation_failure_total",
    }
    _assert_structured_metric_labels_are_safe(sink.events)


@pytest.mark.asyncio
async def test_structured_event_enqueues_parent_spec_for_deterministic_kg(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        event = StructuredSpecEntityUpdated(
            board_id=board_id,
            actor_id=actor_id,
            spec_id=spec_id,
            entity_type="functional_requirement",
            entity_id="fr_abc12345",
            child_ref=f"spec:{spec_id}:functional_requirement:fr_abc12345",
            operation="update",
            changed_fields=["functional_requirements.fr_abc12345.text"],
            spec_version=2,
        )

        await ConsolidationEnqueuer().handle(event, db)
        queue_row = (
            await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "spec",
                    ConsolidationQueue.artifact_id == spec_id,
                )
            )
        ).scalar_one()

        assert queue_row.status == "pending"
        assert queue_row.triggered_by_event == "structured_entity.updated"
        assert queue_row.source == "event:structured_entity.updated"


@pytest.mark.asyncio
async def test_structured_event_kg_reenqueue_logs_child_ref_and_safe_labels(db_factory, caplog):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    child_ref = f"spec:{spec_id}:functional_requirement:fr_stable1234"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id="actor-structured")
        event = StructuredSpecEntityUpdated(
            board_id=board_id,
            actor_id="super-secret@example.com",
            spec_id=spec_id,
            entity_type="functional_requirement",
            entity_id="fr_stable1234",
            child_ref=child_ref,
            operation="update",
            changed_fields=["functional_requirements.fr_stable1234.text"],
            spec_version=2,
        )

        caplog.set_level(logging.INFO, logger="okto_pulse.core.events.consolidation_enqueuer")
        await ConsolidationEnqueuer().handle(event, db)
        queue_row = (
            await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "spec",
                    ConsolidationQueue.artifact_id == spec_id,
                )
            )
        ).scalar_one()

    assert queue_row.source == "event:structured_entity.updated"
    assert queue_row.triggered_by_event == "structured_entity.updated"
    records = [
        record
        for record in caplog.records
        if getattr(record, "metric_name", None) == "spec_structured_entity_kg_reenqueue_total"
    ]
    assert len(records) == 1
    record = records[0]
    labels = {
        "board_id": record.board_id,
        "spec_id": record.spec_id,
        "child_ref": record.child_ref,
        "entity_type": record.entity_type,
        "operation": record.operation,
        "outcome": record.outcome,
        "reason": record.reason,
    }
    assert labels["child_ref"] == child_ref
    _assert_structured_metric_labels_are_safe([labels])


@pytest.mark.asyncio
async def test_structured_event_invalidates_discovery_selector_cache_and_refreshes_metadata(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    cache = get_default_discovery_selector_cache()
    cache.clear()
    decision_key = normalize_selector_cache_key(
        board_id=board_id,
        selector_kind="spec_child",
        spec_id=spec_id,
        child_type="decision",
    )
    business_rule_key = normalize_selector_cache_key(
        board_id=board_id,
        selector_kind="spec_child",
        spec_id=spec_id,
        child_type="business_rule",
    )
    cache.set(decision_key, SelectorOptionsResult(options=[]))
    cache.set(business_rule_key, SelectorOptionsResult(options=[]))

    try:
        async with db_factory() as db:
            await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
            spec = await db.get(Spec, spec_id)
            spec.decisions = [_payload_for("decision")]
            await db.flush()

            service = StructuredSpecEntityService(db)
            result = await service.mutate(
                StructuredSpecEntityCommand(
                    board_id=board_id,
                    spec_id=spec_id,
                    actor_id=actor_id,
                    entity_type="decision",
                    entity_id="dec_struct",
                    operation="update",
                    payload={"title": "Fresh selector decision"},
                    expected_spec_version=1,
                    permission_set=_permission_set("Spec"),
                )
            )
            assert result.success is True
            await db.commit()

            await DiscoverySelectorCacheInvalidationHandler().handle(
                StructuredSpecEntityUpdated(
                    board_id=board_id,
                    actor_id=actor_id,
                    spec_id=spec_id,
                    entity_type="decision",
                    entity_id="dec_struct",
                    child_ref=f"spec:{spec_id}:decision:dec_struct",
                    operation="update",
                    changed_fields=["decisions.dec_struct.title"],
                    spec_version=2,
                ),
                db,
            )
            assert cache.get(decision_key) is None
            assert cache.get(business_rule_key) is not None

            catalog = DiscoverySelectorCatalog(
                AllowAllSelectorAccessPolicy(),
                cache=cache,
            )
            refreshed = await catalog.list_options(
                db,
                board_id=board_id,
                selector_kind="spec_child",
                spec_id=spec_id,
                child_type="decision",
            )
            assert refreshed.cache_status == "miss"
            assert len(refreshed.options) == 1
            option = refreshed.options[0].to_dict()
            assert option["label"] == "Fresh selector decision"
            assert option["child_ref"] == f"spec:{spec_id}:decision:dec_struct"
            _assert_no_sensitive_fragments(option)

            cached = cache.get(decision_key)
            assert cached is not None
            assert cached.options[0].to_dict()["label"] == "Fresh selector decision"
    finally:
        cache.clear()


@pytest.mark.asyncio
async def test_validation_failure_preserves_spec_json_and_version(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        before_snapshot = await _spec_json_snapshot(db, spec_id)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        service = StructuredSpecEntityService(db, metrics_sink=sink)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="business_rule",
                operation="create",
                payload={**_payload_for("business_rule"), "unexpected": "boom"},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is False
        assert result.error_code == StructuredSpecEntityErrorCode.VALIDATION_FAILED
        assert await _spec_json_snapshot(db, spec_id) == before_snapshot
        assert await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == before_side_effects
        assert sink.events[-1].name == "spec_structured_entity_validation_failure_total"
        assert sink.events[-1].labels["outcome"] == "failure"
        assert sink.events[-1].labels["reason"] == StructuredSpecEntityErrorCode.VALIDATION_FAILED


@pytest.mark.asyncio
async def test_version_conflict_returns_typed_error_without_mutation(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        service = StructuredSpecEntityService(db, metrics_sink=sink)
        before_snapshot = await _spec_json_snapshot(db, spec_id)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="create",
                payload=_payload_for("decision"),
                expected_spec_version=9,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is False
        assert result.error_code == StructuredSpecEntityErrorCode.VERSION_CONFLICT
        assert await _spec_json_snapshot(db, spec_id) == before_snapshot
        assert await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == before_side_effects
        assert sink.events[-1].name == "spec_structured_entity_version_conflict_total"
        assert sink.events[-1].labels["outcome"] == "failure"
        assert sink.events[-1].labels["reason"] == StructuredSpecEntityErrorCode.VERSION_CONFLICT


@pytest.mark.asyncio
async def test_reporter_permission_denied_before_mutation(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        service = StructuredSpecEntityService(db)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="api_contract",
                operation="revoke",
                entity_id="api_struct",
                permission_set=_permission_set("Reporter"),
            )
        )

        assert result.success is False
        assert result.error_code == StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED
        assert result.required_permission == "spec.structured_entity.api_contract.revoke"
        assert (await db.get(Spec, spec_id)).version == 1


@pytest.mark.asyncio
async def test_viewer_preset_fails_every_structured_entity_mutation_before_payload_validation(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        before_snapshot = await _spec_json_snapshot(db, spec_id)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        service = StructuredSpecEntityService(db, metrics_sink=sink)

        for entity_type in STRUCTURED_SPEC_ENTITY_FIELDS:
            for operation in STRUCTURED_SPEC_ENTITY_OPERATIONS:
                result = await service.mutate(
                    StructuredSpecEntityCommand(
                        board_id=board_id,
                        spec_id=spec_id,
                        actor_id=actor_id,
                        entity_type=entity_type,
                        operation=operation,
                        entity_id=f"{entity_type}_missing",
                        expected_spec_version=1,
                        permission_set=_permission_set("Reporter"),
                    )
                )

                assert result.success is False
                assert result.error_code == StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED
                assert result.required_permission == f"spec.structured_entity.{entity_type}.{operation}"

        assert await _spec_json_snapshot(db, spec_id) == before_snapshot
        assert await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == before_side_effects
        deny_metrics = [
            event
            for event in sink.events
            if event.name == "spec_structured_entity_authorization_denied_total"
        ]
        assert len(deny_metrics) == len(STRUCTURED_SPEC_ENTITY_FIELDS) * len(STRUCTURED_SPEC_ENTITY_OPERATIONS)
        assert all(event.labels["outcome"] == "failure" for event in deny_metrics)
        assert all(
            event.labels["reason"] == StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED
            for event in deny_metrics
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ["decision", "api_contract"])
@pytest.mark.parametrize("operation", ["revoke", "supersede", "restore", "reorder"])
async def test_operational_agent_preset_blocks_destructive_decision_and_api_contract_edits(
    db_factory,
    entity_type,
    operation,
):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        before_snapshot = await _spec_json_snapshot(db, spec_id)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        service = StructuredSpecEntityService(db)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type=entity_type,
                operation=operation,
                entity_id=f"{entity_type}_restricted",
                expected_spec_version=1,
                permission_set=_permission_set("Executor"),
            )
        )

        assert result.success is False
        assert result.error_code == StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED
        assert result.required_permission == f"spec.structured_entity.{entity_type}.{operation}"
        assert await _spec_json_snapshot(db, spec_id) == before_snapshot
        assert await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == before_side_effects


@pytest.mark.asyncio
async def test_unsupported_operation_and_missing_entity_are_typed_errors(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    sink = InMemoryStructuredSpecEntityMetricsSink()
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.api_contracts = [_payload_for("api_contract")]
        await db.flush()
        before_snapshot = await _spec_json_snapshot(db, spec_id)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        service = StructuredSpecEntityService(db, metrics_sink=sink)

        unsupported = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="api_contract",
                operation="delete",
                entity_id="api_struct",
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )
        missing = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="update",
                entity_id="missing_dec",
                payload={"title": "No-op"},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert unsupported.success is False
        assert unsupported.error_code == StructuredSpecEntityErrorCode.UNSUPPORTED_OPERATION
        assert missing.success is False
        assert missing.error_code == StructuredSpecEntityErrorCode.ENTITY_NOT_FOUND
        assert await _spec_json_snapshot(db, spec_id) == before_snapshot
        assert await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == before_side_effects
        failure_reasons = [
            event.labels["reason"]
            for event in sink.events
            if event.name == "spec_structured_entity_mutation_total"
        ]
        assert failure_reasons == [
            StructuredSpecEntityErrorCode.UNSUPPORTED_OPERATION,
            StructuredSpecEntityErrorCode.ENTITY_NOT_FOUND,
        ]


@pytest.mark.asyncio
async def test_link_task_validates_target_card_before_persisting(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    card_id = f"card-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.decisions = [_payload_for("decision")]
        await db.flush()
        service = StructuredSpecEntityService(db)

        missing = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="link_task",
                entity_id="dec_struct",
                task_id="missing-card",
                permission_set=_permission_set("Spec"),
            )
        )

        assert missing.success is False
        assert missing.error_code == StructuredSpecEntityErrorCode.LINK_TARGET_INVALID
        assert (await db.get(Spec, spec_id)).decisions[0].get("linked_task_ids") is None

        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Implement decision",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        await db.flush()

        linked = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                operation="link_task",
                entity_id="dec_struct",
                task_id=card_id,
                permission_set=_permission_set("Spec"),
            )
        )

        assert linked.success is True
        assert (await db.get(Spec, spec_id)).decisions[0]["linked_task_ids"] == [card_id]


@pytest.mark.asyncio
async def test_legacy_fr_update_materializes_ids_and_migrates_requirement_refs(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.functional_requirements = ["Existing FR"]
        spec.business_rules = [
            {
                "id": "br_by_index",
                "title": "Index ref",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["0"],
            },
            {
                "id": "br_by_text",
                "title": "Text ref",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["Existing FR"],
            },
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                operation="update",
                entity_id="0",
                payload={"text": "Updated FR"},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        assert result.entity_id.startswith("fr_")
        assert result.child_ref == f"spec:{spec_id}:functional_requirement:{result.entity_id}"
        spec = await db.get(Spec, spec_id)
        assert spec.functional_requirements[0] == {
            "id": result.entity_id,
            "text": "Updated FR",
            "status": "active",
        }
        assert spec.business_rules[0]["linked_requirements"] == [result.entity_id]
        assert spec.business_rules[1]["linked_requirements"] == [result.entity_id]
        assert set(result.changed_fields) == {"functional_requirements", "business_rules"}


@pytest.mark.asyncio
async def test_legacy_ac_update_materializes_ids_and_preserves_scenario_coverage(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.acceptance_criteria = ["Existing AC"]
        spec.test_scenarios = [
            {
                "id": "ts_by_index",
                "title": "By index",
                "scenario_type": "unit",
                "given": "G",
                "when": "W",
                "then": "T",
                "linked_criteria": ["0"],
            },
            {
                "id": "ts_by_text",
                "title": "By text",
                "scenario_type": "unit",
                "given": "G",
                "when": "W",
                "then": "T",
                "linked_criteria": ["Existing AC"],
            },
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="acceptance_criterion",
                operation="update",
                entity_id="0",
                payload={"text": "Updated AC"},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        assert result.entity_id.startswith("ac_")
        spec = await db.get(Spec, spec_id)
        assert spec.acceptance_criteria[0]["id"] == result.entity_id
        assert spec.acceptance_criteria[0]["text"] == "Updated AC"
        assert spec.test_scenarios[0]["linked_criteria"] == [result.entity_id]
        assert spec.test_scenarios[1]["linked_criteria"] == [result.entity_id]
        assert set(result.changed_fields) == {"acceptance_criteria", "test_scenarios"}


@pytest.mark.asyncio
async def test_legacy_ac_materialized_ids_survive_reorder_with_scenario_links(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.acceptance_criteria = ["Existing AC", "Second AC"]
        spec.test_scenarios = [
            {
                "id": "ts_by_index",
                "title": "By index",
                "scenario_type": "unit",
                "given": "G",
                "when": "W",
                "then": "T",
                "linked_criteria": ["0"],
            },
            {
                "id": "ts_by_text",
                "title": "By text",
                "scenario_type": "unit",
                "given": "G",
                "when": "W",
                "then": "T",
                "linked_criteria": ["Existing AC"],
            },
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        materialized = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="acceptance_criterion",
                operation="update",
                entity_id="0",
                payload={"text": "Updated AC"},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )
        assert materialized.success is True
        spec = await db.get(Spec, spec_id)
        first_id = spec.acceptance_criteria[0]["id"]
        second_id = spec.acceptance_criteria[1]["id"]
        assert materialized.child_ref == f"spec:{spec_id}:acceptance_criterion:{first_id}"
        assert first_id.startswith("ac_")
        assert second_id.startswith("ac_")
        assert spec.test_scenarios[0]["linked_criteria"] == [first_id]
        assert spec.test_scenarios[1]["linked_criteria"] == [first_id]

        preview = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="acceptance_criterion",
                operation="reorder",
                payload={"ordered_entity_ids": [second_id, first_id]},
                expected_spec_version=2,
                permission_set=_permission_set("Spec"),
            )
        )
        assert preview.error_code == StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED
        assert preview.impact_report["counts_by_type"] == {"test_scenario": 2}

        reordered = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="acceptance_criterion",
                operation="reorder",
                payload={"ordered_entity_ids": [second_id, first_id]},
                expected_spec_version=2,
                ack_token=preview.ack_token,
                permission_set=_permission_set("Spec"),
            )
        )

        assert reordered.success is True
        spec = await db.get(Spec, spec_id)
        assert [item["id"] for item in spec.acceptance_criteria] == [second_id, first_id]
        assert spec.test_scenarios[0]["linked_criteria"] == [first_id]
        assert spec.test_scenarios[1]["linked_criteria"] == [first_id]


@pytest.mark.asyncio
async def test_reorder_materialized_fr_keeps_stable_ids_and_links(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.functional_requirements = [
            {"id": "fr_a", "text": "A", "status": "active"},
            {"id": "fr_b", "text": "B", "status": "active"},
        ]
        spec.business_rules = [
            {
                "id": "br_b",
                "title": "B linked",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["fr_b"],
            }
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        preview = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                operation="reorder",
                payload={"ordered_entity_ids": ["fr_b", "fr_a"]},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert preview.success is False
        assert preview.error_code == StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED
        result = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                operation="reorder",
                payload={"ordered_entity_ids": ["fr_b", "fr_a"]},
                expected_spec_version=1,
                ack_token=preview.ack_token,
                permission_set=_permission_set("Spec"),
            )
        )

        assert result.success is True
        spec = await db.get(Spec, spec_id)
        assert [item["id"] for item in spec.functional_requirements] == ["fr_b", "fr_a"]
        assert spec.business_rules[0]["linked_requirements"] == ["fr_b"]


@pytest.mark.asyncio
async def test_revoke_linked_fr_requires_impact_ack_and_then_marks_revoked(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.functional_requirements = [{"id": "fr_a", "text": "A", "status": "active"}]
        spec.business_rules = [
            {
                "id": "br_a",
                "title": "A linked",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["fr_a"],
            }
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)
        before_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)

        preview = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                entity_id="fr_a",
                operation="revoke",
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert preview.success is False
        assert preview.error_code == StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED
        assert preview.ack_token
        assert preview.impact_report["counts_by_type"] == {"business_rule": 1}
        assert (await db.get(Spec, spec_id)).version == 1
        assert (await db.get(Spec, spec_id)).functional_requirements[0]["status"] == "active"

        applied = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                entity_id="fr_a",
                operation="revoke",
                expected_spec_version=1,
                ack_token=preview.ack_token,
                permission_set=_permission_set("Spec"),
            )
        )

        spec = await db.get(Spec, spec_id)
        assert applied.success is True
        assert spec.version == 2
        assert len(spec.functional_requirements) == 1
        assert spec.functional_requirements[0]["status"] == "revoked"
        assert filter_active_spec_children(spec.functional_requirements) == []
        assert spec.business_rules[0]["linked_requirements"] == ["fr_a"]
        after_side_effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        assert after_side_effects["history"] == before_side_effects["history"] + 1
        assert after_side_effects["events"] == before_side_effects["events"] + 2

        reused = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                entity_id="fr_a",
                operation="revoke",
                expected_spec_version=2,
                ack_token=preview.ack_token,
                permission_set=_permission_set("Spec"),
            )
        )

        assert reused.success is False
        assert reused.error_code == StructuredSpecEntityErrorCode.IMPACT_ACK_INVALID


@pytest.mark.asyncio
async def test_revoke_fr_impact_detects_legacy_index_and_text_links(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.functional_requirements = [{"id": "fr_a", "text": "A", "status": "active"}]
        spec.business_rules = [
            {
                "id": "br_a",
                "title": "A linked by legacy index",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["0"],
            }
        ]
        spec.decisions = [
            {
                "id": "dec_a",
                "title": "A linked by legacy text",
                "rationale": "Keep legacy references impact-aware.",
                "linked_requirements": ["A"],
            }
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        preview = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="functional_requirement",
                entity_id="fr_a",
                operation="revoke",
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        assert preview.success is False
        assert preview.error_code == StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED
        assert preview.impact_report["counts_by_type"] == {
            "business_rule": 1,
            "decision": 1,
        }


@pytest.mark.asyncio
async def test_update_legacy_technical_requirement_materializes_and_preserves_id(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.technical_requirements = ["Legacy TR"]
        await db.flush()
        service = StructuredSpecEntityService(db)
        canonical = canonicalize_spec_children(
            "technical_requirement",
            ["Legacy TR"],
        )
        assert canonical is not None
        stable_id = canonical[0]["id"]

        result = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="technical_requirement",
                entity_id=stable_id,
                operation="update",
                payload={"text": "Edited legacy TR"},
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )

        spec = await db.get(Spec, spec_id)
        assert result.success is True
        assert spec.technical_requirements == [
            {
                "id": stable_id,
                "text": "Edited legacy TR",
                "status": "active",
            }
        ]


@pytest.mark.asyncio
async def test_ack_token_is_single_use_and_version_scoped(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.decisions = [
            {
                "id": "dec_a",
                "title": "A",
                "rationale": "R",
                "linked_task_ids": ["card-a"],
                "status": "active",
            }
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        preview = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                entity_id="dec_a",
                operation="supersede",
                expected_spec_version=1,
                permission_set=_permission_set("Spec"),
            )
        )
        assert preview.error_code == StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED

        spec.version = 2
        await db.flush()
        stale = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="decision",
                entity_id="dec_a",
                operation="supersede",
                expected_spec_version=1,
                ack_token=preview.ack_token,
                permission_set=_permission_set("Spec"),
            )
        )

        assert stale.success is False
        assert stale.error_code == StructuredSpecEntityErrorCode.VERSION_CONFLICT
        assert (await db.get(Spec, spec_id)).decisions[0]["status"] == "active"


@pytest.mark.asyncio
async def test_reorder_requires_full_ordered_entity_id_list(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        spec = await db.get(Spec, spec_id)
        spec.api_contracts = [
            {**_payload_for("api_contract"), "id": "api_a", "path": "/a"},
            {**_payload_for("api_contract"), "id": "api_b", "path": "/b"},
        ]
        await db.flush()
        service = StructuredSpecEntityService(db)

        missing_payload = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="api_contract",
                entity_id="api_b",
                operation="reorder",
                position=0,
                permission_set=_permission_set("Spec"),
            )
        )
        assert missing_payload.success is False
        assert missing_payload.error_code == StructuredSpecEntityErrorCode.VALIDATION_FAILED

        reordered = await service.mutate(
            StructuredSpecEntityCommand(
                spec_id=spec_id,
                actor_id=actor_id,
                entity_type="api_contract",
                operation="reorder",
                payload={"ordered_entity_ids": ["api_b", "api_a"]},
                permission_set=_permission_set("Spec"),
            )
        )

        assert reordered.success is True
        spec = await db.get(Spec, spec_id)
        assert [item["id"] for item in spec.api_contracts] == ["api_b", "api_a"]


def test_active_reader_helper_omits_revoked_and_superseded_by_default():
    items = [
        {"id": "a", "status": "active"},
        {"id": "b", "status": "revoked"},
        {"id": "c", "status": "superseded"},
        "legacy active text",
    ]

    assert filter_active_spec_children(items) == [
        {"id": "a", "status": "active"},
        "legacy active text",
    ]
    assert filter_active_spec_children(items, include_inactive=True) == items


def test_rest_create_structured_spec_entity_uses_service(structured_rest_client):
    client, _board_id, spec_id, _card_id = structured_rest_client

    response = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/functional_requirement",
        json={
            "payload": {"text": "Created through REST structured endpoint"},
            "expected_spec_version": 1,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is True
    assert body["entity_type"] == "functional_requirement"
    assert body["operation"] == "create"
    assert body["entity_id"].startswith("fr_")
    assert body["changed_fields"] == ["functional_requirements"]


def test_rest_impact_preview_does_not_mutate(structured_rest_client):
    client, _board_id, spec_id, card_id = structured_rest_client

    created = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/decision",
        json={
            "payload": {
                "id": "dec_rest",
                "title": "REST decision",
                "rationale": "Exercise impact preview",
                    "linked_task_ids": [card_id],
            },
            "expected_spec_version": 1,
        },
    )
    assert created.status_code == 200, created.text

    preview = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/decision/dec_rest/impact-preview",
        json={"operation": "revoke", "expected_spec_version": 2},
    )

    assert preview.status_code == 200, preview.text
    body = preview.json()
    assert body["success"] is True
    assert body["changed_fields"] == []
    assert body["ack_token"]
    assert body["impact_report"]["counts_by_type"] == {"card": 1}

    loaded = client.get(f"/api/v1/specs/{spec_id}")
    assert loaded.status_code == 200
    decision = loaded.json()["decisions"][0]
    assert decision["id"] == "dec_rest"
    assert decision["status"] == "active"


def _assert_structured_http_error(response, status_code: int, error_code: str):
    assert response.status_code == status_code, response.text
    body = response.json()
    assert body["detail"]["success"] is False
    assert body["detail"]["error_code"] == error_code
    assert body["detail"]["error_message"]
    return body["detail"]


def test_rest_structured_endpoints_return_typed_error_contracts(structured_rest_client):
    client, _board_id, spec_id, card_id = structured_rest_client

    created = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/decision",
        json={
            "payload": {
                "id": "dec_rest_errors",
                "title": "REST typed errors",
                "rationale": "Exercise the route-level error contract",
                "linked_task_ids": [card_id],
            },
            "expected_spec_version": 1,
        },
    )
    assert created.status_code == 200, created.text

    version_conflict = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/functional_requirement",
        json={
            "payload": {"text": "stale write"},
            "expected_spec_version": 1,
        },
    )
    _assert_structured_http_error(
        version_conflict,
        409,
        StructuredSpecEntityErrorCode.VERSION_CONFLICT,
    )

    validation_failed = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/business_rule",
        json={
            "payload": {"id": "br_invalid"},
            "expected_spec_version": 2,
        },
    )
    _assert_structured_http_error(
        validation_failed,
        422,
        StructuredSpecEntityErrorCode.VALIDATION_FAILED,
    )

    impact_ack_required = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/decision/dec_rest_errors",
        json={
            "operation": "revoke",
            "expected_spec_version": 2,
        },
    )
    impact_detail = _assert_structured_http_error(
        impact_ack_required,
        409,
        StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED,
    )
    assert impact_detail["ack_token"]
    assert impact_detail["impact_report"]["counts_by_type"] == {"card": 1}

    client.permission_preset["name"] = "Reporter"
    authorization_denied = client.post(
        f"/api/v1/specs/{spec_id}/structured-entities/functional_requirement",
        json={
            "payload": {"text": "blocked by permission"},
            "expected_spec_version": 2,
        },
    )
    auth_detail = _assert_structured_http_error(
        authorization_denied,
        403,
        StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED,
    )
    assert auth_detail["required_permission"] == "spec.structured_entity.functional_requirement.create"


@pytest.mark.asyncio
async def test_mcp_polymorphic_tool_and_api_contract_wrapper_delegate_to_service(db_factory, monkeypatch):
    from okto_pulse.core.mcp import server as mcp_server

    board_id = f"board-{uuid.uuid4()}"
    spec_id = f"spec-{uuid.uuid4()}"
    actor_id = "actor-structured-mcp"
    card_id = f"card-mcp-{uuid.uuid4()}"
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id=actor_id)
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="MCP linked task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        await db.commit()

    register_mcp_test_runtime(db_factory)
    ctx = type(
        "Ctx",
        (),
        {
            "agent_id": actor_id,
            "agent_name": "structured-mcp-agent",
            "board_id": board_id,
            "permissions": _permission_set("Spec"),
        },
    )()

    async def _ctx(_board_id: str):
        return ctx

    apply_calls: list[tuple[str, str, str | None]] = []
    original_apply = StructuredSpecEntityService.apply

    async def _spy_apply(self, command: StructuredSpecEntityCommand):
        apply_calls.append((command.entity_type, command.operation, command.entity_id))
        return await original_apply(self, command)

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _ctx)
    monkeypatch.setattr(StructuredSpecEntityService, "apply", _spy_apply)

    tool = await mcp_server.mcp.get_tool("okto_pulse_update_spec_entity")
    generic_types = [
        "functional_requirement",
        "business_rule",
        "technical_requirement",
        "decision",
        "acceptance_criterion",
        "integration_requirement",
        "observability_requirement",
    ]
    created_ids: dict[str, str] = {}
    expected_version = 1
    for entity_type in generic_types:
        payload = _payload_for(entity_type)
        if entity_type == "decision":
            payload["linked_task_ids"] = [card_id]
        raw = await tool.fn(
            board_id=board_id,
            spec_id=spec_id,
            entity_type=entity_type,
            operation="create",
            payload_json=json.dumps(payload),
            expected_spec_version=str(expected_version),
        )
        body = json.loads(raw)
        assert body["success"] is True
        assert body["entity_type"] == entity_type
        assert body["operation"] == "create"
        assert body["spec_version"] == expected_version + 1
        assert apply_calls[-1] == (entity_type, "create", None)
        created_ids[entity_type] = body["entity_id"]
        expected_version += 1

    api_tool = await mcp_server.mcp.get_tool("okto_pulse_update_spec_api_contract")
    raw = await api_tool.fn(
        board_id=board_id,
        spec_id=spec_id,
        contract_id="api_mcp",
        operation="create",
        payload_json=json.dumps({
            "id": "api_mcp",
            "method": "COMPONENT",
            "path": "StructuredSpecEntityService.apply",
            "description": "Wrapper delegates to the service",
        }),
        expected_spec_version=str(expected_version),
    )
    wrapper = json.loads(raw)
    assert wrapper["success"] is True
    assert wrapper["entity_type"] == "api_contract"
    assert wrapper["entity_id"] == "api_mcp"
    assert apply_calls[-1] == ("api_contract", "create", "api_mcp")
    expected_version += 1

    stale = await tool.fn(
        board_id=board_id,
        spec_id=spec_id,
        entity_type="business_rule",
        operation="create",
        payload_json=json.dumps(_payload_for("business_rule")),
        expected_spec_version="1",
    )
    stale_body = json.loads(stale)
    assert stale_body["success"] is False
    assert stale_body["error_code"] == StructuredSpecEntityErrorCode.VERSION_CONFLICT

    invalid = await tool.fn(
        board_id=board_id,
        spec_id=spec_id,
        entity_type="business_rule",
        operation="create",
        payload_json=json.dumps({"id": "br_invalid"}),
        expected_spec_version=str(expected_version),
    )
    invalid_body = json.loads(invalid)
    assert invalid_body["success"] is False
    assert invalid_body["error_code"] == StructuredSpecEntityErrorCode.VALIDATION_FAILED

    blocked_revoke = await tool.fn(
        board_id=board_id,
        spec_id=spec_id,
        entity_type="decision",
        operation="revoke",
        entity_id=created_ids["decision"],
        expected_spec_version=str(expected_version),
    )
    blocked_revoke_body = json.loads(blocked_revoke)
    assert blocked_revoke_body["success"] is False
    assert blocked_revoke_body["error_code"] == StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED
    assert blocked_revoke_body["ack_token"]
    assert blocked_revoke_body["impact_report"]["counts_by_type"] == {"card": 1}

    ctx.permissions = _permission_set("Reporter")
    denied = await tool.fn(
        board_id=board_id,
        spec_id=spec_id,
        entity_type="functional_requirement",
        operation="create",
        payload_json=json.dumps({"text": "blocked by MCP permissions"}),
        expected_spec_version=str(expected_version),
    )
    denied_body = json.loads(denied)
    assert denied_body["success"] is False
    assert denied_body["error_code"] == StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED
    assert denied_body["required_permission"] == "spec.structured_entity.functional_requirement.create"
    ctx.permissions = _permission_set("Spec")

    call_count_before_blocked_api_contract = len(apply_calls)
    blocked = await tool.fn(
        board_id=board_id,
        spec_id=spec_id,
        entity_type="api_contract",
        operation="update",
        entity_id="api_mcp",
        payload_json=json.dumps({"description": "blocked"}),
    )
    assert "dedicated okto_pulse_update_spec_api_contract wrapper" in blocked
    assert len(apply_calls) == call_count_before_blocked_api_contract
