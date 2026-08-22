from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest

from okto_pulse.core.domain.knowledge_governance import (
    KnowledgeGovernanceInvalidMetadata,
)
from okto_pulse.core.models.schemas import (
    IdeationKnowledgeCreate,
    IdeationKnowledgeResponse,
    IdeationKnowledgeSummary,
    IdeationKnowledgeUpdate,
    RefinementKnowledgeCreate,
    RefinementKnowledgeResponse,
    RefinementKnowledgeSummary,
    RefinementKnowledgeUpdate,
    SpecKnowledgeCreate,
    SpecKnowledgeResponse,
    SpecKnowledgeSummary,
    SpecKnowledgeUpdate,
)
from okto_pulse.core.ports.application_persistence import ApplicationRecord
from okto_pulse.core.services import main
from okto_pulse.core.services.main import (
    IdeationKnowledgeService,
    RefinementKnowledgeService,
    SpecKnowledgeService,
)


def _valid_metadata() -> dict[str, Any]:
    return {
        "contract_version": 1,
        "authority": "advisory",
        "classification": "technical_reference",
        "purpose": "  Describe the persistence contract  ",
        "audience": [" agent ", "maintainer"],
        "relevance_reason": "Needed to reproduce the baseline",
        "provenance": [{"kind": "code", "reference": " repository:core@abc123 "}],
        "as_of": "2026-07-22T20:00:00-03:00",
        "version_ref": "commit:abc123",
        "version_not_applicable_reason": None,
        "scope": "Knowledge Base writes",
        "limitations": "Does not replace normative entities",
        "stable_references": [
            {
                "entity_type": "technical_requirement",
                "entity_id": "tr_33412250",
                "version_ref": None,
            }
        ],
        "lifecycle_state": "current",
        "superseded_by": None,
        "superseded_reason": None,
        "exclusive_authority_check": "passed",
        "normative_destinations": [],
    }


_CREATE_CASES = (
    (IdeationKnowledgeService, IdeationKnowledgeCreate, "ideation", "ideation-1"),
    (
        RefinementKnowledgeService,
        RefinementKnowledgeCreate,
        "refinement",
        "refinement-1",
    ),
    (SpecKnowledgeService, SpecKnowledgeCreate, "spec", "spec-1"),
)

_UPDATE_CASES = (
    (IdeationKnowledgeService, IdeationKnowledgeUpdate, "ideation_knowledge_base"),
    (
        RefinementKnowledgeService,
        RefinementKnowledgeUpdate,
        "refinement_knowledge_base",
    ),
    (SpecKnowledgeService, SpecKnowledgeUpdate, "spec_knowledge_base"),
)


def _install_propagation_spy(
    monkeypatch: pytest.MonkeyPatch, calls: list[dict]
) -> None:
    class _PropagationSpy:
        def __init__(self, _db: object) -> None:
            pass

        async def propagate_for_spec(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(main, "SpecResourcePropagationService", _PropagationSpy)


def test_all_kb_transport_schemas_expose_raw_governance_metadata() -> None:
    schema_types = (
        IdeationKnowledgeCreate,
        IdeationKnowledgeUpdate,
        IdeationKnowledgeResponse,
        IdeationKnowledgeSummary,
        RefinementKnowledgeCreate,
        RefinementKnowledgeUpdate,
        RefinementKnowledgeResponse,
        RefinementKnowledgeSummary,
        SpecKnowledgeCreate,
        SpecKnowledgeUpdate,
        SpecKnowledgeResponse,
        SpecKnowledgeSummary,
    )

    assert all("governance_metadata" in schema.model_fields for schema in schema_types)
    for schema in (
        IdeationKnowledgeResponse,
        IdeationKnowledgeSummary,
        RefinementKnowledgeResponse,
        RefinementKnowledgeSummary,
        SpecKnowledgeResponse,
        SpecKnowledgeSummary,
    ):
        assert {
            "root_source_kb_id",
            "immediate_parent_kb_id",
            "content_hash",
        } <= set(schema.model_fields)


@pytest.mark.parametrize(
    ("schema_type", "parent_field"),
    (
        (IdeationKnowledgeResponse, "ideation_id"),
        (RefinementKnowledgeResponse, "refinement_id"),
        (SpecKnowledgeResponse, "spec_id"),
    ),
)
def test_rest_response_models_emit_the_canonical_governance_projection(
    schema_type: type,
    parent_field: str,
) -> None:
    payload = {
        "id": "kb-1",
        parent_field: "parent-1",
        "title": "Governed KB",
        "description": None,
        "content": "body",
        "mime_type": "text/markdown",
        "created_by": "agent-1",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "governance_metadata": _valid_metadata(),
    }

    serialized = schema_type.model_validate(payload).model_dump(mode="json")

    assert serialized["governance"]["authority"] == "advisory"
    assert serialized["governance"]["metadata_status"] == "complete"
    assert serialized["governance"]["missing_fields"] == []
    assert serialized["governance"]["metadata"]["purpose"] == (
        "Describe the persistence contract"
    )


def test_inherited_v2_spec_knowledge_validates_without_storage_timestamps() -> None:
    inherited = {
        "id": "source-kb-1",
        "spec_id": "spec-1",
        "title": "Inherited reference",
        "description": None,
        "content": "canonical source bytes",
        "mime_type": "text/markdown",
        "knowledge_assignment": {
            "assignment_id": "assignment-1",
            "mode": "reference",
            "state": "active",
            "stale": False,
            "origin_class": "v2",
        },
    }

    response = SpecKnowledgeResponse.model_validate(inherited)
    summary = SpecKnowledgeSummary.model_validate(inherited)

    assert response.spec_id == "spec-1"
    assert response.created_by is None
    assert response.created_at is None
    assert response.updated_at is None
    assert summary.spec_id == "spec-1"
    assert summary.created_at is None
    assert "content" not in summary.model_dump(mode="json")


@pytest.mark.parametrize(
    "schema_type",
    (IdeationKnowledgeUpdate, RefinementKnowledgeUpdate, SpecKnowledgeUpdate),
)
def test_update_schema_preserves_omitted_null_and_empty_object(
    schema_type: type,
) -> None:
    assert "governance_metadata" not in schema_type().model_dump(exclude_unset=True)
    assert (
        schema_type(governance_metadata=None).model_dump(exclude_unset=True)[
            "governance_metadata"
        ]
        is None
    )
    assert (
        schema_type(governance_metadata={}).model_dump(exclude_unset=True)[
            "governance_metadata"
        ]
        == {}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "schema_type", "parent_entity", "parent_id"),
    _CREATE_CASES,
)
async def test_create_normalizes_metadata_before_persisting(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    schema_type: type,
    parent_entity: str,
    parent_id: str,
) -> None:
    persisted: list[ApplicationRecord] = []
    propagation_calls: list[dict] = []

    async def _get(
        _db: object,
        entity: str,
        record_id: str,
        **_kwargs: Any,
    ) -> ApplicationRecord | None:
        assert entity == parent_entity
        assert record_id == parent_id
        return ApplicationRecord(
            entity=entity,
            values={
                "id": record_id,
                "board_id": "board-1",
                "status": "draft",
                "version": 5,
            },
        )

    async def _add(_db: object, record: ApplicationRecord) -> ApplicationRecord:
        persisted.append(record)
        return record

    monkeypatch.setattr(main, "_application_get", _get)
    monkeypatch.setattr(main, "_application_add", _add)
    _install_propagation_spy(monkeypatch, propagation_calls)
    raw = _valid_metadata()
    before = deepcopy(raw)

    created = await service_type(object()).create_knowledge(
        parent_id,
        "agent-1",
        schema_type(title="KB", content="body", governance_metadata=raw),
    )

    assert created is persisted[0]
    assert raw == before
    assert created.governance_metadata["purpose"] == (
        "Describe the persistence contract"
    )
    assert created.governance_metadata["audience"] == ["agent", "maintainer"]
    assert created.governance_metadata["provenance"][0]["reference"] == (
        "repository:core@abc123"
    )
    assert created.root_source_kb_id == created.id
    assert created.source_version == 5
    assert len(created.content_hash) == 64
    assert len(propagation_calls) == (1 if parent_entity == "spec" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "schema_type", "parent_entity", "parent_id"),
    _CREATE_CASES,
)
async def test_create_accepts_legacy_null_without_coercing_it_to_empty_object(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    schema_type: type,
    parent_entity: str,
    parent_id: str,
) -> None:
    async def _get(
        _db: object,
        entity: str,
        record_id: str,
        **_kwargs: Any,
    ) -> ApplicationRecord:
        return ApplicationRecord(
            entity=entity,
            values={"id": record_id, "board_id": "board-1", "status": "draft"},
        )

    async def _add(_db: object, record: ApplicationRecord) -> ApplicationRecord:
        return record

    monkeypatch.setattr(main, "_application_get", _get)
    monkeypatch.setattr(main, "_application_add", _add)
    _install_propagation_spy(monkeypatch, [])

    created = await service_type(object()).create_knowledge(
        parent_id,
        "agent-1",
        schema_type(title="Legacy KB", content="body"),
    )

    assert created is not None
    assert created.governance_metadata is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "schema_type", "parent_entity", "parent_id"),
    _CREATE_CASES,
)
async def test_invalid_create_is_rejected_before_lookup_persistence_or_propagation(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    schema_type: type,
    parent_entity: str,
    parent_id: str,
) -> None:
    calls: list[str] = []

    async def _unexpected(*_args: Any, **_kwargs: Any) -> None:
        calls.append("application")

    class _UnexpectedPropagation:
        def __init__(self, _db: object) -> None:
            calls.append("propagation")

    monkeypatch.setattr(main, "_application_get", _unexpected)
    monkeypatch.setattr(main, "_application_add", _unexpected)
    monkeypatch.setattr(main, "SpecResourcePropagationService", _UnexpectedPropagation)

    with pytest.raises(KnowledgeGovernanceInvalidMetadata) as caught:
        await service_type(object()).create_knowledge(
            parent_id,
            "agent-1",
            schema_type(title="Invalid", content="body", governance_metadata={}),
        )

    assert caught.value.code == "knowledge_governance_invalid_metadata"
    assert calls == []


def _knowledge_record(entity: str) -> ApplicationRecord:
    return ApplicationRecord(
        entity=entity,
        values={
            "id": "kb-1",
            "ideation_id": "ideation-1",
            "refinement_id": "refinement-1",
            "spec_id": "spec-1",
            "created_by": "agent-1",
            "title": "Existing",
            "governance_metadata": {"legacy": "raw"},
            "updated_at": datetime.now(timezone.utc),
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "schema_type", "knowledge_entity"),
    _UPDATE_CASES,
)
async def test_update_normalizes_metadata_before_record_mutation(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    schema_type: type,
    knowledge_entity: str,
) -> None:
    record = _knowledge_record(knowledge_entity)
    flush_calls: list[str] = []

    async def _get(
        _db: object,
        entity: str,
        record_id: str,
        **_kwargs: Any,
    ) -> ApplicationRecord | None:
        if entity == knowledge_entity:
            return record
        if entity in {"ideation", "refinement", "spec"}:
            return ApplicationRecord(
                entity=entity,
                values={
                    "id": record_id,
                    "board_id": "board-1",
                    "status": "draft",
                },
            )
        return None

    async def _flush(_db: object) -> None:
        flush_calls.append("flush")

    monkeypatch.setattr(main, "_application_get", _get)
    monkeypatch.setattr(main, "_application_flush", _flush)
    _install_propagation_spy(monkeypatch, [])

    updated = await service_type(object()).update_knowledge(
        "kb-1",
        schema_type(governance_metadata=_valid_metadata()),
    )

    assert updated is record
    assert record.governance_metadata["purpose"] == (
        "Describe the persistence contract"
    )
    assert record.dirty_fields == {"governance_metadata", "content_hash"}
    assert len(flush_calls) == (1 if knowledge_entity == "spec_knowledge_base" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "schema_type", "knowledge_entity"),
    _UPDATE_CASES,
)
async def test_update_omission_preserves_metadata_and_explicit_null_clears_it(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    schema_type: type,
    knowledge_entity: str,
) -> None:
    record = _knowledge_record(knowledge_entity)
    original = record.governance_metadata

    async def _get(
        _db: object,
        entity: str,
        record_id: str,
        **_kwargs: Any,
    ) -> ApplicationRecord | None:
        if entity == knowledge_entity:
            return record
        if entity in {"ideation", "refinement", "spec"}:
            return ApplicationRecord(
                entity=entity,
                values={
                    "id": record_id,
                    "board_id": "board-1",
                    "status": "draft",
                },
            )
        return None

    async def _flush(_db: object) -> None:
        return None

    monkeypatch.setattr(main, "_application_get", _get)
    monkeypatch.setattr(main, "_application_flush", _flush)
    _install_propagation_spy(monkeypatch, [])

    await service_type(object()).update_knowledge("kb-1", schema_type(title="Renamed"))
    assert record.governance_metadata is original

    await service_type(object()).update_knowledge(
        "kb-1", schema_type(governance_metadata=None)
    )
    assert record.governance_metadata is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "schema_type", "knowledge_entity"),
    _UPDATE_CASES,
)
async def test_invalid_update_has_no_lookup_mutation_flush_or_propagation(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    schema_type: type,
    knowledge_entity: str,
) -> None:
    record = _knowledge_record(knowledge_entity)
    original_values = deepcopy(record.values)
    calls: list[str] = []

    async def _unexpected(*_args: Any, **_kwargs: Any) -> None:
        calls.append("application")

    class _UnexpectedPropagation:
        def __init__(self, _db: object) -> None:
            calls.append("propagation")

    monkeypatch.setattr(main, "_application_get", _unexpected)
    monkeypatch.setattr(main, "_application_flush", _unexpected)
    monkeypatch.setattr(main, "SpecResourcePropagationService", _UnexpectedPropagation)

    with pytest.raises(KnowledgeGovernanceInvalidMetadata):
        await service_type(object()).update_knowledge(
            "kb-1", schema_type(governance_metadata={})
        )

    assert record.values == original_values
    assert record.dirty_fields == set()
    assert calls == []
