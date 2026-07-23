from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
    McpListKnowledgeCommand,
    McpListKnowledgeUseCase,
    _serialize_knowledge_base as serialize_list_knowledge_base,
)
from okto_pulse.core.mcp.server import (
    _serialize_knowledge_base as serialize_mcp_knowledge_base,
)
from okto_pulse.core.models.schemas import CardResponse
from okto_pulse.core.services.reference_resolution import (
    resolve_artifact_references,
)


def _valid_metadata() -> dict:
    return {
        "contract_version": 1,
        "authority": "advisory",
        "classification": "technical_reference",
        "purpose": "Document a stable interface",
        "audience": ["agent"],
        "relevance_reason": "Required by readers",
        "provenance": [{"kind": "code", "reference": "core@abc123"}],
        "as_of": "2026-07-22T20:00:00-03:00",
        "version_ref": "commit:abc123",
        "version_not_applicable_reason": None,
        "scope": "Knowledge Base read surfaces",
        "limitations": "Advisory reference only",
        "stable_references": [],
        "lifecycle_state": "current",
        "superseded_by": None,
        "superseded_reason": None,
        "exclusive_authority_check": "passed",
        "normative_destinations": [],
    }


def _kb(*, metadata: object | None) -> SimpleNamespace:
    return SimpleNamespace(
        id="kb-1",
        title="Knowledge",
        description="Description",
        content="Body",
        mime_type="text/markdown",
        governance_metadata=metadata,
        created_at=datetime(2026, 7, 22, tzinfo=timezone.utc),
        created_by="author",
        updated_at=None,
    )


def test_mcp_lists_and_context_resolver_share_complete_projection() -> None:
    kb = _kb(metadata=_valid_metadata())

    mcp_payload = serialize_mcp_knowledge_base(kb)
    list_payload = serialize_list_knowledge_base(kb)
    context_payload = resolve_artifact_references(
        SimpleNamespace(knowledge_bases=[kb], screen_mockups=[]),
        source_type="spec",
        source_id="spec-1",
        source_title="Spec",
    )["knowledge_bases"][0]

    expected = {
        "authority": "advisory",
        "metadata_status": "complete",
        "missing_fields": [],
        "metadata": _valid_metadata(),
    }
    assert mcp_payload["governance"] == expected
    assert list_payload["governance"] == expected
    assert context_payload["governance"] == expected
    assert mcp_payload["content"] == "Body"
    assert list_payload["content"] == "Body"
    assert context_payload["content"] == "Body"


@pytest.mark.parametrize(
    "raw",
    [None, {"contract_version": 7, "purpose": "historical partial value"}],
    ids=["null-legacy", "partial-legacy"],
)
def test_cross_surface_legacy_reads_are_tolerant(raw: object | None) -> None:
    kb = {
        "id": "kb-legacy",
        "title": "Legacy",
        "content": "Untrusted body must not drive governance",
        "governance_metadata": raw,
    }

    projections = [
        serialize_mcp_knowledge_base(kb)["governance"],
        serialize_list_knowledge_base(kb)["governance"],
        resolve_artifact_references(
            SimpleNamespace(knowledge_bases=[kb], screen_mockups=[]),
            source_type="ideation",
            source_id="idea-1",
            source_title="Idea",
        )["knowledge_bases"][0]["governance"],
    ]

    assert projections[0] == projections[1] == projections[2]
    assert projections[0]["authority"] == "advisory"
    assert projections[0]["metadata_status"] == "legacy_incomplete"
    assert projections[0]["metadata"] is raw
    assert projections[0]["missing_fields"]


def test_rest_card_response_projects_inline_knowledge_governance() -> None:
    card = CardResponse.model_validate(
        {
            "id": "card-1",
            "board_id": "board-1",
            "title": "Task",
            "description": None,
            "details": None,
            "status": "in_progress",
            "priority": "medium",
            "position": 0,
            "assignee_id": None,
            "created_by": "agent",
            "created_at": "2026-07-22T20:00:00Z",
            "updated_at": "2026-07-22T20:00:00Z",
            "due_date": None,
            "labels": [],
            "knowledge_bases": [
                {
                    "id": "card-kb-1",
                    "title": "Snapshot",
                    "content": "Reference",
                    "governance_metadata": _valid_metadata(),
                }
            ],
        }
    )

    item = card.model_dump()["knowledge_bases"][0]
    assert item["governance"]["metadata_status"] == "complete"
    assert item["governance"]["metadata"] == _valid_metadata()


class _ParentService:
    def __init__(self, parent: SimpleNamespace) -> None:
        self.parent = parent

    async def get_spec(self, _entity_id: str) -> SimpleNamespace:
        return self.parent

    async def get_ideation(self, _entity_id: str) -> SimpleNamespace:
        return self.parent

    async def get_refinement(self, _entity_id: str) -> SimpleNamespace:
        return self.parent


class _KnowledgeService:
    def __init__(self, kb: SimpleNamespace) -> None:
        self.kb = kb

    async def list_knowledge(self, _entity_id: str) -> list[SimpleNamespace]:
        return [self.kb]


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ["ideation", "refinement", "spec"])
async def test_consolidated_list_projects_governance_for_each_entity_type(
    entity_type: str,
) -> None:
    parent = SimpleNamespace(id="entity-1", board_id="board-1")
    parent_service = _ParentService(parent)
    knowledge_service = _KnowledgeService(_kb(metadata=_valid_metadata()))
    services = SimpleNamespace(
        specs=parent_service,
        ideations=parent_service,
        refinements=parent_service,
        spec_knowledge=knowledge_service,
        ideation_knowledge=knowledge_service,
        refinement_knowledge=knowledge_service,
    )
    result = await McpListKnowledgeUseCase().execute(
        McpListKnowledgeCommand(
            board_id="board-1",
            entity_type=entity_type,
            entity_id="entity-1",
            filters={},
        ),
        actor=ActorContext("agent", "mcp", board_id="board-1"),
        uow=SimpleNamespace(services=services),
    )

    item = result.payload["knowledge_bases"][0]
    assert item["id"] == "kb-1"
    assert item["created_at"] == "2026-07-22T00:00:00+00:00"
    assert item["governance"]["metadata_status"] == "complete"
    assert item["governance"]["metadata"] == _valid_metadata()
