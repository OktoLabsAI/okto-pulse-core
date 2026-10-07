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
        source_type="spec",
        source_id="source-1",
        source_version=0,
        source_kb_id="source-kb-1",
        root_source_kb_id="root-kb-1",
        immediate_parent_kb_id="parent-kb-1",
        content_hash="persisted-content-sha256",
        knowledge_assignment={
            "assignment_id": "assignment-1",
            "mode": "reference",
            "state": "active",
            "stale": False,
        },
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


@pytest.mark.parametrize("reader", [
    serialize_mcp_knowledge_base,
    serialize_list_knowledge_base,
    lambda kb: resolve_artifact_references(
        SimpleNamespace(knowledge_bases=[kb], screen_mockups=[]),
        source_type="ideation", source_id="idea-1", source_title="Idea",
    )["knowledge_bases"][0],
])
def test_cross_surface_metadata_is_optional_but_invalid_data_is_refused(reader):
    from okto_pulse.core.domain.knowledge_governance import KnowledgeGovernanceInvalidMetadata
    kb = {"id": "kb-1", "title": "Knowledge", "content": "Advisory data"}
    assert reader(kb)["governance"] == {
        "authority": "advisory", "metadata_status": "omitted",
        "missing_fields": ["governance_metadata"], "metadata": None,
    }
    kb["governance_metadata"] = {"contract_version": 7, "purpose": "unsupported"}
    with pytest.raises(KnowledgeGovernanceInvalidMetadata):
        reader(kb)


def test_rest_card_response_projects_inline_knowledge_governance() -> None:
    card = CardResponse.model_validate(
        {
            "id": "card-1",
            "board_id": "board-1",
            "title": "Task",
            "description": None,
            "details": None,
            "status": "in_progress",
            "subject_version": 1,
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


class _PropagationRead:
    async def read(self, _target: object) -> SimpleNamespace:
        return SimpleNamespace()


class _EffectiveKnowledgeRead:
    def __init__(self, kb: SimpleNamespace) -> None:
        self.kb = kb

    async def get_effective_resources(
        self, board_id: str, entity_type: str, entity_id: str,
    ) -> dict:
        assert (board_id, entity_type, entity_id) == ("board-1", "spec", "entity-1")
        return {"resources": {"knowledge_base": [
            {"id": self.kb.id, "hydrated": True, "resource": vars(self.kb)},
        ]}}


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ["ideation", "refinement", "spec"])
async def test_consolidated_list_projects_governance_for_each_entity_type(
    entity_type: str,
) -> None:
    kb = _kb(metadata=_valid_metadata())
    parent = SimpleNamespace(
        id="entity-1",
        board_id="board-1",
        knowledge_bases=[kb],
    )
    parent_service = _ParentService(parent)
    knowledge_service = _KnowledgeService(kb)
    services = SimpleNamespace(
        specs=parent_service,
        ideations=parent_service,
        refinements=parent_service,
        spec_knowledge=knowledge_service,
        ideation_knowledge=knowledge_service,
        refinement_knowledge=knowledge_service,
        knowledge_propagation=_PropagationRead(),
        resource_gate=_EffectiveKnowledgeRead(kb),
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
    assert item["source_version"] == 0
    assert item["root_source_kb_id"] == "root-kb-1"
    assert item["immediate_parent_kb_id"] == "parent-kb-1"
    assert item["content_hash"] == "persisted-content-sha256"
    assert item["knowledge_assignment"]["assignment_id"] == "assignment-1"
    assert "content" not in item


@pytest.mark.asyncio
async def test_spec_consolidated_list_keeps_effective_summary_bounded() -> None:
    parent = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        knowledge_bases=[{"id": "physical-history", "content": "must not leak"}],
    )
    parent_service = _ParentService(parent)

    class _ResourceGate:
        async def get_effective_resources(
            self,
            board_id: str,
            entity_type: str,
            entity_id: str,
        ) -> dict:
            assert (board_id, entity_type, entity_id) == (
                "board-1",
                "spec",
                "spec-1",
            )
            return {
                "resources": {
                    "knowledge_base": [
                        {
                            "id": "root-1",
                            "hydrated": True,
                            "resource": {
                                "id": "kb-current",
                                "title": "Current",
                                "description": None,
                                "content": "large canonical body",
                                "mime_type": "text/markdown",
                                "root_source_kb_id": "root-1",
                                "created_at": datetime(
                                    2026,
                                    7,
                                    23,
                                    tzinfo=timezone.utc,
                                ),
                            },
                            "ref": {
                                "knowledge_assignment_id": "assignment-1",
                                "knowledge_assignment_mode": "reference",
                                "knowledge_assignment_state": "active",
                                "knowledge_assignment_stale": False,
                                "origin_class": "v2",
                            },
                        }
                    ]
                }
            }

    services = SimpleNamespace(
        specs=parent_service,
        knowledge_propagation=_PropagationRead(),
        resource_gate=_ResourceGate(),
    )

    result = await McpListKnowledgeUseCase().execute(
        McpListKnowledgeCommand(
            board_id="board-1",
            entity_type="spec",
            entity_id="spec-1",
            filters={},
        ),
        actor=ActorContext("agent", "mcp", board_id="board-1"),
        uow=SimpleNamespace(services=services),
    )

    item = result.payload["knowledge_bases"][0]
    assert item == {
        "id": "kb-current",
        "title": "Current",
        "description": None,
        "mime_type": "text/markdown",
        "spec_id": "spec-1",
        "root_source_kb_id": "root-1",
        "knowledge_assignment": {
            "assignment_id": "assignment-1",
            "mode": "reference",
            "state": "active",
            "stale": False,
            "origin_class": "v2",
        },
        "created_at": "2026-07-23T00:00:00+00:00",
        "content_hash": (
            "4ec45d27a1a409364cfd36ff061413f812a42a4fb6c7cff9d81df2a879e81ecd"
        ),
        "governance": {
            "authority": "advisory",
            "metadata_status": "omitted",
            "missing_fields": [
                "governance_metadata",
            ],
            "metadata": None,
        },
    }
