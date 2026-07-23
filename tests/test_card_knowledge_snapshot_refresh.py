from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from knowledge_governance_test_data import valid_governance_metadata
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.mcp_card_crud import (
    McpCopyKnowledgeToCardCommand,
    McpCopyKnowledgeToCardUseCase,
)
from okto_pulse.core.domain.knowledge_selection import KnowledgeSelectionState
from okto_pulse.core.ports.knowledge_propagation import KnowledgePropagationScope
from okto_pulse.core.ports.spec_resource_propagation import (
    ResourcePropagationBoardFact,
    ResourcePropagationCardRecord,
    ResourcePropagationKnowledgeBaseFact,
    ResourcePropagationSpecFact,
)
from okto_pulse.core.services import spec_resource_propagation as propagation_module
from okto_pulse.core.services.card_knowledge_snapshot import (
    build_card_knowledge_snapshot,
)
from okto_pulse.core.services.spec_resource_propagation import (
    SpecResourcePropagationService,
)


class _LookupService:
    def __init__(self, value):
        self.value = value

    async def get_spec(self, _entity_id: str):
        return self.value


class _KnowledgeService:
    def __init__(self, items: list[object]):
        self.items = items

    async def list_knowledge(self, _spec_id: str):
        return self.items


class _KnowledgePropagationService:
    async def read(self, _target):
        return SimpleNamespace(v2_active=False)


class _KnowledgePropagationPort:
    def __init__(self, *, v2_active: bool):
        self.v2_active = v2_active

    async def load_scope(self, _context, request):
        return KnowledgePropagationScope(
            target=request.target,
            scope_revision=1 if self.v2_active else 0,
            v2_active=self.v2_active,
            selection_state=(
                KnowledgeSelectionState.OMITTED if self.v2_active else None
            ),
        )


class _CardService:
    def __init__(self, card):
        self.card = card
        self.update_count = 0

    async def get_card(self, _card_id: str):
        return self.card

    async def update_card(
        self,
        _card_id: str,
        _actor_id: str,
        data,
        *,
        allow_card_resource_write: bool,
    ):
        assert allow_card_resource_write is True
        self.update_count += 1
        self.card.knowledge_bases = copy.deepcopy(data.knowledge_bases)
        return self.card


class _Uow:
    def __init__(self, *, spec, card, knowledge_items: list[object]):
        self.card_service = _CardService(card)
        self.services = SimpleNamespace(
            specs=_LookupService(spec),
            cards=self.card_service,
            spec_knowledge=_KnowledgeService(knowledge_items),
            knowledge_propagation=_KnowledgePropagationService(),
        )
        self.commit_count = 0

    async def commit(self) -> None:
        self.commit_count += 1


def _source_kb(*, content: str, purpose: str):
    return SimpleNamespace(
        id="kb-source",
        title="Governed contract",
        description="Canonical reference",
        content=content,
        mime_type="text/markdown",
        source_kb_id="kb-parent",
        root_source_kb_id="kb-root",
        source_version=3,
        content_hash=None,
        governance_metadata=valid_governance_metadata(purpose=purpose),
    )


def _command() -> McpCopyKnowledgeToCardCommand:
    return McpCopyKnowledgeToCardCommand(
        "board-1",
        "spec-1",
        "card-1",
        None,
    )


def _actor() -> ActorContext:
    return ActorContext("actor-refresh", "mcp", board_id="board-1")


@pytest.mark.asyncio
async def test_manual_copy_refreshes_changed_snapshot_then_true_noops_when_equivalent():
    source = _source_kb(content="new content", purpose="new purpose")
    old_metadata = valid_governance_metadata(purpose="old purpose")
    current = {
        "id": "cardkb-stable",
        "title": source.title,
        "description": source.description,
        "content": "old content",
        "mime_type": source.mime_type,
        "source": "copied_from_spec:spec-1:kb-source",
        "source_kb_id": "lineage-parent",
        "root_source_kb_id": "lineage-root",
        "immediate_parent_kb_id": "lineage-immediate",
        "author_id": "original-author",
        "governance_metadata": old_metadata,
    }
    card = SimpleNamespace(
        id="card-1",
        board_id="board-1",
        knowledge_bases=[current],
    )
    spec = SimpleNamespace(id="spec-1", board_id="board-1", version=7)
    uow = _Uow(spec=spec, card=card, knowledge_items=[source])
    use_case = McpCopyKnowledgeToCardUseCase()

    refreshed = await use_case.execute(_command(), actor=_actor(), uow=uow)

    assert refreshed.copied == 1
    assert refreshed.copied_ids == ["cardkb-stable"]
    assert uow.card_service.update_count == 1
    assert uow.commit_count == 1
    snapshot = card.knowledge_bases[0]
    assert snapshot["content"] == "new content"
    assert snapshot["source_version"] == 7
    assert len(snapshot["content_hash"]) == 64
    assert snapshot["governance_metadata"]["purpose"] == "new purpose"
    assert {
        key: snapshot[key]
        for key in (
            "id",
            "source",
            "author_id",
            "source_kb_id",
            "root_source_kb_id",
            "immediate_parent_kb_id",
        )
    } == {
        "id": "cardkb-stable",
        "source": "copied_from_spec:spec-1:kb-source",
        "author_id": "original-author",
        "source_kb_id": "lineage-parent",
        "root_source_kb_id": "lineage-root",
        "immediate_parent_kb_id": "lineage-immediate",
    }

    reordered = {
        key: source.governance_metadata[key]
        for key in reversed(tuple(source.governance_metadata))
    }
    source.governance_metadata = reordered
    no_op = await use_case.execute(_command(), actor=_actor(), uow=uow)

    assert no_op.copied == 0
    assert no_op.copied_ids == []
    assert uow.card_service.update_count == 1
    assert uow.commit_count == 1


class _AutoStore:
    def __init__(self, card, source) -> None:
        self.card = card
        self.source = source
        self.save_count = 0
        self.audit_count = 0

    async def get_board(self, _context, *, board_id: str):
        return ResourcePropagationBoardFact(
            id=board_id,
            settings={
                "auto_derive_spec_resources_enabled": True,
                "auto_derive_spec_resource_types": ["knowledge_base"],
            },
        )

    async def get_spec(self, _context, *, spec_id: str):
        return ResourcePropagationSpecFact(
            id=spec_id,
            board_id="board-1",
            screen_mockups=(),
            version=7,
        )

    async def get_card(self, _context, *, card_id: str):
        return ResourcePropagationCardRecord(
            id=card_id,
            board_id="board-1",
            knowledge_bases=copy.deepcopy(self.card.knowledge_bases),
            screen_mockups=[],
        )

    async def list_spec_knowledge_bases(self, _context, *, spec_id: str):
        assert spec_id == "spec-1"
        return (self.source,)

    async def save_card(self, _context, record, *, changed_fields):
        assert tuple(changed_fields) == ("knowledge_bases",)
        self.save_count += 1
        self.card.knowledge_bases = copy.deepcopy(record.knowledge_bases)

    async def record_audit(self, _context, **_kwargs):
        self.audit_count += 1


@pytest.mark.asyncio
async def test_manual_then_auto_refresh_preserves_lineage_and_writes_only_on_change(
    monkeypatch,
):
    source = _source_kb(content="v1", purpose="purpose v1")
    card = SimpleNamespace(id="card-1", board_id="board-1", knowledge_bases=[])
    spec = SimpleNamespace(id="spec-1", board_id="board-1", version=7)
    uow = _Uow(spec=spec, card=card, knowledge_items=[source])
    await McpCopyKnowledgeToCardUseCase().execute(
        _command(), actor=_actor(), uow=uow
    )
    manual_snapshot = copy.deepcopy(card.knowledge_bases[0])
    original_hash = manual_snapshot["content_hash"]
    assert manual_snapshot["source_version"] == 7
    preserved = {
        key: manual_snapshot[key]
        for key in (
            "id",
            "source",
            "author_id",
            "source_kb_id",
            "root_source_kb_id",
            "immediate_parent_kb_id",
        )
    }

    auto_source = ResourcePropagationKnowledgeBaseFact(
        id="kb-source",
        title=source.title,
        description=source.description,
        content="v1",
        mime_type=source.mime_type,
        governance_metadata=copy.deepcopy(source.governance_metadata),
    )
    store = _AutoStore(card, auto_source)
    monkeypatch.setattr(
        propagation_module,
        "get_spec_resource_propagation_store",
        lambda: store,
    )
    service = SpecResourcePropagationService(
        object(),
        knowledge_propagation_port=_KnowledgePropagationPort(v2_active=False),
    )

    equivalent = await service.propagate_for_card(
        board_id="board-1",
        spec_id="spec-1",
        card_id="card-1",
        actor_id="auto-actor",
        trigger="mixed_equivalent",
    )
    assert equivalent["results"]["knowledge_base"]["ignored_count"] == 1
    assert store.save_count == 0

    store.source = ResourcePropagationKnowledgeBaseFact(
        id="kb-source",
        title=source.title,
        description=source.description,
        content="v2",
        mime_type=source.mime_type,
        governance_metadata=valid_governance_metadata(purpose="purpose v2"),
    )
    changed = await service.propagate_for_card(
        board_id="board-1",
        spec_id="spec-1",
        card_id="card-1",
        actor_id="auto-actor",
        trigger="mixed_refresh",
    )

    assert changed["results"]["knowledge_base"]["copied_count"] == 1
    assert store.save_count == 1
    assert card.knowledge_bases[0]["content"] == "v2"
    assert card.knowledge_bases[0]["source_version"] == 7
    assert card.knowledge_bases[0]["content_hash"] != original_hash
    assert {
        key: card.knowledge_bases[0][key] for key in preserved
    } == preserved

    retry = await service.propagate_for_card(
        board_id="board-1",
        spec_id="spec-1",
        card_id="card-1",
        actor_id="auto-actor",
        trigger="mixed_retry",
    )
    assert retry["results"]["knowledge_base"]["ignored_count"] == 1
    assert store.save_count == 1


def test_card_snapshot_never_promotes_a_legacy_grandparent_to_canonical_root():
    source = {
        "id": "kb-current",
        "title": "Legacy child",
        "description": None,
        "content": "body",
        "mime_type": "text/markdown",
        "source_kb_id": "kb-grandparent",
        "root_source_kb_id": None,
        "source_version": 4,
    }

    snapshot = build_card_knowledge_snapshot(
        source,
        source_entity_type="spec",
        source_entity_id="spec-1",
        actor_id="actor-1",
        source_version=9,
    )

    assert snapshot["source_kb_id"] == "kb-current"
    assert snapshot["immediate_parent_kb_id"] == "kb-current"
    assert snapshot["root_source_kb_id"] == "kb-current"
    assert snapshot["source_version"] == 9
    assert len(snapshot["content_hash"]) == 64
