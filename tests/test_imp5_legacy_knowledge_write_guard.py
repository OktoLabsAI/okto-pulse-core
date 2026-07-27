from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from typing import Any

import pytest

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
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)
from okto_pulse.core.services.legacy_knowledge_write_guard import (
    LEGACY_KNOWLEDGE_WRITE_FORBIDDEN,
)
from okto_pulse.core.services.spec_resource_propagation import (
    SpecResourcePropagationService,
)


class _ScopePort:
    def __init__(
        self,
        *,
        v2_active: bool = False,
        failure: Exception | None = None,
    ) -> None:
        self.v2_active = v2_active
        self.failure = failure
        self.calls: list[Any] = []

    async def load_scope(self, context: Any, request: Any) -> KnowledgePropagationScope:
        self.calls.append((context, request))
        if self.failure is not None:
            raise self.failure
        return KnowledgePropagationScope(
            target=request.target,
            scope_revision=1 if self.v2_active else 0,
            v2_active=self.v2_active,
            selection_state=(
                KnowledgeSelectionState.OMITTED if self.v2_active else None
            ),
        )


class _PropagationStore:
    def __init__(
        self,
        *,
        card: ResourcePropagationCardRecord,
        resource_types: list[str],
        forbid_knowledge_read: bool = False,
    ) -> None:
        self.card = card
        self.resource_types = resource_types
        self.forbid_knowledge_read = forbid_knowledge_read
        self.spec = ResourcePropagationSpecFact(
            id="spec-1",
            board_id="board-1",
            screen_mockups=({"id": "screen-1", "title": "Screen"},),
            version=7,
        )
        self.knowledge_reads = 0
        self.saved_fields: list[tuple[str, ...]] = []
        self.audits: list[dict[str, Any]] = []

    async def get_board(self, _context: Any, *, board_id: str) -> Any:
        return ResourcePropagationBoardFact(
            id=board_id,
            settings={
                "auto_derive_spec_resources_enabled": True,
                "auto_derive_spec_resource_types": self.resource_types,
            },
        )

    async def get_spec(self, _context: Any, *, spec_id: str) -> Any:
        return self.spec if spec_id == self.spec.id else None

    async def get_card(self, _context: Any, *, card_id: str) -> Any:
        return self.card if card_id == self.card.id else None

    async def list_spec_knowledge_bases(
        self,
        _context: Any,
        *,
        spec_id: str,
    ) -> Any:
        self.knowledge_reads += 1
        if self.forbid_knowledge_read:
            raise AssertionError("v2 target must not read legacy KB sources")
        assert spec_id == self.spec.id
        return (
            ResourcePropagationKnowledgeBaseFact(
                id="kb-1",
                title="Source KB",
                description=None,
                content="source bytes",
                mime_type="text/markdown",
            ),
        )

    async def save_card(
        self,
        _context: Any,
        record: Any,
        *,
        changed_fields: Any,
    ) -> None:
        assert record is self.card
        self.saved_fields.append(tuple(changed_fields))

    async def record_audit(self, _context: Any, **kwargs: Any) -> None:
        self.audits.append(kwargs)


def _card(*, knowledge_bases: list[Any] | None = None) -> ResourcePropagationCardRecord:
    return ResourcePropagationCardRecord(
        id="card-1",
        board_id="board-1",
        knowledge_bases=copy.deepcopy(knowledge_bases or []),
        screen_mockups=[],
    )


@pytest.mark.asyncio
async def test_v1_target_keeps_legacy_autocopy_compatible(monkeypatch: Any) -> None:
    card = _card()
    store = _PropagationStore(
        card=card,
        resource_types=["knowledge_base"],
    )
    monkeypatch.setattr(
        propagation_module,
        "get_spec_resource_propagation_store",
        lambda: store,
    )

    result = await SpecResourcePropagationService(
        object(),
        knowledge_propagation_port=_ScopePort(v2_active=False),
    ).propagate_for_card(
        board_id="board-1",
        spec_id="spec-1",
        card_id="card-1",
        actor_id="actor-1",
        trigger="spec_knowledge_created",
    )

    assert result["results"]["knowledge_base"]["copied_count"] == 1
    assert store.knowledge_reads == 1
    assert store.saved_fields == [("knowledge_bases",)]
    assert card.knowledge_bases[0]["content"] == "source bytes"


@pytest.mark.asyncio
async def test_v2_target_skips_only_knowledge_and_preserves_legacy_bytes(
    monkeypatch: Any,
) -> None:
    original = [
        {
            "id": "cardkb-legacy",
            "content": "immutable legacy bytes",
            "nested": {"order": [3, 2, 1]},
        }
    ]
    card = _card(knowledge_bases=original)
    before = json.dumps(
        card.knowledge_bases,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    store = _PropagationStore(
        card=card,
        resource_types=["knowledge_base", "mockup"],
        forbid_knowledge_read=True,
    )
    monkeypatch.setattr(
        propagation_module,
        "get_spec_resource_propagation_store",
        lambda: store,
    )

    from okto_pulse.core.services import design_system

    gate_calls = 0

    async def _allow_mockups(*_args: Any, **_kwargs: Any) -> None:
        nonlocal gate_calls
        gate_calls += 1

    monkeypatch.setattr(
        design_system,
        "gate_entity_screen_mockups",
        _allow_mockups,
    )

    result = await SpecResourcePropagationService(
        object(),
        knowledge_propagation_port=_ScopePort(v2_active=True),
    ).propagate_for_card(
        board_id="board-1",
        spec_id="spec-1",
        card_id="card-1",
        actor_id="actor-1",
        trigger="spec_knowledge_updated",
    )

    after = json.dumps(
        card.knowledge_bases,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    assert after == before
    assert result["results"]["knowledge_base"]["skipped"] is True
    assert result["results"]["knowledge_base"]["reason"] == "v2_active"
    assert result["results"]["mockup"]["copied_ids"] == ["screen-1"]
    assert store.knowledge_reads == 0
    assert store.saved_fields == [("screen_mockups",)]
    assert gate_calls == 1
    assert (
        store.audits[0]["details"]["results"]["knowledge_base"]["reason"]
        == "v2_active"
    )


@pytest.mark.asyncio
async def test_scope_read_failure_aborts_before_any_resource_write(
    monkeypatch: Any,
) -> None:
    card = _card(knowledge_bases=[{"id": "legacy", "content": "unchanged"}])
    store = _PropagationStore(
        card=card,
        resource_types=["mockup", "knowledge_base"],
        forbid_knowledge_read=True,
    )
    monkeypatch.setattr(
        propagation_module,
        "get_spec_resource_propagation_store",
        lambda: store,
    )

    with pytest.raises(RuntimeError, match="scope unavailable"):
        await SpecResourcePropagationService(
            object(),
            knowledge_propagation_port=_ScopePort(
                failure=RuntimeError("scope unavailable"),
            ),
        ).propagate_for_card(
            board_id="board-1",
            spec_id="spec-1",
            card_id="card-1",
            actor_id="actor-1",
            trigger="spec_knowledge_deleted",
        )

    assert card.knowledge_bases == [{"id": "legacy", "content": "unchanged"}]
    assert card.screen_mockups == []
    assert store.knowledge_reads == 0
    assert store.saved_fields == []
    assert store.audits == []


class _Lookup:
    def __init__(self, value: Any) -> None:
        self.value = value

    async def get_spec(self, _entity_id: str) -> Any:
        return self.value

    async def get_card(self, _entity_id: str) -> Any:
        return self.value


class _V2Read:
    def __init__(self) -> None:
        self.targets: list[Any] = []

    async def read(self, target: Any) -> Any:
        self.targets.append(target)
        return SimpleNamespace(v2_active=True)


class _ForbiddenLegacyCardService(_Lookup):
    def __init__(self, card: Any) -> None:
        super().__init__(card)
        self.update_count = 0

    async def update_card(self, *_args: Any, **_kwargs: Any) -> None:
        self.update_count += 1
        raise AssertionError("v2 rejection must happen before Card update")


class _ForbiddenKnowledgeList:
    async def list_knowledge(self, _spec_id: str) -> Any:
        raise AssertionError("v2 rejection must happen before legacy source reads")


class _McpUow:
    def __init__(self) -> None:
        spec = SimpleNamespace(id="spec-1", board_id="board-1")
        card = SimpleNamespace(
            id="card-1",
            board_id="board-1",
            knowledge_bases=[{"id": "legacy", "content": "unchanged"}],
        )
        self.cards = _ForbiddenLegacyCardService(card)
        self.knowledge = _V2Read()
        self.services = SimpleNamespace(
            specs=_Lookup(spec),
            cards=self.cards,
            spec_knowledge=_ForbiddenKnowledgeList(),
            knowledge_propagation=self.knowledge,
        )
        self.commit_count = 0

    async def commit(self) -> None:
        self.commit_count += 1


@pytest.mark.asyncio
async def test_manual_mcp_copy_rejects_v2_before_read_or_update() -> None:
    uow = _McpUow()

    with pytest.raises(KnowledgePropagationServiceError) as caught:
        await McpCopyKnowledgeToCardUseCase().execute(
            McpCopyKnowledgeToCardCommand(
                "board-1",
                "spec-1",
                "card-1",
                None,
            ),
            actor=ActorContext("actor-1", "mcp", board_id="board-1"),
            uow=uow,
        )

    assert caught.value.code == LEGACY_KNOWLEDGE_WRITE_FORBIDDEN
    assert uow.cards.update_count == 0
    assert uow.commit_count == 0
    assert len(uow.knowledge.targets) == 1
