"""REST-only write fences and provenance for the Research Decision Ledger."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import okto_pulse.core.application.use_cases.research_decision_ledger as rdl_uc
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.research_decision_ledger import (
    GetResearchDecisionHeadCommand,
    GetResearchDecisionHeadUseCase,
    WriteResearchDecisionCommand,
    WriteResearchDecisionUseCase,
)
from okto_pulse.core.domain.enums import RefinementStatus
from okto_pulse.core.domain.permissions import Permissions
from okto_pulse.core.domain.research_decision_ledger import (
    ResearchDecisionAnchor,
    ResearchDecisionAnchorType,
    ResearchDecisionCommitResult,
    ResearchDecisionContent,
    ResearchDecisionEntry,
    ResearchDecisionHead,
    ResearchDecisionStatus,
)


class _Refinements:
    def __init__(self, refinement) -> None:
        self.refinement = refinement

    async def get_refinement(self, _refinement_id: str):
        return self.refinement


class _Store:
    def __init__(self) -> None:
        self.bundle = None
        self.receipt = None
        self.request_digest = None
        self.apply_calls = 0
        self.current = None

    async def resolve_idempotent_result(self, *, request_digest: str, **_kwargs):
        self.request_digest = request_digest
        return self.receipt

    async def apply_bundle_cas(self, bundle):
        self.apply_calls += 1
        self.bundle = bundle
        self.receipt = ResearchDecisionCommitResult(
            entry=bundle.entry,
            head=bundle.next_head,
            refinement_version=bundle.version_bump.resulting_version,
            history_id=bundle.history.id,
            event_id=bundle.event.id,
            outbox_id=bundle.outbox.id,
        )
        return self.receipt

    async def get_current(self, **_kwargs):
        return self.current


class _Services:
    def __init__(self, refinement, store: _Store) -> None:
        self.refinements = _Refinements(refinement)
        self.research_decisions = store

    async def resolve_user_permissions(
        self,
        _user_id: str,
        _board_id: str,
    ) -> frozenset[str]:
        return frozenset(
            {
                Permissions.SPECS_UPDATE,
                Permissions.BOARD_READ,
                "refinement.research_decisions.append",
                "refinement.research_decisions.read",
            }
        )


class _Uow:
    def __init__(self, services: _Services) -> None:
        self.services = services
        self.commit_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1


@pytest.mark.asyncio
async def test_rest_loads_current_version_for_internal_cas_and_exact_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refinement = SimpleNamespace(
        id="refinement-rest",
        board_id="board-rest",
        version=7,
        status=RefinementStatus.DRAFT,
        archived=False,
    )
    store = _Store()
    uow = _Uow(_Services(refinement, store))

    async def accessible_board(*_args, **_kwargs):
        return SimpleNamespace(id="board-rest")

    monkeypatch.setattr(rdl_uc, "load_accessible_board", accessible_board)
    command = WriteResearchDecisionCommand(
        board_id=None,
        refinement_id=refinement.id,
        expected_refinement_version=None,
        expected_head_revision=0,
        content=ResearchDecisionContent(
            unknown="Which retry policy should be used?",
            status=ResearchDecisionStatus.OPEN,
            anchor=ResearchDecisionAnchor(
                anchor_type=(
                    ResearchDecisionAnchorType.FUNCTIONAL_REQUIREMENT
                ),
                anchor_ref="fr_retry",
            ),
        ),
        idempotency_key="rest-exact-replay",
    )
    actor = ActorContext("human-owner", "rest")

    first = await WriteResearchDecisionUseCase().execute(
        command,
        actor=actor,
        uow=uow,
    )
    assert store.bundle.version_bump.expected_version == 7
    assert store.bundle.version_bump.resulting_version == 8
    assert store.bundle.event.actor_type == "user"
    first_digest = store.bundle.request_digest

    refinement.version = 8
    replay = await WriteResearchDecisionUseCase().execute(
        command,
        actor=actor,
        uow=uow,
    )

    assert replay.receipt == first.receipt
    assert store.request_digest == first_digest
    assert store.apply_calls == 1
    assert uow.commit_calls == 1


@pytest.mark.asyncio
async def test_rest_head_read_returns_authoritative_entry_and_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refinement = SimpleNamespace(
        id="refinement-rest",
        board_id="board-rest",
        version=8,
        status=RefinementStatus.DRAFT,
        archived=False,
    )
    store = _Store()
    now = datetime(2026, 7, 28, tzinfo=timezone.utc)
    content = ResearchDecisionContent(
        unknown="Which retry policy should be used?",
        status=ResearchDecisionStatus.OPEN,
        anchor=ResearchDecisionAnchor(
            anchor_type=ResearchDecisionAnchorType.FUNCTIONAL_REQUIREMENT,
            anchor_ref="fr_retry",
        ),
    )
    entry = ResearchDecisionEntry(
        id="entry-rest-4",
        ledger_id="ledger-rest",
        board_id="board-rest",
        refinement_id="refinement-rest",
        refinement_version=8,
        predecessor_entry_id="entry-rest-3",
        content=content,
        created_by="human-owner",
        created_at=now,
    )
    head = ResearchDecisionHead(
        ledger_id=entry.ledger_id,
        board_id=entry.board_id,
        refinement_id=entry.refinement_id,
        current_entry_id=entry.id,
        revision=4,
        refinement_version=entry.refinement_version,
        status=entry.status,
        updated_by="human-owner",
        updated_at=now,
    )
    store.current = (entry, head)
    uow = _Uow(_Services(refinement, store))

    async def accessible_board(*_args, **_kwargs):
        return SimpleNamespace(id="board-rest")

    monkeypatch.setattr(rdl_uc, "load_accessible_board", accessible_board)
    result = await GetResearchDecisionHeadUseCase().execute(
        GetResearchDecisionHeadCommand(
            board_id=None,
            refinement_id=refinement.id,
            ledger_id=head.ledger_id,
        ),
        actor=ActorContext("human-owner", "rest"),
        uow=uow,
    )

    assert result.entry is entry
    assert result.head is head
    assert result.head.revision == 4
    assert uow.commit_calls == 0
