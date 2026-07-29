from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.mcp_refinement_crud import (
    McpMoveRefinementCommand,
    McpMoveRefinementUseCase,
)
from okto_pulse.core.application.use_cases.mcp_spec_crud import (
    McpDeriveSpecCommand,
    McpDeriveSpecUseCase,
)
from okto_pulse.core.domain.enums import RefinementStatus
from okto_pulse.core.domain.research_decision_ledger import (
    ResearchDecisionAnchor,
    ResearchDecisionAnchorType,
    ResearchDecisionContent,
    ResearchDecisionEntry,
    ResearchDecisionHead,
    ResearchDecisionStatus,
)

NOW = datetime(2026, 7, 27, 18, 0, tzinfo=timezone.utc)


def _head(
    ledger_id: str,
    *,
    entry_id: str,
    status: ResearchDecisionStatus,
) -> ResearchDecisionHead:
    return ResearchDecisionHead(
        ledger_id=ledger_id,
        board_id="board-1",
        refinement_id="refinement-1",
        current_entry_id=entry_id,
        revision=1,
        refinement_version=4,
        status=status,
        updated_by="agent-1",
        updated_at=NOW,
    )


class _ResearchDecisionStore:
    def __init__(self, heads=()) -> None:
        self.heads = tuple(heads)
        self.current = tuple(
            (_entry_for_head(head), head) for head in self.heads
        )
        self.snapshot = None
        self.derivation = None

    async def get_snapshot_for_version(self, **_kwargs):
        return self.snapshot

    async def list_current_heads(self, **_kwargs):
        return self.heads

    async def list_current_entries_with_heads(self, **_kwargs):
        return self.current

    async def save_snapshot(self, snapshot):
        self.snapshot = snapshot
        return snapshot

    async def save_derivation(self, derivation):
        self.derivation = derivation
        return derivation


def _entry_for_head(head: ResearchDecisionHead) -> ResearchDecisionEntry:
    resolved = head.status is ResearchDecisionStatus.RESOLVED
    return ResearchDecisionEntry(
        id=head.current_entry_id,
        ledger_id=head.ledger_id,
        board_id=head.board_id,
        refinement_id=head.refinement_id,
        refinement_version=head.refinement_version,
        predecessor_entry_id=None,
        content=ResearchDecisionContent(
            unknown="Which retry policy should be used?",
            status=head.status,
            anchor=ResearchDecisionAnchor(
                anchor_type=(
                    ResearchDecisionAnchorType.FUNCTIONAL_REQUIREMENT
                ),
                anchor_ref="fr_retry",
            ),
            evidence_refs=("kb:retry",) if resolved else (),
            decision="Use bounded retry." if resolved else None,
            rationale="It bounds pressure." if resolved else None,
            confidence=0.9 if resolved else None,
        ),
        created_by=head.updated_by,
        created_at=head.updated_at,
    )


class _RefinementService:
    def __init__(self, refinement, *, spec=None) -> None:
        self.refinement = refinement
        self.spec = spec

    async def get_refinement(self, _refinement_id):
        return self.refinement

    async def move_refinement(self, _refinement_id, _actor_id, _data, **_kwargs):
        self.refinement.status = RefinementStatus.DONE
        return self.refinement

    async def derive_spec(self, _refinement_id, _actor_id, **_kwargs):
        return self.spec


class _Uow:
    def __init__(self, services) -> None:
        self.services = services
        self.commit_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1


@pytest.mark.asyncio
async def test_mcp_done_transition_freezes_current_heads_before_commit() -> None:
    refinement = SimpleNamespace(
        id="refinement-1",
        board_id="board-1",
        version=4,
        status=RefinementStatus.APPROVED,
        archived=False,
    )
    store = _ResearchDecisionStore(
        (
            _head(
                "ledger-resolved",
                entry_id="entry-resolved",
                status=ResearchDecisionStatus.RESOLVED,
            ),
        )
    )
    uow = _Uow(
        SimpleNamespace(
            refinements=_RefinementService(refinement),
            research_decisions=store,
        )
    )

    result = await McpMoveRefinementUseCase().execute(
        McpMoveRefinementCommand(
            "refinement-1",
            "board-1",
            SimpleNamespace(status=RefinementStatus.DONE),
        ),
        actor=ActorContext(
            "agent-1",
            "mcp",
            board_id="board-1",
        ),
        uow=uow,
    )

    assert result.refinement.status is RefinementStatus.DONE
    assert store.snapshot is not None
    assert store.snapshot.refinement_version == 4
    assert store.snapshot.heads[0].entry_id == "entry-resolved"
    assert uow.commit_calls == 1


@pytest.mark.asyncio
async def test_mcp_refinement_derivation_binds_only_resolved_references() -> None:
    refinement = SimpleNamespace(
        id="refinement-1",
        board_id="board-1",
        version=4,
        status=RefinementStatus.DONE,
        archived=False,
    )
    authored_decisions = [{"id": "spec-decision", "title": "Unaffected"}]
    spec = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        refinement_id="refinement-1",
        version=1,
        decisions=authored_decisions,
        resource_propagation=None,
    )
    store = _ResearchDecisionStore(
        (
            _head(
                "ledger-open",
                entry_id="entry-open",
                status=ResearchDecisionStatus.OPEN,
            ),
            _head(
                "ledger-resolved",
                entry_id="entry-resolved",
                status=ResearchDecisionStatus.RESOLVED,
            ),
        )
    )
    uow = _Uow(
        SimpleNamespace(
            refinements=_RefinementService(refinement, spec=spec),
            research_decisions=store,
        )
    )

    result = await McpDeriveSpecUseCase().execute(
        McpDeriveSpecCommand("refinement", "refinement-1"),
        actor=ActorContext(
            "agent-1",
            "mcp",
            board_id="board-1",
        ),
        uow=uow,
    )

    assert result.spec is spec
    assert store.snapshot is not None
    assert store.derivation is not None
    assert [ref.entry_id for ref in store.derivation.references] == [
        "entry-resolved"
    ]
    assert store.derivation.source_snapshot_id == store.snapshot.id
    assert spec.decisions == authored_decisions
    assert uow.commit_calls == 1
