from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.enums import RefinementStatus
from okto_pulse.core.domain.research_decision_ledger import (
    RefinementLedgerContext,
    RefinementVersionBump,
    ResearchDecisionAction,
    ResearchDecisionAnchor,
    ResearchDecisionAnchorType,
    ResearchDecisionContent,
    ResearchDecisionContractError,
    ResearchDecisionEntry,
    ResearchDecisionEventType,
    ResearchDecisionStatus,
    ResearchDecisionSubjectType,
    research_decision_content_digest,
)
from okto_pulse.core.models.research_decision_ledger import (
    ResearchDecisionDerivationRefView,
    ResearchDecisionEntryView,
    ResearchDecisionFrozenHeadView,
)
from okto_pulse.core.ports.research_decision_ledger import (
    ResearchDecisionListQuery,
)
from okto_pulse.core.services.research_decision_ledger import (
    AppendResearchDecisionCommand,
    ResearchDecisionConflictError,
    ResearchDecisionLedgerService,
    ResearchDecisionValidationError,
    SupersedeResearchDecisionCommand,
)

NOW = datetime(2026, 7, 27, 15, 0, tzinfo=timezone.utc)


class _Ids:
    def __init__(self) -> None:
        self._counter = 0

    def __call__(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{self._counter:04d}"


def _service() -> ResearchDecisionLedgerService:
    return ResearchDecisionLedgerService(
        id_factory=_Ids(),
        clock=lambda: NOW,
    )


def _context(
    *,
    version: int = 3,
    status: RefinementStatus = RefinementStatus.DRAFT,
    archived: bool = False,
) -> RefinementLedgerContext:
    return RefinementLedgerContext(
        board_id="board-1",
        refinement_id="refinement-1",
        version=version,
        status=status,
        archived=archived,
    )


def _anchor(
    anchor_type: ResearchDecisionAnchorType = (
        ResearchDecisionAnchorType.FUNCTIONAL_REQUIREMENT
    ),
    anchor_ref: str = "fr_checkout",
) -> ResearchDecisionAnchor:
    return ResearchDecisionAnchor(
        anchor_type=anchor_type,
        anchor_ref=anchor_ref,
    )


def _content(
    *,
    status: ResearchDecisionStatus = ResearchDecisionStatus.OPEN,
    evidence_refs: tuple[str, ...] = (),
    decision: str | None = None,
    rationale: str | None = None,
    confidence: float | None = None,
    evidence_absence_justification: str | None = None,
) -> ResearchDecisionContent:
    return ResearchDecisionContent(
        unknown="Which retry policy protects the checkout?",
        status=status,
        anchor=_anchor(),
        evidence_refs=evidence_refs,
        alternatives=("bounded exponential backoff", "fixed delay"),
        decision=decision,
        rationale=rationale,
        confidence=confidence,
        evidence_absence_justification=evidence_absence_justification,
    )


def _resolved_content(
    *,
    evidence_refs: tuple[str, ...] = ("kb:retry-analysis",),
    evidence_absence_justification: str | None = None,
) -> ResearchDecisionContent:
    return _content(
        status=ResearchDecisionStatus.RESOLVED,
        evidence_refs=evidence_refs,
        decision="Use bounded exponential backoff.",
        rationale="It bounds pressure while preserving recovery.",
        confidence=0.86,
        evidence_absence_justification=evidence_absence_justification,
    )


def _append_command(
    *,
    version: int = 3,
    content: ResearchDecisionContent | None = None,
) -> AppendResearchDecisionCommand:
    return AppendResearchDecisionCommand(
        board_id="board-1",
        refinement_id="refinement-1",
        expected_refinement_version=version,
        content=content or _content(),
        actor_id="agent-1",
    )


def test_status_vocabulary_is_frozen() -> None:
    assert {status.value for status in ResearchDecisionStatus} == {
        "open",
        "investigating",
        "resolved",
        "deferred",
    }

    with pytest.raises(
        ResearchDecisionContractError,
        match="research_decision_status_invalid",
    ):
        replace(_content(), status="closed")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("head_revision", "error_code"),
    [
        (1, "research_decision_append_requires_empty_head"),
        (False, "research_decision_append_requires_empty_head"),
    ],
)
def test_append_requires_initial_head_revision_zero(
    head_revision: object,
    error_code: str,
) -> None:
    with pytest.raises(
        ResearchDecisionValidationError,
        match=error_code,
    ):
        _service().prepare_append(
            replace(
                _append_command(),
                expected_head_revision=head_revision,  # type: ignore[arg-type]
            ),
            context=_context(),
        )


@pytest.mark.parametrize(
    ("overrides", "error_code"),
    [
        ({"decision": None}, "resolved_research_decision_required"),
        ({"rationale": None}, "resolved_research_rationale_required"),
        ({"confidence": None}, "resolved_research_confidence_required"),
        (
            {
                "evidence_refs": (),
                "evidence_absence_justification": None,
            },
            "resolved_research_evidence_required",
        ),
    ],
)
def test_resolved_requires_complete_decision_and_evidence(
    overrides: dict[str, object],
    error_code: str,
) -> None:
    values = {
        "status": ResearchDecisionStatus.RESOLVED,
        "evidence_refs": ("kb:retry-analysis",),
        "decision": "Use bounded backoff.",
        "rationale": "It bounds retry pressure.",
        "confidence": 0.8,
    }
    values.update(overrides)

    with pytest.raises(ResearchDecisionContractError, match=error_code):
        _content(**values)  # type: ignore[arg-type]


def test_resolved_accepts_explicit_evidence_absence_justification() -> None:
    content = _resolved_content(
        evidence_refs=(),
        evidence_absence_justification=(
            "No production evidence exists before the first controlled rollout."
        ),
    )

    assert content.status is ResearchDecisionStatus.RESOLVED
    assert content.evidence_refs == ()
    assert content.evidence_absence_justification is not None


@pytest.mark.parametrize(
    "anchor_ref",
    ["0", "requirements[0]", "requirements/1/title", "   "],
)
def test_anchor_rejects_mutable_or_blank_references(anchor_ref: str) -> None:
    with pytest.raises(ResearchDecisionContractError):
        _anchor(anchor_ref=anchor_ref)


def test_anchor_accepts_stable_typed_ids_including_uuid() -> None:
    anchor = _anchor(
        ResearchDecisionAnchorType.QA,
        "42d7969a-3c53-47ad-99d0-8ca45f333bca",
    )

    assert anchor.anchor_type is ResearchDecisionAnchorType.QA


def test_refinement_context_rejects_non_refinement_subject() -> None:
    with pytest.raises(
        ResearchDecisionContractError,
        match="research_decision_subject_type_invalid",
    ):
        RefinementLedgerContext(
            board_id="board-1",
            refinement_id="refinement-1",
            version=3,
            status=RefinementStatus.DRAFT,
            archived=False,
            subject_type="spec",  # type: ignore[arg-type]
        )

    assert _context().subject_type is ResearchDecisionSubjectType.REFINEMENT


def test_append_prepares_one_complete_atomic_bundle() -> None:
    service = _service()

    bundle = service.prepare_append(
        _append_command(),
        context=_context(),
    )

    assert bundle.expected_head_revision == 0
    assert bundle.expected_head_entry_id is None
    assert bundle.version_bump.expected_version == 3
    assert bundle.version_bump.resulting_version == 4
    assert bundle.entry.refinement_version == 4
    assert bundle.entry.predecessor_entry_id is None
    assert bundle.next_head.current_entry_id == bundle.entry.id
    assert bundle.next_head.revision == 1
    assert bundle.history.action is ResearchDecisionAction.APPEND
    assert bundle.event.event_type is ResearchDecisionEventType.APPENDED
    assert bundle.outbox.event_id == bundle.event.id
    assert bundle.outbox.partition_key == "board-1"
    assert {
        bundle.entry.id,
        bundle.history.id,
        bundle.event.id,
        bundle.outbox.id,
    } == {
        "rdle_0002",
        "rdlh_0004",
        "evt_0003",
        "out_0005",
    }


def test_entry_is_immutable_and_has_no_update_surface() -> None:
    entry = (
        _service()
        .prepare_append(
            _append_command(),
            context=_context(),
        )
        .entry
    )

    with pytest.raises(FrozenInstanceError):
        entry.created_by = "other"  # type: ignore[misc]
    assert not hasattr(entry, "updated_at")


@pytest.mark.parametrize(
    ("context", "command", "error_code"),
    [
        (
            _context(status=RefinementStatus.REVIEW),
            _append_command(),
            "research_decision_refinement_not_draft",
        ),
        (
            _context(archived=True),
            _append_command(),
            "research_decision_refinement_archived",
        ),
        (
            _context(version=4),
            _append_command(version=3),
            "research_decision_refinement_version_conflict",
        ),
        (
            _context(),
            replace(_append_command(), board_id="board-2"),
            "research_decision_scope_mismatch",
        ),
        (
            _context(),
            replace(_append_command(), refinement_id="refinement-2"),
            "research_decision_scope_mismatch",
        ),
    ],
)
def test_append_fails_closed_on_refinement_fences(
    context: RefinementLedgerContext,
    command: AppendResearchDecisionCommand,
    error_code: str,
) -> None:
    with pytest.raises(
        (ResearchDecisionConflictError, ResearchDecisionValidationError),
        match=error_code,
    ):
        _service().prepare_append(command, context=context)


def test_supersede_appends_successor_and_advances_both_cas_fences_once() -> None:
    service = _service()
    first = service.prepare_append(_append_command(), context=_context())
    original_entry = first.entry
    original_head = first.next_head
    command = SupersedeResearchDecisionCommand(
        board_id="board-1",
        refinement_id="refinement-1",
        ledger_id=original_entry.ledger_id,
        predecessor_entry_id=original_entry.id,
        expected_refinement_version=4,
        expected_head_revision=1,
        content=_resolved_content(),
        actor_id="agent-2",
    )

    successor = service.prepare_supersede(
        command,
        context=_context(version=4),
        current_head=original_head,
        predecessor=original_entry,
    )

    assert successor.entry.id != original_entry.id
    assert successor.entry.predecessor_entry_id == original_entry.id
    assert successor.entry.ledger_id == original_entry.ledger_id
    assert successor.expected_head_entry_id == original_entry.id
    assert successor.expected_head_revision == 1
    assert successor.next_head.revision == 2
    assert successor.version_bump.expected_version == 4
    assert successor.version_bump.resulting_version == 5
    assert successor.history.action is ResearchDecisionAction.SUPERSEDE
    assert successor.event.event_type is ResearchDecisionEventType.SUPERSEDED
    assert original_entry.status is ResearchDecisionStatus.OPEN
    assert original_head.current_entry_id == original_entry.id


@pytest.mark.parametrize(
    ("change", "error_code"),
    [
        (
            {"expected_head_revision": 2},
            "research_decision_head_revision_conflict",
        ),
        (
            {"predecessor_entry_id": "rdle-stale"},
            "research_decision_head_entry_conflict",
        ),
        (
            {"ledger_id": "rdl-other"},
            "research_decision_scope_mismatch",
        ),
    ],
)
def test_supersede_validates_head_cas_and_scope(
    change: dict[str, object],
    error_code: str,
) -> None:
    service = _service()
    first = service.prepare_append(_append_command(), context=_context())
    command = SupersedeResearchDecisionCommand(
        board_id="board-1",
        refinement_id="refinement-1",
        ledger_id=first.entry.ledger_id,
        predecessor_entry_id=first.entry.id,
        expected_refinement_version=4,
        expected_head_revision=1,
        content=_resolved_content(),
        actor_id="agent-2",
    )

    with pytest.raises(
        (ResearchDecisionConflictError, ResearchDecisionValidationError),
        match=error_code,
    ):
        service.prepare_supersede(
            replace(command, **change),
            context=_context(version=4),
            current_head=first.next_head,
            predecessor=first.entry,
        )


def test_version_bump_contract_rejects_zero_or_multiple_bumps() -> None:
    with pytest.raises(
        ResearchDecisionContractError,
        match="research_decision_requires_single_refinement_version_bump",
    ):
        RefinementVersionBump(
            board_id="board-1",
            refinement_id="refinement-1",
            expected_version=3,
            resulting_version=5,
        )


def test_snapshot_freezes_heads_and_derivation_exports_only_resolved_refs() -> None:
    service = _service()
    open_bundle = service.prepare_append(
        _append_command(),
        context=_context(),
    )
    resolved_bundle = service.prepare_supersede(
        SupersedeResearchDecisionCommand(
            board_id="board-1",
            refinement_id="refinement-1",
            ledger_id=open_bundle.entry.ledger_id,
            predecessor_entry_id=open_bundle.entry.id,
            expected_refinement_version=4,
            expected_head_revision=1,
            content=_resolved_content(),
            actor_id="agent-2",
        ),
        context=_context(version=4),
        current_head=open_bundle.next_head,
        predecessor=open_bundle.entry,
    )
    other_open = service.prepare_append(
        _append_command(version=5),
        context=_context(version=5),
    )
    snapshot = service.freeze_heads(
        context=_context(version=6),
        current=(
            (resolved_bundle.entry, resolved_bundle.next_head),
            (other_open.entry, other_open.next_head),
        ),
    )

    derivation = service.derive_resolved_references(
        snapshot=snapshot,
        board_id="board-1",
        spec_id="spec-1",
        spec_version=1,
    )

    assert snapshot.refinement_version == 6
    assert len(snapshot.heads) == 2
    assert len(derivation.references) == 1
    reference = derivation.references[0]
    assert reference.entry_id == resolved_bundle.entry.id
    assert reference.status is ResearchDecisionStatus.RESOLVED
    assert reference.content_digest == snapshot.heads[0].content_digest
    assert len(reference.content_digest) == 64
    assert (
        ResearchDecisionEntryView.from_domain(
            resolved_bundle.entry
        ).content_digest
        == reference.content_digest
    )
    assert (
        ResearchDecisionFrozenHeadView.from_domain(
            snapshot.heads[0]
        ).content_digest
        == reference.content_digest
    )
    assert (
        ResearchDecisionDerivationRefView.from_domain(
            reference
        ).content_digest
        == reference.content_digest
    )
    assert reference.source_snapshot_id == snapshot.id
    assert reference.source_refinement_version == 6
    assert not hasattr(derivation, "decisions")
    assert not hasattr(reference, "decision")

    advanced_head = replace(
        resolved_bundle.next_head,
        current_entry_id="rdle_future",
        revision=3,
        refinement_version=7,
        updated_at=NOW + timedelta(minutes=1),
    )
    assert advanced_head.current_entry_id == "rdle_future"
    assert snapshot.heads[0].entry_id == resolved_bundle.entry.id


def test_content_digest_is_canonical_and_content_sensitive() -> None:
    content = _resolved_content()
    equivalent = ResearchDecisionContent(
        unknown=content.unknown,
        status=content.status,
        anchor=ResearchDecisionAnchor(
            anchor_type=content.anchor.anchor_type,
            anchor_ref=content.anchor.anchor_ref,
        ),
        evidence_refs=list(content.evidence_refs),
        alternatives=list(content.alternatives),
        decision=content.decision,
        rationale=content.rationale,
        confidence=content.confidence,
        evidence_absence_justification=(
            content.evidence_absence_justification
        ),
    )
    changed = replace(content, rationale="A different rationale.")

    assert research_decision_content_digest(content) == (
        research_decision_content_digest(equivalent)
    )
    assert research_decision_content_digest(content) != (
        research_decision_content_digest(changed)
    )


def _entry(
    entry_id: str,
    *,
    created_at: datetime,
    board_id: str = "board-1",
    status: ResearchDecisionStatus = ResearchDecisionStatus.OPEN,
) -> ResearchDecisionEntry:
    content = (
        _resolved_content()
        if status is ResearchDecisionStatus.RESOLVED
        else _content(status=status)
    )
    return ResearchDecisionEntry(
        id=entry_id,
        ledger_id=f"ledger-{entry_id}",
        board_id=board_id,
        refinement_id="refinement-1",
        refinement_version=3,
        predecessor_entry_id=None,
        content=content,
        created_by="agent-1",
        created_at=created_at,
    )


def test_pagination_is_bounded_and_uses_created_at_then_id_desc() -> None:
    with pytest.raises(ValueError, match="limit must be 1..200"):
        ResearchDecisionListQuery(
            board_id="board-1",
            refinement_id="refinement-1",
            limit=0,
        )
    with pytest.raises(ValueError, match="limit must be 1..200"):
        ResearchDecisionListQuery(
            board_id="board-1",
            refinement_id="refinement-1",
            limit=201,
        )

    service = _service()
    entries = (
        _entry("entry-a", created_at=NOW),
        _entry("entry-c", created_at=NOW),
        _entry("entry-b", created_at=NOW),
        _entry("entry-z", created_at=NOW - timedelta(seconds=1)),
        _entry("entry-foreign", created_at=NOW, board_id="board-2"),
    )
    first = service.paginate_entries(
        entries,
        ResearchDecisionListQuery(
            board_id="board-1",
            refinement_id="refinement-1",
            limit=2,
        ),
    )
    second = service.paginate_entries(
        entries,
        ResearchDecisionListQuery(
            board_id="board-1",
            refinement_id="refinement-1",
            limit=2,
            cursor=first.next_cursor,
        ),
    )

    assert [entry.id for entry in first.items] == ["entry-c", "entry-b"]
    assert first.has_more is True
    assert first.next_cursor is not None
    assert [entry.id for entry in second.items] == ["entry-a", "entry-z"]
    assert second.has_more is False
    assert second.next_cursor is None
    assert ResearchDecisionListQuery.ordering == (
        "created_at DESC",
        "id DESC",
    )


def test_pagination_can_filter_resolved_entries() -> None:
    service = _service()
    entries = (
        _entry(
            "entry-resolved",
            created_at=NOW,
            status=ResearchDecisionStatus.RESOLVED,
        ),
        _entry("entry-open", created_at=NOW),
    )

    page = service.paginate_entries(
        entries,
        ResearchDecisionListQuery(
            board_id="board-1",
            refinement_id="refinement-1",
            status=ResearchDecisionStatus.RESOLVED,
        ),
    )

    assert [entry.id for entry in page.items] == ["entry-resolved"]
