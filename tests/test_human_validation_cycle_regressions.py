from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import func, select

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.quality_assessment import (
    RecordAmbiguityAssessmentCommand,
    RecordAmbiguityAssessmentUseCase,
)
from okto_pulse.core.domain.enums import (
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
)
from okto_pulse.core.domain.human_validation_cycle import (
    LifecycleTransitionConflictError,
    SubjectEditRequiresDraftError,
)
from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentSubjectType,
)
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.domain.spec_validation import RequirementLintRequired
from okto_pulse.core.models.schemas import (
    IdeationMove,
    IdeationUpdate,
    RefinementMove,
    RefinementUpdate,
    SpecMove,
    SpecUpdate,
)
from okto_pulse.core.services import main as main_service
from okto_pulse.core.services.main import (
    IdeationService,
    RefinementService,
    SpecLineagePreflightError,
    SpecService,
)
from okto_pulse.core.ports.quality_assessment import (
    AssessmentSubjectEditionConflict,
)
from sqlalchemy_test_models import (
    Board,
    Ideation,
    IdeationHistory,
    Refinement,
    RefinementHistory,
    Spec,
    SpecHistory,
)


_ACTOR = "validation-cycle-owner"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("subject_type", "initial_status"),
    (
        ("ideation", IdeationStatus.REVIEW),
        ("ideation", IdeationStatus.DONE),
        ("ideation", IdeationStatus.CANCELLED),
        ("refinement", RefinementStatus.REVIEW),
        ("refinement", RefinementStatus.DONE),
        ("refinement", RefinementStatus.CANCELLED),
        ("spec", SpecStatus.REVIEW),
        ("spec", SpecStatus.DONE),
        ("spec", SpecStatus.CANCELLED),
    ),
)
async def test_ac02_real_reopen_opens_exactly_one_edition(
    db_factory,
    subject_type,
    initial_status,
) -> None:
    board_id = _id("board")
    subject_id = _id(subject_type)
    parent_id = _id("ideation-parent")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lifecycle", owner_id=_ACTOR, settings={}))
        if subject_type == "ideation":
            db.add(
                Ideation(
                    id=subject_id,
                    board_id=board_id,
                    title="Lifecycle ideation",
                    status=initial_status,
                    edition=7,
                    version=11,
                    created_by=_ACTOR,
                    skip_ambiguity_gate=True,
                    skip_ambiguity_gate_edition=7,
                )
            )
        elif subject_type == "refinement":
            db.add(
                Ideation(
                    id=parent_id,
                    board_id=board_id,
                    title="Parent",
                    status=IdeationStatus.DONE,
                    created_by=_ACTOR,
                )
            )
            db.add(
                Refinement(
                    id=subject_id,
                    ideation_id=parent_id,
                    board_id=board_id,
                    title="Lifecycle refinement",
                    in_scope=["human validation cycle"],
                    status=initial_status,
                    edition=7,
                    version=11,
                    created_by=_ACTOR,
                    skip_ambiguity_gate=True,
                    skip_ambiguity_gate_edition=7,
                )
            )
        else:
            db.add(
                Spec(
                    id=subject_id,
                    board_id=board_id,
                    title="Lifecycle spec",
                    description="Before reopen",
                    status=initial_status,
                    edition=7,
                    version=11,
                    created_by=_ACTOR,
                    validations=[{"id": "validation-old", "edition": 7}],
                    current_validation_id="validation-old",
                )
            )
        await db.flush()

        if subject_type == "ideation":
            service = IdeationService(db)
            moved = await service.move_ideation(
                subject_id,
                _ACTOR,
                IdeationMove(status=IdeationStatus.DRAFT),
                actor_name="Owner",
            )
            assert moved is not None
            assert moved.edition == 8
            assert moved.skip_ambiguity_gate is False
            await service.update_ideation(
                subject_id,
                _ACTOR,
                IdeationUpdate(description="Edited in Draft"),
            )
            assert moved.edition == 8
            await service.move_ideation(
                subject_id,
                _ACTOR,
                IdeationMove(status=IdeationStatus.REVIEW),
                actor_name="Owner",
            )
        elif subject_type == "refinement":
            service = RefinementService(db)
            moved = await service.move_refinement(
                subject_id,
                _ACTOR,
                RefinementMove(status=RefinementStatus.DRAFT),
                actor_name="Owner",
            )
            assert moved is not None
            assert moved.edition == 8
            assert moved.skip_ambiguity_gate is False
            await service.update_refinement(
                subject_id,
                _ACTOR,
                RefinementUpdate(description="Edited in Draft"),
            )
            assert moved.edition == 8
            await service.move_refinement(
                subject_id,
                _ACTOR,
                RefinementMove(status=RefinementStatus.REVIEW),
                actor_name="Owner",
            )
        else:
            service = SpecService(db)
            moved = await service.move_spec(
                subject_id,
                _ACTOR,
                SpecMove(status=SpecStatus.DRAFT),
                actor_name="Owner",
            )
            assert moved is not None
            assert moved.edition == 8
            assert moved.current_validation_id is None
            assert moved.validations == [{"id": "validation-old", "edition": 7}]
            await service.update_spec(
                subject_id,
                _ACTOR,
                SpecUpdate(description="Edited in Draft"),
            )
            assert moved.edition == 8
            await service.move_spec(
                subject_id,
                _ACTOR,
                SpecMove(status=SpecStatus.REVIEW),
                actor_name="Owner",
            )

        assert moved.edition == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_type", ("ideation", "refinement", "spec"))
async def test_ac03_non_draft_content_rejection_has_zero_writes(
    db_factory,
    subject_type,
) -> None:
    board_id = _id("board")
    subject_id = _id(subject_type)
    parent_id = _id("parent")
    history_model = {
        "ideation": IdeationHistory,
        "refinement": RefinementHistory,
        "spec": SpecHistory,
    }[subject_type]

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Draft boundary", owner_id=_ACTOR, settings={}))
        if subject_type == "ideation":
            entity = Ideation(
                id=subject_id,
                board_id=board_id,
                title="Read only",
                description="unchanged",
                status=IdeationStatus.REVIEW,
                edition=4,
                version=7,
                created_by=_ACTOR,
            )
        elif subject_type == "refinement":
            db.add(
                Ideation(
                    id=parent_id,
                    board_id=board_id,
                    title="Parent",
                    status=IdeationStatus.DONE,
                    created_by=_ACTOR,
                )
            )
            entity = Refinement(
                id=subject_id,
                ideation_id=parent_id,
                board_id=board_id,
                title="Read only",
                description="unchanged",
                status=RefinementStatus.REVIEW,
                edition=4,
                version=7,
                created_by=_ACTOR,
            )
        else:
            entity = Spec(
                id=subject_id,
                board_id=board_id,
                title="Read only",
                description="unchanged",
                status=SpecStatus.APPROVED,
                edition=4,
                version=7,
                validations=[{"id": "validation-current", "edition": 4}],
                current_validation_id="validation-current",
                created_by=_ACTOR,
            )
        db.add(entity)
        await db.flush()
        before_history = await db.scalar(
            select(func.count()).select_from(history_model)
        )

        with pytest.raises(SubjectEditRequiresDraftError) as raised:
            if subject_type == "ideation":
                await IdeationService(db).update_ideation(
                    subject_id,
                    _ACTOR,
                    IdeationUpdate(description="forbidden"),
                )
            elif subject_type == "refinement":
                await RefinementService(db).update_refinement(
                    subject_id,
                    _ACTOR,
                    RefinementUpdate(description="forbidden"),
                )
            else:
                await SpecService(db).update_spec(
                    subject_id,
                    _ACTOR,
                    SpecUpdate(description="forbidden"),
                )

        after_history = await db.scalar(
            select(func.count()).select_from(history_model)
        )
        assert raised.value.code == "subject_edit_requires_draft"
        assert entity.description == "unchanged"
        assert entity.version == 7
        assert entity.edition == 4
        assert after_history == before_history
        if subject_type == "spec":
            assert entity.current_validation_id == "validation-current"


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_type", ("ideation", "refinement", "spec"))
async def test_lifecycle_move_lost_write_fence_has_zero_writes(
    db_factory,
    monkeypatch,
    subject_type,
) -> None:
    """A stale transition cannot compose status, edition, or history writes."""

    board_id = _id("board")
    subject_id = _id(subject_type)
    parent_id = _id("parent")
    history_model = {
        "ideation": IdeationHistory,
        "refinement": RefinementHistory,
        "spec": SpecHistory,
    }[subject_type]

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Fenced lifecycle", owner_id=_ACTOR, settings={}))
        if subject_type == "ideation":
            entity = Ideation(
                id=subject_id,
                board_id=board_id,
                title="Fenced ideation",
                status=IdeationStatus.REVIEW,
                edition=7,
                version=11,
                created_by=_ACTOR,
            )
            service = IdeationService(db)
            move = IdeationMove(status=IdeationStatus.DRAFT)
        elif subject_type == "refinement":
            db.add(
                Ideation(
                    id=parent_id,
                    board_id=board_id,
                    title="Parent",
                    status=IdeationStatus.DONE,
                    created_by=_ACTOR,
                )
            )
            entity = Refinement(
                id=subject_id,
                ideation_id=parent_id,
                board_id=board_id,
                title="Fenced refinement",
                in_scope=["lifecycle"],
                status=RefinementStatus.REVIEW,
                edition=7,
                version=11,
                created_by=_ACTOR,
            )
            service = RefinementService(db)
            move = RefinementMove(status=RefinementStatus.DRAFT)
        else:
            entity = Spec(
                id=subject_id,
                board_id=board_id,
                title="Fenced spec",
                status=SpecStatus.REVIEW,
                edition=7,
                version=11,
                validations=[{"id": "validation-old", "edition": 7}],
                current_validation_id="validation-old",
                created_by=_ACTOR,
            )
            service = SpecService(db)
            move = SpecMove(status=SpecStatus.DRAFT)
        db.add(entity)
        await db.flush()
        history_before = await db.scalar(select(func.count()).select_from(history_model))

        async def lost_fence(*_args, **_kwargs):
            return False

        recorded_allow_decisions: list[object] = []

        async def record_allow(*_args, **kwargs):
            recorded_allow_decisions.append(kwargs["decision"])

        monkeypatch.setattr(main_service, "_application_fence", lost_fence)
        monkeypatch.setattr(
            main_service,
            "_record_critical_context_decision",
            record_allow,
        )
        with pytest.raises(LifecycleTransitionConflictError) as raised:
            if subject_type == "ideation":
                await service.move_ideation(
                    subject_id, _ACTOR, move, actor_name="Owner"
                )
            elif subject_type == "refinement":
                await service.move_refinement(
                    subject_id, _ACTOR, move, actor_name="Owner"
                )
            else:
                await service.move_spec(subject_id, _ACTOR, move, actor_name="Owner")

        assert raised.value.code == "subject_lifecycle_transition_conflict"
        assert raised.value.to_error_dict()["retryable"] is True
        assert entity.status.value == "review"
        assert entity.edition == 7
        assert entity.version == 11
        assert recorded_allow_decisions == []
        assert await db.scalar(select(func.count()).select_from(history_model)) == history_before
        if subject_type == "spec":
            assert entity.current_validation_id == "validation-old"


@pytest.mark.asyncio
async def test_spec_current_previous_and_legacy_history_follow_real_reopen(
    db_factory,
) -> None:
    board_id = _id("board")
    spec_id = _id("spec")
    validations = [
        {"id": "legacy", "outcome": "success"},
        {"id": "prior-edition", "edition": 3, "outcome": "success"},
        {"id": "attempt-1", "edition": 4, "outcome": "failed"},
        {"id": "attempt-2", "edition": 4, "outcome": "success"},
    ]
    async with db_factory() as db:
        db.add(Board(id=board_id, name="History", owner_id=_ACTOR, settings={}))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Validation history",
                status=SpecStatus.VALIDATED,
                edition=4,
                version=9,
                validations=validations,
                current_validation_id="attempt-2",
                created_by=_ACTOR,
            )
        )
        await db.flush()
        service = SpecService(db)

        current = await service.list_spec_validations(
            spec_id,
            lifecycle_state="current",
        )
        previous = await service.list_spec_validations(
            spec_id,
            lifecycle_state="previous",
        )
        legacy = await service.list_spec_validations(
            spec_id,
            lifecycle_state="history_only",
        )

        assert [item["id"] for item in current["validations"]] == ["attempt-2"]
        assert current["validations"][0]["is_current"] is True
        assert [item["id"] for item in previous["validations"]] == [
            "attempt-1",
            "prior-edition",
            "legacy",
        ]
        assert previous["previous_count"] == 3
        assert legacy["validations"] == [
            {
                "id": "legacy",
                "outcome": "success",
                "is_current": False,
                "active": False,
                "lifecycle_state": "history_only",
            }
        ]

        moved = await service.move_spec(
            spec_id,
            _ACTOR,
            SpecMove(status=SpecStatus.DRAFT),
            actor_name="Owner",
        )
        after_reopen = await service.list_spec_validations(
            spec_id,
            lifecycle_state="previous",
        )

        assert moved is not None
        assert moved.edition == 5
        assert moved.current_validation_id is None
        assert after_reopen["current_validation"] is None
        assert after_reopen["previous_count"] == 4
        assert [item["id"] for item in after_reopen["validations"]] == [
            "attempt-2",
            "attempt-1",
            "prior-edition",
            "legacy",
        ]


@pytest.mark.asyncio
async def test_ac04_old_ambiguity_edition_is_rejected_before_any_write_uow() -> None:
    class Reader:
        async def resolve_assessment_preflight_request(self, *_args, **_kwargs):
            raise AssessmentSubjectEditionConflict(
                "assessment_subject_edition_mismatch",
                details={"expected": 3, "current": 4},
            )

        async def lookup_assessment_replay(self, **_kwargs):
            raise AssertionError("replay lookup must not run after rejected preflight")

    class UowFactory:
        def __init__(self) -> None:
            self.opened = 0
            self.receipts: list[object] = []
            self.findings: list[object] = []
            self.heads: list[object] = []

        def __call__(self, **_kwargs):
            self.opened += 1
            raise AssertionError("write UoW must not open for an old edition")

    uow_factory = UowFactory()
    actor = ActorContext(
        _ACTOR,
        "mcp",
        board_id="board-1",
        realm_scope=RealmScope.local(),
    )

    with pytest.raises(AssessmentSubjectEditionConflict) as raised:
        await RecordAmbiguityAssessmentUseCase(
            preflight_reader=Reader(),
            uow_factory=uow_factory,
        ).execute(
            RecordAmbiguityAssessmentCommand(
                board_id="board-1",
                subject_type=AssessmentSubjectType.REFINEMENT,
                subject_id="refinement-1",
                idempotency_key="ambiguity-edition-3",
                expected_subject_version=8,
                expected_subject_edition=3,
                expected_head_revision=0,
                score=0,
                summary="No ambiguity found.",
            ),
            actor=actor,
        )

    assert raised.value.code == "assessment_subject_edition_conflict"
    assert raised.value.details == {"expected": 3, "current": 4}
    assert uow_factory.opened == 0
    assert uow_factory.receipts == []
    assert uow_factory.findings == []
    assert uow_factory.heads == []


@pytest.mark.asyncio
async def test_spec_validation_requires_lint_for_the_exact_edition(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class Persistence:
        async def get_current(self, **kwargs):
            calls.append(kwargs)
            return None

    persistence = Persistence()
    adapter = SimpleNamespace(quality_assessments=lambda _db: persistence)
    from okto_pulse.core.ports import relational_application

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        lambda: adapter,
    )
    spec = SimpleNamespace(id="spec-1", board_id="board-1", edition=4)

    with pytest.raises(RequirementLintRequired) as raised:
        await SpecService(object())._enforce_spec_requirement_lint_gate(spec)

    assert raised.value.code == "requirement_lint_required"
    assert raised.value.to_error_dict()["error_code"] == "requirement_lint_required"
    assert calls == [
        {
            "board_id": "board-1",
            "subject_type": AssessmentSubjectType.SPEC,
            "subject_id": "spec-1",
            "assessment_kind": AssessmentKind.REQUIREMENT_LINT,
            "subject_edition": 4,
        }
    ]


@pytest.mark.asyncio
async def test_spec_validation_accepts_advisory_lint_with_findings(monkeypatch) -> None:
    class Persistence:
        async def get_current(self, **_kwargs):
            return (SimpleNamespace(score=7), SimpleNamespace(revision=1))

    adapter = SimpleNamespace(
        quality_assessments=lambda _db: Persistence()
    )
    from okto_pulse.core.ports import relational_application

    monkeypatch.setattr(
        relational_application,
        "require_relational_application_adapter",
        lambda: adapter,
    )

    await SpecService(object())._enforce_spec_requirement_lint_gate(
        SimpleNamespace(id="spec-1", board_id="board-1", edition=4)
    )


@pytest.mark.asyncio
async def test_ac23_resolves_only_the_proven_status_only_done_snapshot(
    monkeypatch,
) -> None:
    snapshot = SimpleNamespace(id="snapshot-v12", version=12)
    snapshot_reads: list[int] = []
    query_kinds: list[str] = []
    service = RefinementService(object())

    async def get_snapshot(_refinement_id: str, version: int):
        snapshot_reads.append(version)
        return snapshot if version == 12 else None

    async def list_records(_db, kind: str, **kwargs):
        query_kinds.append(kind)
        assert kwargs["limit"] == 2
        return [
            SimpleNamespace(
                changes=[{"field": "status", "old": "approved", "new": "done"}]
            )
        ]

    monkeypatch.setattr(service, "get_snapshot", get_snapshot)
    monkeypatch.setattr(main_service, "_application_list", list_records)

    result = await service.resolve_completed_snapshot(
        SimpleNamespace(
            id="refinement-1",
            status=RefinementStatus.DONE,
            version=13,
        )
    )

    assert result is snapshot
    assert snapshot_reads == [13, 12]
    assert query_kinds == ["refinement_history"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "history_rows",
    (
        [],
        [SimpleNamespace(changes=[{"field": "status", "old": "review", "new": "done"}])],
        [
            SimpleNamespace(
                changes=[
                    {"field": "status", "old": "approved", "new": "done"},
                    {"field": "description", "old": "a", "new": "b"},
                ]
            )
        ],
        [
            SimpleNamespace(changes=[{"field": "status", "old": "approved", "new": "done"}]),
            SimpleNamespace(changes=[{"field": "status", "old": "approved", "new": "done"}]),
        ],
    ),
)
async def test_ac23_rejects_missing_or_additional_done_proof(
    monkeypatch,
    history_rows,
) -> None:
    service = RefinementService(object())

    async def get_snapshot(_refinement_id: str, version: int):
        return SimpleNamespace(id="snapshot-v12", version=12) if version == 12 else None

    async def list_records(_db, _kind: str, **_kwargs):
        return history_rows

    monkeypatch.setattr(service, "get_snapshot", get_snapshot)
    monkeypatch.setattr(main_service, "_application_list", list_records)

    with pytest.raises(SpecLineagePreflightError) as raised:
        await service.resolve_completed_snapshot(
            SimpleNamespace(
                id="refinement-1",
                status=RefinementStatus.DONE,
                version=13,
            )
        )

    assert raised.value.code == "spec_refinement_snapshot_required"


def test_production_sources_do_not_call_the_compatibility_lint_writer() -> None:
    root = Path(__file__).parents[1] / "src" / "okto_pulse" / "core"
    callers = []
    for path in root.rglob("*.py"):
        if path.name == "requirement_lint_writer.py":
            continue
        if "stage_spec_requirement_lint" in path.read_text(encoding="utf-8"):
            callers.append(path.relative_to(root).as_posix())
    assert callers == []
