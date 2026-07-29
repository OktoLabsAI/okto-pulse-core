"""Regression matrix for allowed-transition preview/mutation parity.

These tests intentionally exercise the entity-scoped read model rather than the
static lifecycle registry.  Every asserted blocker is a persisted fact that the
corresponding mutation service also rejects; request-local inputs such as a card
completion report remain outside this preview contract.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import uuid
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from okto_pulse.core.application.use_cases.allowed_transitions import (
    ListAllowedTransitionsCommand,
    ListAllowedTransitionsUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import (
    CardStatus,
    CardType,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.kg.cognitive_closeout_gate import (
    ELIGIBLE_CLOSEOUT_ENTITY_TYPES,
)
from okto_pulse.core.models.schemas import (
    IdeationMove,
    RefinementCreate,
    SpecMove,
)
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services import main as main_service
from okto_pulse.core.services.main import (
    IdeationService,
    RefinementService,
    SpecService,
)
from okto_pulse.core.services.resource_gate import ResourceGateService
from sqlalchemy_test_models import Board, Card, Ideation, Refinement, Spec, Sprint


USER_ID = "allowed-transition-parity-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _board(board_id: str, *, settings: dict | None = None) -> Board:
    return Board(
        id=board_id,
        name=f"Parity board {board_id}",
        owner_id=USER_ID,
        realm_id=LOCAL_REALM_ID,
        settings=settings or {},
    )


async def _persist(db_factory, *rows: object) -> None:
    async with db_factory() as db:
        db.add_all(list(rows))
        await db.commit()


async def _preview_transition(
    db,
    *,
    board_id: str,
    entity_type: str,
    entity_id: str,
    to_status: str,
    configure_services=None,
):
    uow = resolve_unit_of_work_factory().wrap(db)
    if configure_services is not None:
        configure_services(uow.services)
    result = await ListAllowedTransitionsUseCase().execute(
        ListAllowedTransitionsCommand(
            board_id,
            entity_type,
            entity_id=entity_id,
        ),
        actor=ActorContext(
            USER_ID,
            "test",
            board_id=board_id,
            realm_id=LOCAL_REALM_ID,
        ),
        uow=uow,
    )
    return next(
        transition
        for transition in result.read_model.allowed_transitions
        if transition.to_status == to_status
    )


def _install_cognitive_block(services, service_name: str) -> None:
    service = getattr(services, service_name)
    service._validate_cognitive_done = AsyncMock(
        side_effect=ValueError(
            "cognitive_consolidation_pending: done transition blocked (1)"
        )
    )


@pytest.mark.asyncio
async def test_spec_done_preview_blocks_acceptance_criterion_without_scenario(
    db_factory,
) -> None:
    board_id = _id("spec-ac-board")
    spec_id = _id("spec-ac")
    await _persist(
        db_factory,
        _board(board_id),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Uncovered acceptance criterion",
            status=SpecStatus.IN_PROGRESS,
            acceptance_criteria=[
                {
                    "id": "ac-uncovered",
                    "title": "Every criterion needs a scenario",
                }
            ],
            test_scenarios=[],
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="spec",
            entity_id=spec_id,
            to_status="done",
        )

    assert done.blocked_reason is not None
    assert "acceptance criteria lack test scenarios" in done.blocked_reason.lower()


@pytest.mark.asyncio
async def test_spec_done_preview_uses_canonical_cognitive_gate(db_factory) -> None:
    board_id = _id("spec-cog-board")
    spec_id = _id("spec-cog")
    await _persist(
        db_factory,
        _board(board_id),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Cognitively blocked spec",
            status=SpecStatus.IN_PROGRESS,
            acceptance_criteria=[],
            test_scenarios=[],
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="spec",
            entity_id=spec_id,
            to_status="done",
            configure_services=lambda services: _install_cognitive_block(
                services, "specs"
            ),
        )

    assert "cognitive_consolidation_pending" in (done.blocked_reason or "")


def _sprint_scope_case(
    case: str,
    *,
    board_id: str,
    spec_id: str,
    sprint_id: str,
) -> tuple[Spec, Sprint, list[Card]]:
    scenario_id = f"scenario-{case}"
    rule_id = f"rule-{case}"
    scenarios: list[dict] = []
    rules: list[dict] = []
    sprint_scenario_ids: list[str] = []
    sprint_rule_ids: list[str] = []
    cards: list[Card] = []

    if case == "coverage":
        scenarios = [
            {
                "id": scenario_id,
                "title": "Scoped but not executed",
                "status": "draft",
            }
        ]
        sprint_scenario_ids = [scenario_id]
    elif case == "evidence":
        scenarios = [
            {
                "id": scenario_id,
                "title": "Successful status without authenticated evidence",
                "status": "passed",
            }
        ]
        cards = [
            Card(
                id=_id("scope-test-card"),
                board_id=board_id,
                spec_id=spec_id,
                sprint_id=sprint_id,
                title="Assigned test card",
                card_type=CardType.TEST,
                status=CardStatus.DONE,
                test_scenario_ids=[scenario_id],
                position=0,
                created_by=USER_ID,
            )
        ]
    elif case == "rules":
        rules = [
            {
                "id": rule_id,
                "title": "Scoped rule without an assigned-card backlink",
                "linked_task_ids": [],
            }
        ]
        sprint_rule_ids = [rule_id]
    else:  # pragma: no cover - test helper misuse
        raise AssertionError(case)

    spec = Spec(
        id=spec_id,
        board_id=board_id,
        title=f"Sprint scope spec: {case}",
        status=SpecStatus.IN_PROGRESS,
        acceptance_criteria=[],
        test_scenarios=scenarios,
        business_rules=rules,
        created_by=USER_ID,
    )
    sprint = Sprint(
        id=sprint_id,
        board_id=board_id,
        spec_id=spec_id,
        title=f"Sprint scope: {case}",
        status=SprintStatus.REVIEW,
        test_scenario_ids=sprint_scenario_ids,
        business_rule_ids=sprint_rule_ids,
        skip_qualitative_validation=True,
        created_by=USER_ID,
    )
    return spec, sprint, cards


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("coverage", "sprint_test_not_successful"),
        ("evidence", "sprint_test_evidence_missing"),
        ("rules", "sprint_business_rule_uncovered"),
    ],
)
async def test_sprint_closed_preview_applies_canonical_scope_blockers(
    db_factory,
    case: str,
    expected_code: str,
) -> None:
    board_id = _id(f"sprint-{case}-board")
    spec_id = _id(f"sprint-{case}-spec")
    sprint_id = _id(f"sprint-{case}")
    spec, sprint, cards = _sprint_scope_case(
        case,
        board_id=board_id,
        spec_id=spec_id,
        sprint_id=sprint_id,
    )
    await _persist(db_factory, _board(board_id), spec, sprint, *cards)

    async with db_factory() as db:
        closed = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="sprint",
            entity_id=sprint_id,
            to_status="closed",
        )

    assert closed.blocked_reason is not None
    assert "sprint_scope_gate_blocked" in closed.blocked_reason
    assert expected_code in closed.blocked_reason


@pytest.mark.asyncio
async def test_sprint_closed_preview_blocks_approval_below_threshold(
    db_factory,
) -> None:
    board_id = _id("sprint-threshold-board")
    spec_id = _id("sprint-threshold-spec")
    sprint_id = _id("sprint-threshold")
    await _persist(
        db_factory,
        _board(board_id, settings={"validation_threshold_global": 70}),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Threshold spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
        ),
        Sprint(
            id=sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Below-threshold sprint",
            status=SprintStatus.REVIEW,
            evaluations=[
                {
                    "recommendation": "approve",
                    "overall_score": 40,
                    "stale": False,
                }
            ],
            skip_qualitative_validation=False,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        closed = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="sprint",
            entity_id=sprint_id,
            to_status="closed",
        )

    assert "sprint_evaluation_below_threshold" in (closed.blocked_reason or "")


@pytest.mark.asyncio
async def test_refinement_review_preview_blocks_empty_in_scope(db_factory) -> None:
    board_id = _id("ref-scope-board")
    ideation_id = _id("ref-scope-idea")
    refinement_id = _id("ref-scope")
    await _persist(
        db_factory,
        _board(board_id),
        Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Parent ideation",
            status=IdeationStatus.DRAFT,
            created_by=USER_ID,
        ),
        Refinement(
            id=refinement_id,
            board_id=board_id,
            ideation_id=ideation_id,
            title="Empty refinement",
            status=RefinementStatus.DRAFT,
            in_scope=["", "   "],
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        review = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="refinement",
            entity_id=refinement_id,
            to_status="review",
        )

    assert "refinement_scope_required" in (review.blocked_reason or "")


@pytest.mark.asyncio
async def test_refinement_done_preview_uses_canonical_cognitive_gate(
    db_factory,
) -> None:
    board_id = _id("ref-cog-board")
    ideation_id = _id("ref-cog-idea")
    refinement_id = _id("ref-cog")
    await _persist(
        db_factory,
        _board(board_id),
        Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Parent ideation",
            status=IdeationStatus.DONE,
            created_by=USER_ID,
        ),
        Refinement(
            id=refinement_id,
            board_id=board_id,
            ideation_id=ideation_id,
            title="Cognitively blocked refinement",
            status=RefinementStatus.APPROVED,
            in_scope=["real scope"],
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        resource_gate = ResourceGateService(db)
        for resource_type in ("architecture", "mockup", "knowledge_base"):
            await resource_gate.mark_not_applicable(
                board_id,
                "refinement",
                refinement_id,
                resource_type,
                USER_ID,
                justification=(
                    f"{resource_type} is not applicable to this cognitive "
                    "preview regression."
                ),
                source_channel="ui",
            )
        await db.commit()
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="refinement",
            entity_id=refinement_id,
            to_status="done",
            configure_services=lambda services: _install_cognitive_block(
                services, "refinements"
            ),
        )

    assert "cognitive_consolidation_pending" in (done.blocked_reason or "")


@pytest.mark.asyncio
async def test_ideation_done_preview_does_not_apply_unsupported_cognitive_gate(
    db_factory,
) -> None:
    board_id = _id("idea-cog-board")
    ideation_id = _id("idea-cog")
    await _persist(
        db_factory,
        _board(board_id),
        Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Cognitively blocked ideation",
            status=IdeationStatus.EVALUATING,
            created_by=USER_ID,
        ),
    )

    cognitive_gate = AsyncMock(
        side_effect=AssertionError("ideations are not cognitive-closeout entities")
    )

    def configure_services(services) -> None:
        # A legacy adapter may still expose this attribute. The ideation
        # transition must not call it because the cognitive contract excludes
        # ideations.
        services.ideations._validate_cognitive_done = cognitive_gate
        services.resource_gate.validate_or_raise_entity_completion = AsyncMock(
            return_value=None
        )

    async with db_factory() as db:
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="ideation",
            entity_id=ideation_id,
            to_status="done",
            configure_services=configure_services,
        )

    assert done.blocked_reason is None
    cognitive_gate.assert_not_awaited()


@pytest.mark.asyncio
async def test_ideation_evaluating_done_bypasses_cognitive_closeout_and_derives_refinement(
    db_factory,
    monkeypatch,
) -> None:
    """Regression for the published ideation→cognitive-closeout routing bug."""

    assert "ideation" not in ELIGIBLE_CLOSEOUT_ENTITY_TYPES
    board_id = _id("idea-done-board")
    ideation_id = _id("idea-done")
    cognitive_closeout = Mock(
        side_effect=AssertionError(
            "ideation completion must not enter cognitive closeout"
        )
    )
    monkeypatch.setattr(
        main_service,
        "_evaluate_cognitive_closeout_or_raise",
        cognitive_closeout,
    )

    async with db_factory() as db:
        db.add(_board(board_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Ideation completing without cognitive closeout",
                description="Immutable parent context for refinement derivation.",
                status=IdeationStatus.EVALUATING,
                created_by=USER_ID,
            )
        )
        await db.flush()

        resource_gate = ResourceGateService(db)
        for resource_type in ("architecture", "mockup", "knowledge_base"):
            await resource_gate.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                resource_type,
                USER_ID,
                justification=f"{resource_type} is not applicable to this regression.",
                source_channel="ui",
            )

        ideation_service = IdeationService(db)
        moved = await ideation_service.move_ideation(
            ideation_id,
            USER_ID,
            IdeationMove(status=IdeationStatus.DONE),
        )
        assert moved is not None
        assert moved.status is IdeationStatus.DONE

        snapshots = await ideation_service.list_snapshots(ideation_id)
        assert len(snapshots) == 1
        assert snapshots[0].version == moved.version
        assert snapshots[0].title == moved.title

        refinement = await RefinementService(db).create_refinement(
            ideation_id,
            USER_ID,
            RefinementCreate(
                ideation_id=ideation_id,
                title="Derived after ideation completion",
            ),
            skip_ownership_check=True,
        )
        assert refinement is not None
        refinement_id = refinement.id
        assert refinement.ideation_id == ideation_id
        assert refinement.status is RefinementStatus.DRAFT
        await db.commit()

    cognitive_closeout.assert_not_called()
    async with db_factory() as db:
        persisted = await IdeationService(db).get_ideation(ideation_id)
        persisted_refinement = await RefinementService(db).get_refinement(
            refinement_id
        )
        snapshots = await IdeationService(db).list_snapshots(ideation_id)

    assert persisted is not None
    assert persisted.status is IdeationStatus.DONE
    assert persisted_refinement is not None
    assert persisted_refinement.ideation_id == ideation_id
    assert len(snapshots) == 1


@pytest.mark.asyncio
async def test_card_cognitive_preview_reads_same_graph_health_as_mutation(
    db_factory,
    monkeypatch,
) -> None:
    board_id = _id("card-health-board")
    spec_id = _id("card-health-spec")
    card_id = _id("card-health")
    await _persist(
        db_factory,
        _board(board_id, settings={"require_task_validation": False}),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Card health spec",
            status=SpecStatus.IN_PROGRESS,
            test_scenarios=[],
            created_by=USER_ID,
        ),
        Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Card health parity",
            status=CardStatus.IN_PROGRESS,
            card_type=CardType.NORMAL,
            position=0,
            created_by=USER_ID,
        ),
    )
    probe = AsyncMock(return_value="healthy")
    monkeypatch.setattr(main_service, "_resolve_closeout_graph_state", probe)
    observed: dict = {}

    class AllowingGate:
        def evaluate(self, **kwargs):
            observed.update(kwargs)
            return SimpleNamespace(allowed=True, blocking_count=0)

    def configure_services(services) -> None:
        services.cards._cognitive_closeout_gate_factory = AllowingGate
        services.resource_gate.validate_or_raise_entity_completion = AsyncMock(
            return_value=None
        )

    async with db_factory() as db:
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="card",
            entity_id=card_id,
            to_status="done",
            configure_services=configure_services,
        )

    assert done.blocked_reason is None
    probe.assert_awaited_once_with(board_id, ANY)
    assert observed["graph_state"] == "healthy"


@pytest.mark.asyncio
async def test_test_card_done_preview_requires_authenticated_scenario_evidence(
    db_factory,
) -> None:
    board_id = _id("test-evidence-board")
    spec_id = _id("test-evidence-spec")
    card_id = _id("test-evidence-card")
    scenario_id = _id("test-evidence-scenario")
    await _persist(
        db_factory,
        _board(
            board_id,
            settings={
                "require_task_validation": False,
                "skip_test_coverage_global": False,
            },
        ),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Evidence spec",
            status=SpecStatus.IN_PROGRESS,
            acceptance_criteria=[],
            test_scenarios=[
                {
                    "id": scenario_id,
                    "title": "Passed without authenticated evidence",
                    "status": "passed",
                }
            ],
            created_by=USER_ID,
        ),
        Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Test card without evidence",
            status=CardStatus.IN_PROGRESS,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            position=0,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="card",
            entity_id=card_id,
            to_status="done",
        )

    assert done.blocked_reason is not None
    assert (
        "evidence" in done.blocked_reason.lower()
        or "test_scenarios_pending" in done.blocked_reason
    )


@pytest.mark.asyncio
async def test_bug_execution_preview_blocks_missing_linked_test_task_id(
    db_factory,
) -> None:
    board_id = _id("bug-missing-link-board")
    spec_id = _id("bug-missing-link-spec")
    bug_id = _id("bug-missing-link")
    missing_test_id = _id("missing-test")
    await _persist(
        db_factory,
        _board(board_id, settings={"require_test_task_for_bug": True}),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Bug spec",
            status=SpecStatus.IN_PROGRESS,
            test_scenarios=[],
            created_by=USER_ID,
        ),
        Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug with a dangling test link",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            linked_test_task_ids=[missing_test_id],
            position=0,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        started = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="card",
            entity_id=bug_id,
            to_status="in_progress",
        )

    assert started.blocked_reason is not None
    assert "regression_test_not_found" in started.blocked_reason


@pytest.mark.asyncio
async def test_bug_execution_preview_applies_deep_regression_gate(
    db_factory,
) -> None:
    board_id = _id("bug-deep-board")
    spec_id = _id("bug-deep-spec")
    bug_id = _id("bug-deep")
    test_id = _id("bug-deep-test")
    await _persist(
        db_factory,
        _board(board_id, settings={"require_test_task_for_bug": True}),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Deep-gate bug spec",
            status=SpecStatus.IN_PROGRESS,
            test_scenarios=[],
            created_by=USER_ID,
        ),
        Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug with structurally invalid regression task",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            linked_test_task_ids=[test_id],
            position=0,
            created_by=USER_ID,
        ),
        Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Regression task without a scenario",
            status=CardStatus.DONE,
            card_type=CardType.TEST,
            test_scenario_ids=[],
            position=1,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        started = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="card",
            entity_id=bug_id,
            to_status="in_progress",
        )

    assert started.blocked_reason is not None
    assert "test_scenario" in started.blocked_reason.lower()


@pytest.mark.asyncio
async def test_card_done_preview_uses_canonical_cognitive_gate(db_factory) -> None:
    board_id = _id("card-cog-board")
    card_id = _id("card-cog")
    await _persist(
        db_factory,
        _board(board_id, settings={"require_task_validation": False}),
        Card(
            id=card_id,
            board_id=board_id,
            title="Cognitively blocked card",
            status=CardStatus.VALIDATION,
            card_type=CardType.NORMAL,
            position=0,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        done = await _preview_transition(
            db,
            board_id=board_id,
            entity_type="card",
            entity_id=card_id,
            to_status="done",
            configure_services=lambda services: _install_cognitive_block(
                services, "cards"
            ),
        )

    assert "cognitive_consolidation_pending" in (done.blocked_reason or "")


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", [SpecStatus.DONE, SpecStatus.CANCELLED])
async def test_spec_terminal_to_draft_starts_new_version_and_clears_cancellation(
    db_factory,
    terminal_status: SpecStatus,
) -> None:
    board_id = _id(f"spec-reopen-{terminal_status.value}-board")
    spec_id = _id(f"spec-reopen-{terminal_status.value}")
    cancelled_at = datetime.now(timezone.utc)
    is_cancelled = terminal_status == SpecStatus.CANCELLED
    await _persist(
        db_factory,
        _board(board_id),
        Spec(
            id=spec_id,
            board_id=board_id,
            title=f"Reopen {terminal_status.value} spec",
            status=terminal_status,
            version=7,
            cancellation_reason="Superseded delivery" if is_cancelled else None,
            cancelled_at=cancelled_at if is_cancelled else None,
            cancelled_by=USER_ID if is_cancelled else None,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        reopened = await SpecService(db).move_spec(
            spec_id,
            USER_ID,
            SpecMove(status=SpecStatus.DRAFT),
            actor_name=USER_ID,
        )

    assert reopened is not None
    assert reopened.status == SpecStatus.DRAFT
    assert reopened.version == 8
    assert reopened.cancellation_reason is None
    assert reopened.cancelled_at is None
    assert reopened.cancelled_by is None
