"""Regression coverage for bug-card test gates on already validated specs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)
from sqlalchemy_test_models import (
    AmendmentHotfixRevision,
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    DomainEventRow,
    Spec,
    SpecStatus,
    Sprint,
    SprintLaneType,
    SprintStatus,
)
from okto_pulse.core.models.schemas import CardMove, SprintCreate, SprintMove
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewService,
)
from okto_pulse.core.services.bug_regression_observability import (
    BUG_REGRESSION_DECISION_EVENT_TYPE,
    METRIC_SEMANTIC_GAP_TOTAL,
    METRIC_UNRELATED_REJECTED_TOTAL,
    assert_bug_regression_payload_is_safe,
    get_bug_regression_metric_samples,
    reset_bug_regression_observability_for_tests,
)
from okto_pulse.core.services.main import CardOperationError, CardService, SprintService


pytestmark = pytest.mark.asyncio

USER_ID = "bug-regression-agent"


async def test_bug_gate_allows_existing_scenario_with_new_test_card():
    """A locked spec can reuse an existing scenario if the test card is new."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"bug-gate-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bug-gate-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-existing-regression"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Bug Gate Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Validated regression spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Existing regression scenario",
                "linked_criteria": [0],
                "linked_task_ids": [origin_id],
                "status": "passed",
                "evidence": {
                    "last_run_at": "2026-05-01T12:00:00Z",
                    "output_snippet": "passed",
                },
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug needing regression",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Regression test created after bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        moved = await CardService(db).move_card(
            bug_id,
            USER_ID,
            CardMove(status=CardStatus.IN_PROGRESS),
        )

    assert moved is not None
    assert moved.status == CardStatus.IN_PROGRESS


async def test_active_hotfix_lane_allows_bug_with_new_regression_test_task():
    """Active hotfix lane satisfies sprint ownership while bug gate still passes normally."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"hotfix-gate-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"hotfix-gate-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    original_sprint_id = f"normal-sprint-{uuid.uuid4().hex[:8]}"
    hotfix_sprint_id = f"hotfix-sprint-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-hotfix-regression"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Hotfix Gate Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Done hotfix spec",
            status=SpecStatus.DONE,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Existing hotfix regression scenario",
                "linked_criteria": [0],
                "linked_task_ids": [origin_id],
                "status": "passed",
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Sprint(
            id=original_sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Original closed sprint",
            status=SprintStatus.CLOSED,
            lane_type=SprintLaneType.NORMAL,
            created_by=USER_ID,
        ))
        db.add(Sprint(
            id=hotfix_sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Active hotfix lane",
            status=SprintStatus.ACTIVE,
            lane_type=SprintLaneType.HOTFIX,
            origin_sprint_id=original_sprint_id,
            created_by=USER_ID,
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=original_sprint_id,
            title="Origin implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=hotfix_sprint_id,
            title="Hotfix bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=hotfix_sprint_id,
            title="Hotfix regression test",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        moved = await CardService(db).move_card(
            bug_id,
            USER_ID,
            CardMove(status=CardStatus.IN_PROGRESS),
        )
        original_sprint = await db.get(Sprint, original_sprint_id)

    assert moved is not None
    assert moved.status == CardStatus.IN_PROGRESS
    assert moved.sprint_id == hotfix_sprint_id
    assert moved.linked_test_task_ids == [test_id]
    assert original_sprint.status == SprintStatus.CLOSED


async def test_active_hotfix_lane_does_not_bypass_missing_bug_test_task():
    """Active hotfix lane is not a bypass for the bug regression test gate."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"hotfix-no-bypass-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"hotfix-no-bypass-spec-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    hotfix_sprint_id = f"hotfix-sprint-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Hotfix No Bypass Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Done hotfix spec",
            status=SpecStatus.DONE,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Sprint(
            id=hotfix_sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Active hotfix lane",
            status=SprintStatus.ACTIVE,
            lane_type=SprintLaneType.HOTFIX,
            created_by=USER_ID,
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=hotfix_sprint_id,
            title="Hotfix bug without test",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[],
            created_by=USER_ID,
            created_at=now,
        ))
        await db.flush()

        with pytest.raises(ValueError) as exc:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )

    message = str(exc.value)
    assert "requires at least 1 new test task" in message
    assert "assign_hotfix_lane" not in message
    assert "activate_hotfix_lane" not in message
    assert "sprint_not_active" not in message
    payload = exc.value.to_dict()
    assert payload["code"] == "missing_regression_test_task"
    assert payload["next_action"] == "create_regression_test_card"
    assert payload["remediation_path"] == "path_a_reuse_existing_scenario"
    assert payload["semantic_gap_required"] is False
    assert payload["hotfix_lane_status"] == "not_applicable"
    assert payload["actions"][0]["primary"] is True


async def test_bug_gate_rejects_same_spec_unrelated_scenario():
    """Same-spec membership alone cannot satisfy the regression gate."""
    from okto_pulse.core.infra.database import get_session_factory

    reset_bug_regression_observability_for_tests()
    factory = get_session_factory()
    board_id = f"bug-unrelated-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bug-unrelated-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-unrelated-regression"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Bug Unrelated Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Validated unrelated spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Unrelated existing scenario",
                "linked_criteria": [0],
                "linked_task_ids": [],
                "status": "passed",
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin implementation without this scenario",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug trying unrelated scenario",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds without SECRET-EXPECTED",
            observed_behavior="request fails with SECRET-OBSERVED",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Regression test using unrelated scenario",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        with pytest.raises(ValueError) as exc:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
        bug = await db.get(Card, bug_id)
        events = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == BUG_REGRESSION_DECISION_EVENT_TYPE,
                )
            )
        ).scalars().all()

    message = str(exc.value)
    assert "unrelated_scenario" in message
    assert "semantic_gap_required=true" in message
    assert "next_action=escalate_semantic_gap" in message
    error_payload = exc.value.to_dict()
    assert error_payload["code"] == "block_semantic_gap"
    assert error_payload["reason_code"] == "unrelated_scenario"
    assert error_payload["next_action"] == "escalate_semantic_gap"
    assert error_payload["remediation_path"] == "path_b_semantic_gap"
    assert error_payload["semantic_gap_required"] is True
    assert bug.status == CardStatus.NOT_STARTED
    assert len(events) == 1
    payload = events[0].payload_json
    assert payload["bug_id"] == bug_id
    assert payload["spec_id"] == spec_id
    assert payload["decision"] == "semantic_gap"
    assert payload["reason_code"] == "unrelated_scenario"
    assert payload["scenario_count"] == 1
    assert payload["test_task_count"] == 1
    assert_bug_regression_payload_is_safe(payload)

    samples = get_bug_regression_metric_samples()
    metric_names = {sample["metric_name"] for sample in samples}
    assert METRIC_UNRELATED_REJECTED_TOTAL in metric_names
    assert METRIC_SEMANTIC_GAP_TOTAL in metric_names
    for sample in samples:
        assert_bug_regression_payload_is_safe(sample["labels"])
    reset_bug_regression_observability_for_tests()


async def test_bug_gate_rejects_cross_spec_scenario_reference():
    """A test task cannot satisfy a bug by referencing another spec's scenario."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"bug-cross-spec-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bug-cross-spec-{uuid.uuid4().hex[:8]}"
    other_spec_id = f"other-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    foreign_scenario_id = "ts-foreign-regression"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Bug Cross Spec Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Bug spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Spec(
            id=other_spec_id,
            board_id=board_id,
            title="Other spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": foreign_scenario_id,
                "title": "Foreign scenario",
                "linked_criteria": [0],
                "status": "passed",
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug trying foreign scenario",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Regression test using foreign scenario",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[foreign_scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        with pytest.raises(ValueError) as exc:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
        bug = await db.get(Card, bug_id)

    message = str(exc.value)
    # Path B (card ead17e4d): cross-spec evidence with no formal amendment is
    # fail-closed via the shared predicate with the precise reason. Still blocked
    # (bug stays not_started); the foreign spec id is surfaced via the rejected
    # detail.
    assert "missing_amendment_revision" in message
    assert other_spec_id in message
    assert bug.status == CardStatus.NOT_STARTED


async def test_bug_gate_rejects_pre_bug_test_card_even_when_scenario_eligible():
    """Fresh regression evidence is still required after eligibility passes."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"bug-stale-test-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bug-stale-test-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-stale-but-eligible"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Bug Stale Test Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Validated stale test spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Eligible regression scenario",
                "linked_criteria": [0],
                "linked_task_ids": [origin_id],
                "status": "passed",
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=10),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug with stale test card",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Regression test created too early",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now - timedelta(seconds=1),
        ))
        await db.flush()

        with pytest.raises(ValueError) as exc:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
        bug = await db.get(Card, bug_id)

    assert "created before this bug card" in str(exc.value)
    assert bug.status == CardStatus.NOT_STARTED


async def test_bug_gate_path_a_preserves_locked_spec_canonical_content():
    """Path A can move the bug without unlocking or mutating canonical scenario fields."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"bug-preserve-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bug-preserve-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-preserve-canonical"
    now = datetime.now(timezone.utc)

    scenario = {
        "id": scenario_id,
        "title": "Preserve canonical regression scenario",
        "linked_criteria": [0],
        "given": "a locked spec",
        "when": "a post-bug regression test is linked",
        "then": "canonical scenario content is preserved",
        "linked_task_ids": [origin_id],
        "status": "passed",
    }

    async with factory() as db:
        db.add(Board(id=board_id, name="Bug Preserve Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Validated locked spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[dict(scenario)],
            business_rules=[],
            api_contracts=[],
            validations=[{"id": "val-preserve", "outcome": "success"}],
            current_validation_id="val-preserve",
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug with eligible scenario",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Fresh post-bug regression test",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        moved = await CardService(db).move_card(
            bug_id,
            USER_ID,
            CardMove(status=CardStatus.IN_PROGRESS),
        )
        spec = await db.get(Spec, spec_id)

    assert moved.status == CardStatus.IN_PROGRESS
    assert spec.status == SpecStatus.IN_PROGRESS
    assert spec.current_validation_id == "val-preserve"
    assert spec.test_scenarios == [scenario]


@pytest.mark.parametrize(
    "lane_status",
    [
        SprintStatus.DRAFT,
        SprintStatus.REVIEW,
        SprintStatus.CLOSED,
        SprintStatus.CANCELLED,
    ],
    ids=["draft", "review", "closed", "cancelled"],
)
async def test_inactive_hotfix_lane_blocks_with_activate_hotfix_remediation(
    lane_status: SprintStatus,
):
    """Inactive hotfix lane blocks before the bug gate and exposes remediation facts."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"hotfix-inactive-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"hotfix-inactive-spec-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    hotfix_sprint_id = f"hotfix-sprint-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-inactive-hotfix"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Inactive Hotfix Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Done hotfix spec",
            status=SpecStatus.DONE,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Existing regression scenario",
                "linked_criteria": [0],
                "linked_task_ids": [test_id],
                "status": "passed",
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Sprint(
            id=hotfix_sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title=f"{lane_status.value.title()} hotfix lane",
            status=lane_status,
            lane_type=SprintLaneType.HOTFIX,
            created_by=USER_ID,
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=hotfix_sprint_id,
            title="Hotfix bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=hotfix_sprint_id,
            title="Regression test",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )

    assert exc.value.code == "sprint_not_active"
    assert exc.value.remediation == "activate_hotfix_lane"
    assert exc.value.facts["lane_type"] == "hotfix"
    assert exc.value.facts["sprint_status"] == lane_status.value
    assert exc.value.facts["next_action"] == "activate_hotfix_lane"


async def test_done_spec_bug_without_lane_reports_assign_hotfix_lane():
    """A post-closure bug without sprint assignment points to hotfix lane assignment."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"hotfix-missing-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"hotfix-missing-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    original_sprint_id = f"normal-sprint-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Missing Hotfix Lane Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Done hotfix spec",
            status=SpecStatus.DONE,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Sprint(
            id=original_sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Original closed sprint",
            status=SprintStatus.CLOSED,
            lane_type=SprintLaneType.NORMAL,
            created_by=USER_ID,
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=original_sprint_id,
            title="Original delivered task",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=None,
            title="Unassigned post-closure bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[],
            created_by=USER_ID,
            created_at=now,
        ))
        await db.flush()

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
        original_sprint = await db.get(Sprint, original_sprint_id)

    assert exc.value.code == "sprint_required"
    assert exc.value.remediation == "assign_hotfix_lane"
    assert exc.value.facts["lane_type"] == "hotfix"
    assert exc.value.facts["next_action"] == "assign_hotfix_lane"
    assert original_sprint.status == SprintStatus.CLOSED
    assert "reopen" not in str(exc.value).lower()


async def test_post_closure_bug_uses_hotfix_lane_without_reopening_history():
    """Closed sprint block is remediated by an active hotfix lane, not by reopening."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"hotfix-flow-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"hotfix-flow-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    original_sprint_id = f"normal-sprint-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-post-closure-hotfix-flow"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Post-Closure Hotfix Flow", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Done hotfix flow spec",
            status=SpecStatus.DONE,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Existing post-closure regression scenario",
                "linked_criteria": [0],
                "linked_task_ids": [origin_id],
                "status": "passed",
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Sprint(
            id=original_sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Original closed delivery sprint",
            status=SprintStatus.CLOSED,
            lane_type=SprintLaneType.NORMAL,
            created_by=USER_ID,
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=original_sprint_id,
            title="Delivered implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=10),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=original_sprint_id,
            title="Post-closure bug initially on original sprint",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=original_sprint_id,
            title="Post-closure regression test",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        card_service = CardService(db)
        sprint_service = SprintService(db)

        with pytest.raises(CardOperationError) as blocked:
            await card_service.move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
        assert blocked.value.code == "sprint_not_active"
        assert blocked.value.remediation == "assign_hotfix_lane"
        assert blocked.value.facts["sprint_status"] == SprintStatus.CLOSED.value
        assert blocked.value.facts["lane_type"] == SprintLaneType.NORMAL.value
        assert "reopen" not in str(blocked.value).lower()

        hotfix = await sprint_service.create_sprint(
            board_id,
            USER_ID,
            SprintCreate(
                title="Active post-closure hotfix lane",
                spec_id=spec_id,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=original_sprint_id,
                origin_bug_id=bug_id,
            ),
        )
        assert hotfix is not None
        assert hotfix.status == SprintStatus.DRAFT
        assert hotfix.lane_type == SprintLaneType.HOTFIX
        assert hotfix.origin_sprint_id == original_sprint_id
        assert hotfix.origin_bug_id == bug_id
        assert hotfix.normal_sprint_created is False

        assigned = await sprint_service.assign_tasks(hotfix.id, [bug_id, test_id], USER_ID)
        assert assigned == 2
        activated = await sprint_service.move_sprint(
            hotfix.id,
            USER_ID,
            SprintMove(status=SprintStatus.ACTIVE),
        )
        assert activated.status == SprintStatus.ACTIVE

        moved = await card_service.move_card(
            bug_id,
            USER_ID,
            CardMove(status=CardStatus.IN_PROGRESS),
        )
        original_sprint = await db.get(Sprint, original_sprint_id)
        bug = await db.get(Card, bug_id)
        regression = await db.get(Card, test_id)

    assert moved is not None
    assert moved.status == CardStatus.IN_PROGRESS
    assert moved.sprint_id == hotfix.id
    assert bug.sprint_id == hotfix.id
    assert regression.sprint_id == hotfix.id
    assert original_sprint.status == SprintStatus.CLOSED


# ---------------------------------------------------------------------------
# Path B integration (spec f5a7cae7 / card ead17e4d): the gate loads amendment
# lineage facts and routes cross-spec evidence through the shared predicate.
# ---------------------------------------------------------------------------


async def _seed_path_b_board(db, *, amendment_kwargs):
    """Seed a bug whose only regression evidence is a CROSS-SPEC scenario, plus
    an AmendmentHotfixRevision built from amendment_kwargs. Returns the ids."""
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"pb-board-{suffix}",
        "spec": f"pb-spec-{suffix}",
        "other_spec": f"pb-other-{suffix}",
        "origin": f"pb-origin-{suffix}",
        "bug": f"pb-bug-{suffix}",
        "test": f"pb-test-{suffix}",
        "amendment": f"pb-amd-{suffix}",
        "foreign_scenario": "ts-foreign-pathb",
    }
    now = datetime.now(timezone.utc)
    db.add(Board(id=ids["board"], name="Path B Board", owner_id=USER_ID))
    db.add(Spec(
        id=ids["spec"], board_id=ids["board"], title="Bug spec",
        status=SpecStatus.IN_PROGRESS, created_by=USER_ID,
        functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[], business_rules=[], api_contracts=[],
    ))
    db.add(Spec(
        id=ids["other_spec"], board_id=ids["board"], title="Other spec",
        status=SpecStatus.IN_PROGRESS, created_by=USER_ID,
        functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[{
            "id": ids["foreign_scenario"], "title": "Foreign scenario",
            "linked_criteria": [0], "status": "passed",
        }],
        business_rules=[], api_contracts=[],
    ))
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"],
        title="Origin", status=CardStatus.DONE, card_type=CardType.NORMAL,
        created_by=USER_ID, created_at=now - timedelta(minutes=5),
    ))
    db.add(Card(
        id=ids["bug"], board_id=ids["board"], spec_id=ids["spec"],
        title="Bug needing cross-spec evidence", status=CardStatus.NOT_STARTED,
        card_type=CardType.BUG, origin_task_id=ids["origin"],
        severity=BugSeverity.MAJOR, expected_behavior="ok", observed_behavior="bad",
        linked_test_task_ids=[ids["test"]], created_by=USER_ID, created_at=now,
    ))
    db.add(Card(
        id=ids["test"], board_id=ids["board"], spec_id=ids["spec"],
        title="Regression test using foreign scenario", status=CardStatus.NOT_STARTED,
        card_type=CardType.TEST, test_scenario_ids=[ids["foreign_scenario"]],
        created_by=USER_ID, created_at=now + timedelta(seconds=1),
    ))
    base_amendment = dict(
        id=ids["amendment"], board_id=ids["board"],
        original_spec_id=ids["spec"], origin_bug_id=ids["bug"],
        status=AmendmentRevisionStatus.DONE,
        lineage_state=AmendmentLineageState.COMPLETE,
        origin_task_ids=[ids["origin"]], affected_task_ids=[],
        regression_scenario_ids=[ids["foreign_scenario"]],
        regression_test_task_ids=[ids["test"]], automated_regression_refs=[],
        created_by=USER_ID,
    )
    base_amendment.update(amendment_kwargs)
    db.add(AmendmentHotfixRevision(**base_amendment))
    await db.flush()
    return ids


async def test_bug_gate_path_b_full_lineage_blocks_coverage_pending_and_preview_agrees():
    # A fully lineage-eligible Path B amendment (done + complete + declares the
    # artifact + authoritative task membership) is NOT closure-ready in
    # production: coverage_confirmed is hardcoded False (ADJ-B), so the gate
    # blocks coverage_pending. The preview must agree (ADJ-D parity).
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        ids = await _seed_path_b_board(db, amendment_kwargs={})

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).move_card(
                ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
            )
        bug = await db.get(Card, ids["bug"])

        # gate blocked, fail-closed on coverage (NOT a hard reject, NOT allowed).
        message = str(exc.value)
        assert "coverage_pending" in message
        assert bug.status == CardStatus.NOT_STARTED

        # ADJ-D: the preview uses the same predicate + facts and agrees.
        preview = await BugRegressionScenarioPreviewService(db).resolve(
            board_id=ids["board"],
            bug_id=ids["bug"],
            candidate_scenario_ids=[ids["foreign_scenario"]],
        )
    assert preview["coverage_state"] == "coverage_pending"
    assert preview["amendment_revision_id"] is not None
    assert ids["foreign_scenario"] in preview["coverage_pending_scenarios"]


async def test_bug_gate_path_b_draft_amendment_blocks_blocked_status():
    # The TS1 (ts_cc824ace) behaviour at the gate: a draft amendment with
    # complete lineage stays blocked (blocked_amendment_status), fail-closed.
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        ids = await _seed_path_b_board(
            db, amendment_kwargs={"status": AmendmentRevisionStatus.DRAFT}
        )

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).move_card(
                ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
            )
        bug = await db.get(Card, ids["bug"])

    assert "blocked_amendment_status" in str(exc.value)
    assert bug.status == CardStatus.NOT_STARTED


# ---------------------------------------------------------------------------
# G2 coverage_confirmed signal (card c9cf9781): validator-only writer + the
# non-forgeable persisted attestation that flips the gate to ALLOW.
# ---------------------------------------------------------------------------


async def _make_artifact_ready(db, ids):
    """Mark the regression test task DONE and its declared scenario
    passed/automated with SPEC3 reexecutable evidence (the NECESSARY
    precondition for confirm_amendment_coverage)."""
    test_task = await db.get(Card, ids["test"])
    test_task.status = CardStatus.DONE
    other_spec = await db.get(Spec, ids["other_spec"])
    scenarios = list(other_spec.test_scenarios or [])
    for sc in scenarios:
        if sc.get("id") == ids["foreign_scenario"]:
            sc["status"] = "automated"
            sc["evidence"] = {
                "test_file_path": "tests/test_reg.py",
                "test_function": "test_reg_case",
            }
    other_spec.test_scenarios = scenarios
    flag_modified(other_spec, "test_scenarios")
    await db.flush()


async def test_confirm_amendment_coverage_enables_gate_allow():
    # End-to-end G2: a bound validator attestation written via the validator-only
    # writer flips the bug gate from coverage_pending (BLOCK) to path_b_ready
    # (ALLOW); the preview agrees (ADJ-D parity).
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        ids = await _seed_path_b_board(db, amendment_kwargs={})
        await _make_artifact_ready(db, ids)

        confirmation = await CardService(db).confirm_amendment_coverage(
            amendment_id=ids["amendment"],
            regression_test_task_id=ids["test"],
            regression_scenario_id=ids["foreign_scenario"],
            reviewer_id=USER_ID,
            reviewer_name=USER_ID,
        )
        assert confirmation["validator_id"] == USER_ID
        assert confirmation["amendment_revision_id"] == ids["amendment"]
        assert confirmation["evidence_ref"] == "tests/test_reg.py::test_reg_case"

        # gate now ALLOWS the bug move (path_b_ready).
        moved = await CardService(db).move_card(
            ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
        )
        assert moved.status == CardStatus.IN_PROGRESS

        preview = await BugRegressionScenarioPreviewService(db).resolve(
            board_id=ids["board"],
            bug_id=ids["bug"],
            candidate_scenario_ids=[ids["foreign_scenario"]],
        )
    assert preview["coverage_state"] == "path_b_ready"


async def test_confirm_rejects_foreign_artifact_binding():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        ids = await _seed_path_b_board(db, amendment_kwargs={})
        await _make_artifact_ready(db, ids)
        # a test task NOT declared by this amendment cannot be confirmed.
        with pytest.raises(CardOperationError) as exc:
            await CardService(db).confirm_amendment_coverage(
                amendment_id=ids["amendment"],
                regression_test_task_id="not-declared-tc",
                regression_scenario_id=ids["foreign_scenario"],
                reviewer_id=USER_ID,
                reviewer_name=USER_ID,
            )
    assert exc.value.code == "coverage_binding_invalid"


async def test_confirm_rejects_unmet_preconditions():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        ids = await _seed_path_b_board(db, amendment_kwargs={})
        # test task still not_started -> precondition unmet.
        with pytest.raises(CardOperationError) as exc:
            await CardService(db).confirm_amendment_coverage(
                amendment_id=ids["amendment"],
                regression_test_task_id=ids["test"],
                regression_scenario_id=ids["foreign_scenario"],
                reviewer_id=USER_ID,
                reviewer_name=USER_ID,
            )
        assert exc.value.code == "coverage_precondition_unmet"

        # test task done BUT scenario has no reexecutable evidence -> still unmet
        # (lineage/status alone is NOT sufficient — G2).
        test_task = await db.get(Card, ids["test"])
        test_task.status = CardStatus.DONE
        await db.flush()
        with pytest.raises(CardOperationError) as exc2:
            await CardService(db).confirm_amendment_coverage(
                amendment_id=ids["amendment"],
                regression_test_task_id=ids["test"],
                regression_scenario_id=ids["foreign_scenario"],
                reviewer_id=USER_ID,
                reviewer_name=USER_ID,
            )
    assert exc2.value.code == "coverage_precondition_unmet"


async def test_create_strips_reserved_coverage_confirmation_key():
    # NON-FORGEABILITY: a generic create can never inject the coverage attestation
    # (it is stripped); only confirm_amendment_coverage may write it. Other
    # validation_metadata is preserved.
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    forged = {
        "validator_id": "attacker",
        "amendment_revision_id": "whatever",
        "regression_test_task_id": "tc",
        "regression_scenario_id": "ts",
        "evidence_ref": "x::y",
    }
    async with factory() as db:
        amendment = await AmendmentRevisionService(db).create(
            board_id="pb-forge-board",
            original_spec_id="pb-forge-spec",
            origin_bug_id="pb-forge-bug",
            author=USER_ID,
            validation_metadata={"coverage_confirmation": forged, "keep": "ok"},
        )
        await db.flush()
        metadata = amendment.validation_metadata or {}
    assert "coverage_confirmation" not in metadata
    assert metadata.get("keep") == "ok"


async def test_hotfix_lane_does_not_bypass_cross_spec_without_amendment():
    # ts_9a56cf73 (AC1): a cross-spec regression test task in an ACTIVE HOTFIX
    # lane, with NO amendment lineage, is still fail-closed — the hotfix lane is
    # NOT a Path B bypass. The gate blocks with missing_amendment_revision and the
    # bug stays not_started.
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"hl-xspec-board-{suffix}",
        "spec": f"hl-xspec-spec-{suffix}",
        "other_spec": f"hl-xspec-other-{suffix}",
        "origin": f"hl-origin-{suffix}",
        "bug": f"hl-bug-{suffix}",
        "test": f"hl-test-{suffix}",
        "sprint": f"hl-sprint-{suffix}",
    }
    foreign_scenario_id = "ts-foreign-hotfix"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=ids["board"], name="Hotfix XSpec Board", owner_id=USER_ID))
        db.add(Spec(
            id=ids["spec"], board_id=ids["board"], title="Bug spec", status=SpecStatus.DONE,
            created_by=USER_ID, functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        db.add(Spec(
            id=ids["other_spec"], board_id=ids["board"], title="Other spec",
            status=SpecStatus.IN_PROGRESS, created_by=USER_ID,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": foreign_scenario_id, "title": "Foreign", "linked_criteria": [0], "status": "passed",
            }],
            business_rules=[], api_contracts=[],
        ))
        db.add(Sprint(
            id=ids["sprint"], board_id=ids["board"], spec_id=ids["spec"], title="Active hotfix lane",
            status=SprintStatus.ACTIVE, lane_type=SprintLaneType.HOTFIX, created_by=USER_ID,
        ))
        db.add(Card(
            id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
            status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=ids["bug"], board_id=ids["board"], spec_id=ids["spec"], sprint_id=ids["sprint"],
            title="Bug in hotfix lane w/ cross-spec task", status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG, origin_task_id=ids["origin"], severity=BugSeverity.MAJOR,
            expected_behavior="ok", observed_behavior="bad", linked_test_task_ids=[ids["test"]],
            created_by=USER_ID, created_at=now,
        ))
        db.add(Card(
            id=ids["test"], board_id=ids["board"], spec_id=ids["spec"], sprint_id=ids["sprint"],
            title="Cross-spec regression test in hotfix lane", status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST, test_scenario_ids=[foreign_scenario_id],
            created_by=USER_ID, created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).move_card(
                ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
            )
        bug = await db.get(Card, ids["bug"])

    assert "missing_amendment_revision" in str(exc.value)
    assert bug.status == CardStatus.NOT_STARTED
