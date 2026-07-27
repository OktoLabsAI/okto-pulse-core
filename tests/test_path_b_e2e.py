"""SPEC 473c900d / card 44f70d31 — E2E Path B regression with KG checkpoints.

Chains the WHOLE Path B lifecycle through the REAL services (amendment create/
associate/lifecycle, validator coverage confirmation, the bug-regression gate +
preview) on a deterministically seeded board, and at each stage asserts the
KG maturity rule for the amendment partition.

KG checkpoints here are the DETERMINISTIC source-maturity / rebuild rule
(``classify_source_for_kg`` + ``_expected_layers_from_sources`` +
``_verify_materialized_layers``) — the rule a rebuild WOULD apply — NOT a live
read of a board's LadybugDB graph (that is heavy/flaky and exercised elsewhere).

  TS1 (ts_64a2d5aa): complete Path B positive flow + KG checkpoints.
  TS2 (ts_38dc9e19): false Path B variants stay blocked, never closure-ready,
      and never leak to the canonical KG partition.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_path_b_e2e.py
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import uuid

import pytest
from sqlalchemy.orm.attributes import flag_modified

import okto_pulse.core.kg.rebuild_service as rebuild_service
from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)
from okto_pulse.core.kg.board_rebuild_adapter import _expected_layers_from_sources
from okto_pulse.core.kg.rebuild_service import (
    RebuildStepResult,
    _verify_materialized_layers,
)
from okto_pulse.core.kg.source_maturity import classify_source_for_kg
from sqlalchemy_test_models import (
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintLaneType,
    SprintStatus,
)
from okto_pulse.core.models.schemas import CardMove, SprintCreate, SprintMove
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiService
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewService,
)
from okto_pulse.core.services.main import (
    CardOperationError,
    CardService,
    SprintService,
)

pytestmark = pytest.mark.asyncio

USER_ID = "path-b-e2e-agent"
AMD = "amendment_hotfix_revision"


# ---------------------------------------------------------------------------
# Seeding + helpers
# ---------------------------------------------------------------------------


async def _seed_e2e(db, *, hotfix_lane: bool = False):
    """Seed an E2E Path B board: a bug on a DONE (locked) spec whose only
    regression evidence is a CROSS-SPEC scenario (on another spec) + a regression
    test card. No amendment yet — Path B starts from here. ``hotfix_lane=True``
    places the bug + test on an ACTIVE hotfix lane (Path C execution lane)."""
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"pbe2e-board-{suffix}",
        "spec": f"pbe2e-spec-{suffix}",
        "other_spec": f"pbe2e-other-{suffix}",
        "origin": f"pbe2e-origin-{suffix}",
        "bug": f"pbe2e-bug-{suffix}",
        "test": f"pbe2e-test-{suffix}",
        "sprint": f"pbe2e-sprint-{suffix}",
        "foreign_scenario": f"ts-foreign-{suffix}",
    }
    now = datetime.now(timezone.utc)
    sprint_id = ids["sprint"] if hotfix_lane else None

    db.add(Board(id=ids["board"], name="Path B E2E Board", owner_id=USER_ID))
    db.add(Spec(
        id=ids["spec"], board_id=ids["board"], title="Bug spec", status=SpecStatus.DONE,
        created_by=USER_ID, functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[], business_rules=[], api_contracts=[],
    ))
    db.add(Spec(
        id=ids["other_spec"], board_id=ids["board"], title="Other spec",
        status=SpecStatus.DONE, created_by=USER_ID,
        functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[{
            "id": ids["foreign_scenario"], "title": "Foreign scenario",
            "linked_criteria": [0], "status": "passed",
        }],
        business_rules=[], api_contracts=[],
    ))
    if hotfix_lane:
        db.add(Sprint(
            id=ids["sprint"], board_id=ids["board"], spec_id=ids["spec"],
            title="Active hotfix lane", status=SprintStatus.ACTIVE,
            lane_type=SprintLaneType.HOTFIX, created_by=USER_ID,
        ))
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
        status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
        created_at=now - timedelta(minutes=5),
    ))
    db.add(Card(
        id=ids["bug"], board_id=ids["board"], spec_id=ids["spec"], sprint_id=sprint_id,
        title="Bug needing cross-spec evidence", status=CardStatus.NOT_STARTED,
        card_type=CardType.BUG, origin_task_id=ids["origin"],
        severity=BugSeverity.MAJOR, expected_behavior="ok", observed_behavior="bad",
        linked_test_task_ids=[ids["test"]], created_by=USER_ID, created_at=now,
    ))
    db.add(Card(
        id=ids["test"], board_id=ids["board"], spec_id=ids["spec"], sprint_id=sprint_id,
        title="Regression test using foreign scenario", status=CardStatus.NOT_STARTED,
        card_type=CardType.TEST, test_scenario_ids=[ids["foreign_scenario"]],
        created_by=USER_ID, created_at=now + timedelta(seconds=1),
    ))
    await db.flush()
    return ids


def _amendment_layer(status: str, *, lineage_complete: bool) -> str:
    """KG maturity-rule checkpoint (the deterministic source-maturity rule a
    rebuild applies — NOT a live board-graph read). working before done+complete;
    canonical ONLY at done + complete lineage."""
    return classify_source_for_kg(
        artifact_type=AMD,
        artifact_status=status,
        content_hash="h",
        lineage_complete=lineage_complete,
    ).graph_layer


async def _coverage_state(db, ids) -> str:
    preview = await BugRegressionScenarioPreviewService(db).resolve(
        board_id=ids["board"], bug_id=ids["bug"],
        candidate_scenario_ids=[ids["foreign_scenario"]],
    )
    return preview["coverage_state"]


async def _assert_gate_blocks(db, ids, reason: str) -> str:
    """The bug-regression gate must block the bug move with ``reason`` and leave
    the bug not_started (never readiness/allow)."""
    with pytest.raises(CardOperationError) as exc:
        await CardService(db).move_card(
            ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
        )
    message = str(exc.value)
    assert reason in message, f"expected {reason!r} in gate error, got: {message}"
    assert "path_b_ready" not in message  # never closure-ready while blocked
    bug = await db.get(Card, ids["bug"])
    assert bug.status == CardStatus.NOT_STARTED
    return message


async def _make_artifact_ready(db, ids, *, with_evidence: bool = True):
    """Mark the regression test task DONE and its declared scenario automated.
    ``with_evidence=False`` omits the re-executable evidence fields (test_file_path
    / test_function) so the validator coverage precondition cannot be met."""
    test_task = await db.get(Card, ids["test"])
    test_task.status = CardStatus.DONE
    other_spec = await db.get(Spec, ids["other_spec"])
    scenarios = list(other_spec.test_scenarios or [])
    for sc in scenarios:
        if sc.get("id") == ids["foreign_scenario"]:
            sc["status"] = "automated"
            if with_evidence:
                sc["evidence"] = {
                    "test_file_path": "tests/test_reg.py",
                    "test_function": "test_reg_case",
                }
    other_spec.test_scenarios = scenarios
    flag_modified(other_spec, "test_scenarios")
    await db.flush()


# ---------------------------------------------------------------------------
# TS1 (ts_64a2d5aa) — complete Path B positive flow + KG checkpoints
# ---------------------------------------------------------------------------


async def test_ts_64a2d5aa_positive_path_b_lifecycle_with_kg_checkpoints(monkeypatch):
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_e2e(db)
        api = AmendmentRevisionApiService(db)
        lifecycle = AmendmentRevisionService(db)

        # Checkpoint 1 — pre-amendment: no amendment exists; the gate is
        # fail-closed and there is nothing to materialize into the KG.
        assert await _coverage_state(db, ids) == "not_applicable"
        await _assert_gate_blocks(db, ids, "missing_amendment_revision")

        # Stage 2 — create the draft amendment via the agent-facing API.
        # KG maturity rule: a draft amendment lives in the WORKING partition only
        # (never leaks to canonical). Gate still blocked (blocked_amendment_status).
        created = await api.create(
            board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            origin_task_ids=[ids["origin"]], regression_scenario_ids=[ids["foreign_scenario"]],
        )
        amendment_id = created["id"]
        assert created["status"] == "draft"
        assert _amendment_layer("draft", lineage_complete=False) == "working"
        await _assert_gate_blocks(db, ids, "blocked_amendment_status")

        # Stage 3 — associate the regression test task, complete lineage and
        # promote to done via the PUBLIC lifecycle. Lineage is now eligible but
        # coverage is UNCONFIRMED -> coverage_pending (blocking, NOT closure-ready).
        # KG maturity rule: done + complete -> CANONICAL partition.
        await api.associate(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id,
            actor=USER_ID, regression_test_task_ids=[ids["test"]],
        )
        await lifecycle.set_lineage_state(amendment_id, AmendmentLineageState.COMPLETE, USER_ID)
        await lifecycle.set_status(amendment_id, AmendmentRevisionStatus.DONE, USER_ID)
        assert _amendment_layer("done", lineage_complete=True) == "canonical"
        assert await _coverage_state(db, ids) == "coverage_pending"
        await _assert_gate_blocks(db, ids, "coverage_pending")

        # Stage 4 — the validator confirms coverage on re-executable evidence ->
        # path_b_ready; the gate now ALLOWS the bug to move to in_progress.
        await _make_artifact_ready(db, ids)
        confirmation = await CardService(db).confirm_amendment_coverage(
            amendment_id=amendment_id, regression_test_task_id=ids["test"],
            regression_scenario_id=ids["foreign_scenario"],
            reviewer_id=USER_ID, reviewer_name=USER_ID,
        )
        assert confirmation["amendment_revision_id"] == amendment_id
        assert confirmation["evidence_ref"] == "tests/test_reg.py::test_reg_case"

        await CardService(db).move_card(
            ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
        )
        bug = await db.get(Card, ids["bug"])
        assert bug.status == CardStatus.IN_PROGRESS
        assert await _coverage_state(db, ids) == "path_b_ready"

        # Checkpoint 5 — rebuild maturity guard (deterministic, NOT a live graph):
        # the done+complete amendment contributes to the expected CANONICAL
        # partition and materializing it there raises no MATERIALIZED_LAYER_MISMATCH.
        canonical_source = {
            "id": amendment_id, "artifact_type": AMD,
            "graph_layer": _amendment_layer("done", lineage_complete=True),
        }
        expected_layers = _expected_layers_from_sources([canonical_source])
        assert expected_layers == {"canonical": 1}
        monkeypatch.setattr(
            rebuild_service, "_materialized_layer_counts", lambda _b: {"canonical": 1}
        )
        guard = _verify_materialized_layers(
            ids["board"],
            RebuildStepResult(ok=True, counts={"expected_by_layer": expected_layers}),
        )
        assert guard is None  # no MATERIALIZED_LAYER_MISMATCH


# ---------------------------------------------------------------------------
# TS2 (ts_38dc9e19) — false Path B variants stay blocked + never canonical
# ---------------------------------------------------------------------------


async def _drive_amendment_variant(db, ids, variant: str) -> tuple[str, bool]:
    """Seed the amendment into the requested invalid state via the public
    lifecycle. Returns (status, lineage_complete) for the KG maturity check."""
    lifecycle = AmendmentRevisionService(db)
    amendment = await lifecycle.create(
        board_id=ids["board"], original_spec_id=ids["spec"], origin_bug_id=ids["bug"],
        author=USER_ID, origin_task_ids=[ids["origin"]],
        regression_scenario_ids=[ids["foreign_scenario"]],
        regression_test_task_ids=[ids["test"]],
    )
    if variant == "draft":
        # Complete lineage but still draft -> status blocks independently of lineage.
        await lifecycle.set_lineage_state(amendment.id, AmendmentLineageState.COMPLETE, USER_ID)
        return "draft", True
    if variant == "cancelled":
        await lifecycle.set_lineage_state(amendment.id, AmendmentLineageState.COMPLETE, USER_ID)
        await lifecycle.set_status(amendment.id, AmendmentRevisionStatus.CANCELLED, USER_ID)
        return "cancelled", True
    if variant == "superseded":
        await lifecycle.set_lineage_state(amendment.id, AmendmentLineageState.COMPLETE, USER_ID)
        await lifecycle.set_status(amendment.id, AmendmentRevisionStatus.SUPERSEDED, USER_ID)
        return "superseded", True
    if variant == "incomplete_lineage":
        # done status but lineage never completed.
        await lifecycle.set_status(amendment.id, AmendmentRevisionStatus.DONE, USER_ID)
        return "done", False
    raise AssertionError(f"unknown variant {variant!r}")


@pytest.mark.parametrize(
    "variant,reason",
    [
        ("no_amendment", "missing_amendment_revision"),
        ("draft", "blocked_amendment_status"),
        ("cancelled", "blocked_amendment_status"),
        ("superseded", "blocked_amendment_status"),
        ("incomplete_lineage", "incomplete_amendment_lineage"),
    ],
)
async def test_ts_38dc9e19_false_variants_stay_blocked(variant, reason):
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_e2e(db)

        if variant != "no_amendment":
            status, lineage_complete = await _drive_amendment_variant(db, ids, variant)
            # No false canonical leak: an invalid amendment NEVER reaches the
            # canonical partition (working for draft/incomplete-lineage; dropped/
            # "none" for cancelled/superseded — both strictly non-canonical).
            assert _amendment_layer(status, lineage_complete=lineage_complete) != "canonical"

        # Gate stays blocked with the precise reason, never readiness/allow.
        await _assert_gate_blocks(db, ids, reason)
        assert await _coverage_state(db, ids) != "path_b_ready"


async def test_ts_38dc9e19_hotfix_lane_without_amendment_stays_blocked():
    """Path C (hotfix lane) is execution-only and does NOT bypass Path B: a
    cross-spec bug on an ACTIVE hotfix lane with NO amendment is still
    fail-closed (missing_amendment_revision), never closure-ready."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_e2e(db, hotfix_lane=True)
        await _assert_gate_blocks(db, ids, "missing_amendment_revision")
        assert await _coverage_state(db, ids) == "not_applicable"


async def test_ts_38dc9e19_automated_evidence_without_required_fields_blocks_confirmation():
    """Automated regression evidence WITHOUT the re-executable fields
    (test_file_path / test_function) cannot satisfy the validator coverage
    precondition (G2): confirmation is refused, coverage stays pending and the
    gate keeps blocking — lineage/status alone is never sufficient."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_e2e(db)
        lifecycle = AmendmentRevisionService(db)
        amendment = await lifecycle.create(
            board_id=ids["board"], original_spec_id=ids["spec"], origin_bug_id=ids["bug"],
            author=USER_ID, origin_task_ids=[ids["origin"]],
            regression_scenario_ids=[ids["foreign_scenario"]],
            regression_test_task_ids=[ids["test"]],
        )
        await lifecycle.set_lineage_state(amendment.id, AmendmentLineageState.COMPLETE, USER_ID)
        await lifecycle.set_status(amendment.id, AmendmentRevisionStatus.DONE, USER_ID)

        # automated but NO re-executable evidence fields.
        await _make_artifact_ready(db, ids, with_evidence=False)

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).confirm_amendment_coverage(
                amendment_id=amendment.id, regression_test_task_id=ids["test"],
                regression_scenario_id=ids["foreign_scenario"],
                reviewer_id=USER_ID, reviewer_name=USER_ID,
            )
        assert exc.value.code == "coverage_precondition_unmet"

        # Refused confirmation => still coverage_pending, gate still blocks, never ready.
        assert await _coverage_state(db, ids) == "coverage_pending"
        await _assert_gate_blocks(db, ids, "coverage_pending")


# ---------------------------------------------------------------------------
# Card 14ddfab0 — the WHOLE Path B closure is reachable via the AGENT-FACING
# surface alone (create + associate + transition_lifecycle, no raw service
# set_status/set_lineage). This is what unblocks reprocessing a historical bug
# (21dcca7f). The lifecycle tool still NEVER closes the bug: before the validator
# confirms coverage it is coverage_pending; only confirm flips it to path_b_ready.
# ---------------------------------------------------------------------------


async def test_agent_facing_lifecycle_reaches_path_b_ready_and_gate_allows():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiService

    async with get_session_factory()() as db:
        ids = await _seed_e2e(db)
        api = AmendmentRevisionApiService(db)

        created = await api.create(
            board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            origin_task_ids=[ids["origin"]], regression_scenario_ids=[ids["foreign_scenario"]],
        )
        amd_id = created["id"]
        await api.associate(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
            actor=USER_ID, regression_test_task_ids=[ids["test"]],
        )
        # Promote via the AGENT-FACING lifecycle (NOT the raw service set_status/set_lineage).
        await api.transition_lifecycle(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
            actor=USER_ID, lineage_state="complete",
        )
        await api.transition_lifecycle(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
            actor=USER_ID, status="done",
        )

        # Lifecycle promotion alone does NOT close the bug — still coverage_pending.
        assert await _coverage_state(db, ids) == "coverage_pending"
        await _assert_gate_blocks(db, ids, "coverage_pending")

        # Validator confirm flips coverage_pending -> path_b_ready; gate ALLOWS.
        await _make_artifact_ready(db, ids)
        await CardService(db).confirm_amendment_coverage(
            amendment_id=amd_id, regression_test_task_id=ids["test"],
            regression_scenario_id=ids["foreign_scenario"],
            reviewer_id=USER_ID, reviewer_name=USER_ID,
        )
        await CardService(db).move_card(
            ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
        )
        bug = await db.get(Card, ids["bug"])
        assert bug.status == CardStatus.IN_PROGRESS
        assert await _coverage_state(db, ids) == "path_b_ready"


# ---------------------------------------------------------------------------
# Card 676b2aa6 (spec 62cf2d36) — Path B for an in_progress + CONTENT-LOCKED spec,
# the bug gate recognising an eligible amendment as an ADDITIVE regression source
# for a bug with NO directly-linked test task.
# ---------------------------------------------------------------------------


async def _seed_content_locked_no_direct_link(db):
    """Mutate the e2e seed into the 676b shape: the bug's spec is in_progress AND
    content-locked (current_validation_id -> outcome=success) and the bug has NO
    directly-linked test task — the only regression path is a Path B amendment."""
    ids = await _seed_e2e(db)
    spec = await db.get(Spec, ids["spec"])
    spec.status = SpecStatus.IN_PROGRESS
    vid = f"val_{uuid.uuid4().hex[:8]}"
    spec.current_validation_id = vid
    spec.validations = [{"id": vid, "outcome": "success"}]
    flag_modified(spec, "validations")
    bug = await db.get(Card, ids["bug"])
    bug.linked_test_task_ids = []
    flag_modified(bug, "linked_test_task_ids")
    await db.flush()
    return ids


async def test_676b_eligible_amendment_unblocks_content_locked_bug_without_bypass():
    """GOV-like end-to-end: a bug on an in_progress + content-locked spec, with NO
    direct test link, advances ONLY via an eligible Path B amendment + validator-
    confirmed coverage — never a bypass (fr_0d2f84a1, fr_646e69d2, fr_68dddce5)."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_content_locked_no_direct_link(db)
        api = AmendmentRevisionApiService(db)
        lifecycle = AmendmentRevisionService(db)

        # No direct link + no amendment: the gate still requires a regression
        # (require_test_task_for_bug intact) -> missing_regression_test_task.
        await _assert_gate_blocks(db, ids, "zero eligible existing scenarios")

        # 676b Part A: Path B ACCEPTS the in_progress + content-locked spec.
        created = await api.create(
            board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            origin_task_ids=[ids["origin"]], regression_scenario_ids=[ids["foreign_scenario"]],
        )
        amendment_id = created["id"]
        await api.associate(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id,
            actor=USER_ID, regression_test_task_ids=[ids["test"]],
        )

        # Anti-bypass: a DRAFT (ineligible) amendment contributes NO test task, so
        # the gate keeps blocking with missing_regression_test_task.
        await _assert_gate_blocks(db, ids, "zero eligible existing scenarios")

        # 676b Part B: once the amendment is eligible (done + complete lineage) its
        # regression test task is an ADDITIVE source -> the gate advances PAST the
        # first check, but coverage is UNCONFIRMED -> coverage_pending (validator-
        # only, never forged by the recognition).
        await lifecycle.set_lineage_state(amendment_id, AmendmentLineageState.COMPLETE, USER_ID)
        await lifecycle.set_status(amendment_id, AmendmentRevisionStatus.DONE, USER_ID)
        await _assert_gate_blocks(db, ids, "coverage_pending")

        # The validator confirms coverage on re-executable evidence -> the bug
        # finally advances. require_test_task_for_bug and validator-only coverage
        # held throughout — no bypass.
        await _make_artifact_ready(db, ids)
        await CardService(db).confirm_amendment_coverage(
            amendment_id=amendment_id, regression_test_task_id=ids["test"],
            regression_scenario_id=ids["foreign_scenario"],
            reviewer_id=USER_ID, reviewer_name=USER_ID,
        )
        await CardService(db).move_card(
            ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
        )
        bug = await db.get(Card, ids["bug"])
        assert bug.status == CardStatus.IN_PROGRESS
        assert await _coverage_state(db, ids) == "path_b_ready"


# ---------------------------------------------------------------------------
# Path B + Path C composition — a validator-confirmed test task on the revision
# spec can execute in the original-spec hotfix lane.  This is the narrow
# cross-spec exception required by the public workflow; all unconfirmed or
# unrelated cross-spec cards remain rejected by sprint assignment.
# ---------------------------------------------------------------------------


async def _prepare_cross_spec_hotfix(db, *, confirmed: bool):
    ids = await _seed_e2e(db)
    test_task = await db.get(Card, ids["test"])
    test_task.spec_id = ids["other_spec"]
    await db.flush()

    lifecycle = AmendmentRevisionService(db)
    amendment = await lifecycle.create(
        board_id=ids["board"],
        original_spec_id=ids["spec"],
        origin_bug_id=ids["bug"],
        author=USER_ID,
        origin_task_ids=[ids["origin"]],
        revision_spec_id=ids["other_spec"],
        regression_scenario_ids=[ids["foreign_scenario"]],
        regression_test_task_ids=[ids["test"]],
    )
    await lifecycle.set_lineage_state(
        amendment.id,
        AmendmentLineageState.COMPLETE,
        USER_ID,
    )
    await lifecycle.set_status(
        amendment.id,
        AmendmentRevisionStatus.DONE,
        USER_ID,
    )
    if confirmed:
        await _make_artifact_ready(db, ids)
        await CardService(db).confirm_amendment_coverage(
            amendment_id=amendment.id,
            regression_test_task_id=ids["test"],
            regression_scenario_id=ids["foreign_scenario"],
            reviewer_id=USER_ID,
            reviewer_name=USER_ID,
        )

    sprint = await SprintService(db).create_sprint(
        ids["board"],
        USER_ID,
        SprintCreate(
            title="Path B evidence hotfix",
            spec_id=ids["spec"],
            lane_type=SprintLaneType.HOTFIX,
            origin_bug_id=ids["bug"],
        ),
        skip_ownership_check=True,
    )
    assert sprint is not None
    ids["hotfix"] = sprint.id
    return ids


async def test_confirmed_path_b_test_can_join_and_activate_path_c_hotfix():
    """The exact validator-confirmed revision task satisfies Path C directly."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _prepare_cross_spec_hotfix(db, confirmed=True)
        service = SprintService(db)

        assigned = await service.assign_tasks(
            ids["hotfix"],
            [ids["bug"], ids["test"]],
            USER_ID,
        )
        assert assigned == 2
        activated = await service.move_sprint(
            ids["hotfix"],
            USER_ID,
            SprintMove(status=SprintStatus.ACTIVE),
        )
        assert activated is not None
        assert activated.status == SprintStatus.ACTIVE
        assert (await db.get(Card, ids["test"])).spec_id == ids["other_spec"]
        assert (await db.get(Card, ids["test"])).sprint_id == ids["hotfix"]


async def test_unconfirmed_cross_spec_test_still_fails_hotfix_assignment_atomically():
    """Complete lineage alone never opens the cross-spec sprint boundary."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _prepare_cross_spec_hotfix(db, confirmed=False)
        service = SprintService(db)

        with pytest.raises(ValueError, match="belongs to a different spec"):
            await service.assign_tasks(
                ids["hotfix"],
                [ids["bug"], ids["test"]],
                USER_ID,
            )
        assert (await db.get(Card, ids["bug"])).sprint_id is None
        assert (await db.get(Card, ids["test"])).sprint_id is None
