"""BUG-02 B (card cd1cd433 / spec 7337a5f7) — gate-consumability regression matrix.

Granular, traceable matrix proving consumable vs INERT coverage at the
``confirm_amendment_coverage`` writer + the bug regression gate, WITHOUT relaxing
the resolver. Maps 1:1 to the spec test scenarios; scenarios already proven by an
existing test are referenced (not duplicated):

  TS-BUG02-1 (same-spec inert + intersecting claim -> coverage_not_gate_consumable)
      -> tests/test_bug01_a2_confirm_consumability.py::
         test_a2_same_spec_inert_confirmation_rejected_before_persist (A2 smoke).
  TS-BUG02-2 (no-persist + gate stays blocked) -> test_bug02_2_* below.
  TS-BUG02-3 (cross-spec positive still consumed -> path_b_ready + move allowed)
      -> tests/test_path_b_e2e.py::
         test_ts_64a2d5aa_positive_path_b_lifecycle_with_kg_checkpoints.
  TS-BUG02-4 (old error codes preserved BEFORE the new error) -> test_bug02_4* below.
  TS-BUG02-5 (focused suite never relaxes unrelated_scenario) -> test_bug02_5_* below.

Reproduce:
  uv run pytest tests/test_bug02_coverage_matrix.py -p no:cacheprovider -q
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
    COVERAGE_CONFIRMATION_KEY,
)
from sqlalchemy_test_models import (
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiService
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    BugRegressionGateDecision,
    BugRegressionRejectionReason,
    CoverageConfirmationFact,
    evaluate_coverage_confirmation_consumability,
)
from okto_pulse.core.services.main import CardOperationError, CardService

USER_ID = "bug02-matrix-agent"


async def _seed(db, *, with_evidence: bool = True):
    """Bug on a DONE spec whose regression evidence is a SAME-SPEC scenario NOT
    linked to the bug lineage (Path A unrelated). ``with_evidence=False`` omits
    the reexecutable evidence so the coverage precondition cannot be met."""
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"b02-board-{suffix}",
        "spec": f"b02-spec-{suffix}",
        "origin": f"b02-origin-{suffix}",
        "bug": f"b02-bug-{suffix}",
        "test": f"b02-test-{suffix}",
        "scenario": f"ts-samespec-{suffix}",
    }
    now = datetime.now(timezone.utc)
    evidence = (
        {"test_file_path": "tests/test_reg.py", "test_function": "test_reg"}
        if with_evidence
        else None
    )
    scenario = {
        "id": ids["scenario"], "title": "AC happy-path scenario",
        "linked_criteria": [0], "status": "automated",
    }
    if evidence is not None:
        scenario["evidence"] = evidence

    db.add(Board(id=ids["board"], name="BUG-02 matrix", owner_id=USER_ID))
    db.add(Spec(
        id=ids["spec"], board_id=ids["board"], title="Bug spec", status=SpecStatus.DONE,
        created_by=USER_ID, functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[scenario], business_rules=[], api_contracts=[],
    ))
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
        status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
        test_scenario_ids=[], created_at=now - timedelta(minutes=5),
    ))
    db.add(Card(
        id=ids["bug"], board_id=ids["board"], spec_id=ids["spec"],
        title="Bug reusing an unrelated AC scenario", status=CardStatus.NOT_STARTED,
        card_type=CardType.BUG, origin_task_id=ids["origin"],
        severity=BugSeverity.MAJOR, expected_behavior="ok", observed_behavior="bad",
        linked_test_task_ids=[ids["test"]], created_by=USER_ID, created_at=now,
    ))
    db.add(Card(
        id=ids["test"], board_id=ids["board"], spec_id=ids["spec"],
        title="Regression test", status=CardStatus.DONE,
        card_type=CardType.TEST, test_scenario_ids=[ids["scenario"]],
        created_by=USER_ID, created_at=now + timedelta(seconds=1),
    ))
    await db.flush()
    return ids


async def _ready_amendment(db, ids):
    """Create a done/complete amendment declaring the scenario + test task and
    claiming the origin task (intersects the bug authoritative set)."""
    api = AmendmentRevisionApiService(db)
    lifecycle = AmendmentRevisionService(db)
    created = await api.create(
        board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
        origin_task_ids=[ids["origin"]], regression_scenario_ids=[ids["scenario"]],
    )
    amendment_id = created["id"]
    await api.associate(
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id,
        actor=USER_ID, regression_test_task_ids=[ids["test"]],
    )
    await lifecycle.set_lineage_state(amendment_id, AmendmentLineageState.COMPLETE, USER_ID)
    await lifecycle.set_status(amendment_id, AmendmentRevisionStatus.DONE, USER_ID)
    return amendment_id


async def _confirm(db, *, amendment_id, test_task_id, scenario_id):
    return await CardService(db).confirm_amendment_coverage(
        amendment_id=amendment_id, regression_test_task_id=test_task_id,
        regression_scenario_id=scenario_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
    )


# ---------------------------------------------------------------------------
# TS-BUG02-2 — inert confirmation does not persist AND the gate stays blocked.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bug02_2_no_persist_and_gate_stays_blocked():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        amendment_id = await _ready_amendment(db, ids)

        with pytest.raises(CardOperationError) as exc:
            await _confirm(db, amendment_id=amendment_id,
                           test_task_id=ids["test"], scenario_id=ids["scenario"])
        assert exc.value.code == "coverage_not_gate_consumable"

        # No-persist: the rejected attempt left no attestation on the amendment.
        amendment = await AmendmentRevisionService(db).get(amendment_id)
        assert COVERAGE_CONFIRMATION_KEY not in (amendment.validation_metadata or {})

        # Gate stays blocked: the bug cannot move while the only scenario is
        # unrelated. TR3: assert the STRUCTURED CardOperationError, not a
        # substring. A single same-spec unrelated scenario leaves no eligible/
        # pending candidate, so the gate decision is BLOCK_SEMANTIC_GAP (the same
        # decision the original E2E reproduced) and the bounded remediation
        # reason_code is the scenario rejection unrelated_scenario.
        with pytest.raises(CardOperationError) as move_exc:
            await CardService(db).move_card(
                ids["bug"], USER_ID, CardMove(status=CardStatus.IN_PROGRESS)
            )
        assert move_exc.value.code == BugRegressionGateDecision.BLOCK_SEMANTIC_GAP.value
        assert move_exc.value.workflow_remediation is not None
        assert (
            move_exc.value.workflow_remediation.reason_code
            == BugRegressionRejectionReason.UNRELATED_SCENARIO.value
        )
        # Complementary (NOT the oracle): never closure-ready while blocked.
        assert "path_b_ready" not in str(move_exc.value)
        bug = await db.get(Card, ids["bug"])
        assert bug.status == CardStatus.NOT_STARTED


# ---------------------------------------------------------------------------
# TS-BUG02-4 — pre-existing binding/precondition errors fire BEFORE the new one.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bug02_4a_binding_invalid_scenario_before_consumability():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        amendment_id = await _ready_amendment(db, ids)
        with pytest.raises(CardOperationError) as exc:
            await _confirm(db, amendment_id=amendment_id, test_task_id=ids["test"],
                           scenario_id="ts-not-declared-by-amendment")
        # Binding is checked before the consumability preflight.
        assert exc.value.code == "coverage_binding_invalid"


@pytest.mark.asyncio
async def test_bug02_4b_precondition_unmet_before_consumability():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db, with_evidence=False)
        amendment_id = await _ready_amendment(db, ids)
        with pytest.raises(CardOperationError) as exc:
            await _confirm(db, amendment_id=amendment_id,
                           test_task_id=ids["test"], scenario_id=ids["scenario"])
        # Reexecutable-evidence precondition is checked before consumability.
        assert exc.value.code == "coverage_precondition_unmet"


@pytest.mark.asyncio
async def test_bug02_4c_amendment_mismatch_test_task_before_consumability():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        amendment_id = await _ready_amendment(db, ids)
        # confirm_amendment_coverage receives no external bug_id, so the
        # writer-side "mismatch/stale" is a tuple NOT declared by THIS amendment:
        # a test task the amendment never bound. The binding check fires first.
        with pytest.raises(CardOperationError) as exc:
            await _confirm(db, amendment_id=amendment_id,
                           test_task_id="tc-not-declared", scenario_id=ids["scenario"])
        assert exc.value.code == "coverage_binding_invalid"


@pytest.mark.asyncio
async def test_bug02_4d_test_task_not_done_before_consumability():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        amendment_id = await _ready_amendment(db, ids)
        # Regress the declared regression test task out of DONE -> the existing
        # precondition fires before the new consumability preflight (kept local
        # so the matrix is self-contained and diagnostic).
        test_task = await db.get(Card, ids["test"])
        test_task.status = CardStatus.IN_PROGRESS
        await db.flush()
        with pytest.raises(CardOperationError) as exc:
            await _confirm(db, amendment_id=amendment_id,
                           test_task_id=ids["test"], scenario_id=ids["scenario"])
        assert exc.value.code == "coverage_precondition_unmet"


# ---------------------------------------------------------------------------
# TS-BUG02-5 — the resolver/helper never relaxes unrelated_scenario, with OR
# without an intersecting amendment claim (the no-relax invariant).
# ---------------------------------------------------------------------------


def _pure_same_spec_verdict(*, intersecting_claim: bool):
    spec = Spec(
        id="spec-1", board_id="board-1", title="s", created_by="a",
        test_scenarios=[{"id": "ts-x", "title": "x", "status": "passed"}],
    )
    bug = Card(id="bug-1", board_id="board-1", spec_id="spec-1", title="bug",
               status=CardStatus.DONE, card_type=CardType.BUG, created_by="a")
    origin = Card(id="origin-1", board_id="board-1", spec_id="spec-1", title="o",
                  status=CardStatus.DONE, card_type=CardType.NORMAL, created_by="a",
                  test_scenario_ids=[])  # ts-x NOT linked -> Path A unrelated
    fact = AmendmentLineageFact(
        amendment_revision_id="amd-1", board_id="board-1", original_spec_id="spec-1",
        origin_bug_id="bug-1", status="done", lineage_state="complete",
        origin_task_ids=("origin-1",) if intersecting_claim else ("foreign",),
        regression_scenario_ids=("ts-x",), regression_test_task_ids=("tc-1",),
    )
    candidate = CoverageConfirmationFact(
        validator_id="v", amendment_revision_id="amd-1",
        regression_test_task_id="tc-1", regression_scenario_id="ts-x",
        evidence_ref="tests/t.py::t",
    )
    return evaluate_coverage_confirmation_consumability(
        bug_card=bug, original_spec=spec, origin_task=origin, affected_tasks=None,
        amendment_fact=fact, candidate_confirmation=candidate,
        scenario_id="ts-x", scenario_spec_id="spec-1",
    )


def test_bug02_5_resolver_never_relaxes_unrelated_scenario():
    # The intersecting-claim case is the one a Path-B-only impl would relax.
    for intersecting in (True, False):
        verdict = _pure_same_spec_verdict(intersecting_claim=intersecting)
        assert verdict.consumable is False
        assert verdict.routed_path == "path_a"
        assert verdict.reject_reason is BugRegressionRejectionReason.UNRELATED_SCENARIO
