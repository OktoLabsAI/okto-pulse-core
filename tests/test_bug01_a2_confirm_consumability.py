"""BUG-01 A2 (card b7b3fa94) — confirm_amendment_coverage gate-consumability wiring.

The exhaustive regression matrix is BUG-02; this is the A2 WIRING smoke: through
the REAL ``CardService.confirm_amendment_coverage`` on a seeded board, a
syntactically valid but gate-INERT SAME-SPEC confirmation must be rejected
``coverage_not_gate_consumable`` BEFORE persistence (the exact E2E finding), and
the amendment must carry NO persisted coverage confirmation (atomic).

The amendment's task claim INTERSECTS the bug authoritative tasks, so a
Path-B-only wiring would reach rank 5 and wrongly persist — this test fails such
an implementation. The consumable cross-spec positive path is already proven by
``tests/test_path_b_e2e.py::test_ts_64a2d5aa_positive_path_b_lifecycle_*``.

Reproduce:
  uv run pytest tests/test_bug01_a2_confirm_consumability.py -p no:cacheprovider -q
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
    COVERAGE_CONFIRMATION_KEY,
)
from okto_pulse.core.models.db import (
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiService
from okto_pulse.core.services.main import CardOperationError, CardService

pytestmark = pytest.mark.asyncio

USER_ID = "bug01-a2-agent"


async def _seed_same_spec_inert(db):
    """Bug on a DONE spec whose regression evidence is a SAME-SPEC scenario that
    is NOT linked to the bug's origin/affected lineage (Path A unrelated). The
    amendment declares that scenario AND claims the origin task (intersecting the
    bug authoritative set) — the inert tuple the E2E exposed."""
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"a2-board-{suffix}",
        "spec": f"a2-spec-{suffix}",
        "origin": f"a2-origin-{suffix}",
        "bug": f"a2-bug-{suffix}",
        "test": f"a2-test-{suffix}",
        "same_spec_scenario": f"ts-samespec-{suffix}",
    }
    now = datetime.now(timezone.utc)

    db.add(Board(id=ids["board"], name="BUG-01 A2 Board", owner_id=USER_ID))
    db.add(Spec(
        id=ids["spec"], board_id=ids["board"], title="Bug spec", status=SpecStatus.DONE,
        created_by=USER_ID, functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        # The regression scenario lives on the BUG's OWN spec (same-spec) and is
        # NOT linked to the origin task -> Path A classifies it unrelated.
        test_scenarios=[{
            "id": ids["same_spec_scenario"], "title": "AC happy-path scenario",
            "linked_criteria": [0], "status": "passed",
        }],
        business_rules=[], api_contracts=[],
    ))
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
        status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
        test_scenario_ids=[],  # NOT linked to the same-spec scenario
        created_at=now - timedelta(minutes=5),
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
        title="Regression test reusing same-spec scenario", status=CardStatus.NOT_STARTED,
        card_type=CardType.TEST, test_scenario_ids=[ids["same_spec_scenario"]],
        created_by=USER_ID, created_at=now + timedelta(seconds=1),
    ))
    await db.flush()
    return ids


async def _ready_amendment(db, ids):
    """Create a done/complete amendment that DECLARES the same-spec scenario and
    CLAIMS the origin task (intersects the bug authoritative set), then mark the
    test task DONE and the scenario automated WITH reexecutable evidence — so the
    binding + precondition checks all pass and only consumability remains."""
    api = AmendmentRevisionApiService(db)
    lifecycle = AmendmentRevisionService(db)
    created = await api.create(
        board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
        origin_task_ids=[ids["origin"]],
        regression_scenario_ids=[ids["same_spec_scenario"]],
    )
    amendment_id = created["id"]
    await api.associate(
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id,
        actor=USER_ID, regression_test_task_ids=[ids["test"]],
    )
    await lifecycle.set_lineage_state(amendment_id, AmendmentLineageState.COMPLETE, USER_ID)
    await lifecycle.set_status(amendment_id, AmendmentRevisionStatus.DONE, USER_ID)

    test_task = await db.get(Card, ids["test"])
    test_task.status = CardStatus.DONE
    spec = await db.get(Spec, ids["spec"])
    scenarios = list(spec.test_scenarios or [])
    for sc in scenarios:
        if sc.get("id") == ids["same_spec_scenario"]:
            sc["status"] = "automated"
            sc["evidence"] = {
                "test_file_path": "tests/test_reg.py", "test_function": "test_reg",
            }
    spec.test_scenarios = scenarios
    flag_modified(spec, "test_scenarios")
    await db.flush()
    return amendment_id


async def test_a2_same_spec_inert_confirmation_rejected_before_persist():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_same_spec_inert(db)
        amendment_id = await _ready_amendment(db, ids)

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).confirm_amendment_coverage(
                amendment_id=amendment_id,
                regression_test_task_id=ids["test"],
                regression_scenario_id=ids["same_spec_scenario"],
                reviewer_id=USER_ID, reviewer_name=USER_ID,
            )

        # The new fail-closed code, NOT the old binding/precondition errors
        # (binding + reexecutable evidence are satisfied here).
        assert exc.value.code == "coverage_not_gate_consumable"
        facts = exc.value.facts or {}
        assert facts.get("routed_path") == "path_a"
        assert facts.get("resolver_reason") == "unrelated_scenario"
        assert facts.get("bug_id") == ids["bug"]

        # Atomic: nothing was persisted onto the amendment.
        amendment = await AmendmentRevisionService(db).get(amendment_id)
        metadata = amendment.validation_metadata or {}
        assert COVERAGE_CONFIRMATION_KEY not in metadata
