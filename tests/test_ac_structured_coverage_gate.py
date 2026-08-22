"""Regression tests for the AC→Scenario coverage gate with STRUCTURED ACs.

Okto Pulse 0.2.3 · item #3.

Background
----------
The deterministic coverage gate builds the "uncovered" error list with a slice
``criterion[:80]``. When acceptance criteria are stored as structured dicts
(``{"id": "AC-1", "text": "..."}``) instead of legacy strings, slicing a dict
with ``[:80]`` raises ``KeyError: slice(None, 80, None)`` — crashing the gate
INSTEAD of returning the intended ``ValueError`` listing the uncovered ACs.

The bug lived at TWO call sites in ``services/main.py``:
  * ``CardService.check_ac_scenario_coverage`` (submit_spec_validation path)
  * ``SpecService.move_spec`` → ``done`` gate

Fix: extract text via ``_structured_ref_text(criterion)[:80]`` at both sites.

These tests assert that, for structured ACs referenced by id / text / title /
prefix / index / mixed-with-string, the gate:
  * NEVER raises ``KeyError`` (the regression),
  * counts coverage correctly (covered → passes; uncovered → clean ValueError).

Plus an anti-drift test pinning ``api/analytics.py``'s resolver alias to the
canonical ``services/analytics_service.py`` implementation, and a bidirectional
prefix-matching contract test.
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.domain.code_traceability import (
    DeliveryContext,
    DirectSpecDeliveryContextProvenance,
)
from okto_pulse.core.models.schemas import SpecMove
from okto_pulse.core.services import main as main_service
from okto_pulse.core.services.main import CardService, SpecService

# NOTE: no module-level ``pytestmark = pytest.mark.asyncio`` — the project runs
# with ``asyncio_mode = "auto"`` (pyproject.toml), so async tests are detected
# automatically and the sync anti-drift/contract tests stay un-marked.

USER_ID = "ac-structured-gate-agent"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _scenario(sid: str, linked: list) -> dict:
    return {
        "id": sid,
        "title": f"covers {linked}",
        "given": "a precondition",
        "when": "an action runs",
        "then": "an expected result",
        "scenario_type": "integration",
        "linked_criteria": linked,
        "status": "passed",
        "linked_task_ids": [],
    }


def _direct_spec_context_fields(spec_id: str) -> dict[str, object]:
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD,
        source_spec_id=spec_id,
        source_spec_version=1,
    )
    manifest, manifest_sha256 = main_service._direct_spec_source_context_manifest(
        spec_id=spec_id,
        delivery_context=DeliveryContext.BROWNFIELD,
        provenance=provenance,
        subject_version=1,
    )
    return {
        "delivery_context": DeliveryContext.BROWNFIELD.value,
        "delivery_context_provenance": {
            "value": provenance.value.value,
            "source_spec_id": provenance.source_spec_id,
            "source_spec_version": provenance.source_spec_version,
        },
        "source_context_manifest": manifest,
        "source_context_sha256": manifest_sha256,
    }


def _make_spec(spec_id: str, board_id: str, *, acs: list, scenarios: list) -> Spec:
    return Spec(
        id=spec_id,
        board_id=board_id,
        title="AC Structured Coverage Spec",
        status=SpecStatus.IN_PROGRESS,
        created_by=USER_ID,
        skip_test_coverage=False,
        skip_qualitative_validation=True,
        functional_requirements=["FR1"],
        acceptance_criteria=acs,
        test_scenarios=scenarios,
        business_rules=[],
        api_contracts=[],
        **_direct_spec_context_fields(spec_id),
    )


async def _seed(db_factory, *, acs: list, scenarios: list) -> tuple[str, str]:
    board_id = _id("ac-board")
    spec_id = _id("ac-spec")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="AC Structured Board",
                owner_id=USER_ID,
                settings={"skip_test_coverage_global": False},
            )
        )
        db.add(_make_spec(spec_id, board_id, acs=acs, scenarios=scenarios))
        await db.commit()
    return board_id, spec_id


# Structured AC fixtures referenced via every supported shape.
AC_DICTS = [
    {"id": "AC-1", "text": "User can log in with valid credentials and reach dashboard"},
    {"id": "AC-2", "text": "User sees an error message when the password is wrong"},
]
AC_DICT_TITLE = {"id": "AC-3", "title": "Password reset via email link"}  # title fallback


# Each param: (case label, linked_criteria reference that covers BOTH ACs).
_FULLY_COVERED_REFS = [
    pytest.param(["AC-1", "AC-2"], id="by-id"),
    pytest.param(
        [
            "User can log in with valid credentials and reach dashboard",
            "User sees an error message when the password is wrong",
        ],
        id="by-full-text",
    ),
    pytest.param([0, 1], id="by-int-index"),
    pytest.param(["0", "1"], id="by-str-index"),
    pytest.param(["User can log in", "User sees an error"], id="by-prefix-scenario-shorter"),
    pytest.param(["AC-1", 1], id="mixed-id-and-index"),
    pytest.param(["AC-1", "User sees an error message when the password is wrong"], id="mixed-id-and-text"),
]


# ---------------------------------------------------------------------------
# Call site 1 — CardService.check_ac_scenario_coverage (submit_spec_validation)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("linked", _FULLY_COVERED_REFS)
async def test_check_ac_scenario_coverage_structured_covered_passes(db_factory, linked):
    """Structured ACs fully covered → gate passes (no exception of any kind)."""
    _, spec_id = await _seed(
        db_factory,
        acs=list(AC_DICTS),
        scenarios=[_scenario("ts-all", linked)],
    )
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        board = await db.get(Board, spec.board_id)
        # Must not raise KeyError NOR ValueError when fully covered.
        await CardService(db).check_ac_scenario_coverage(spec, board)


async def test_check_ac_scenario_coverage_structured_uncovered_raises_valueerror_not_keyerror(
    db_factory,
):
    """An UNCOVERED structured AC must raise the intended ValueError — never KeyError.

    This is the core regression: ``criterion[:80]`` on a dict used to raise
    ``KeyError: slice(None, 80, None)``.
    """
    _, spec_id = await _seed(
        db_factory,
        acs=list(AC_DICTS),
        scenarios=[_scenario("ts-one", ["AC-1"])],  # AC-2 left uncovered
    )
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        board = await db.get(Board, spec.board_id)
        with pytest.raises(ValueError) as ei:
            await CardService(db).check_ac_scenario_coverage(spec, board)
    msg = str(ei.value)
    assert "acceptance criteria lack test scenarios" in msg
    # AC text (not a dict repr) must be surfaced in the error.
    assert "User sees an error message when the password is wrong"[:80] in msg
    assert "slice(" not in msg  # guards against KeyError leakage into the message


async def test_check_ac_scenario_coverage_title_only_dict_does_not_crash(db_factory):
    """AC dict with only `title` (no `text`) must use the title in the error, not crash."""
    _, spec_id = await _seed(
        db_factory,
        acs=[dict(AC_DICT_TITLE)],  # single, uncovered, title-only AC
        scenarios=[],
    )
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        board = await db.get(Board, spec.board_id)
        with pytest.raises(ValueError) as ei:
            await CardService(db).check_ac_scenario_coverage(spec, board)
    assert "Password reset via email link" in str(ei.value)


async def test_check_ac_scenario_coverage_mixed_dict_and_string_acs(db_factory):
    """A spec mixing structured dict ACs and legacy string ACs is handled without KeyError."""
    acs = [
        {"id": "AC-1", "text": "Structured covered criterion"},
        "Legacy string uncovered criterion",  # uncovered → triggers slice path
    ]
    _, spec_id = await _seed(
        db_factory,
        acs=acs,
        scenarios=[_scenario("ts-s", ["AC-1"])],
    )
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        board = await db.get(Board, spec.board_id)
        with pytest.raises(ValueError) as ei:
            await CardService(db).check_ac_scenario_coverage(spec, board)
    msg = str(ei.value)
    assert "Legacy string uncovered criterion" in msg
    assert "1 acceptance criteria lack test scenarios" in msg


# ---------------------------------------------------------------------------
# Call site 2 — SpecService.move_spec → done
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("linked", _FULLY_COVERED_REFS)
async def test_move_spec_done_structured_covered_passes(db_factory, linked):
    """Structured ACs fully covered → move_spec→done succeeds (gate does not crash)."""
    _, spec_id = await _seed(
        db_factory,
        acs=list(AC_DICTS),
        scenarios=[_scenario("ts-all", linked)],
    )
    async with db_factory() as db:
        moved = await SpecService(db).move_spec(
            spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
        )
        await db.commit()
    assert moved is not None
    assert moved.status == SpecStatus.DONE


async def test_move_spec_done_structured_uncovered_raises_valueerror_not_keyerror(db_factory):
    """move_spec→done with an UNCOVERED structured AC raises ValueError, never KeyError."""
    _, spec_id = await _seed(
        db_factory,
        acs=list(AC_DICTS),
        scenarios=[_scenario("ts-one", ["AC-1"])],  # AC-2 uncovered
    )
    async with db_factory() as db:
        with pytest.raises(ValueError) as ei:
            await SpecService(db).move_spec(
                spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
            )
    msg = str(ei.value)
    assert "Cannot move spec to 'done'" in msg
    assert "User sees an error message when the password is wrong"[:80] in msg
    assert "slice(" not in msg


async def test_move_spec_done_title_only_dict_does_not_crash(db_factory):
    """move_spec→done with a title-only uncovered AC dict raises ValueError using the title."""
    _, spec_id = await _seed(
        db_factory,
        acs=[dict(AC_DICT_TITLE)],
        scenarios=[],
    )
    async with db_factory() as db:
        with pytest.raises(ValueError) as ei:
            await SpecService(db).move_spec(
                spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
            )
    assert "Password reset via email link" in str(ei.value)


# ---------------------------------------------------------------------------
# Anti-drift — api/analytics.py resolver must be the canonical service function
# ---------------------------------------------------------------------------


def test_api_analytics_resolver_is_canonical_service_function():
    """The api/analytics.py resolver alias MUST be the identical object exported by
    services/analytics_service.py — guarding against the historical copy/paste drift
    that let the two implementations diverge.
    """
    from okto_pulse.community.api import analytics as api_analytics
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_criteria_to_indices as canonical,
    )

    assert api_analytics._resolve_linked_criteria_to_indices is canonical


def test_mcp_server_resolver_is_canonical_service_function():
    """mcp/server.py likewise re-exports the canonical resolver (parity guard)."""
    from okto_pulse.core.mcp import server as mcp_server
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_criteria_to_indices as canonical,
    )

    assert mcp_server._resolve_linked_criteria_to_indices is canonical


# ---------------------------------------------------------------------------
# Contract — bidirectional prefix matching on the canonical resolver
# ---------------------------------------------------------------------------


def test_resolver_prefix_matching_is_bidirectional():
    """The resolver matches when EITHER the AC text starts with the ref OR the ref
    starts with the AC text — the contracted tolerant behavior shared with the gate.
    """
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_criteria_to_indices,
    )

    ac_list = [{"id": "AC-1", "text": "User can log in with valid credentials"}]

    # ref shorter than AC text → AC text startswith(ref)
    assert resolve_linked_criteria_to_indices(["User can log in"], ac_list) == {0}
    # ref longer than AC text → ref startswith(AC text)
    assert (
        resolve_linked_criteria_to_indices(
            ["User can log in with valid credentials and a TOTP token"], ac_list
        )
        == {0}
    )
    # unrelated text → no match
    assert resolve_linked_criteria_to_indices(["Completely different"], ac_list) == set()


def test_resolver_dict_ids_text_indices_dedup_to_same_ac():
    """id, full text, prefix and index that all point at the same AC dedup to one index."""
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_criteria_to_indices,
    )

    ac_list = [{"id": "AC-1", "text": "User can log in with valid credentials"}]
    refs = [
        "AC-1",  # by id
        "User can log in with valid credentials",  # by full text
        "User can log in",  # by prefix
        0,  # by int index
        "0",  # by str index
    ]
    assert resolve_linked_criteria_to_indices(refs, ac_list) == {0}


def test_resolver_rejects_out_of_range_and_bool():
    """Out-of-range indices and bool entries are dropped (covered_ac <= total_ac invariant)."""
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_criteria_to_indices,
    )

    ac_list = [{"id": "AC-1", "text": "only one AC"}]
    # index 5 out of range; True is bool (subclass of int) and must be rejected.
    assert resolve_linked_criteria_to_indices([5, True], ac_list) == set()
