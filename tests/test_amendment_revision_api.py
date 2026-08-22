"""SPEC be089cd3 / card 4e7e1143 — REST + MCP amendment-revision tools.

create / list / get / associate AmendmentHotfixRevision for a bug, with structured
payloads + fail-closed errors + NO gate bypass (FR1/FR2/FR5, TR1/TR4). The MCP twin
(ir_54ceb69b) shares the AmendmentRevisionApiService orchestrator with REST, so the
shapes match. Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_amendment_revision_api.py
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

from datetime import datetime, timedelta, timezone
import json
import uuid

import pytest
from pydantic import ValidationError

from okto_pulse.community.api.amendment_revisions import AmendmentRevisionCreateRequest
from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)
from okto_pulse.core.domain.permissions import ALL_FLAGS
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    AmendmentHotfixRevision,
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.amendment_revision_api import (
    AmendmentRevisionApiError,
    AmendmentRevisionApiService,
    reject_bypass_fields,
)
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewService,
)

pytestmark = pytest.mark.asyncio

USER_ID = "amendment-api-agent"


async def _seed(db, *, spec_status=SpecStatus.DONE):
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"ar-board-{suffix}",
        "spec": f"ar-spec-{suffix}",
        "origin": f"ar-origin-{suffix}",
        "bug": f"ar-bug-{suffix}",
    }
    db.add(Board(id=ids["board"], name="AR Board", owner_id=USER_ID))
    db.add(Spec(
        id=ids["spec"], board_id=ids["board"], title="Original spec", status=spec_status,
        created_by=USER_ID, functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[], business_rules=[], api_contracts=[],
    ))
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
        status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
    ))
    db.add(Card(
        id=ids["bug"], board_id=ids["board"], spec_id=ids["spec"], title="Bug",
        status=CardStatus.NOT_STARTED, card_type=CardType.BUG, origin_task_id=ids["origin"],
        severity=BugSeverity.MAJOR, expected_behavior="ok", observed_behavior="bad",
        created_by=USER_ID,
    ))
    await db.flush()
    return ids


# ---------------------------------------------------------------------------
# Orchestrator (AmendmentRevisionApiService) — logic + structured errors.
# ---------------------------------------------------------------------------


async def test_create_lists_gets_and_associates():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        svc = AmendmentRevisionApiService(db)

        created = await svc.create(
            board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            origin_task_ids=[ids["origin"]], regression_scenario_ids=["ts_a"],
        )
        assert created["status"] == "draft"
        assert created["lineage_state"] == "incomplete"
        assert created["origin_bug_id"] == ids["bug"]
        assert created["original_spec_id"] == ids["spec"]
        assert created["eligibility"]["blocked"] is True  # draft is blocking
        amendment_id = created["id"]

        listed = await svc.list_for_bug(board_id=ids["board"], bug_id=ids["bug"])
        assert [r["id"] for r in listed["revisions"]] == [amendment_id]
        # FR2: bug-level Path B resolution payload exposed.
        assert "coverage_state" in listed["path_b_resolution"]
        assert "missing_links" in listed["path_b_resolution"]
        assert "safe_next_actions" in listed["path_b_resolution"]

        got = await svc.get(board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id)
        assert got["id"] == amendment_id

        associated = await svc.associate(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id,
            actor=USER_ID, regression_test_task_ids=["tc_1"], regression_scenario_ids=["ts_a", "ts_b"],
        )
        # additive + de-dup (ts_a already present).
        assert associated["regression_test_task_ids"] == ["tc_1"]
        assert associated["regression_scenario_ids"] == ["ts_a", "ts_b"]


async def test_create_rejects_in_progress_spec():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db, spec_status=SpecStatus.IN_PROGRESS)
        with pytest.raises(AmendmentRevisionApiError) as exc:
            await AmendmentRevisionApiService(db).create(
                board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            )
    assert exc.value.code == "original_spec_not_done_or_locked"
    assert exc.value.status_code == 409
    # fr_58b6aa0b: actionable hint — in_progress without lock is still editable.
    assert "still editable" in exc.value.message
    assert exc.value.details.get("content_locked") is False


async def _lock_spec(db, spec_id, *, outcome="success", dangling=False):
    """Mark a spec content-locked (current_validation_id -> success validation)."""
    spec = await db.get(Spec, spec_id)
    vid = f"val_{uuid.uuid4().hex[:8]}"
    spec.current_validation_id = vid
    spec.validations = (
        []
        if dangling
        else [{"id": vid, "outcome": outcome, "edition": spec.edition}]
    )
    await db.flush()
    return vid


async def test_create_accepts_in_progress_content_locked_spec():
    # spec 62cf2d36 fr_0d2f84a1 / ac_b81f927c: in_progress + current_validation_id
    # pointing to an outcome=success validation IS Path-B eligible.
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db, spec_status=SpecStatus.IN_PROGRESS)
        await _lock_spec(db, ids["spec"])
        created = await AmendmentRevisionApiService(db).create(
            board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            origin_task_ids=[ids["origin"]], regression_scenario_ids=["ts_a"],
        )
        assert created["status"] == "draft"
        assert created["original_spec_id"] == ids["spec"]


async def test_path_b_docs_distinguish_content_locked_in_progress():
    # fr_6348d040: agent-facing docs/tool-contracts must distinguish
    # in_progress-editable from in_progress-content-locked; the stale absolute
    # "only done/validated" / "rejects in_progress" claims must not survive.
    import re
    from pathlib import Path

    base = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core" / "mcp"
    files = {
        "errors.md": base / "resources" / "reference" / "errors.md",
        "cards.md": base / "resources" / "workflows" / "cards.md",
        "card.md": base / "resources" / "reference" / "tool-docs" / "card.md",
        "misc.md": base / "resources" / "reference" / "tool-docs" / "misc.md",
        "server.py": base / "server.py",
    }
    # Whitespace- AND backtick-normalized so markdown formatting can't hide a
    # stale claim (this is how a backticked checklist variant slipped through once).
    stale = [
        "binds to the bug's own done/validated (locked) spec and always",
        "rejects creating against an in_progress spec",
        "only attach to a done/validated (locked) spec",
        "amendment revision for a bug tied to a locked spec.",
        "if it is still in_progress, edit the spec directly",
        "binds to the bug's own locked spec and starts as draft",
    ]
    for name, path in files.items():
        raw = path.read_text(encoding="utf-8").replace("`", "")
        norm = re.sub(r"\s+", " ", raw).lower()
        assert "content-lock" in norm, f"{name}: content-lock guidance missing"
        for phrase in stale:
            assert phrase not in norm, f"{name}: stale Path B text survived: {phrase!r}"


async def test_create_rejects_in_progress_failed_stale_or_superseded():
    # ac_6e16f722: current_validation_id pointing to a failed validation, or a
    # dangling/stale pointer, is NOT a content lock -> still rejected.
    from okto_pulse.core.infra.database import get_session_factory

    for kw in ({"outcome": "failed"}, {"dangling": True}):
        async with get_session_factory()() as db:
            ids = await _seed(db, spec_status=SpecStatus.IN_PROGRESS)
            await _lock_spec(db, ids["spec"], **kw)
            with pytest.raises(AmendmentRevisionApiError) as exc:
                await AmendmentRevisionApiService(db).create(
                    board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
                )
        assert exc.value.code == "original_spec_not_done_or_locked", kw


async def test_create_rejects_spec_mismatch_and_bad_status_and_non_bug():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        svc = AmendmentRevisionApiService(db)

        with pytest.raises(AmendmentRevisionApiError) as e1:
            await svc.create(board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
                             original_spec_id="some-other-spec")
        assert e1.value.code == "bug_spec_mismatch"

        with pytest.raises(AmendmentRevisionApiError) as e2:
            await svc.create(board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
                             initial_status="approved")
        assert e2.value.code == "invalid_initial_status"

        with pytest.raises(AmendmentRevisionApiError) as e3:
            await svc.create(board_id=ids["board"], bug_id="no-such-bug", author=USER_ID)
        assert e3.value.code == "bug_not_found"

        # a NORMAL card is not a bug.
        with pytest.raises(AmendmentRevisionApiError) as e4:
            await svc.create(board_id=ids["board"], bug_id=ids["origin"], author=USER_ID)
        assert e4.value.code == "not_bug_card"


async def test_associate_and_get_reject_foreign_or_empty():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        svc = AmendmentRevisionApiService(db)
        created = await svc.create(board_id=ids["board"], bug_id=ids["bug"], author=USER_ID)
        amendment_id = created["id"]

        # empty associate -> no silent no-op.
        with pytest.raises(AmendmentRevisionApiError) as e_empty:
            await svc.associate(board_id=ids["board"], bug_id=ids["bug"],
                                amendment_id=amendment_id, actor=USER_ID)
        assert e_empty.value.code == "no_artifacts_to_associate"

        # amendment not found.
        with pytest.raises(AmendmentRevisionApiError) as e_nf:
            await svc.get(board_id=ids["board"], bug_id=ids["bug"], amendment_id="no-amd")
        assert e_nf.value.code == "amendment_not_found"

        # a DIFFERENT bug cannot see/associate this amendment (no reparenting leak).
        other = await _seed(db)
        with pytest.raises(AmendmentRevisionApiError) as e_mm:
            await svc.get(board_id=other["board"], bug_id=other["bug"], amendment_id=amendment_id)
        assert e_mm.value.code in ("amendment_bug_mismatch", "bug_not_found")


async def test_reject_bypass_fields_and_request_model_forbids_extra():
    for field in ("skip_gate", "override_gate", "bypass", "force"):
        with pytest.raises(AmendmentRevisionApiError) as exc:
            reject_bypass_fields({field: True})
        assert exc.value.code == "gate_bypass_not_allowed"
    # the REST request model forbids ANY unknown field (defence in depth).
    with pytest.raises(ValidationError):
        AmendmentRevisionCreateRequest.model_validate({"skip_gate": True})
    # valid fields parse fine.
    ok = AmendmentRevisionCreateRequest.model_validate({"regression_scenario_ids": ["ts"]})
    assert ok.regression_scenario_ids == ["ts"]


# ---------------------------------------------------------------------------
# MCP twin (ir_54ceb69b) — same orchestrator, same shapes, fail-closed.
# ---------------------------------------------------------------------------


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "amendment api agent"
        self.permissions = list(ALL_FLAGS)


async def _call(name: str, **kwargs) -> dict:
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def test_mcp_twin_create_list_get_associate():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        await db.commit()

    created = await _call(
        "okto_pulse_create_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], origin_task_ids=[ids["origin"]],
        regression_scenario_ids=["ts_a"],
    )
    assert created.get("success") is True, created
    rev = created["amendment_revision"]
    assert rev["status"] == "draft" and rev["origin_bug_id"] == ids["bug"]
    amendment_id = rev["id"]

    listed = await _call("okto_pulse_list_amendment_revisions", board_id=ids["board"], bug_id=ids["bug"])
    assert [r["id"] for r in listed["revisions"]] == [amendment_id]
    assert "coverage_state" in listed["path_b_resolution"]

    got = await _call("okto_pulse_get_amendment_revision",
                      board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id)
    assert got["amendment_revision"]["id"] == amendment_id

    associated = await _call(
        "okto_pulse_associate_amendment_revision_artifacts",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amendment_id,
        regression_test_task_ids=["tc_1"],
    )
    assert associated["success"] is True
    assert associated["amendment_revision"]["regression_test_task_ids"] == ["tc_1"]


async def test_mcp_twin_returns_structured_error_not_raw_exception():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db, spec_status=SpecStatus.IN_PROGRESS)
        await db.commit()

    result = await _call(
        "okto_pulse_create_amendment_revision", board_id=ids["board"], bug_id=ids["bug"]
    )
    # AC3: structured payload, no raw exception text.
    assert result["code"] == "original_spec_not_done_or_locked"
    assert result["status_code"] == 409
    assert "message" in result


async def test_mcp_create_tool_has_no_bypass_param():
    # FR5: the MCP create tool has NO skip/override/bypass parameter — an agent
    # literally cannot ask for a gate bypass through the twin.
    import inspect

    from okto_pulse.core.services.amendment_revision_api import BYPASS_FIELD_NAMES

    tool = await mcp_server.mcp.get_tool("okto_pulse_create_amendment_revision")
    params = set(inspect.signature(tool.fn).parameters)
    assert not (params & BYPASS_FIELD_NAMES)


# ---------------------------------------------------------------------------
# Card 62f6f196 (TEST UI/MCP) — Path B blocked + coverage states surfaced by the
# agent-facing MCP/API payload. AC3/TS1 (ts_b6d87391): a blocked bug exposes a
# create OR associate amendment-lineage safe action plus the
# missing_amendment_revision reason — never a bypass. AC4/TS2 (ts_5b0f1272):
# lineage-eligible-but-unconfirmed is coverage_pending and never closure-ready.
# Reproduce:
#   .venv/Scripts/python -m pytest -p no:logging -q tests/test_amendment_revision_api.py
# ---------------------------------------------------------------------------

# A safe action string must never read as a gate escape hatch.
_BYPASS_TOKENS = ("skip", "bypass", "override", "force")


async def _seed_cross_spec_bug(db, *, amendment_kwargs=None):
    """Seed a bug whose only regression evidence is a CROSS-SPEC scenario (on
    another spec). ``amendment_kwargs=None`` leaves the bug with NO amendment
    (the ``missing_amendment_revision`` state); a dict seeds an
    ``AmendmentHotfixRevision`` (defaults to done + complete lineage)."""
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"xb-board-{suffix}",
        "spec": f"xb-spec-{suffix}",
        "other_spec": f"xb-other-{suffix}",
        "origin": f"xb-origin-{suffix}",
        "bug": f"xb-bug-{suffix}",
        "test": f"xb-test-{suffix}",
        "amendment": f"xb-amd-{suffix}",
        "foreign_scenario": f"ts-foreign-{suffix}",
    }
    now = datetime.now(timezone.utc)
    db.add(Board(id=ids["board"], name="Cross-spec Board", owner_id=USER_ID))
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
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
        status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
        created_at=now - timedelta(minutes=5),
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
    # AmendmentHotfixRevision intentionally exposes only scalar lineage fields
    # (no ORM relationship to Board), so the unit of work cannot infer insert
    # ordering from an object relationship.  Materialize the real board/spec/card
    # parents before the FK-backed amendment row is added.
    await db.flush()
    if amendment_kwargs is not None:
        base = dict(
            id=ids["amendment"], board_id=ids["board"],
            original_spec_id=ids["spec"], origin_bug_id=ids["bug"],
            status=AmendmentRevisionStatus.DONE,
            lineage_state=AmendmentLineageState.COMPLETE,
            origin_task_ids=[ids["origin"]], affected_task_ids=[],
            regression_scenario_ids=[ids["foreign_scenario"]],
            regression_test_task_ids=[ids["test"]], automated_regression_refs=[],
            created_by=USER_ID,
        )
        base.update(amendment_kwargs)
        db.add(AmendmentHotfixRevision(**base))
    await db.flush()
    return ids


async def test_ts_b6d87391_blocked_missing_amendment_exposes_create_action():
    """TS1a (ts_b6d87391): a cross-spec bug with NO amendment is blocked by
    missing_amendment_revision. The resolve payload (API + MCP twin) exposes the
    create_amendment_revision safe action and the reason code — and NOT an
    associate action (no amendment yet) nor any bypass."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        await db.commit()

        preview = await BugRegressionScenarioPreviewService(db).resolve(
            board_id=ids["board"], bug_id=ids["bug"],
            candidate_scenario_ids=[ids["foreign_scenario"]],
        )

    # Reason code sufficient: the agent learns it lacks amendment lineage.
    assert preview["coverage_state"] == "not_applicable"
    assert preview["semantic_gap_required"] is True
    assert any(
        r["reason"] == "missing_amendment_revision" for r in preview["rejected_scenarios"]
    )
    assert "amendment_revision" in preview["missing_links"]
    # Operational safe action (create) + backcompat escalate; NO associate
    # (nothing to associate to yet); NO bypass token anywhere.
    actions = preview["safe_next_actions"]
    assert "create_amendment_revision" in actions
    assert "escalate_semantic_gap" in actions
    assert "associate_amendment_revision_artifacts" not in actions
    assert not any(tok in a for a in actions for tok in _BYPASS_TOKENS)

    # MCP twin exposes the SAME create action + reason (parity, no exception text).
    twin = await _call(
        "okto_pulse_resolve_bug_regression_scenarios",
        board_id=ids["board"], bug_id=ids["bug"],
        candidate_scenario_ids=[ids["foreign_scenario"]],
    )
    assert "create_amendment_revision" in twin.get("safe_next_actions", []), twin
    assert any(
        r["reason"] == "missing_amendment_revision" for r in twin["rejected_scenarios"]
    )


async def test_ts_b6d87391_blocked_draft_amendment_exposes_associate_action():
    """TS1b (ts_b6d87391): when a (still-blocking) draft amendment already exists,
    the list_for_bug payload exposes BOTH create and associate amendment-lineage
    safe actions, so the agent can complete the existing revision or start a new
    one. Still blocked, still no bypass."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(
            db, amendment_kwargs={"status": AmendmentRevisionStatus.DRAFT}
        )
        await db.commit()
        listed = await AmendmentRevisionApiService(db).list_for_bug(
            board_id=ids["board"], bug_id=ids["bug"]
        )

    res = listed["path_b_resolution"]
    actions = res["safe_next_actions"]
    assert "create_amendment_revision" in actions
    assert "associate_amendment_revision_artifacts" in actions
    assert "escalate_semantic_gap" in actions
    # A blocking draft is never closure-ready.
    assert res["coverage_state"] != "path_b_ready"
    assert not any(tok in a for a in actions for tok in _BYPASS_TOKENS)

    # MCP twin (list tool) carries the same safe actions.
    twin = await _call(
        "okto_pulse_list_amendment_revisions", board_id=ids["board"], bug_id=ids["bug"]
    )
    twin_actions = twin["path_b_resolution"]["safe_next_actions"]
    assert "create_amendment_revision" in twin_actions
    assert "associate_amendment_revision_artifacts" in twin_actions


async def test_ts_5b0f1272_lineage_eligible_unconfirmed_is_coverage_pending():
    """TS2 (ts_5b0f1272): a fully lineage-eligible amendment (done + complete +
    declares the artifact + authoritative task membership) WITHOUT a validator
    coverage confirmation is coverage_pending — blocking, never closure-ready.
    The safe action is the validator confirmation, never a bypass."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs={})  # done + complete
        await db.commit()
        listed = await AmendmentRevisionApiService(db).list_for_bug(
            board_id=ids["board"], bug_id=ids["bug"]
        )

    res = listed["path_b_resolution"]
    assert res["coverage_state"] == "coverage_pending"
    assert res["coverage_state"] != "path_b_ready"  # eligible lineage is NOT closure-ready
    assert ids["foreign_scenario"] in res["coverage_pending_scenarios"]
    actions = res["safe_next_actions"]
    assert actions == ["confirm_validator_coverage"]
    assert not any(tok in a for a in actions for tok in _BYPASS_TOKENS)

    # MCP twin agrees: pending, not closure-ready.
    twin = await _call(
        "okto_pulse_list_amendment_revisions", board_id=ids["board"], bug_id=ids["bug"]
    )
    assert twin["path_b_resolution"]["coverage_state"] == "coverage_pending"


# ---------------------------------------------------------------------------
# Card 14ddfab0 — agent-facing amendment lifecycle (transition_lifecycle): the
# missing MCP/API step that promotes a created/associated amendment to
# approved/done + complete lineage. Fail-closed; NEVER writes coverage.
# ---------------------------------------------------------------------------


async def _create_and_associate(db, ids):
    """create(draft) + associate the regression test task — the pre-promotion state."""
    api = AmendmentRevisionApiService(db)
    created = await api.create(
        board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
        origin_task_ids=[ids["origin"]], regression_scenario_ids=[ids["foreign_scenario"]],
    )
    await api.associate(
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=created["id"],
        actor=USER_ID, regression_test_task_ids=[ids["test"]],
    )
    return created["id"]


async def test_transition_lifecycle_promotes_to_done_complete_via_agent_surface():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        amd_id = await _create_and_associate(db, ids)
        api = AmendmentRevisionApiService(db)

        after_lineage = await api.transition_lifecycle(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
            actor=USER_ID, lineage_state="complete",
        )
        assert after_lineage["lineage_state"] == "complete"
        after_status = await api.transition_lifecycle(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
            actor=USER_ID, status="done",
        )
        assert after_status["status"] == "done"
        assert after_status["lineage_state"] == "complete"


async def test_transition_rejects_unknown_status_and_lineage():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        amd_id = await _create_and_associate(db, ids)
        api = AmendmentRevisionApiService(db)
        with pytest.raises(AmendmentRevisionApiError) as e1:
            await api.transition_lifecycle(
                board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
                actor=USER_ID, status="frozen",
            )
        assert e1.value.code == "invalid_amendment_status"
        with pytest.raises(AmendmentRevisionApiError) as e2:
            await api.transition_lifecycle(
                board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
                actor=USER_ID, lineage_state="mostly",
            )
        assert e2.value.code == "invalid_lineage_state"


async def test_transition_complete_lineage_requires_artifacts():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        # create WITHOUT associating a regression test task (artifacts insufficient).
        api = AmendmentRevisionApiService(db)
        created = await api.create(
            board_id=ids["board"], bug_id=ids["bug"], author=USER_ID,
            origin_task_ids=[ids["origin"]], regression_scenario_ids=[ids["foreign_scenario"]],
        )
        with pytest.raises(AmendmentRevisionApiError) as e:
            await api.transition_lifecycle(
                board_id=ids["board"], bug_id=ids["bug"], amendment_id=created["id"],
                actor=USER_ID, lineage_state="complete",
            )
        assert e.value.code == "incomplete_lineage_artifacts"


async def test_transition_cannot_promote_without_complete_lineage():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        amd_id = await _create_and_associate(db, ids)  # lineage still incomplete
        api = AmendmentRevisionApiService(db)
        with pytest.raises(AmendmentRevisionApiError) as e:
            await api.transition_lifecycle(
                board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
                actor=USER_ID, status="done",
            )
        assert e.value.code == "cannot_promote_incomplete_lineage"


async def test_transition_terminal_state_cannot_resurrect():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        amd_id = await _create_and_associate(db, ids)
        api = AmendmentRevisionApiService(db)
        cancelled = await api.transition_lifecycle(
            board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
            actor=USER_ID, status="cancelled",
        )
        assert cancelled["status"] == "cancelled"
        with pytest.raises(AmendmentRevisionApiError) as e:
            await api.transition_lifecycle(
                board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id,
                actor=USER_ID, status="done",
            )
        assert e.value.code == "terminal_amendment_revision"


@pytest.mark.parametrize("terminal_status", ["cancelled", "superseded"])
async def test_terminal_revision_api_is_monotonic_and_returns_typed_409(
    terminal_status,
):
    """Every API mutation is blocked after terminality, not only promotion."""
    from sqlalchemy import select

    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed(db)
        api = AmendmentRevisionApiService(db)
        created = await api.create(
            board_id=ids["board"],
            bug_id=ids["bug"],
            author=USER_ID,
        )
        amendment_id = created["id"]
        terminal = await api.transition_lifecycle(
            board_id=ids["board"],
            bug_id=ids["bug"],
            amendment_id=amendment_id,
            actor=USER_ID,
            status=terminal_status,
        )
        before_updated_at = terminal["updated_at"]
        audit_count = len(
            (
                await db.execute(
                    select(ActivityLog).where(
                        ActivityLog.board_id == ids["board"]
                    )
                )
            )
            .scalars()
            .all()
        )

        # An exact terminal-status retry succeeds without a persistence/audit
        # side effect.  It is the only accepted operation after terminality.
        retry = await api.transition_lifecycle(
            board_id=ids["board"],
            bug_id=ids["bug"],
            amendment_id=amendment_id,
            actor=USER_ID,
            status=terminal_status,
        )
        assert retry["status"] == terminal_status
        assert datetime.fromisoformat(retry["updated_at"]).replace(
            tzinfo=None
        ) == datetime.fromisoformat(before_updated_at).replace(tzinfo=None)

        other_terminal = (
            "superseded" if terminal_status == "cancelled" else "cancelled"
        )
        lifecycle_mutations = [
            {"status": "draft"},
            {"status": "review"},
            {"status": other_terminal},
            {"lineage_state": "complete"},
            {"lineage_state": "incomplete"},
            {"status": terminal_status, "lineage_state": "incomplete"},
        ]
        for mutation in lifecycle_mutations:
            with pytest.raises(AmendmentRevisionApiError) as exc:
                await api.transition_lifecycle(
                    board_id=ids["board"],
                    bug_id=ids["bug"],
                    amendment_id=amendment_id,
                    actor=USER_ID,
                    **mutation,
                )
            assert exc.value.code == "terminal_amendment_revision", mutation
            assert exc.value.status_code == 409
            assert exc.value.to_dict()["details"] == {
                "amendment_id": amendment_id,
                "current_status": terminal_status,
                "mutation_applied": False,
            }

        with pytest.raises(AmendmentRevisionApiError) as associate_exc:
            await api.associate(
                board_id=ids["board"],
                bug_id=ids["bug"],
                amendment_id=amendment_id,
                actor=USER_ID,
                regression_test_task_ids=["test-1"],
            )
        assert associate_exc.value.code == "terminal_amendment_revision"
        assert associate_exc.value.status_code == 409
        assert associate_exc.value.details["mutation_applied"] is False

        assert (
            len(
                (
                    await db.execute(
                        select(ActivityLog).where(
                            ActivityLog.board_id == ids["board"]
                        )
                    )
                )
                .scalars()
                .all()
            )
            == audit_count
        )


async def test_transition_lifecycle_tool_has_no_coverage_or_bypass_param():
    # FR5/G2: the lifecycle MCP tool structurally accepts ONLY status + lineage_state
    # — it can never carry coverage_confirmation/coverage_confirmed nor a bypass field.
    import inspect

    from okto_pulse.core.services.amendment_revision_api import BYPASS_FIELD_NAMES

    tool = await mcp_server.mcp.get_tool("okto_pulse_transition_amendment_revision")
    params = set(inspect.signature(tool.fn).parameters)
    assert params == {"board_id", "bug_id", "amendment_id", "status", "lineage_state"}
    assert "coverage_confirmation" not in params and "coverage_confirmed" not in params
    assert not (params & BYPASS_FIELD_NAMES)


async def test_mcp_twin_transition_amendment_revision():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_cross_spec_bug(db, amendment_kwargs=None)
        amd_id = await _create_and_associate(db, ids)
        await db.commit()

    done = await _call(
        "okto_pulse_transition_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id, lineage_state="complete",
    )
    assert done["success"] is True
    assert done["amendment_revision"]["lineage_state"] == "complete"
    promoted = await _call(
        "okto_pulse_transition_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id, status="done",
    )
    assert promoted["amendment_revision"]["status"] == "done"
    # structured error twin (unknown status), never a raw exception.
    err = await _call(
        "okto_pulse_transition_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd_id, status="frozen",
    )
    assert err["code"] == "invalid_amendment_status"


# ---------------------------------------------------------------------------
# BUG-03 (spec e5f61c7f) — the MCP confirm handler must PRESERVE the structured
# CardOperationError (e.g. coverage_not_gate_consumable with bounded facts)
# instead of degrading it to a textual {"error": str(e)} via the ValueError arm.
# ---------------------------------------------------------------------------


async def _seed_inert_confirm(db):
    """A bug whose only regression scenario is a SAME-SPEC AC scenario NOT linked
    to its lineage (Path A unrelated). The board owner is USER_ID so the MCP
    `_Ctx` agent passes the validator critical-context guard."""
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"c3-board-{suffix}", "spec": f"c3-spec-{suffix}",
        "origin": f"c3-origin-{suffix}", "bug": f"c3-bug-{suffix}",
        "test": f"c3-test-{suffix}", "scenario": f"ts-samespec-{suffix}",
    }
    db.add(Board(id=ids["board"], name="C3 Board", owner_id=USER_ID))
    db.add(Spec(
        id=ids["spec"], board_id=ids["board"], title="Bug spec", status=SpecStatus.DONE,
        created_by=USER_ID, functional_requirements=["FR1"], acceptance_criteria=["AC1"],
        test_scenarios=[{
            "id": ids["scenario"], "title": "AC scenario", "status": "automated",
            "evidence": {"test_file_path": "tests/test_reg.py", "test_function": "test_reg"},
        }],
        business_rules=[], api_contracts=[],
    ))
    db.add(Card(
        id=ids["origin"], board_id=ids["board"], spec_id=ids["spec"], title="Origin",
        status=CardStatus.DONE, card_type=CardType.NORMAL, created_by=USER_ID,
        test_scenario_ids=[],
    ))
    db.add(Card(
        id=ids["bug"], board_id=ids["board"], spec_id=ids["spec"], title="Bug",
        status=CardStatus.NOT_STARTED, card_type=CardType.BUG, origin_task_id=ids["origin"],
        severity=BugSeverity.MAJOR, expected_behavior="ok", observed_behavior="bad",
        linked_test_task_ids=[ids["test"]], created_by=USER_ID,
    ))
    db.add(Card(
        id=ids["test"], board_id=ids["board"], spec_id=ids["spec"], title="Regression test",
        status=CardStatus.DONE, card_type=CardType.TEST,
        test_scenario_ids=[ids["scenario"]], created_by=USER_ID,
    ))
    await db.flush()
    return ids


async def test_mcp_confirm_preserves_structured_coverage_not_gate_consumable():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ids = await _seed_inert_confirm(db)
        await db.commit()

    # Build a done/complete amendment that DECLARES the same-spec scenario + test
    # task and CLAIMS the origin (intersecting the bug authoritative set) — a
    # syntactically valid but gate-inert tuple.
    created = await _call(
        "okto_pulse_create_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], origin_task_ids=[ids["origin"]],
        regression_scenario_ids=[ids["scenario"]],
    )
    amd = created["amendment_revision"]["id"]
    await _call(
        "okto_pulse_associate_amendment_revision_artifacts",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd,
        regression_test_task_ids=[ids["test"]],
    )
    await _call(
        "okto_pulse_transition_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd, lineage_state="complete",
    )
    await _call(
        "okto_pulse_transition_amendment_revision",
        board_id=ids["board"], bug_id=ids["bug"], amendment_id=amd, status="done",
    )

    result = await _call(
        "okto_pulse_confirm_amendment_coverage",
        board_id=ids["board"], amendment_id=amd,
        regression_test_task_id=ids["test"], regression_scenario_id=ids["scenario"],
    )

    # The handler serialized the STRUCTURED error, not a degraded textual one.
    assert result.get("code") == "coverage_not_gate_consumable", result
    facts = result.get("facts") or {}
    assert facts.get("routed_path") == "path_a"
    assert facts.get("resolver_reason") == "unrelated_scenario"
    assert facts.get("bug_id") == ids["bug"]
    # Regression guard: a degraded handler would return ONLY {"error": "<text>"}.
    assert set(result) != {"error"}
