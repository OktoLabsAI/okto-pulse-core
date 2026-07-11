"""Spec 3a006f65 / card 0192f58d — MockupDesignSystemGate (FR6/FR7/FR8/FR9,
AC6-AC10), scenarios ts_a43eadd2 (blocking) + ts_e137ccd9 (advisory).

Validator KILL vectors covered:
  K1 BOTH write paths: MCP (okto_pulse_add_screen_mockup) AND service (SpecService.update_spec).
  K2 real persisted identity: decision derives from get_board_effective_design_system;
     synthetic / non-existent / dangling-link refs reject; payload is only a cross-check.
  K3 canonical gate mode: only BoardSettings.design_system_gate_mode; blocking beats an
     advisory mirror in the snapshot/ref.
  K4 blocking BEFORE persist (zero partial write); advisory is a separate branch (persist
     + warning + queryable audit). off / NO-DS-configured passes (AC8); dangling DS fail-closed.
  K5 no 4th Resource Gate type.

Boards are built directly (not via create_board) so a leaked active default template can
never inject an effective Design System and skew the no-DS vectors. Single-session tests
do not commit (rolled back at close — gotcha ts_cdb70cc0); MCP tests commit + clean up.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_mockup_design_system_gate.py
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import delete, select
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    BoardDesignSystem,
    DesignSystem,
    DesignSystemGateAudit,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import ScreenMockup, SpecUpdate
from okto_pulse.core.services.design_system import (
    DesignSystemError,
    DesignSystemService,
    MockupDesignSystemGate,
)
from okto_pulse.core.services.main import SpecService

pytestmark = pytest.mark.asyncio

USER_ID = "mockup-gate-user"


class _Ctx:
    agent_id = USER_ID
    permissions: list = []


async def _call(name: str, **kwargs) -> dict:
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def _board(db, gate_mode: str = "off", *, settings_extra: dict | None = None) -> Board:
    board = Board(
        id=str(uuid.uuid4()),
        name=f"b-{uuid.uuid4().hex[:8]}",
        owner_id=USER_ID,
        settings={"design_system_gate_mode": gate_mode, **(settings_extra or {})},
    )
    db.add(board)
    await db.flush()
    return board


async def _global_ds(db, title: str = "DS") -> DesignSystem:
    return await DesignSystemService(db).create_design_system(USER_ID, title=title, scope="global")


async def _link(db, board_id: str, ds_id: str):
    return await DesignSystemService(db).link_design_system_to_board(board_id, ds_id)


async def _spec(db, board_id: str) -> Spec:
    spec = Spec(
        id=str(uuid.uuid4()), board_id=board_id, title="Gate Spec",
        status=SpecStatus.DRAFT, created_by=USER_ID,
    )
    db.add(spec)
    await db.flush()
    return spec


def _mk(ds_id=None, version=None, evidence=None, **extra) -> dict:
    screen = {
        "id": "sm_" + uuid.uuid4().hex[:8], "title": "S", "html_content": "<div/>",
        "design_system_ref": {"design_system_id": ds_id, "version": version} if ds_id else None,
        "design_system_evidence": evidence,
    }
    screen.update(extra)
    return screen


# ---------------------------------------------------------------------------
# ts_a43eadd2 — blocking rejects all invalid vectors BEFORE persistence
# ---------------------------------------------------------------------------


async def test_ts_a43eadd2_blocking_rejects_every_invalid_vector():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)  # effective version = ds.version (1)
        other = await _global_ds(db, "Other")  # real but NOT the board's effective DS
        gate = MockupDesignSystemGate(db)

        async def reject(screen, code):
            with pytest.raises(DesignSystemError) as exc:
                await gate.evaluate_screen(board.id, screen, entity_type="spec", entity_id="x")
            assert exc.value.code == code

        await reject(_mk(), "design_system_required")  # no ref
        await reject(_mk(ds_id="synthetic-id", version=1, evidence="e"), "design_system_not_found")
        await reject(_mk(ds_id=other.id, version=1, evidence="e"), "design_system_not_found")  # real != effective
        await reject(_mk(ds_id=ds.id, version=99, evidence="e"), "design_system_version_mismatch")
        await reject(_mk(ds_id=ds.id, version=ds.version), "design_system_evidence_missing")  # no evidence

        # a VALID mockup (real effective identity + version + evidence) passes.
        ok = await gate.evaluate_screen(
            board.id, _mk(ds_id=ds.id, version=ds.version, evidence="figma://proof"),
            entity_type="spec", entity_id="x",
        )
        assert ok["outcome"] == "pass"


async def test_blocking_rejects_before_persistence_via_mcp():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        spec = await _spec(db, board.id)
        await db.commit()
        ids = {"board": board.id, "spec": spec.id, "ds": ds.id}

    try:
        # blocking + no evidence -> structured error, mockup NOT persisted.
        err = await _call(
            "okto_pulse_add_screen_mockup", board_id=ids["board"], entity_id=ids["spec"],
            entity_type="spec", title="M", design_system_ref=ids["ds"], design_system_version=1,
        )
        assert err["code"] == "design_system_evidence_missing"

        from okto_pulse.core.infra.database import get_session_factory as gsf
        async with gsf()() as db:
            spec = await db.get(Spec, ids["spec"])
            assert not (spec.screen_mockups or [])  # zero partial write

        # valid submission succeeds + persists.
        ok = await _call(
            "okto_pulse_add_screen_mockup", board_id=ids["board"], entity_id=ids["spec"],
            entity_type="spec", title="M", design_system_ref=ids["ds"], design_system_version=1,
            design_system_evidence="figma://proof",
        )
        assert ok["success"] is True and ok["design_system_gate"]["outcome"] == "pass"
        async with gsf()() as db:
            spec = await db.get(Spec, ids["spec"])
            assert len(spec.screen_mockups or []) == 1
    finally:
        from okto_pulse.core.infra.database import get_session_factory as gsf
        async with gsf()() as db:
            await db.execute(delete(BoardDesignSystem).where(BoardDesignSystem.board_id == ids["board"]))
            await db.execute(delete(DesignSystem).where(DesignSystem.owner_id == USER_ID))
            await db.execute(delete(Spec).where(Spec.id == ids["spec"]))
            await db.execute(delete(Board).where(Board.id == ids["board"]))
            await db.commit()


# ---------------------------------------------------------------------------
# K1 service path — SpecService.update_spec gates the bulk write pre-persist
# ---------------------------------------------------------------------------


async def test_service_path_update_spec_blocks_synthetic_ref():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        spec = await _spec(db, board.id)

        # a synthetic-ref mockup pushed through the bulk update path must reject
        # BEFORE persistence (this is the frontend/REST bypass the validator flagged).
        with pytest.raises(DesignSystemError) as exc:
            await SpecService(db).update_spec(
                spec.id, USER_ID,
                SpecUpdate(screen_mockups=[ScreenMockup(
                    id="sm_x", title="S", html_content="<div/>",
                    design_system_ref={"design_system_id": "fake", "version": 1},
                    design_system_evidence="e",
                )]),
            )
        assert exc.value.code == "design_system_not_found"

        # nothing persisted on the spec.
        reloaded = await db.get(Spec, spec.id)
        assert not (reloaded.screen_mockups or [])

        # a valid mockup through the same path succeeds + persists.
        await SpecService(db).update_spec(
            spec.id, USER_ID,
            SpecUpdate(screen_mockups=[ScreenMockup(
                id="sm_ok", title="S", html_content="<div/>",
                design_system_ref={"design_system_id": ds.id, "version": ds.version},
                design_system_evidence="figma://proof",
            )]),
        )
        reloaded = await db.get(Spec, spec.id)
        assert [m["id"] for m in reloaded.screen_mockups] == ["sm_ok"]


# ---------------------------------------------------------------------------
# K4 — delta-only: legacy untouched mockup is safe; editing it (remove evidence) blocks
# ---------------------------------------------------------------------------


async def test_delta_only_legacy_safe_but_edit_removing_evidence_blocks():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "off")  # created while gate was off
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        spec = await _spec(db, board.id)
        # two compliant mockups persisted while off.
        m1 = {"id": "M1", "title": "M1", "html_content": "<a/>",
              "design_system_ref": {"design_system_id": ds.id, "version": ds.version},
              "design_system_evidence": "e1"}
        m2 = {"id": "M2", "title": "M2", "html_content": "<b/>",
              "design_system_ref": {"design_system_id": ds.id, "version": ds.version},
              "design_system_evidence": "e2"}
        spec.screen_mockups = [m1, m2]
        flag_modified(spec, "screen_mockups")
        await db.flush()

        # flip board to blocking.
        board.settings = {**board.settings, "design_system_gate_mode": "blocking"}
        flag_modified(board, "settings")
        await db.flush()

        # unrelated update (re-submit identical array) does NOT re-gate legacy -> no raise.
        await SpecService(db).update_spec(
            spec.id, USER_ID,
            SpecUpdate(screen_mockups=[ScreenMockup(**m1), ScreenMockup(**m2)]),
        )

        # editing M2 to REMOVE its evidence (same id) is in-delta -> blocks pre-persist.
        m2_broken = dict(m2, design_system_evidence=None)
        with pytest.raises(DesignSystemError) as exc:
            await SpecService(db).update_spec(
                spec.id, USER_ID,
                SpecUpdate(screen_mockups=[ScreenMockup(**m1), ScreenMockup(**m2_broken)]),
            )
        assert exc.value.code == "design_system_evidence_missing"


# ---------------------------------------------------------------------------
# ts_e137ccd9 — advisory persists + warning + queryable audit row
# ---------------------------------------------------------------------------


async def test_ts_e137ccd9_advisory_persists_with_warning_and_audit():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "advisory")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        gate = MockupDesignSystemGate(db)

        screen = _mk()  # no ref / no evidence
        screen["id"] = "sm_adv"
        outcome = await gate.evaluate_screen(board.id, screen, entity_type="spec", entity_id="sp1")
        assert outcome["outcome"] == "warning"
        assert outcome["reason"] == "design_system_required"
        assert outcome["expected_design_system_id"] == ds.id

        # the audit row is queryable (mockup_id, board_id, expected identity, reason).
        rows = (
            await db.execute(
                select(DesignSystemGateAudit).where(DesignSystemGateAudit.board_id == board.id)
            )
        ).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.mockup_id == "sm_adv" and row.reason == "design_system_required"
        assert row.expected_design_system_id == ds.id and row.mode == "advisory"


# ---------------------------------------------------------------------------
# AC8 + ruling: no-DS-configured passes even under blocking; dangling fail-closes
# ---------------------------------------------------------------------------


async def test_no_design_system_configured_passes_even_blocking():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")  # blocking but NO DS configured
        gate = MockupDesignSystemGate(db)
        outcome = await gate.evaluate_screen(board.id, _mk(), entity_type="spec", entity_id="x")
        assert outcome["outcome"] == "not_applicable"  # AC8 — never blocks a board with no DS


async def test_dangling_design_system_fail_closed_under_blocking():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        mapped_ds = await db.get(DesignSystem, ds.id)
        await db.delete(mapped_ds)  # link now dangles -> configured but broken
        await db.flush()
        gate = MockupDesignSystemGate(db)
        with pytest.raises(DesignSystemError) as exc:
            await gate.evaluate_screen(
                board.id, _mk(ds_id="anything", version=1, evidence="e"),
                entity_type="spec", entity_id="x",
            )
        assert exc.value.code == "design_system_not_found"


async def test_off_mode_never_blocks():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db, "off")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        gate = MockupDesignSystemGate(db)
        outcome = await gate.evaluate_screen(board.id, _mk(), entity_type="spec", entity_id="x")
        assert outcome["outcome"] == "not_applicable"


# ---------------------------------------------------------------------------
# K3 — canonical gate mode: BoardSettings blocking beats an advisory snapshot mirror
# ---------------------------------------------------------------------------


async def test_canonical_gate_mode_blocking_beats_advisory_mirror():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        ds = await _global_ds(db)
        # BoardSettings says blocking; the default_config_snapshot mirror says advisory.
        board = await _board(db, "blocking")
        board.default_config_snapshot = {
            "design_system": {"design_system_id": ds.id, "version": ds.version, "gate_mode": "advisory"}
        }
        flag_modified(board, "default_config_snapshot")
        await db.flush()
        gate = MockupDesignSystemGate(db)
        # blocking (canonical) wins over the advisory mirror -> raises, not a warning.
        with pytest.raises(DesignSystemError) as exc:
            await gate.evaluate_screen(board.id, _mk(), entity_type="spec", entity_id="x")
        assert exc.value.code == "design_system_required"


# ---------------------------------------------------------------------------
# Bypass closure (adversarial review) — CREATE + PROPAGATION write paths gated
# ---------------------------------------------------------------------------


async def test_create_spec_gates_embedded_mockups_at_creation():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.schemas import SpecCreate

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)

        # a synthetic-ref mockup embedded at SPEC CREATION must reject before persist
        # (the create twin of the update_spec gate — closes the create_spec bypass).
        with pytest.raises(DesignSystemError) as exc:
            await SpecService(db).create_spec(
                board.id, USER_ID,
                SpecCreate(title="S", screen_mockups=[ScreenMockup(
                    id="sm_x", title="S", html_content="<div/>",
                    design_system_ref={"design_system_id": "fake", "version": 1},
                    design_system_evidence="e",
                )]),
            )
        assert exc.value.code == "design_system_not_found"

        # a valid embedded mockup is accepted at creation.
        spec = await SpecService(db).create_spec(
            board.id, USER_ID,
            SpecCreate(title="S2", screen_mockups=[ScreenMockup(
                id="sm_ok", title="S", html_content="<div/>",
                design_system_ref={"design_system_id": ds.id, "version": ds.version},
                design_system_evidence="figma://proof",
            )]),
        )
        assert [m["id"] for m in spec.screen_mockups] == ["sm_ok"]


async def test_spec_to_card_propagation_gates_noncompliant_mockup():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Card, CardStatus
    from okto_pulse.core.services.spec_resource_propagation import SpecResourcePropagationService

    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        # a spec carrying a NON-compliant (legacy) mockup — e.g. added while off.
        spec = await _spec(db, board.id)
        spec.screen_mockups = [{"id": "leg", "title": "L", "html_content": "<i/>",
                                "design_system_ref": None, "design_system_evidence": None}]
        flag_modified(spec, "screen_mockups")
        card = Card(id=str(uuid.uuid4()), board_id=board.id, spec_id=spec.id,
                    title="C", status=CardStatus.NOT_STARTED, created_by=USER_ID)
        db.add(card)
        await db.flush()

        # auto-propagation onto a blocking board must NOT launder the legacy mockup.
        with pytest.raises(DesignSystemError) as exc:
            await SpecResourcePropagationService(db)._copy_mockups(spec, card)
        assert exc.value.code == "design_system_required"
        assert not ((await db.get(Card, card.id)).screen_mockups or [])  # nothing laundered


# ---------------------------------------------------------------------------
# REST twin (api_a5e13bc5) — same gate via the REST surface
# ---------------------------------------------------------------------------


async def test_rest_create_screen_mockup_gates_blocking_and_valid():
    from okto_pulse.community.api.screen_mockups import create_screen_mockup
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWork
    async with get_session_factory()() as db:
        board = await _board(db, "blocking")
        ds = await _global_ds(db)
        await _link(db, board.id, ds.id)
        spec = await _spec(db, board.id)

        # blocking + synthetic ref -> DesignSystemError (the app handler maps it to 422).
        with pytest.raises(DesignSystemError) as exc:
            await create_screen_mockup(
                "spec", spec.id,
                raw={"title": "M", "design_system_ref": "fake", "design_system_version": 1,
                     "design_system_evidence": "e"},
                db=SQLAlchemyUnitOfWork(db), actor=USER_ID,
            )
        assert exc.value.code == "design_system_not_found"
        assert not ((await db.get(Spec, spec.id)).screen_mockups or [])  # nothing persisted

        # valid submission through the REST endpoint succeeds + persists.
        result = await create_screen_mockup(
            "spec", spec.id,
            raw={"title": "M", "design_system_ref": ds.id, "design_system_version": ds.version,
                 "design_system_evidence": "figma://proof"},
            db=SQLAlchemyUnitOfWork(db), actor=USER_ID,
        )
        assert result["success"] is True and result["design_system_gate"]["outcome"] == "pass"
        assert len((await db.get(Spec, spec.id)).screen_mockups or []) == 1


async def test_cross_board_propagation_gates_against_destination_design_system():
    """A mockup valid for board X's Design System, copied to board Y (different DS,
    blocking), must be gated against Y's effective Design System (destination), not X's."""
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Card, CardStatus
    from okto_pulse.core.services.spec_resource_propagation import SpecResourcePropagationService

    async with get_session_factory()() as db:
        board_x = await _board(db, "off")  # source board, gate off
        board_y = await _board(db, "blocking")  # destination board, blocking
        ds_x = await _global_ds(db, "DS-X")
        ds_y = await _global_ds(db, "DS-Y")
        await _link(db, board_x.id, ds_x.id)
        await _link(db, board_y.id, ds_y.id)

        # a spec on board X with a mockup VALID for DS-X (ref=ds_x, evidence present).
        spec = await _spec(db, board_x.id)
        spec.screen_mockups = [{"id": "mx", "title": "X", "html_content": "<i/>",
                                "design_system_ref": {"design_system_id": ds_x.id, "version": ds_x.version},
                                "design_system_evidence": "x-proof"}]
        flag_modified(spec, "screen_mockups")
        # a card on the DESTINATION board Y (blocking, DS-Y).
        card = Card(id=str(uuid.uuid4()), board_id=board_y.id, spec_id=spec.id,
                    title="C", status=CardStatus.NOT_STARTED, created_by=USER_ID)
        db.add(card)
        await db.flush()

        # the X-valid mockup is NOT compliant with Y's effective DS -> rejected against Y.
        with pytest.raises(DesignSystemError) as exc:
            await SpecResourcePropagationService(db)._copy_mockups(spec, card)
        assert exc.value.code == "design_system_not_found"  # ds_x != ds_y (effective of Y)


def _screen_mockup_attribute_writers(core_dir) -> set:
    """Every function that DIRECTLY assigns ``<entity>.screen_mockups = ...`` anywhere
    under core — these are the persistence writers that MUST route through the gate."""
    import ast
    import pathlib

    writers: set = set()

    class _V(ast.NodeVisitor):
        def __init__(self, rel):
            self.rel = rel
            self.stack: list = []

        def visit_FunctionDef(self, node):
            self.stack.append(node.name)
            self.generic_visit(node)
            self.stack.pop()

        visit_AsyncFunctionDef = visit_FunctionDef

        def _check(self, targets):
            for t in targets:
                if isinstance(t, ast.Attribute) and t.attr == "screen_mockups" and self.stack:
                    writers.add((self.rel, self.stack[-1]))

        def visit_Assign(self, node):
            self._check(node.targets)
            self.generic_visit(node)

        def visit_AugAssign(self, node):
            self._check([node.target])
            self.generic_visit(node)

    for py in pathlib.Path(core_dir).rglob("*.py"):
        rel = py.relative_to(core_dir).as_posix()
        _V(rel).visit(ast.parse(py.read_text(encoding="utf-8")))
    return writers


async def test_screen_mockups_writer_census_all_gated():
    """Census guard (validator requirement): EVERY function that directly assigns
    entity.screen_mockups must be a known gated writer. A NEW ungated writer fails this
    test — forcing a conscious MockupDesignSystemGate decision. Each entry below was
    verified to call gate_entity_screen_mockups / MockupDesignSystemGate (directly or,
    for _propagate_story_mockups, in its caller convert_stories) before persistence."""
    import pathlib

    import okto_pulse.core as core_pkg

    core_dir = pathlib.Path(core_pkg.__file__).parent
    writers = _screen_mockup_attribute_writers(core_dir)
    expected = {
        ("application/artifact_propagation.py", "propagate_artifacts"),  # gated delta
        ("services/main.py", "_legacy_propagate_artifacts"),  # pending dead-code removal
        ("services/main.py", "create_spec"),               # gated (old=[] baseline)
        ("services/main.py", "create_story"),              # gated (old=[] baseline)
        ("services/main.py", "create_refinement"),         # gated manual (old=[]) + propagate_artifacts
        ("services/main.py", "_propagate_story_mockups"),  # gated in caller convert_stories (pre-flush)
        ("services/spec_resource_propagation.py", "_copy_mockups"),  # gated (delta vs card existing)
    }
    assert writers == expected, (
        f"Ungated or unexpected screen_mockups writer(s): new={writers - expected}, "
        f"missing={expected - writers}. Every direct screen_mockups writer must route "
        f"through gate_entity_screen_mockups (MockupDesignSystemGate)."
    )


async def test_no_fourth_resource_gate_type():
    from typing import get_args

    from okto_pulse.core.services import resource_gate, resource_lineage

    canonical = ("architecture", "mockup", "knowledge_base")
    assert resource_gate.RESOURCE_TYPES == canonical
    assert len(resource_gate.RESOURCE_TYPES) == 3
    assert get_args(resource_gate.ResourceType) == canonical
    assert resource_lineage.RESOURCE_TYPES == canonical
    assert "design_system" not in resource_gate.RESOURCE_TYPES
    # the mockup Design System gate is a separate mechanism, not a registered resource:
    # resource_gate must not import or reference the design-system gate symbols.
    import inspect

    src = inspect.getsource(resource_gate)
    assert "MockupDesignSystemGate" not in src and "design_system_gate" not in src
