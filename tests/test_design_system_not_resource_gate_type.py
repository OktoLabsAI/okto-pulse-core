"""Spec 3a006f65 / card 390ccd50 — Design System is NOT a Resource Gate resource type
(AC10, TR3), scenario ts_a11fe539.

PROOF / regression card: the invariant already holds by design (Design System
compliance is enforced ONLY by the MockupDesignSystemGate on screen mockup
create/update — card 0192f58d — never as a Resource Gate type / coverage obligation /
required resource / registry enum / Resource Gate filter). No production code is added;
these re-executable guards FAIL if ``design_system`` ever becomes a Resource Gate type,
across registries, static typing, the runtime summary, task-resource coverage, and the
decoupling of the two mechanisms.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_design_system_not_resource_gate_type.py
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import uuid
from typing import get_args

import pytest

from sqlalchemy_test_models import Board, SpecStatus
from okto_pulse.core.services import resource_gate, resource_lineage, spec_resource_propagation
from okto_pulse.core.services.design_system import DesignSystemService

CANONICAL = ("architecture", "mockup", "knowledge_base")
USER_ID = "rg-no-ds-user"


# ---------------------------------------------------------------------------
# registries + static typing — exactly the 3 canonical types, no design_system
# ---------------------------------------------------------------------------


def test_resource_type_registries_are_exactly_three_canonical():
    # tuple registries
    assert resource_gate.RESOURCE_TYPES == CANONICAL
    assert len(resource_gate.RESOURCE_TYPES) == 3
    assert resource_lineage.RESOURCE_TYPES == CANONICAL
    assert spec_resource_propagation.SUPPORTED_RESOURCE_TYPES == ("knowledge_base", "architecture", "mockup")
    # Literal static typing (the union of allowed resource_type values)
    from okto_pulse.community.api import resource_gate as api_resource_gate

    assert set(get_args(resource_gate.ResourceType)) == set(CANONICAL)
    assert set(get_args(resource_lineage.ResourceType)) == set(CANONICAL)
    assert set(get_args(api_resource_gate.ResourceType)) == set(CANONICAL)
    # design_system never appears in any registry
    for reg in (
        resource_gate.RESOURCE_TYPES,
        resource_lineage.RESOURCE_TYPES,
        spec_resource_propagation.SUPPORTED_RESOURCE_TYPES,
    ):
        assert "design_system" not in reg


def test_resource_gate_hardcoded_dispatch_and_label_dicts_have_only_canonical_keys():
    # _remediation / _resource_label label maps cover exactly the 3 types.
    for rtype in CANONICAL:
        assert resource_gate.ResourceGateService._resource_label(rtype) != rtype  # has a real label
    assert resource_gate.ResourceGateService._resource_label("design_system") == "design_system"  # no label
    # The lazy facade delegates these policies to the registered implementation.
    for marker in ("_collect_refs", "_remediation", "_resource_label"):
        source = inspect.getsource(getattr(resource_gate.ResourceGateService, marker))
        assert "design_system" not in source


# ---------------------------------------------------------------------------
# ts_a11fe539 runtime — summary + coverage on a board WITH Design System enabled
# ---------------------------------------------------------------------------


async def _board_with_design_system(db):
    """A board with Design System ENABLED (effective DS linked + gate_mode blocking)."""
    board = Board(
        id=str(uuid.uuid4()), name=f"b-{uuid.uuid4().hex[:8]}", owner_id=USER_ID,
        settings={"design_system_gate_mode": "blocking"},
    )
    db.add(board)
    await db.flush()
    ds = await DesignSystemService(db).create_design_system(USER_ID, title="DS", scope="global")
    await DesignSystemService(db).link_design_system_to_board(board.id, ds.id)
    return board


@pytest.mark.asyncio
async def test_runtime_resource_gate_summary_excludes_design_system():
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import Spec

    async with get_session_factory()() as db:
        board = await _board_with_design_system(db)
        spec = Spec(id=str(uuid.uuid4()), board_id=board.id, title="S",
                    status=SpecStatus.DRAFT, created_by=USER_ID)
        db.add(spec)
        await db.flush()

        # ts_a11fe539: with Design System enabled on the board, the Resource Gate summary
        # still exposes ONLY architecture/mockup/knowledge_base — design_system is not a
        # tracked Resource Gate resource (its compliance lives in MockupDesignSystemGate).
        summary = await resource_gate.ResourceGateService(db).get_summary(board.id, "spec", spec.id)
        types = {r["resource_type"] for r in summary["resources"]}
        assert types == set(CANONICAL)
        assert "design_system" not in types
        import json

        assert "design_system" not in json.dumps(summary, default=str)


@pytest.mark.asyncio
async def test_runtime_resource_coverage_keys_exclude_design_system():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board_with_design_system(db)
        assert board.settings["design_system_gate_mode"] == "blocking"  # DS enabled context
        svc = resource_gate.ResourceGateService(db)
        # the per-type task-resource coverage scaffold is keyed strictly by RESOURCE_TYPES;
        # with Design System enabled on the board it still has only the 3 canonical types.
        coverage = await svc._collect_task_resource_id_coverage([])
        assert set(coverage.keys()) == set(CANONICAL)
        assert "design_system" not in coverage


# ---------------------------------------------------------------------------
# decoupling — Resource Gate has zero knowledge of the Design System gate
# ---------------------------------------------------------------------------


def test_resource_gate_modules_are_decoupled_from_design_system_gate():
    for module in (resource_gate, resource_lineage):
        src = inspect.getsource(module)
        assert "design_system" not in src, f"{module.__name__} unexpectedly references design_system"
        assert "MockupDesignSystemGate" not in src
        assert "DesignSystemService" not in src


# ---------------------------------------------------------------------------
# census guard — no core registry/Literal ever lists design_system as a resource type
# ---------------------------------------------------------------------------


def _resource_type_registry_literals(core_dir) -> list[tuple[str, list[str]]]:
    """Every assignment named *RESOURCE_TYPES* (tuple) or a ResourceType ``Literal[...]``
    under core, returned as (where, values) so a new design_system entry is caught."""
    found: list[tuple[str, list[str]]] = []
    for py in pathlib.Path(core_dir).rglob("*.py"):
        rel = py.relative_to(core_dir).as_posix()
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            # tuple registries: *RESOURCE_TYPES
            if any(n.endswith("RESOURCE_TYPES") for n in names) and isinstance(
                node.value, (ast.Tuple, ast.List)
            ):
                vals = [e.value for e in node.value.elts if isinstance(e, ast.Constant)]
                found.append((f"{rel}:{names}", vals))
            # ResourceType = Literal[...]
            if "ResourceType" in names and isinstance(node.value, ast.Subscript):
                sl = node.value.slice
                elts = sl.elts if isinstance(sl, ast.Tuple) else [sl]
                vals = [e.value for e in elts if isinstance(e, ast.Constant)]
                if vals:
                    found.append((f"{rel}:ResourceType", vals))
    return found


def test_census_no_resource_type_registry_lists_design_system():
    import okto_pulse.core as core_pkg

    core_dir = pathlib.Path(core_pkg.__file__).parent
    registries = _resource_type_registry_literals(core_dir)
    # sanity: we actually discovered the known registries (guard isn't a no-op).
    assert len(registries) >= 4, registries
    for where, vals in registries:
        assert "design_system" not in vals, (
            f"{where} lists design_system as a Resource Gate type — Design System must "
            f"stay a mockup-submission gate (MockupDesignSystemGate), not a Resource Gate type."
        )
        # every resource-type registry must be a subset of the canonical 3.
        assert set(vals) <= set(CANONICAL), f"{where} has unexpected resource types: {vals}"
