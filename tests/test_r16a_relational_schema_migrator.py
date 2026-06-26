"""R16-A — RelationalSchemaMigrator port + schema DTOs (contract-only).

Covers the 4 scenarios 1:1, a binding DTO-field-set contract guard, and the
AST import-purity gate:

  ts_1b1d24cc (import isolation) -> test_ts_1b1d24cc_*
  ts_be7f3586 (fake init_db plan) -> test_ts_be7f3586_*
  ts_be9d7af1 (fail-closed)       -> test_ts_be9d7af1_*
  ts_1c6ac6cf (import gate AST)    -> test_ts_1c6ac6cf_*

The port is contract-only: it does NOT move/alter init_db or the real
migrations, and never imports SQLAlchemy / infra.database / _migrate_* /
okto_pulse.community.
"""

from __future__ import annotations

import ast
import dataclasses
import pathlib
import subprocess
import sys

from okto_pulse.core.ports.relational_schema_migrator import (
    MIGRATION_PHASES,
    MigrationPlan,
    MigrationResult,
    MigrationStep,
    MigrationStepResult,
    RelationalSchemaMigrator,
    SchemaMigrationError,
    require_migrator,
)

# Path to the port module SOURCE (computed without importing the package, so the
# isolation test can load it in a clean interpreter).
PORT_FILE = (
    pathlib.Path(__file__).resolve().parents[1]
    / "src" / "okto_pulse" / "core" / "ports" / "relational_schema_migrator.py"
)

#: Tokens the port module must never import/reference (tr_6b25ea0e).
FORBIDDEN_IMPORT_ROOTS = (
    "sqlalchemy",
    "okto_pulse.core.infra.database",
    "okto_pulse.community",
)
FORBIDDEN_IDENTIFIERS = ("get_engine", "create_all")


# ---------------------------------------------------------------------------
# Conformance helper (tr_cc5c47bd): a fake plan representing the EFFECTIVE order
# of init_db, WITHOUT importing or executing any real migration.
# ---------------------------------------------------------------------------
def build_init_db_baseline_plan() -> MigrationPlan:
    """Static, documentable representation of init_db's phase order.

    Mirrors infra.database.init_db: schema ALTERs before create_all, the
    Base.metadata.create_all boundary, schema ALTERs after, then the data
    bootstrap (seed / reconcile / discovery). Declarative data only — no
    migration is imported or run.
    """
    raw = [
        ("migrate_card_statuses", "pre_create_all"),
        ("migrate_add_event_tables", "pre_create_all"),
        ("base_metadata_create_all", "create_all_boundary"),
        ("migrate_add_card_sprint_id", "post_create_all"),
        ("migrate_add_board_guideline_provenance", "post_create_all"),
        ("seed_builtin_presets", "data_bootstrap_boundary"),
        ("reconcile_builtin_presets", "data_bootstrap_boundary"),
        ("bootstrap_default_discovery_intents", "data_bootstrap_boundary"),
    ]
    steps = tuple(
        MigrationStep(
            step_id=sid,
            order=i,
            phase=phase,  # type: ignore[arg-type]
            description=f"baseline:{sid}",
            idempotent=True,
            destructive=False,
            owner="community-adapter",
        )
        for i, (sid, phase) in enumerate(raw)
    )
    return MigrationPlan(plan_id="init_db_baseline", target="sqlite", steps=steps)


class _FakeMigrator:
    """In-memory RelationalSchemaMigrator satisfying the Protocol — no SQL."""

    def __init__(self, *, fail_step_id: str | None = None) -> None:
        self._fail_step_id = fail_step_id

    def plan(self, *, target: str) -> MigrationPlan:
        plan = build_init_db_baseline_plan()
        return MigrationPlan(
            plan_id=plan.plan_id, target=target, steps=plan.steps,
            boundaries=plan.boundaries, metadata=plan.metadata,
        )

    def validate_plan(self, plan: MigrationPlan) -> None:
        for step in plan.steps:
            if (
                not step.step_id
                or step.order is None
                or step.phase not in MIGRATION_PHASES
            ):
                raise SchemaMigrationError(
                    "invalid_step_shape",
                    step_id=step.step_id or "<missing>",
                    phase=step.phase if step.phase in MIGRATION_PHASES else None,
                    remediation="Each step needs a stable step_id, order and valid phase.",
                )

    def execute(self, plan: MigrationPlan) -> MigrationResult:
        self.validate_plan(plan)
        applied: list[MigrationStepResult] = []
        for step in plan.steps:
            if step.step_id == self._fail_step_id:
                failed = MigrationStepResult(
                    step_id=step.step_id,
                    status="failed",
                    phase=step.phase,
                    failure_reason="step raised during apply",
                    remediation="Inspect the failing migration and re-run from this step.",
                )
                return MigrationResult.failed_result(
                    failed, applied_steps=tuple(applied), partial=bool(applied)
                )
            applied.append(
                MigrationStepResult(
                    step_id=step.step_id, status="applied", phase=step.phase
                )
            )
        return MigrationResult(status="success", applied_steps=tuple(applied))


# ===========================================================================
# ts_1b1d24cc — the port imports in isolation, no SQLAlchemy / database / ORM.
# ===========================================================================
def test_ts_1b1d24cc_standard_import_pulls_no_forbidden_deps():
    # PRIMARY coverage: the STANDARD dotted import of the port/export must NOT
    # drag SQLAlchemy / infra.database / community into sys.modules. Run in a
    # clean subprocess because the pytest process already has sqlalchemy loaded.
    script = r"""
import sys
import okto_pulse.core.ports.relational_schema_migrator as m
from okto_pulse.core.ports import RelationalSchemaMigrator  # export path too
bad = sorted(
    n for n in sys.modules
    if n == "sqlalchemy" or n.startswith("sqlalchemy.")
    or n == "okto_pulse.core.infra.database"
    or n == "okto_pulse.community" or n.startswith("okto_pulse.community.")
)
assert not bad, f"standard port import pulled forbidden modules: {bad}"
# DTOs instantiable + Protocol usable without any ORM/engine loaded.
plan = m.MigrationPlan(plan_id="p", target="sqlite")
assert plan.boundaries == m.MIGRATION_PHASES
assert m.MigrationResult(status="success").is_success is True
assert RelationalSchemaMigrator is m.RelationalSchemaMigrator
print("STD_IMPORT_OK")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"
    assert "STD_IMPORT_OK" in proc.stdout


def test_ts_1b1d24cc_core_init_is_lazy_and_back_compatible():
    # Boundary fix: `from okto_pulse.core import __version__` must NOT eagerly
    # load infra.database/SQLAlchemy; `Base`/`init_db` still resolve lazily on
    # first access (back-compat preserved) and only THEN load the module.
    script = r"""
import sys
from okto_pulse.core import __version__
assert __version__, "missing __version__"
assert "okto_pulse.core.infra.database" not in sys.modules, "version import eagerly loaded database"
assert "sqlalchemy" not in sys.modules, "version import eagerly loaded sqlalchemy"
# Lazy back-compat: the established API still works on demand.
from okto_pulse.core import Base, init_db, get_db
assert Base is not None and callable(init_db) and callable(get_db)
assert "okto_pulse.core.infra.database" in sys.modules, "Base access did not load database"
print("LAZY_OK")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"
    assert "LAZY_OK" in proc.stdout


def test_ts_1b1d24cc_protocol_is_runtime_checkable():
    # A fake satisfies the Protocol structurally (typed/usable in isolation).
    assert isinstance(_FakeMigrator(), RelationalSchemaMigrator)


# ===========================================================================
# ts_be7f3586 — fake plan represents init_db order; boundaries explicit; no run.
# ===========================================================================
def test_ts_be7f3586_fake_plan_makes_phases_explicit_and_ordered():
    plan = build_init_db_baseline_plan()
    phases_in_order = [s.phase for s in plan.steps]
    # create_all_boundary and data_bootstrap_boundary are explicit.
    assert "create_all_boundary" in phases_in_order
    assert "data_bootstrap_boundary" in phases_in_order
    # The phases appear in the canonical lifecycle order (no reordering).
    first_idx = {ph: phases_in_order.index(ph) for ph in set(phases_in_order)}
    ordered_present = [ph for ph in MIGRATION_PHASES if ph in first_idx]
    assert [first_idx[ph] for ph in ordered_present] == sorted(
        first_idx[ph] for ph in ordered_present
    )
    # Steps are globally ordered by `order`.
    assert [s.order for s in plan.steps] == sorted(s.order for s in plan.steps)


def test_ts_be7f3586_data_bootstrap_is_distinct_from_schema_migration():
    # fr_ce1f2628 / br_e16ff5a1: seed/reconcile/discovery live in the data
    # bootstrap boundary, NOT in a schema phase.
    plan = build_init_db_baseline_plan()
    bootstrap_ids = {
        s.step_id for s in plan.steps if s.phase == "data_bootstrap_boundary"
    }
    assert {"seed_builtin_presets", "reconcile_builtin_presets",
            "bootstrap_default_discovery_intents"} <= bootstrap_ids
    schema_ids = {
        s.step_id for s in plan.steps if s.phase != "data_bootstrap_boundary"
    }
    assert not (bootstrap_ids & schema_ids)


# ===========================================================================
# ts_be9d7af1 — failing step / invalid plan is fail-closed; no false success.
# ===========================================================================
def test_ts_be9d7af1_failing_step_yields_fail_closed_result():
    migrator = _FakeMigrator(fail_step_id="migrate_add_card_sprint_id")
    result = migrator.execute(build_init_db_baseline_plan())
    assert result.status in ("failed", "partial")
    assert result.is_success is False
    assert result.failed_step is not None
    fs = result.failed_step
    assert fs.status == "failed"
    assert fs.step_id == "migrate_add_card_sprint_id"
    assert fs.phase == "post_create_all"
    assert fs.failure_reason and fs.remediation
    # Earlier steps applied, but the run is partial (non-success), never success.
    assert result.applied_steps
    assert result.status == "partial"


def test_ts_be9d7af1_invalid_plan_raises_structured_error():
    bad_plan = MigrationPlan(
        plan_id="bad", target="sqlite",
        steps=(MigrationStep(
            step_id="", order=0, phase="pre_create_all",  # empty step_id
            description="x", idempotent=True, destructive=False, owner="o",
        ),),
    )
    try:
        _FakeMigrator().validate_plan(bad_plan)
        raised = False
    except SchemaMigrationError as e:
        raised = True
        assert e.failure_reason == "invalid_step_shape"
        assert e.remediation
    assert raised, "invalid plan must fail closed with SchemaMigrationError"


def test_ts_be9d7af1_absent_migrator_is_fail_closed():
    # fr_3d223793 / tr_88498ca7: an ABSENT (unregistered) migrator must fail
    # closed with a structured error carrying reason + remediation — never a
    # silent no-op success.
    try:
        require_migrator(None, target="sqlite")
        raised = False
    except SchemaMigrationError as e:
        raised = True
        assert e.failure_reason == "migrator_absent"
        assert e.remediation and "adapter" in e.remediation.lower()
    assert raised, "absent migrator must raise SchemaMigrationError (fail-closed)"
    # A present migrator is returned unchanged (resolver is transparent).
    migrator = _FakeMigrator()
    assert require_migrator(migrator) is migrator


def test_ts_be9d7af1_result_cannot_label_failure_as_success():
    # The fail-closed invariant is structural: success + a failed step is rejected.
    failed = MigrationStepResult(
        step_id="s1", status="failed", phase="pre_create_all",
        failure_reason="boom",
    )
    try:
        MigrationResult(status="success", failed_step=failed)
        raised = False
    except ValueError:
        raised = True
    assert raised, "status='success' with a failed_step must raise (fail-closed)"

    try:
        MigrationResult(status="success", failed_steps=(failed,))
        raised2 = False
    except ValueError:
        raised2 = True
    assert raised2, "status='success' with failed_steps must raise (fail-closed)"


# ===========================================================================
# ts_1c6ac6cf — AST import gate: no SQLAlchemy / engine / migrations in the port.
# ===========================================================================
def _port_ast() -> ast.Module:
    return ast.parse(PORT_FILE.read_text(encoding="utf-8"))


def test_ts_1c6ac6cf_no_forbidden_imports():
    tree = _port_ast()
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    for mod in imported:
        for root in FORBIDDEN_IMPORT_ROOTS:
            assert mod != root and not mod.startswith(root + "."), (
                f"port must not import {mod!r} (forbidden root {root!r})"
            )


def test_ts_1c6ac6cf_no_forbidden_identifiers_in_code():
    # Walk Name/Attribute identifiers (NOT strings/docstrings, which legitimately
    # mention these tokens in prose) — proves no get_engine/_migrate_*/create_all
    # / Base.metadata usage in executable code.
    tree = _port_ast()
    idents: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            idents.add(node.id)
        elif isinstance(node, ast.Attribute):
            idents.add(node.attr)
    assert not any(i.startswith("_migrate_") for i in idents), "no _migrate_* refs"
    for bad in FORBIDDEN_IDENTIFIERS:
        assert bad not in idents, f"port references forbidden identifier {bad!r}"
    assert "Base" not in idents, "port must not reference ORM Base"


# ===========================================================================
# Contract: DTO field sets are EXACTLY the api_bb6411fd contract (binding names).
# ===========================================================================
def _field_names(dc) -> set[str]:
    return {f.name for f in dataclasses.fields(dc)}


def test_contract_dto_field_sets_match_api_contract():
    assert _field_names(MigrationStep) == {
        "step_id", "order", "phase", "description",
        "idempotent", "destructive", "owner", "metadata",
    }
    assert _field_names(MigrationStepResult) == {
        "step_id", "status", "phase",
        "failure_reason", "remediation", "duration_ms", "metadata",
    }
    assert _field_names(MigrationPlan) == {
        "plan_id", "target", "steps", "boundaries", "metadata",
    }
    assert _field_names(MigrationResult) == {
        "status", "applied_steps", "skipped_steps", "failed_steps",
        "warnings", "duration_ms", "failed_step", "failure_reason", "remediation",
    }


def test_contract_canonical_phases_and_statuses():
    assert MIGRATION_PHASES == (
        "pre_create_all", "create_all_boundary",
        "post_create_all", "data_bootstrap_boundary",
    )
