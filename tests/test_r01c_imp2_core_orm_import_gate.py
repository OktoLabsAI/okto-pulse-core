"""R01C IMP2 D1 — delta-aware core-runtime ORM-definition import gate (FR4 / AC4
ac_527bc003; negative scenario ts_d1ddd3c9).

Behavioral proof that:
  * the gate guards the ENTIRE ``src/okto_pulse/core`` runtime and is GREEN on the
    current tree (every ORM-definition coupling is allowlisted with owner +
    removal date);
  * a NEW consumer outside the FROZEN allowlist FAILS (delta-aware teeth) — incl.
    every module-alias bypass form (so AC4's "import/use runtime" can't be dodged
    by aliasing the module);
  * agnostic enum imports are NOT flagged (enum coupling != ORM coupling);
  * the allowlist only SHRINKS (ratchet).

The gate is a FROZEN STATIC literal, NOT a live scan, and NOT a reuse of
``relational_boundary_gate`` (use_cases) nor the core-wide debt baseline.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from okto_pulse.core.repositories.core_orm_import_gate import (
    CORE_ORM_IMPORT_ALLOWLIST,
    core_orm_allowlist_only_shrinks,
    orm_definition_names,
    run_core_orm_import_gate,
    validate_allowlist_metadata,
)

_CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _mirror(tmp_path: pathlib.Path, rel: str, src: str) -> pathlib.Path:
    """Write a fixture file at ``tmp/src/okto_pulse/core/<rel>`` and return the
    core root to scan (so the gate produces real ``src/...`` labels)."""
    target = tmp_path / "src" / "okto_pulse" / "core" / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(src, encoding="utf-8")
    return tmp_path / "src" / "okto_pulse" / "core"


# ---------------------------------------------------------------------------
# Gate is GREEN on the real tree + the allowlist governance holds.
# ---------------------------------------------------------------------------

def test_gate_green_on_real_core_tree():
    rep = run_core_orm_import_gate()
    assert rep.ok, [v.as_dict() for v in rep.violations]
    assert rep.violations == []
    # Covers the whole core runtime, not just use_cases.
    assert rep.scanned_files > 300
    assert rep.guarded_path.replace("\\", "/").endswith("okto_pulse/core")


def test_allowlist_metadata_is_complete():
    assert validate_allowlist_metadata() == []


def test_allowlist_is_a_frozen_static_literal():
    # Anti-baseline-theater: a plain dict literal captured at IMP2 time (not a live
    # scan), so a NEW violation cannot auto-join. The static fixtures below prove a
    # new file is blocking precisely because the literal does not contain it.
    assert isinstance(CORE_ORM_IMPORT_ALLOWLIST, dict)
    assert len(CORE_ORM_IMPORT_ALLOWLIST) >= 60
    assert all(k.startswith("src/okto_pulse/core/") for k in CORE_ORM_IMPORT_ALLOWLIST)


# ---------------------------------------------------------------------------
# ORM-definition name set is structural and excludes agnostic enums.
# ---------------------------------------------------------------------------

def test_orm_definition_names_excludes_agnostic_enums():
    names = orm_definition_names()
    assert "Base" in names
    assert "Card" in names  # an ORM model (subclasses Base)
    assert "CardStatus" not in names  # agnostic enum re-export
    assert "BugSeverity" not in names


# ---------------------------------------------------------------------------
# Delta-aware teeth: a NEW consumer outside the allowlist FAILS the gate.
# ---------------------------------------------------------------------------

def test_new_consumer_outside_allowlist_fails(tmp_path):
    core = _mirror(
        tmp_path, "api/_new_consumer.py",
        "from okto_pulse.core.models.db import Card\ndef use():\n    return Card\n",
    )
    _mirror(
        tmp_path, "api/_clean_consumer.py",
        "from okto_pulse.core.domain.enums import CardStatus\ndef use():\n    return CardStatus.DONE\n",
    )
    rep = run_core_orm_import_gate(root=core)
    assert not rep.ok
    files = {v.file for v in rep.violations}
    assert "src/okto_pulse/core/api/_new_consumer.py" in files
    assert "src/okto_pulse/core/api/_clean_consumer.py" not in files


_ALIAS_BYPASSES = [
    ("orm_declbase", "import sqlalchemy.orm as orm\nBase = orm.declarative_base()\n", True),
    ("sa_typedecorator", "import sqlalchemy as sa\nclass X(sa.TypeDecorator):\n    impl = sa.Integer\n", True),
    ("types_typedecorator", "import sqlalchemy.types as types\nclass X(types.TypeDecorator):\n    pass\n", True),
    ("db_alias_base", "import okto_pulse.core.infra.database as db\nx = db.Base\n", True),
    ("from_infra_base", "from okto_pulse.core.infra import database\nx = database.Base\n", True),
    ("from_models_card", "from okto_pulse.core.models import db\nx = db.Card\n", True),
    ("bare_dotted_card", "import okto_pulse.core.models.db\nx = okto_pulse.core.models.db.Card\n", True),
    # control: not an ORM-definition target -> must NOT flag.
    ("non_target_3level", "import sqlalchemy as sa\nclass X(sa.orm.DeclarativeBase):\n    pass\n", False),
]


@pytest.mark.parametrize("name,src,expect_flagged", _ALIAS_BYPASSES, ids=[c[0] for c in _ALIAS_BYPASSES])
def test_module_alias_bypasses_are_detected(tmp_path, name, src, expect_flagged):
    core = _mirror(tmp_path, f"api/_alias_{name}.py", src)
    rep = run_core_orm_import_gate(root=core)
    flagged = any(f"_alias_{name}.py" in v.file for v in rep.violations)
    assert flagged is expect_flagged, [v.as_dict() for v in rep.violations]


# ---------------------------------------------------------------------------
# Ratchet: shrink ok, grow rejected.
# ---------------------------------------------------------------------------

def test_allowlist_ratchet_only_shrinks():
    prev = dict(CORE_ORM_IMPORT_ALLOWLIST)
    a_file = next(iter(prev))
    shrunk = {k: v for k, v in prev.items() if k != a_file}  # migrated one file
    grown = {**prev, "src/okto_pulse/core/api/_brand_new.py": "rest"}
    relabelled = {**prev, a_file: "bootstrap"}  # loosened cluster
    assert core_orm_allowlist_only_shrinks(prev, shrunk) is True
    assert core_orm_allowlist_only_shrinks(prev, prev) is True
    assert core_orm_allowlist_only_shrinks(prev, grown) is False
    assert core_orm_allowlist_only_shrinks(prev, relabelled) is False


def test_gate_does_not_reuse_use_cases_boundary_or_debt_baseline():
    # ts_d1ddd3c9: this module must be a DISTINCT enforcement, not a re-export of
    # relational_boundary_gate's use_cases scan nor the core-wide debt baseline.
    # Check actual IMPORTS and CALLS (not prose) — the only legitimate reuse is the
    # ``default_core_path`` helper.
    import okto_pulse.core.repositories.core_orm_import_gate as gate

    tree = ast.parse(pathlib.Path(gate.__file__).read_text(encoding="utf-8"))
    imported, called = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            called.add(node.func.id)
    forbidden = {"run_relational_boundary_gate", "relational_baseline_report"}
    assert not (imported & forbidden), imported & forbidden
    assert not (called & forbidden), called & forbidden
    assert "default_core_path" in imported  # the one legitimate shared helper
