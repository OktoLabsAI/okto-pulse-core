"""R01C IMP2 D2 — cross-surface ORM consumer inventory + split-inventory guard
(AC5 ac_0f56b2b2, TR tr_65b25d4d; negative scenario ts_c9653b5c).

Behavioral proof that:
  * the inventory spans the core surfaces and is CONSISTENT (every ORM-definition
    coupling is classified/allowlisted — no silent bridge);
  * ``core.infra.database`` consumers are captured across all syntactic forms
    (direct / module-alias / bare dotted) and kept as the preserved-R01B category
    distinct from the ORM-definition removal target;
  * the split-inventory guard FAILS a heterogeneous / unowned / no-before-after
    batch removal BEFORE any move, and PASSES small per-cluster owned batches;
  * ``verify_migrated`` RE-SCANS to expose a file claimed migrated that still couples.
"""

from __future__ import annotations

import ast

import pytest

from okto_pulse.core.repositories.core_orm_import_gate import CORE_ORM_IMPORT_ALLOWLIST
from okto_pulse.core.repositories.orm_consumer_split_inventory import (
    CATEGORY_DB_PROVIDER,
    CATEGORY_ORM_DEFINITION,
    STATUS_PRESERVED_R01B,
    STATUS_TEMPORARY_EXCEPTION,
    OrmBatchRemovalDeclaration,
    OrmRemovalGroup,
    _scan_db_provider,
    build_orm_consumer_inventory,
    guard_orm_batch_removal,
    inventory_consistency_errors,
    verify_migrated,
)

# A registered file per cluster, for building well-formed guard fixtures.
_REST_FILE = "src/okto_pulse/core/api/qa.py"
_SERVICE_FILE = "src/okto_pulse/core/services/board_governance.py"
_WORKER_FILE = "src/okto_pulse/core/kg/health.py"


# ---------------------------------------------------------------------------
# Inventory: cross-surface, consistent, two distinct categories.
# ---------------------------------------------------------------------------

def test_inventory_is_consistent_on_real_tree():
    inv = build_orm_consumer_inventory()
    assert inventory_consistency_errors(inv) == []
    # every orm_definition record is a registered temporary_exception (no unclassified)
    assert inv.by_category.get(CATEGORY_ORM_DEFINITION, 0) > 0
    assert inv.by_status.get(STATUS_TEMPORARY_EXCEPTION, 0) == inv.by_category[CATEGORY_ORM_DEFINITION]


def test_inventory_keeps_provider_distinct_from_removal_target():
    inv = build_orm_consumer_inventory()
    assert inv.by_category.get(CATEGORY_DB_PROVIDER, 0) > 0
    # provider couplings are the preserved-R01B surface, never a removal target
    provider = [r for r in inv.records if r.category == CATEGORY_DB_PROVIDER]
    assert all(r.status == STATUS_PRESERVED_R01B for r in provider)
    assert all(r.group_id == "db_provider:r01b" for r in provider)


def test_inventory_spans_multiple_surfaces():
    inv = build_orm_consumer_inventory()
    # the cut touches REST, MCP, services and workers (not a single surface)
    for surface in ("rest", "mcp", "service", "worker"):
        assert inv.by_surface.get(surface, 0) > 0, surface


_PROVIDER_FORMS = [
    ("direct", "from okto_pulse.core.infra.database import get_engine\nx = get_engine()\n", {"get_engine"}),
    ("alias_module", "import okto_pulse.core.infra.database as dbmod\nx = dbmod.get_session_factory()\n", {"get_session_factory"}),
    ("from_pkg_attr", "from okto_pulse.core.infra import database\nx = database.get_engine()\n", {"get_engine"}),
    ("bare_dotted", "import okto_pulse.core.infra.database\nx = okto_pulse.core.infra.database.get_db\n", {"get_db"}),
    ("base_excluded", "from okto_pulse.core.infra import database\nx = database.Base\n", set()),
]


@pytest.mark.parametrize("name,src,expect", _PROVIDER_FORMS, ids=[c[0] for c in _PROVIDER_FORMS])
def test_db_provider_consumers_detected_across_forms(name, src, expect):
    hits = _scan_db_provider(ast.parse(src), "f.py")
    assert {symbol for _, symbol in hits} == expect


# ---------------------------------------------------------------------------
# Split-inventory guard (ts_c9653b5c).
# ---------------------------------------------------------------------------

def test_guard_fails_heterogeneous_unowned_batch():
    bad = OrmBatchRemovalDeclaration(groups=(
        OrmRemovalGroup(
            group_id="", owner="",
            files=(_REST_FILE, _SERVICE_FILE),  # heterogeneous clusters
            before_count=None, after_count=None, register_before_remove=False,
        ),
    ))
    rep = guard_orm_batch_removal(bad)
    assert not rep.ok
    joined = " | ".join(rep.failures)
    assert "missing group_id" in joined
    assert "missing owner" in joined
    assert "heterogeneous" in joined
    assert "register_before_remove not asserted" in joined
    assert "before/after" in joined


def test_guard_passes_clean_per_cluster_batch():
    clean = OrmBatchRemovalDeclaration(groups=(
        OrmRemovalGroup(
            group_id="rest-1", owner="core-refactor:rest", files=(_REST_FILE,),
            before_count=11, after_count=10, register_before_remove=True,
        ),
        OrmRemovalGroup(
            group_id="service-1", owner="core-refactor:service", files=(_SERVICE_FILE,),
            before_count=29, after_count=28, register_before_remove=True,
        ),
    ))
    rep = guard_orm_batch_removal(clean)
    assert rep.ok, rep.failures


def test_guard_individual_failure_modes():
    # register-before-remove: a file NOT in the allowlist cannot be batch-removed.
    unreg = OrmBatchRemovalDeclaration(groups=(
        OrmRemovalGroup(group_id="g", owner="o", files=("src/okto_pulse/core/api/_never_registered.py",),
                        before_count=2, after_count=1, register_before_remove=True),
    ))
    assert any("register-before-remove violated" in f for f in guard_orm_batch_removal(unreg).failures)

    # before/after must actually reduce coupling.
    noreduce = OrmBatchRemovalDeclaration(groups=(
        OrmRemovalGroup(group_id="g", owner="o", files=(_REST_FILE,),
                        before_count=5, after_count=5, register_before_remove=True),
    ))
    assert any("does not reduce" in f for f in guard_orm_batch_removal(noreduce).failures)

    # duplicate group ids are rejected.
    dup = OrmBatchRemovalDeclaration(groups=(
        OrmRemovalGroup(group_id="g", owner="o", files=(_REST_FILE,), before_count=2, after_count=1, register_before_remove=True),
        OrmRemovalGroup(group_id="g", owner="o", files=(_SERVICE_FILE,), before_count=2, after_count=1, register_before_remove=True),
    ))
    assert any("duplicate group_id" in f for f in guard_orm_batch_removal(dup).failures)

    # empty batch is rejected.
    assert any("empty batch" in f for f in guard_orm_batch_removal(OrmBatchRemovalDeclaration(groups=())).failures)


# ---------------------------------------------------------------------------
# verify_migrated: re-scan exposes a still-coupled "migrated" claim.
# ---------------------------------------------------------------------------

def test_verify_migrated_happy_path_for_vanished_file():
    prev = {**CORE_ORM_IMPORT_ALLOWLIST, "src/okto_pulse/core/_gone.py": "service"}
    res = verify_migrated(prev)
    assert "src/okto_pulse/core/_gone.py" in res["migrated"]
    assert res["still_coupled"] == []


def test_verify_migrated_detects_still_coupled(tmp_path):
    api = tmp_path / "src" / "okto_pulse" / "core" / "api"
    api.mkdir(parents=True)
    (api / "_drifter.py").write_text(
        "from okto_pulse.core.models.db import Board\nx = Board\n", encoding="utf-8")
    prev = {"src/okto_pulse/core/api/_drifter.py": "rest"}
    res = verify_migrated(prev, core_root=tmp_path / "src" / "okto_pulse" / "core")
    assert "src/okto_pulse/core/api/_drifter.py" in res["still_coupled"]
    assert res["migrated"] == []
