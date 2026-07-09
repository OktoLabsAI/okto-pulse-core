"""R01C IMP3 — governed ORM-import allowlist + deterministic drainability report
+ 4 real port drains (card 48ad9f81; AC ts_0f55da9e is NOT claimed resolved here —
the domain/ORM separation axis + IMP4 remain).

Proves, deterministically and re-executably (Codex IMP3 acceptance criteria):
  1. metadata FAIL-CLOSED — an incomplete/dangling cluster governance is an error
     (withdrawal_criterion + blocked_by axis + debt_ref are mandatory);
  2. debt_ref / withdrawal_criterion / blocked_by are present AND resolve to the
     real debt ledger (ORM_RETURN_DEBT / SESSION_BRIDGE_DEBT) — unified governance;
  3. the allowlist only SHRINKS (ratchet) and a NEW core ORM consumer still FAILS;
  4. the drainability rule is DETERMINISTIC and tied to the ledger (port-backed ⟺
     the ORM type is a registered ORM_RETURN_DEBT aggregate) — not a hardcoded 0;
  5. the 4 drained files are coupling-free (re-scan) and the report keeps an
     EXPLICIT temporary remainder (nothing hidden, no silent bridge);
  6. behavioural regression — the EXACT ``db.get(Board, id) -> uow.boards.get(id)``
     substitution the 4 drains made is equivalent for the existence get-by-id.

The endpoint-level behavioural regression of the 4 drains lives in the files'
EXISTING suites (test_cognitive_action_center_rest_s3_3 / _s3_1,
test_kg_rebuild_*, test for default_board_config diff, kg health readiness) —
re-run as part of the R01C gate.
"""

from __future__ import annotations

import pytest

import okto_pulse.core.repositories.core_orm_import_gate as gate
from okto_pulse.core.repositories.core_orm_import_gate import (
    BLOCKED_BY_AXES,
    CORE_ORM_IMPORT_ALLOWLIST,
    REQUIRED_ALLOWLIST_FIELDS,
    allowlist_entry,
    core_orm_allowlist_only_shrinks,
    resolve_debt_ref,
    run_core_orm_import_gate,
    validate_allowlist_metadata,
)
from okto_pulse.core.repositories.debt import ORM_RETURN_DEBT, SESSION_BRIDGE_DEBT
from okto_pulse.core.repositories.orm_consumer_split_inventory import (
    CATEGORY_ORM_DEFINITION,
    ORM_DRAINED_BY_IMP3,
    PORTED_AGGREGATE_ORM_TYPES,
    STATUS_TEMPORARY_EXCEPTION,
    build_orm_consumer_inventory,
    core_orm_governance_report,
    drainability_classification,
)


# ---------------------------------------------------------------------------
# 1+2. Governed allowlist: fail-closed metadata, debt-ledger linkage.
# ---------------------------------------------------------------------------

def test_governance_metadata_complete_on_real_tree():
    assert validate_allowlist_metadata() == []
    # the shrunk allowlist includes AF35-S2 worker helper drains (65 -> 63)
    assert len(CORE_ORM_IMPORT_ALLOWLIST) == 63


def test_every_cluster_carries_full_governance_resolving_to_ledger():
    # allowlist_entry exposes the full per-file governance contract.
    for file_label in CORE_ORM_IMPORT_ALLOWLIST:
        entry = allowlist_entry(file_label)
        for field in REQUIRED_ALLOWLIST_FIELDS:
            assert str(entry.get(field, "")).strip(), (file_label, field)
        assert entry["blocked_by"] in BLOCKED_BY_AXES, file_label
        assert "debt_ref" in entry, file_label
        ref = entry["debt_ref"]
        if ref is not None:
            target = resolve_debt_ref(ref)
            assert target in (ORM_RETURN_DEBT, SESSION_BRIDGE_DEBT), (file_label, ref)
            assert target, file_label  # non-empty ledger entry


_GOOD_CLUSTER = {
    "owner": "o",
    "removal_date": "2026-12-31",
    "reason": "r",
    "withdrawal_criterion": "w",
    "blocked_by": "domain_orm_separation_axis",
    "debt_ref": "orm_return_debt",
}

_FAIL_CLOSED_CASES = [
    ("missing_withdrawal", {**_GOOD_CLUSTER, "withdrawal_criterion": ""},
     "missing required field 'withdrawal_criterion'"),
    ("empty_owner", {**_GOOD_CLUSTER, "owner": ""},
     "missing required field 'owner'"),
    ("missing_blocked_by", {**_GOOD_CLUSTER, "blocked_by": ""},
     "missing required field 'blocked_by'"),
    ("unknown_axis", {**_GOOD_CLUSTER, "blocked_by": "made_up_axis"},
     "unknown blocked_by axis"),
    ("dangling_debt_ref", {**_GOOD_CLUSTER, "debt_ref": "nonexistent_debt"},
     "does not resolve to a registered debt-ledger entry"),
    ("missing_debt_ref_key", {k: v for k, v in _GOOD_CLUSTER.items() if k != "debt_ref"},
     "missing the explicit 'debt_ref' declaration"),
]


@pytest.mark.parametrize(
    "name,bad,expected", _FAIL_CLOSED_CASES, ids=[c[0] for c in _FAIL_CLOSED_CASES]
)
def test_metadata_validation_is_fail_closed(monkeypatch, name, bad, expected):
    # Inject a malformed cluster; validation must flag it (and leave the real
    # clusters valid). monkeypatch.setitem restores the ledger after the test.
    monkeypatch.setitem(gate._ALLOWLIST_CLUSTERS, "imp3_bad_cluster", bad)
    errors = validate_allowlist_metadata()
    assert any(expected in e and "imp3_bad_cluster" in e for e in errors), errors


def test_good_clusters_remain_valid_under_injection(monkeypatch):
    # A well-formed extra cluster does NOT introduce errors — the gate is not
    # globally tripping; only malformed governance fails.
    monkeypatch.setitem(gate._ALLOWLIST_CLUSTERS, "imp3_good_cluster", dict(_GOOD_CLUSTER))
    assert validate_allowlist_metadata() == []


# ---------------------------------------------------------------------------
# 3. Ratchet (only shrinks) + a new ORM consumer still fails.
# ---------------------------------------------------------------------------

def test_allowlist_ratchet_only_shrinks():
    prev = dict(CORE_ORM_IMPORT_ALLOWLIST)
    a_file = next(iter(prev))
    shrunk = {k: v for k, v in prev.items() if k != a_file}
    grown = {**prev, "src/okto_pulse/core/api/_imp3_new.py": "rest"}
    relabelled = {**prev, a_file: "bootstrap"}
    assert core_orm_allowlist_only_shrinks(prev, shrunk) is True
    assert core_orm_allowlist_only_shrinks(prev, prev) is True
    assert core_orm_allowlist_only_shrinks(prev, grown) is False
    assert core_orm_allowlist_only_shrinks(prev, relabelled) is False


def test_gate_green_and_new_consumer_outside_allowlist_fails(tmp_path):
    # green on the real (shrunk) tree
    assert run_core_orm_import_gate().ok
    # a NEW core file that couples ORM but is absent from the allowlist is blocking
    core = tmp_path / "src" / "okto_pulse" / "core"
    target = core / "api" / "_imp3_drifter.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "from okto_pulse.core.models.db import Board\ndef use():\n    return Board\n",
        encoding="utf-8",
    )
    rep = run_core_orm_import_gate(root=core)
    assert not rep.ok
    assert any("_imp3_drifter.py" in v.file for v in rep.violations)


# ---------------------------------------------------------------------------
# 4. Drainability rule: deterministic + tied to the debt ledger (not hardcoded).
# ---------------------------------------------------------------------------

def test_ported_aggregates_derived_from_debt_ledger():
    expected = {e.orm_type.rsplit(".", 1)[-1] for e in ORM_RETURN_DEBT}
    assert set(PORTED_AGGREGATE_ORM_TYPES) == expected == {"Board", "Ideation", "Spec"}
    # an aggregate with NO repository port is correctly excluded
    assert "Card" not in PORTED_AGGREGATE_ORM_TYPES


def test_drainability_classifies_port_backed_vs_no_port_from_tree():
    classification = drainability_classification()
    sites = classification["sites"]
    assert sites, "expected ORM get-by-id sites on the real tree"
    # every port-backed site's symbol is a ledger aggregate; no-port sites are not
    for s in sites:
        if s["port_backed"]:
            assert s["symbol"] in PORTED_AGGREGATE_ORM_TYPES
        else:
            assert s["symbol"] not in PORTED_AGGREGATE_ORM_TYPES
    # the rule is real: both partitions are populated (Board is port-backed; at
    # least one no-port aggregate like Card exists in the tree)
    assert any(s["port_backed"] and s["symbol"] == "Board" for s in sites)
    assert any(not s["port_backed"] for s in sites)


def test_no_file_fully_drainable_now_after_imp3_drains():
    # The 4 fully-Board-port-backed files were drained this card; nothing else
    # fully vacates the allowlist now (the genuine "0 drainable-to-vacate" state).
    assert drainability_classification()["would_vacate_files"] == {}


# ---------------------------------------------------------------------------
# 5. The 4 drains are real (re-scan) + report keeps an explicit remainder.
# ---------------------------------------------------------------------------

def test_drained_files_absent_and_coupling_free():
    inv = build_orm_consumer_inventory()
    coupled = {
        r.file for r in inv.records if r.category == CATEGORY_ORM_DEFINITION
    }
    for f in ORM_DRAINED_BY_IMP3:
        assert f not in CORE_ORM_IMPORT_ALLOWLIST, f  # left the allowlist
        assert f not in coupled, f                    # genuinely no ORM coupling


def test_governance_report_explicit_remainder_nothing_hidden():
    rep = core_orm_governance_report()
    assert rep["allowlisted_files"] == 63
    rem = rep["remainder"]
    assert rem["hidden"] == 0
    assert rem["all_temporary"] is True
    assert rem["unclassified"] == 0  # no silent bridge
    # per-surface counts sum to the total — the remainder is fully accounted
    assert sum(rem["by_surface"].values()) == rem["total_orm_definition_couplings"]
    # the 4 drains are re-proven coupling-free in the report
    assert rep["drained_this_card"]["still_coupled"] == []
    assert rep["drained_this_card"]["count"] == 4
    # drainability counts are internally consistent
    d = rep["drainability"]
    g = d["get_by_id_sites"]
    assert g["total"] == g["port_backed"] + g["no_port_blocked_by_axis"]
    assert d["files_fully_drainable_now"] == {}
    # any remaining import-drainable port-backed site is surfaced explicitly
    assert len(d["deferred_port_backed_count_only"]) == d["import_drainable_now_sites"]
    assert d["ports_return_orm"] is True


def test_report_clusters_governance_resolves():
    rep = core_orm_governance_report()
    for cluster, c in rep["clusters"].items():
        assert c["status"] == STATUS_TEMPORARY_EXCEPTION
        assert str(c["withdrawal_criterion"]).strip()
        assert c["blocked_by"] in BLOCKED_BY_AXES
        if c["debt_ref"] is not None:
            assert c["debt_ref_resolves"] is True
    # session-bridge debt is reported with an objective (non-hidden) zero-reduction
    sb = rep["session_bridge"]
    assert sb["reduction_now"] == 0
    assert sb["withdrawal_criterion"] == SESSION_BRIDGE_DEBT.withdrawal_criterion


# ---------------------------------------------------------------------------
# 6. Behavioural regression: the drain substitution is equivalent.
# ---------------------------------------------------------------------------

async def test_board_port_drain_is_equivalent_to_orm_get(db_factory):
    """The EXACT substitution the 4 drains made — ``db.get(Board, id)`` →
    ``resolve_unit_of_work_factory().wrap(db).boards.get(id)`` — returns the same
    board for an existing id and None for a missing id (existence get-by-id,
    no owner/permission predicate lost)."""
    from okto_pulse.core.models.db import Board
    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

    bid = "imp3-drain-equiv"
    async with db_factory() as db:
        db.add(Board(id=bid, name="imp3", owner_id="o", settings={}))
        await db.commit()

    async with db_factory() as db:
        via_orm = await db.get(Board, bid)
        via_port = await resolve_unit_of_work_factory().wrap(db).boards.get(bid)
    assert via_orm is not None and via_port is not None
    assert via_port.id == via_orm.id == bid

    async with db_factory() as db:
        assert await db.get(Board, "imp3-missing") is None
        assert await resolve_unit_of_work_factory().wrap(db).boards.get("imp3-missing") is None
