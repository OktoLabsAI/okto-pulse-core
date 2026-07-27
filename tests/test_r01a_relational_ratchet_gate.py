"""Spec R01A IMP7 — baseline-aware relational ratchet gate (FR4 / AC4 ac_877f378b,
ts_8e14d173).

Proves the COUNT-aware ratchet FAILS for a NEW direct relational coupling injected
into the strangled ``application/use_cases`` layer (``Depends(get_db)`` /
``get_db_for_mcp`` / ``AsyncSession`` beyond the declared baseline) — reporting
file, line, symbol and per-surface counters — and PASSES for the clean baseline.
The baseline is per ``(file, symbol)`` COUNT, so a second call-site of an already
baselined symbol in the same file still fails (it does not collapse to one key).
The declared baseline may only shrink.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.repositories.relational_boundary_gate import (
    RELATIONAL_USE_CASES_BASELINE,
    default_use_cases_path,
    ratchet_baseline_only_shrinks,
    relational_ratchet_key,
    run_relational_ratchet_gate,
)

_ROGUE_MULTI_FILENAME = "_r01a_imp7_rogue_multi_tmp.py"
_ROGUE_MULTI_SRC = '''"""TEMP rogue: one of each relational symbol (R01A IMP7 teeth)."""
from __future__ import annotations

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.database import get_db, get_db_for_mcp


async def rogue_rest(db: AsyncSession = Depends(get_db)) -> None:
    ...


async def rogue_mcp() -> None:
    async with get_db_for_mcp() as _db:
        ...
'''

_ROGUE_COUNT_FILENAME = "_r01a_imp7_rogue_count_tmp.py"
_ROGUE_COUNT_SRC = '''"""TEMP rogue: TWO get_db_for_mcp call-sites in the SAME file (count teeth)."""
from __future__ import annotations

from okto_pulse.core.infra.database import get_db_for_mcp


async def rogue_first() -> None:
    async with get_db_for_mcp() as _db:
        ...


async def rogue_second() -> None:
    async with get_db_for_mcp() as _db:
        ...
'''


def _write_rogue(filename: str, src: str):
    target = default_use_cases_path() / filename
    target.write_text(src, encoding="utf-8")
    return target


@pytest.fixture
def rogue_multi():
    """Write a multi-symbol rogue into the REAL use_cases package; remove it after
    (the ts_8e14d173 'fixture temporaria')."""
    target = _write_rogue(_ROGUE_MULTI_FILENAME, _ROGUE_MULTI_SRC)
    try:
        yield target
    finally:
        target.unlink(missing_ok=True)


@pytest.fixture
def rogue_count():
    """Write a rogue with TWO get_db_for_mcp call-sites in one file; remove after."""
    target = _write_rogue(_ROGUE_COUNT_FILENAME, _ROGUE_COUNT_SRC)
    try:
        yield target
    finally:
        target.unlink(missing_ok=True)


def _counts(report) -> dict:
    """Group the report's new violations into a ``(file, symbol) -> count`` baseline."""
    out: dict = {}
    for v in report.new_violations:
        key = relational_ratchet_key(v)
        out[key] = out.get(key, 0) + 1
    return out


def test_ratchet_fails_on_new_relational_coupling_in_use_cases(rogue_multi) -> None:
    report = run_relational_ratchet_gate(baseline=RELATIONAL_USE_CASES_BASELINE)

    assert report.ok is False
    rogue = [v for v in report.new_violations if _ROGUE_MULTI_FILENAME in v.file]
    assert rogue, "the rogue consumer was not flagged with file/line"

    symbols = {v.symbol for v in rogue}
    assert "AsyncSession" in symbols
    assert any(s == "get_db" or s.startswith("Depends(get_db)") for s in symbols)
    assert any(s == "get_db_for_mcp" for s in symbols)
    for v in rogue:
        assert v.line > 0  # file:line reported

    # AC4: per-surface counters for the new direct uses.
    assert report.new_by_surface.get("rest", 0) >= 1
    assert report.new_by_surface.get("mcp", 0) >= 1
    assert report.new_by_surface.get("service", 0) >= 1


def test_ratchet_passes_for_clean_use_cases_baseline() -> None:
    """No rogue present: the strangled use_cases layer is clean at the 0 baseline,
    and the clean (port/repo-based) use cases there are not flagged."""
    report = run_relational_ratchet_gate(baseline=RELATIONAL_USE_CASES_BASELINE)
    assert report.ok is True, report.as_dict()
    assert report.new_violations == []


def test_ratchet_is_baseline_aware_not_a_blind_threshold(rogue_multi) -> None:
    """If the rogue's exact per-(file,symbol) COUNTS are declared as the baseline,
    the ratchet tolerates them instead of failing — proving it is baseline-aware."""
    flagged = run_relational_ratchet_gate(baseline=RELATIONAL_USE_CASES_BASELINE)
    declared = _counts(flagged)
    tolerated = run_relational_ratchet_gate(baseline=declared)
    assert tolerated.ok is True, tolerated.as_dict()
    assert tolerated.new_violations == []


def test_ratchet_is_count_aware_second_callsite_same_symbol_still_fails(rogue_count) -> None:
    """Adversarial (codex): a (file, symbol) key must NOT collapse duplicates.

    The rogue has N>=2 get_db_for_mcp couplings in one file. A baseline tolerating
    only N-1 must still FAIL on the N-th — a bare ``(file, symbol)`` set key would
    have masked all of them with a single entry (the bug this rework fixes)."""
    full = run_relational_ratchet_gate(baseline=RELATIONAL_USE_CASES_BASELINE)
    rogue = [
        v
        for v in full.new_violations
        if _ROGUE_COUNT_FILENAME in v.file and v.symbol == "get_db_for_mcp"
    ]
    n = len(rogue)
    assert n >= 2, f"expected >=2 get_db_for_mcp couplings, got {n}"
    file_key = relational_ratchet_key(rogue[0])

    # Tolerate all but one → the extra call-site is still a new violation.
    all_but_one = run_relational_ratchet_gate(baseline={file_key: n - 1})
    assert all_but_one.ok is False
    leaked = [
        v
        for v in all_but_one.new_violations
        if _ROGUE_COUNT_FILENAME in v.file and v.symbol == "get_db_for_mcp"
    ]
    assert len(leaked) == 1, leaked
    exceeded = [e for e in all_but_one.exceeded if e["symbol"] == "get_db_for_mcp" and _ROGUE_COUNT_FILENAME in e["file"]]
    assert exceeded and exceeded[0]["baseline"] == n - 1 and exceeded[0]["live"] == n

    # Tolerate all N → clean (count exactly matched).
    all_n = run_relational_ratchet_gate(baseline={file_key: n})
    leaked_all = [
        v
        for v in all_n.new_violations
        if _ROGUE_COUNT_FILENAME in v.file and v.symbol == "get_db_for_mcp"
    ]
    assert leaked_all == []


def test_ratchet_baseline_only_shrinks() -> None:
    previous = {("core/x.py", "get_db"): 2, ("core/y.py", "AsyncSession"): 1}
    shrunk = {("core/x.py", "get_db"): 1}
    raised = {("core/x.py", "get_db"): 3}
    new_key = {("core/z.py", "get_db_for_mcp"): 1}
    assert ratchet_baseline_only_shrinks(previous, shrunk) is True
    assert ratchet_baseline_only_shrinks(previous, previous) is True
    assert ratchet_baseline_only_shrinks(previous, raised) is False
    assert ratchet_baseline_only_shrinks(previous, new_key) is False
