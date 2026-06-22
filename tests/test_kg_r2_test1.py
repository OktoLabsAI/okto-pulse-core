"""R2-TEST1 (card 7cf83fef) — status regression + stale sweep.

Scenarios: ts_d2ec814b (AC1 — a source status regression removes the
canonical-current publication via the event fast-path) and ts_db296285 (AC2 —
a full sweep with no triggering event still detects and demotes stale canonical),
plus the cognitive carve-out (Learning/Alternative/Assumption never demoted).

Anti-test-theater: the canonical-current starting state is materialized by the
REAL DeterministicWorker + commit_consolidation pipeline; the regression is the
real SQL maturity signal. The reconciler has teeth — the canonical count must go
from >=1 to 0; a no-op reconciler leaves it >=1 and fails the test.
"""

from __future__ import annotations

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t1_"))

from r2_scenario_helpers import (
    count_canonical,
    new_board,
    node_layer,
    seed_canonical_cognitive,
    seed_done_spec_canonical,
    set_spec_status,
)

from okto_pulse.core.kg.canonical_stale_reconciler import reconcile_stale_canonical
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ===========================================================================
# ts_d2ec814b — AC1: a status regression removes the canonical-current
# publication (event fast-path scoped to the regressed source).
# ===========================================================================


@pytest.mark.asyncio
async def test_status_regression_removes_canonical_publication(db_factory):
    board_id = await new_board(db_factory, "r2t1")
    spec_id, spec_ref = await seed_done_spec_canonical(db_factory, board_id)
    canonical_before = count_canonical(board_id, "Requirement")
    assert canonical_before >= 1, "real pipeline must publish canonical Requirements"

    # Source regresses done -> draft (the real maturity signal an event carries).
    await set_spec_status(db_factory, spec_id, "draft")

    # Event fast-path scoped to the regressed source removes canonical publication.
    async with db_factory() as db:
        result = await reconcile_stale_canonical(
            db, board_id=board_id, source_refs=[spec_ref],
        )
        await db.commit()

    assert result.demoted, result.to_dict()
    rec = result.demoted[0]
    assert rec["prev_layer"] == "canonical" and rec["expected_layer"] == "working"
    assert rec["source_artifact_ref"].startswith("spec:")
    # TEETH: the canonical publication is GONE. A no-op reconciler leaves the
    # canonical count unchanged (>=1) and this assertion fails.
    assert count_canonical(board_id, "Requirement") == 0


# ===========================================================================
# ts_db296285 — AC2: a full sweep (no triggering event) detects + demotes the
# stale canonical, and the cognitive carve-out still holds.
# ===========================================================================


@pytest.mark.asyncio
async def test_sweep_detects_and_demotes_stale_and_preserves_cognitive(db_factory):
    board_id = await new_board(db_factory, "r2t1")
    spec_id, _spec_ref = await seed_done_spec_canonical(db_factory, board_id)
    # A canonical cognitive Alternative whose (absent) source could look stale —
    # it must be PRESERVED by the carve-out, never demoted.
    alt_id = seed_canonical_cognitive(
        board_id, "Alternative", source_ref=f"spec:{spec_id}:alternative:1",
    )
    assert count_canonical(board_id, "Requirement") >= 1

    await set_spec_status(db_factory, spec_id, "draft")

    # Full sweep — source_refs=None, i.e. NO specific triggering event.
    async with db_factory() as db:
        result = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()

    # The stale deterministic canonical is demoted by the sweep ...
    assert result.demoted
    assert count_canonical(board_id, "Requirement") == 0
    # ... and the cognitive Alternative is preserved (carve-out, no auto-debt).
    assert node_layer(board_id, "Alternative", alt_id) == GRAPH_LAYER_CANONICAL
    assert any(s["node_id"] == alt_id for s in result.skipped_cognitive)
    assert all(d["node_id"] != alt_id for d in result.demoted)
    assert result.routed_to_debt == []


@pytest.mark.asyncio
async def test_sweep_is_idempotent(db_factory):
    """AC8/TR7 supporting check: a second sweep over the already-demoted state is
    a no-op (convergent) — guards against a reconciler that re-churns."""
    board_id = await new_board(db_factory, "r2t1")
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)
    await set_spec_status(db_factory, spec_id, "draft")

    async with db_factory() as db:
        first = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert first.demoted

    async with db_factory() as db:
        second = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert second.demoted == []
    assert count_canonical(board_id, "Requirement") == 0
