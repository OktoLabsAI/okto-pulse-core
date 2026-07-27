"""R2-TEST6 (card 309d1ba6) — fast-path/sweep idempotency + anti-test-theater.

Scenarios: ts_bb459cfe (AC8 — the event fast-path and the full sweep converge to
the SAME final state in any order; the second run is a no-op) and ts_f2b3f62c
(AC11/TR12 — the canonical-current starting state comes from the REAL consolidation
pipeline, NOT a direct Kuzu/DecisionDigest seed, and the reconciler is convergent
over it).

Teeth: the second run in each ordering must report ``demoted == []`` and leave the
canonical count at 0; a non-convergent reconciler that re-churns would break this.
"""

from __future__ import annotations

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t6_"))

from r2_scenario_helpers import (
    count_canonical,
    first_canonical_node,
    new_board,
    seed_done_spec_canonical,
    set_spec_status,
)

from okto_pulse.core.kg.canonical_stale_reconciler import reconcile_stale_canonical


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ===========================================================================
# ts_bb459cfe — AC8: fast-path and sweep converge to the same state, any order
# ===========================================================================


@pytest.mark.asyncio
async def test_fast_path_then_sweep_is_idempotent(db_factory):
    board_id = await new_board(db_factory, "r2t6")
    spec_id, spec_ref = await seed_done_spec_canonical(db_factory, board_id)
    await set_spec_status(db_factory, spec_id, "draft")

    # Event fast-path (scoped) demotes ...
    async with db_factory() as db:
        fast = await reconcile_stale_canonical(db, board_id=board_id, source_refs=[spec_ref])
        await db.commit()
    assert fast.demoted
    assert await count_canonical(board_id, "Requirement") == 0

    # ... a following full sweep is a NO-OP (already converged).
    async with db_factory() as db:
        sweep = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert sweep.demoted == []
    assert await count_canonical(board_id, "Requirement") == 0


@pytest.mark.asyncio
async def test_sweep_then_fast_path_is_idempotent(db_factory):
    board_id = await new_board(db_factory, "r2t6")
    spec_id, spec_ref = await seed_done_spec_canonical(db_factory, board_id)
    await set_spec_status(db_factory, spec_id, "draft")

    # Full sweep demotes first ...
    async with db_factory() as db:
        sweep = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert sweep.demoted
    assert await count_canonical(board_id, "Requirement") == 0

    # ... a following event fast-path is a NO-OP (same converged final state).
    async with db_factory() as db:
        fast = await reconcile_stale_canonical(db, board_id=board_id, source_refs=[spec_ref])
        await db.commit()
    assert fast.demoted == []
    assert await count_canonical(board_id, "Requirement") == 0


# ===========================================================================
# ts_f2b3f62c — AC11/TR12: real-pipeline canonical-current (not a direct seed)
# ===========================================================================


@pytest.mark.asyncio
async def test_canonical_current_is_real_pipeline_and_reconcile_is_convergent(db_factory):
    board_id = await new_board(db_factory, "r2t6")
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)

    # ANTI-THEATER: the canonical-current node was produced by the real
    # DeterministicWorker + commit_consolidation — its source_artifact_ref points
    # at the real SQL spec, not a synthetic seed key.
    canonical = await first_canonical_node(board_id, "Requirement")
    assert canonical is not None
    _node_id, source_ref = canonical
    assert source_ref.startswith(f"spec:{spec_id}"), source_ref

    await set_spec_status(db_factory, spec_id, "draft")

    # Convergent: first reconcile demotes, second is a no-op.
    async with db_factory() as db:
        first = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert first.demoted
    assert await count_canonical(board_id, "Requirement") == 0

    async with db_factory() as db:
        again = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert again.demoted == []
    assert await count_canonical(board_id, "Requirement") == 0
