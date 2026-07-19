"""R2-TEST7 (card 1e15a2a6) — canonical cognitive Learning is never demoted.

Scenario ts_6532f6ed: a canonical Learning created by the cognitive/R7 flow must
NOT be demoted to working when its related evidence regresses to working/immature.
The carve-out preserves ``graph_layer=canonical`` on the Learning; a material
bug-derived Learning routes the irregularity to the EXISTING R7 CanonicalDebt
(never a parallel taxonomy), and a non-bug-derived cognitive node is a preserved
skip with no auto-debt.

Anti-test-theater: the canonical Learning is materialized via the orchestrator
write primitive (R7-validated), the bug source regression is the real SQL maturity
signal, and the same sweep that demotes a stale DETERMINISTIC node is exercised —
proving the carve-out is selective, not a blanket skip.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t7_"))

from r2_scenario_helpers import (
    count_canonical,
    insert_bug_card,
    new_board,
    node_layer,
    seed_canonical_cognitive,
    seed_done_spec_canonical,
    set_spec_status,
)

from okto_pulse.core.kg.canonical_stale_reconciler import (
    ACTION_ROUTED_DEBT,
    reconcile_stale_canonical,
)
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ===========================================================================
# ts_6532f6ed — bug-derived canonical Learning preserved + routed to R7 debt
# ===========================================================================


@pytest.mark.asyncio
async def test_bug_derived_canonical_learning_preserved_and_routed_to_debt(db_factory):
    board_id = await new_board(db_factory, "r2t7")
    bug_id = uuid.uuid4().hex
    # Bug source regressed (draft -> classifies working, not canonical).
    await insert_bug_card(db_factory, board_id, bug_id, status="draft")
    learning_ref = f"card:bug:{bug_id}:learning:{uuid.uuid4().hex}"
    learning_id = await seed_canonical_cognitive(
        board_id, "Learning", source_ref=learning_ref
    )

    async with db_factory() as db:
        result = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()

    # PRESERVED: the canonical Learning is never demoted ...
    assert await node_layer(
        board_id, "Learning", learning_id
    ) == GRAPH_LAYER_CANONICAL
    assert all(d["node_id"] != learning_id for d in result.demoted)
    # ... and the material irregularity is routed to the EXISTING R7 CanonicalDebt.
    routed = [r for r in result.routed_to_debt if r["node_id"] == learning_id]
    assert routed and routed[0]["action"] == ACTION_ROUTED_DEBT

    # Idempotent: a second sweep still never demotes the Learning.
    async with db_factory() as db:
        result2 = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert all(d["node_id"] != learning_id for d in result2.demoted)
    assert await node_layer(
        board_id, "Learning", learning_id
    ) == GRAPH_LAYER_CANONICAL


# ===========================================================================
# Non-bug-derived cognitive node preserved (skipped_cognitive, no auto-debt),
# while a stale DETERMINISTIC node in the SAME sweep IS demoted (selective).
# ===========================================================================


@pytest.mark.asyncio
async def test_carveout_is_selective_cognitive_preserved_deterministic_demoted(db_factory):
    board_id = await new_board(db_factory, "r2t7")
    # A stale deterministic spec (its canonical children WILL be demoted) ...
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)
    await set_spec_status(db_factory, spec_id, "draft")
    # ... alongside a non-bug-derived canonical cognitive Assumption that MUST be
    # preserved (carve-out) and create NO debt.
    assumption_id = await seed_canonical_cognitive(
        board_id, "Assumption", source_ref=f"spec:{spec_id}:assumption:1",
    )

    async with db_factory() as db:
        result = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()

    # Deterministic stale canonical WAS demoted (proves the sweep is live) ...
    assert result.demoted
    assert await count_canonical(board_id, "Requirement") == 0
    # ... but the cognitive Assumption is preserved as a skip, with no auto-debt.
    assert await node_layer(
        board_id, "Assumption", assumption_id
    ) == GRAPH_LAYER_CANONICAL
    assert any(s["node_id"] == assumption_id for s in result.skipped_cognitive)
    assert all(d["node_id"] != assumption_id for d in result.demoted)
    assert all(r["node_id"] != assumption_id for r in result.routed_to_debt)
