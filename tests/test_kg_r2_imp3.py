"""R2-IMP3 — maturity replay of CanonicalDebt.

Spec 9aedfe78 / card d3263170 (FR6/TR6/TR11/TR12; ts_fe9fe207, ts_110b573e).

Anti-test-theater: the canonical evidence comes from the REAL SQL source + the
maturity classifier; the post-commit chain runs the REAL worker
``_process_queue_entry``. No direct Kuzu/DecisionDigest seed of canonical state.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2i3_"))

from okto_pulse.core.kg.board_source_store import BoardSourceStore
from okto_pulse.core.kg.canonical_debt_replay import (
    _pulse_db_path,
    replay_canonical_debt_by_maturity,
)
from okto_pulse.core.kg.schema import bootstrap_board_graph
from okto_pulse.core.models.db import Board, Spec
from okto_pulse.core.services.canonical_debt_service import (
    list_canonical_debt,
    upsert_canonical_debt,
)

USER_ID = "user-r2-imp3"
TARGET_STATUS = "canonical_consolidation"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _new_board(db_factory) -> str:
    board_id = f"r2i3-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r2 imp3", owner_id=USER_ID))
            await db.commit()
    return board_id


async def _insert_spec(db_factory, board_id, spec_id, *, status):
    async with db_factory() as db:
        db.add(Spec(
            id=spec_id, board_id=board_id, title="replay spec", status=status,
            created_by=USER_ID,
            functional_requirements=["FR alpha replay", "FR beta replay"],
            acceptance_criteria=["AC alpha replay"],
        ))
        await db.commit()


async def _set_spec_status(db_factory, spec_id, status):
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.status = status
        await db.commit()


def _spec_source(board_id, spec_id) -> dict:
    for row in BoardSourceStore(db_path=_pulse_db_path()).fetch(board_id):
        if str(row.get("id")) == spec_id and row.get("artifact_type") == "spec":
            return row
    raise AssertionError(f"spec {spec_id} not found in BoardSourceStore")


async def _open_debt_count(db_factory, board_id) -> int:
    async with db_factory() as db:
        res = await list_canonical_debt(db, board_id=board_id, limit=200)
    from okto_pulse.core.services.canonical_debt_service import OPEN_STATES
    return sum(1 for r in res.items if str(r.get("canonical_state") or "") in OPEN_STATES)


async def _seed_open_debt(db_factory, board_id, spec_id, content_hash, source_ref, source_version):
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="spec", artifact_id=spec_id,
            source_ref=source_ref, content_hash=content_hash,
            target_status=TARGET_STATUS, canonical_state="failed",
            source_version=str(source_version) if source_version is not None else None,
            failure_reason="r2imp3_pending_maturity",
        )
        await db.commit()


# ===========================================================================
# Entry point: maturity replay closes a debt once its source is canonical (#5)
# ===========================================================================


@pytest.mark.asyncio
async def test_replay_closes_debt_only_after_source_matures_canonical(db_factory):
    board_id = await _new_board(db_factory)
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="draft")
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, src["content_hash"],
                          src["source_ref"], src.get("source_version"))
    assert await _open_debt_count(db_factory, board_id) == 1

    # Source is still draft (classifies working) -> replay must NOT close (#5).
    async with db_factory() as db:
        neg = await replay_canonical_debt_by_maturity(db, board_id=board_id)
    assert neg["committed_count"] == 0
    assert neg["skipped_non_canonical"] >= 1
    assert await _open_debt_count(db_factory, board_id) == 1

    # Source matures to done (content_hash is status-invariant) -> replay closes.
    await _set_spec_status(db_factory, spec_id, "done")
    async with db_factory() as db:
        pos = await replay_canonical_debt_by_maturity(db, board_id=board_id)
        await db.commit()
    assert pos["committed_count"] == 1, pos
    assert await _open_debt_count(db_factory, board_id) == 0

    # Idempotent: a repeated trigger commits nothing more.
    async with db_factory() as db:
        again = await replay_canonical_debt_by_maturity(db, board_id=board_id)
    assert again["committed_count"] == 0
    assert again["open_before"] == 0


@pytest.mark.asyncio
async def test_replay_does_not_close_on_hash_mismatch(db_factory):
    """Canonical source but a DIFFERENT content_hash (content drifted) must not
    close the debt — never close on similar-id/status alone (#5)."""
    board_id = await _new_board(db_factory)
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")  # canonical
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, "STALE_DIFFERENT_HASH",
                          src["source_ref"], src.get("source_version"))

    async with db_factory() as db:
        res = await replay_canonical_debt_by_maturity(db, board_id=board_id)
    assert res["committed_count"] == 0
    assert res["skipped_hash_mismatch"] >= 1
    assert await _open_debt_count(db_factory, board_id) == 1


# ===========================================================================
# #6 — cognitive pending is NOT closed as a side effect of debt replay
# ===========================================================================


@pytest.mark.asyncio
async def test_replay_does_not_touch_cognitive_pending(db_factory, _tmp_rebuild_dir):
    from okto_pulse.core.kg.connectivity_guard import (
        CANONICAL_LEARNING_WORKING_ONLY_REASON,
    )
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        record_cognitive_working_only_hold,
    )

    board_id = await _new_board(db_factory)
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, src["content_hash"],
                          src["source_ref"], src.get("source_version"))
    # A live cognitive pending hold that must remain pending after debt replay.
    hold_ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    record_cognitive_working_only_hold(
        board_id=board_id,
        hold_payload={"reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
                      "source_ref": hold_ref, "artifact_type": "bug",
                      "observed_endpoints": [], "session_id": "sess-r2i3"},
        actor_id="system:test", base_dir=_tmp_rebuild_dir,
    )

    async with db_factory() as db:
        res = await replay_canonical_debt_by_maturity(db, board_id=board_id)
        await db.commit()
    assert res["committed_count"] == 1  # the debt closed ...

    # ... but the cognitive pending hold is untouched (still pending).
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    gen = store.latest_generation(board_id)
    items = store.list_items(board_id, gen)
    held = [i for i in items if str(getattr(i, "reason_code", "") or "")
            == CANONICAL_LEARNING_WORKING_ONLY_REASON]
    assert held and all(i.status == "pending" for i in held)


# ===========================================================================
# #3 — the REAL worker post-commit chain triggers the replay
# ===========================================================================


@pytest.mark.asyncio
async def test_worker_post_commit_chain_triggers_replay(db_factory):
    """ts_110b573e: a real consolidation through the worker's _process_queue_entry
    fires the post-commit replay, closing a now-canonical debt — the same chain a
    rebuild drain uses (no theoretical-only coverage)."""
    from okto_pulse.core.kg.workers.consolidation import _process_queue_entry
    from okto_pulse.core.models.db import ConsolidationQueue

    board_id = await _new_board(db_factory)
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")  # canonical
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, src["content_hash"],
                          src["source_ref"], src.get("source_version"))
    assert await _open_debt_count(db_factory, board_id) == 1

    # Enqueue + run the REAL worker entry processor (the path a rebuild drain uses).
    async with db_factory() as db:
        entry = ConsolidationQueue(
            id=str(uuid.uuid4()), board_id=board_id, artifact_type="spec",
            artifact_id=spec_id, priority="high", source="r2i3_test",
            status="pending", attempts=0,
        )
        db.add(entry)
        await db.commit()
        await db.refresh(entry)
        await _process_queue_entry(db, entry)
        await db.commit()

    # The post-commit replay closed the now-canonical debt via the real chain.
    assert await _open_debt_count(db_factory, board_id) == 0
