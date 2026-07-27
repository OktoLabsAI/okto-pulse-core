"""R2-TEST5 (card 3b50f481) — maturity replay of CanonicalDebt + idempotency.

Scenarios: ts_74356b61 (AC6 — replay closes a debt ONLY when its canonical
evidence is verifiable: source classified canonical AND content_hash matches;
never on status/id alone) and ts_ec445b2a (AC12 — a second replay is a no-op, and
cognitive pending / DLQ stay distinct domains, never closed as a side effect).

Anti-test-theater: the canonical evidence is the REAL SQL source + the maturity
classifier; nothing is closed by a raw seed. Teeth: while the source is draft (or
the hash differs) the debt MUST stay open — a replay that closed it anyway fails.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t5_"))

from r2_scenario_helpers import insert_spec, new_board, set_spec_status

from okto_pulse.core.kg.canonical_debt_replay import (
    replay_canonical_debt_by_maturity,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.services.canonical_debt_service import (
    OPEN_STATES,
    list_canonical_debt,
    upsert_canonical_debt,
)

TARGET_STATUS = "canonical_consolidation"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


def _spec_source(board_id, spec_id) -> dict:
    reader = get_kg_registry().require_board_source_reader()
    for row in reader.fetch(board_id):
        if str(row.get("id")) == spec_id and row.get("artifact_type") == "spec":
            return row
    raise AssertionError(f"spec {spec_id} not found in BoardSourceReader")


async def _open_debt_count(db_factory, board_id) -> int:
    async with db_factory() as db:
        res = await list_canonical_debt(db, board_id=board_id, limit=200)
    return sum(1 for r in res.items if str(r.get("canonical_state") or "") in OPEN_STATES)


async def _seed_open_debt(db_factory, board_id, spec_id, content_hash, source_ref, source_version):
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="spec", artifact_id=spec_id,
            source_ref=source_ref, content_hash=content_hash,
            target_status=TARGET_STATUS, canonical_state="failed",
            source_version=str(source_version) if source_version is not None else None,
            failure_reason="r2t5_pending_maturity",
        )
        await db.commit()


# ===========================================================================
# ts_74356b61 — AC6: replay closes a debt only on verifiable canonical evidence
# ===========================================================================


@pytest.mark.asyncio
async def test_replay_closes_only_on_verifiable_canonical_evidence(db_factory):
    board_id = await new_board(db_factory, "r2t5")
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await insert_spec(db_factory, board_id, spec_id, status="draft")
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, src["content_hash"],
                          src["source_ref"], src.get("source_version"))
    assert await _open_debt_count(db_factory, board_id) == 1

    # Source still draft (classifies working) -> NOT canonical evidence -> no close.
    async with db_factory() as db:
        neg = await replay_canonical_debt_by_maturity(db, board_id=board_id)
    assert neg["committed_count"] == 0
    assert neg["skipped_non_canonical"] >= 1
    # TEETH: a replay that closed on textual status would drop this to 0.
    assert await _open_debt_count(db_factory, board_id) == 1

    # Source matures to done (content_hash is status-invariant) -> close.
    await set_spec_status(db_factory, spec_id, "done")
    async with db_factory() as db:
        pos = await replay_canonical_debt_by_maturity(db, board_id=board_id)
        await db.commit()
    assert pos["committed_count"] == 1, pos
    assert await _open_debt_count(db_factory, board_id) == 0


@pytest.mark.asyncio
async def test_replay_does_not_close_on_hash_mismatch(db_factory):
    """Canonical source but a DIFFERENT content_hash -> never close (content
    drifted; not the same evidence)."""
    board_id = await new_board(db_factory, "r2t5")
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await insert_spec(db_factory, board_id, spec_id, status="done")  # canonical
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, "STALE_DIFFERENT_HASH",
                          src["source_ref"], src.get("source_version"))

    async with db_factory() as db:
        res = await replay_canonical_debt_by_maturity(db, board_id=board_id)
    assert res["committed_count"] == 0
    assert res["skipped_hash_mismatch"] >= 1
    assert await _open_debt_count(db_factory, board_id) == 1


# ===========================================================================
# ts_ec445b2a — AC12: idempotent second run + cognitive pending stays distinct
# ===========================================================================


@pytest.mark.asyncio
async def test_replay_is_idempotent_and_keeps_cognitive_pending_distinct(
    db_factory, _tmp_rebuild_dir
):
    from okto_pulse.core.kg.connectivity_guard import (
        CANONICAL_LEARNING_WORKING_ONLY_REASON,
    )
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        record_cognitive_working_only_hold,
    )

    board_id = await new_board(db_factory, "r2t5")
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await insert_spec(db_factory, board_id, spec_id, status="done")  # canonical
    src = _spec_source(board_id, spec_id)
    await _seed_open_debt(db_factory, board_id, spec_id, src["content_hash"],
                          src["source_ref"], src.get("source_version"))

    # A live cognitive pending hold (a DISTINCT domain) that must NOT be closed
    # as a side effect of debt replay.
    hold_ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    record_cognitive_working_only_hold(
        board_id=board_id,
        hold_payload={"reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
                      "source_ref": hold_ref, "artifact_type": "bug",
                      "observed_endpoints": [], "session_id": "sess-r2t5"},
        actor_id="system:test", base_dir=_tmp_rebuild_dir,
    )

    # First replay closes the now-canonical debt ...
    async with db_factory() as db:
        first = await replay_canonical_debt_by_maturity(db, board_id=board_id)
        await db.commit()
    assert first["committed_count"] == 1
    assert await _open_debt_count(db_factory, board_id) == 0

    # ... a second replay is a no-op (idempotent / convergent).
    async with db_factory() as db:
        second = await replay_canonical_debt_by_maturity(db, board_id=board_id)
        await db.commit()
    assert second["committed_count"] == 0
    assert second["open_before"] == 0

    # ... and the cognitive pending hold is untouched (distinct domain).
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    gen = store.latest_generation(board_id)
    items = store.list_items(board_id, gen)
    held = [i for i in items if str(getattr(i, "reason_code", "") or "")
            == CANONICAL_LEARNING_WORKING_ONLY_REASON]
    assert held and all(i.status == "pending" for i in held)
