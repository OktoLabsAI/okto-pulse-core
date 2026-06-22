"""R5-IMP2 (card 710c3eb0) — read-only skip-override read-model.

Anti-test-theater: the ambiguity skip is set through the REAL
``IdeationService.set_ambiguity_gate_skip`` (which writes the audit log the
read-model reads); the cognitive skip is a REAL ``CognitiveConsolidationItem``
ledger record. The read-model is proven read-only (mutation_allowed=false) and
proven NOT to mask a separate open CanonicalDebt.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5i2_"))

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
)
from okto_pulse.core.models.db import Board, Ideation
from okto_pulse.core.services.main import IdeationService
from okto_pulse.core.services.skip_overrides import (
    GATE_AMBIGUITY,
    GATE_COGNITIVE,
    ideation_skip_overrides,
    spec_skip_overrides,
)

USER_ID = "user-r5-imp2"
NOW = datetime.now(timezone.utc)


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


def _seed_skipped(base_dir, board, gen, *, source_ref, reason_code="trivial_fix",
                  justification="reviewed", evidence_refs=("ev:1",), revisit_at=None,
                  actor="agent:reviewer"):
    """Write a generation record with one SKIPPED cognitive ledger item."""
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    path = store._record_path(board, gen)
    path.parent.mkdir(parents=True, exist_ok=True)
    iid = compute_cognitive_item_id(board, gen, source_ref)
    item = {
        "item_id": iid, "board_id": board, "kg_generation_id": gen,
        "source_ref": source_ref, "artifact_type": source_ref.split(":", 1)[0],
        "status": CognitiveItemStatus.SKIPPED.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
        "updated_at": "2026-06-17T01:00:00+00:00",
        "reason_code": reason_code, "justification": justification,
        "evidence_refs": list(evidence_refs), "actor": actor,
    }
    if revisit_at is not None:
        item["revisit_at"] = revisit_at
    record = {"pending_count": 0, "pending_refs": [], "status": "complete",
              "recorded_at": "2026-06-17T00:00:00+00:00", "items": [item]}
    path.write_text(json.dumps(record), encoding="utf-8")
    return store


async def _board_ideation(db_factory) -> tuple[str, str]:
    board_id = f"r5i2-{uuid.uuid4().hex[:10]}"
    ideation_id = f"idea-{uuid.uuid4().hex[:10]}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp2", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="idea", created_by=USER_ID))
        await db.commit()
    return board_id, ideation_id


# ===========================================================================
# Cognitive skip read-model (spec context)
# ===========================================================================


@pytest.mark.asyncio
async def test_spec_cognitive_skip_override_full_fields(db_factory, _tmp_rebuild_dir):
    board_id = f"r5i2-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    card_id = f"card-{uuid.uuid4().hex[:10]}"
    _seed_skipped(_tmp_rebuild_dir, board_id, "gen-1", source_ref=f"card:{card_id}",
                  reason_code="duplicate_bug", justification="dup of X",
                  evidence_refs=("card:other",))

    spec = SimpleNamespace(id=spec_id, cards=[SimpleNamespace(id=card_id)])
    async with db_factory() as db:
        overrides = await spec_skip_overrides(db, spec, board_id)

    assert len(overrides) == 1, overrides
    o = overrides[0]
    assert o["gate"] == GATE_COGNITIVE
    assert o["effective_state"] == "active"
    assert o["reason_code"] == "duplicate_bug"
    assert o["justification"] == "dup of X"
    assert o["evidence_refs"] == ["card:other"]
    assert o["applied_by"] == "agent:reviewer"
    assert o["applied_at"] == "2026-06-17T01:00:00+00:00"
    assert o["mutation_allowed"] is False
    assert o["target_ref"] == f"card:{card_id}"


@pytest.mark.asyncio
async def test_spec_cognitive_skip_expired_when_revisit_past(db_factory, _tmp_rebuild_dir):
    board_id = f"r5i2-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    card_id = f"card-{uuid.uuid4().hex[:10]}"
    _seed_skipped(_tmp_rebuild_dir, board_id, "gen-1", source_ref=f"card:{card_id}",
                  reason_code="evidence_insufficient",
                  revisit_at=(NOW - timedelta(hours=1)).isoformat())

    spec = SimpleNamespace(id=spec_id, cards=[SimpleNamespace(id=card_id)])
    async with db_factory() as db:
        overrides = await spec_skip_overrides(db, spec, board_id)
    assert overrides[0]["effective_state"] == "expired"


@pytest.mark.asyncio
async def test_spec_skip_filters_to_spec_cards(db_factory, _tmp_rebuild_dir):
    board_id = f"r5i2-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    own_card = f"card-{uuid.uuid4().hex[:10]}"
    other_card = f"card-{uuid.uuid4().hex[:10]}"
    store = _seed_skipped(_tmp_rebuild_dir, board_id, "gen-1", source_ref=f"card:{own_card}")
    # A second skipped item for a card NOT on this spec must be excluded.
    path = store._record_path(board_id, "gen-1")
    rec = json.loads(path.read_text(encoding="utf-8"))
    rec["items"].append({
        "item_id": compute_cognitive_item_id(board_id, "gen-1", f"card:{other_card}"),
        "board_id": board_id, "kg_generation_id": "gen-1",
        "source_ref": f"card:{other_card}", "artifact_type": "card",
        "status": CognitiveItemStatus.SKIPPED.value,
        "recorded_at": "2026-06-17T00:00:00+00:00", "reason_code": "trivial_fix",
    })
    path.write_text(json.dumps(rec), encoding="utf-8")

    spec = SimpleNamespace(id=spec_id, cards=[SimpleNamespace(id=own_card)])
    async with db_factory() as db:
        overrides = await spec_skip_overrides(db, spec, board_id)
    assert [o["target_ref"] for o in overrides] == [f"card:{own_card}"]


@pytest.mark.asyncio
async def test_cognitive_skip_does_not_mask_open_canonical_debt(db_factory, _tmp_rebuild_dir):
    """A skip in the read-model never hides a SEPARATE open CanonicalDebt — the debt
    stays visible via its own diagnostic (distinct domain)."""
    from okto_pulse.core.services.canonical_debt_service import (
        OPEN_STATES,
        list_canonical_debt,
        upsert_canonical_debt,
    )

    board_id = f"r5i2-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    card_id = f"card-{uuid.uuid4().hex[:10]}"
    _seed_skipped(_tmp_rebuild_dir, board_id, "gen-1", source_ref=f"card:{card_id}")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5", owner_id=USER_ID))
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="card", artifact_id=card_id,
            source_ref=f"card:{card_id}", content_hash="h", target_status="canonical",
            canonical_state="pending", failure_reason="x",
        )
        await db.commit()

    spec = SimpleNamespace(id=spec_id, cards=[SimpleNamespace(id=card_id)])
    async with db_factory() as db:
        overrides = await spec_skip_overrides(db, spec, board_id)
        debt = await list_canonical_debt(db, board_id=board_id, limit=50)
    # The skip is surfaced ...
    assert overrides and overrides[0]["gate"] == GATE_COGNITIVE
    # ... and the open canonical debt remains visible SEPARATELY (not masked).
    assert any(str(r.get("canonical_state")) in OPEN_STATES for r in debt.items)


# ===========================================================================
# Ambiguity skip read-model (ideation context) — via the REAL write path
# ===========================================================================


@pytest.mark.asyncio
async def test_ideation_ambiguity_skip_override_via_real_service(db_factory):
    board_id, ideation_id = await _board_ideation(db_factory)
    async with db_factory() as db:
        await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id, USER_ID, True, source="rest",
        )
        await db.commit()

    async with db_factory() as db:
        ideation = await IdeationService(db).get_ideation(ideation_id)
        overrides = await ideation_skip_overrides(db, ideation, board_id)

    assert len(overrides) == 1
    o = overrides[0]
    assert o["gate"] == GATE_AMBIGUITY
    assert o["effective_state"] == "active"
    assert o["applied_by"] == USER_ID
    assert o["applied_at"] is not None
    assert o["source"].endswith(":rest")
    # ambiguity skip carries no reason_code/justification/evidence.
    assert o["reason_code"] is None and o["justification"] is None
    assert o["evidence_refs"] == []
    assert o["mutation_allowed"] is False
    assert o["target_ref"] == f"ideation:{ideation_id}"


@pytest.mark.asyncio
async def test_ideation_skip_overrides_empty_when_not_skipped(db_factory):
    board_id, ideation_id = await _board_ideation(db_factory)
    async with db_factory() as db:
        ideation = await IdeationService(db).get_ideation(ideation_id)
        overrides = await ideation_skip_overrides(db, ideation, board_id)
    assert overrides == []
