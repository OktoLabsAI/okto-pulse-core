"""RKG-01 — cognitive-effectiveness inventory (read-only).

Scenarios (spec RKG-01 ca373f51):
  * ts_3f30b26c / AC1 — board with KG healthy, queue_depth=0, dead_letter_count=4:
    the inventory returns the 4 DLQ artifacts as technical DLQ and does NOT
    declare the cognitive KG effective.
  * ts_44a6d0c7 / AC2 — done artifact with an emitted candidate but no persisted
    node/edge and no DLQ: classified extractor_triggered_but_not_persisted with
    an explicit inference label + a next_action pointing at the consumer/RKG-03.
  * ts_0dd43261 / AC3 — done artifact with no alternative/risk/decision/root
    cause/fix: classified no_material, never a failure, never a synthetic node.

Plus (codex guardrail): a read-only fake candidate source proves the literal
``candidate_log`` evidence replaces the inference WITHOUT changing the contract,
and the read-only / error contracts.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    ConsolidationDeadLetter,
    Spec,
    SpecStatus,
)
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.services import cognitive_effectiveness_service as ces

UUID_A = "11111111-1111-1111-1111-111111111111"
UUID_B = "22222222-2222-2222-2222-222222222222"
UUID_C = "33333333-3333-3333-3333-333333333333"
UUID_D = "44444444-4444-4444-4444-444444444444"
UUID_E = "55555555-5555-5555-5555-555555555555"
DLQ_SPECS = [
    "3f346654-65a7-4416-a10b-b3f2ee0dfe2e",
    "8c45a58b-c8c4-41d0-898d-b1e4897d02bd",
    "e47a0940-e310-4843-a38d-0bd01e9c52f0",
    "32f909c6-0242-4b4a-8591-7645c1ee1a01",
]


@pytest.fixture(autouse=True)
def _no_graph(monkeypatch):
    """Force the read-only graph projection to degrade to empty so the SQL/store
    classification logic is exercised deterministically without a bootstrapped
    board graph. (The persisted-node path is covered separately.)"""
    import okto_pulse.core.kg.schema as schema_mod

    def _boom(*_a, **_k):
        raise RuntimeError("graph not bootstrapped in this unit test")

    monkeypatch.setattr(schema_mod, "open_board_connection", _boom, raising=True)


async def _board(db_factory, board_id):
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="rkg01", owner_id="owner-r"))
            await db.commit()


def _done_card(card_id, board_id, *, card_type, action_plan=None, spec_id=None):
    return Card(
        id=card_id, board_id=board_id, title="t", created_by="u",
        status=CardStatus.DONE, card_type=card_type,
        action_plan=action_plan, spec_id=spec_id,
    )


def _store(tmp_path):
    return CognitiveConsolidationItemStore(base_dir=tmp_path)


# ---------------------------------------------------------------------------
# AC1 / ts_3f30b26c — technical DLQ invalidates effectiveness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts1_dlq_invalidates_effectiveness(tmp_path, db_factory):
    board = "rkg01-dlq"
    await _board(db_factory, board)
    async with db_factory() as db:
        for i, spec_id in enumerate(DLQ_SPECS):
            db.add(ConsolidationDeadLetter(
                id=f"rkg01-dlq-{i}", board_id=board, artifact_type="spec", artifact_id=spec_id,
                original_queue_id=f"rkg01-q{i}", attempts=5,
                errors=[{"attempt": 1, "error_type": "KGPrimitiveError",
                         "message": "KG node connectivity guard rejected the commit"}],
            ))
        await db.commit()

    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available",
        )

    assert snap["totals"]["dlq"] == 4
    assert snap["cognitively_effective"] is False
    dlq_entries = [a for a in snap["artifacts"] if a["state"] == "dlq"]
    assert len(dlq_entries) == 4
    for entry in dlq_entries:
        assert entry["evidence_source"] == "technical_dlq"
        assert entry["confidence"] == "observed"
        assert "reprocess" in entry["next_action"].lower()
        assert entry["dead_letter_ids"]


# ---------------------------------------------------------------------------
# AC2 / ts_44a6d0c7 — extractor_triggered_but_not_persisted (inferred)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts2_extractor_triggered_but_not_persisted_inferred(tmp_path, db_factory):
    board = "rkg01-trig"
    await _board(db_factory, board)
    async with db_factory() as db:
        # Bug card with a substantial action_plan → Learning-eligible.
        db.add(_done_card(UUID_A, board, card_type=CardType.BUG, action_plan="x" * 80))
        await db.commit()

    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available",
            include_candidate_logs=True,
        )

    entry = next(a for a in snap["artifacts"] if a["artifact_ref"] == f"card:{UUID_A}")
    assert entry["state"] == "extractor_triggered_but_not_persisted"
    assert entry["evidence_source"] == "inferred_eligibility"
    assert entry["confidence"] == "inferred"
    assert entry["inferred_candidate"] is True
    assert entry["candidate_log_refs"] == []
    assert "learning" in entry["inference_reason"]
    assert "rkg-03" in entry["next_action"].lower()
    assert snap["totals"]["extractor_triggered_but_not_persisted"] >= 1
    # attempted rollup counts an engaged closeout.
    assert snap["totals"]["attempted"] >= 1


# ---------------------------------------------------------------------------
# AC3 / ts_0dd43261 — no_material is never a failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts3_no_material_not_a_failure(tmp_path, db_factory):
    board = "rkg01-nomat"
    await _board(db_factory, board)
    async with db_factory() as db:
        # Normal card, no spec_id, no bug → no cognitive material.
        db.add(_done_card(UUID_B, board, card_type=CardType.NORMAL))
        await db.commit()

    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available",
        )

    entry = next(a for a in snap["artifacts"] if a["artifact_ref"] == f"card:{UUID_B}")
    assert entry["state"] == "no_material"
    assert entry["inferred_candidate"] is False
    assert snap["totals"]["no_material"] >= 1
    # no_material is NOT counted as an attempted closeout and is not a dlq.
    assert snap["totals"]["dlq"] == 0
    assert snap["cognitively_effective"] is True


# ---------------------------------------------------------------------------
# Codex guardrail — a literal candidate source replaces inference WITHOUT
# changing the contract (same state, stronger evidence).
# ---------------------------------------------------------------------------


class _FakeCandidateSource:
    """Read-only fake of the future RKG-03 candidate-log store."""

    def __init__(self, mapping):
        self._mapping = mapping

    def candidates_for(self, board_id, artifact_id):
        return list(self._mapping.get(artifact_id, []))


@pytest.mark.asyncio
async def test_literal_candidate_source_replaces_inference(tmp_path, db_factory):
    board = "rkg01-literal"
    await _board(db_factory, board)
    async with db_factory() as db:
        db.add(_done_card(UUID_C, board, card_type=CardType.BUG, action_plan="x" * 80))
        await db.commit()

    source = _FakeCandidateSource({
        f"card:{UUID_C}": [{"ref": "cand-1", "candidate_type": "learning"}],
    })
    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available",
            include_candidate_logs=True, candidate_source=source,
        )

    entry = next(a for a in snap["artifacts"] if a["artifact_ref"] == f"card:{UUID_C}")
    # Same primary state as the inferred case — the CONTRACT is unchanged.
    assert entry["state"] == "extractor_triggered_but_not_persisted"
    # ...but the evidence is now observed, not inferred.
    assert entry["evidence_source"] == "candidate_log"
    assert entry["confidence"] == "observed"
    assert entry["inferred_candidate"] is False
    assert entry["candidate_log_refs"] == ["cand-1"]


# ---------------------------------------------------------------------------
# Persisted-node path + read-only / error contracts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persisted_node_classified_consolidated(tmp_path, db_factory, monkeypatch):
    board = "rkg01-persist"
    await _board(db_factory, board)
    async with db_factory() as db:
        db.add(_done_card(UUID_D, board, card_type=CardType.BUG, action_plan="x" * 80))
        await db.commit()

    # Inject a controlled read-only projection with a persisted Learning node.
    monkeypatch.setattr(ces, "_project_cognitive_graph", lambda *_a, **_k: {
        "persisted_refs": {f"card:{UUID_D}": {"Learning": 1}},
        "edge_refs": {f"card:{UUID_D}": {"validates": 1}},
        "node_type_counts": {"Decision": 0, "Alternative": 0, "Assumption": 0, "Learning": 1},
        "edge_type_counts": {e: (1 if e == "validates" else 0) for e in ces.COGNITIVE_EDGE_TYPES},
        "available": True,
    })
    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available",
        )
    entry = next(a for a in snap["artifacts"] if a["artifact_ref"] == f"card:{UUID_D}")
    assert entry["state"] == "persisted_or_consolidated"
    assert entry["node_counts"] == {"Learning": 1}
    assert entry["edge_counts"] == {"validates": 1}
    assert snap["cognitive_graph"]["node_type_counts"]["Learning"] == 1


@pytest.mark.asyncio
async def test_invalid_graph_layer_raises_400(tmp_path, db_factory):
    board = "rkg01-bad"
    await _board(db_factory, board)
    async with db_factory() as db:
        with pytest.raises(ces.CognitiveEffectivenessError) as ei:
            await ces.build_cognitive_effectiveness_inventory(
                db, board, store=_store(tmp_path), graph_layer="bogus",
                metric_status="available",
            )
    assert ei.value.http_status == 400
    assert ei.value.code == "invalid_artifact_filter"


@pytest.mark.asyncio
async def test_metric_unavailable_raises_503(tmp_path, db_factory):
    board = "rkg01-unavail"
    await _board(db_factory, board)
    async with db_factory() as db:
        with pytest.raises(ces.CognitiveEffectivenessError) as ei:
            await ces.build_cognitive_effectiveness_inventory(
                db, board, store=_store(tmp_path), metric_status="unavailable",
            )
    assert ei.value.http_status == 503
    assert ei.value.code == "kg_metric_unavailable"


# ---------------------------------------------------------------------------
# Validation findings (codex 2026-06-25)
# ---------------------------------------------------------------------------


def test_base_artifact_ref_correlation():
    """Finding #3: per-concept cognitive refs attribute to their base artifact;
    bug refs collapse to card. So a card-with-spec is never the Alternative/
    Assumption artifact (the spec is)."""
    u = "11111111-1111-1111-1111-111111111111"
    assert ces._base_artifact_ref("spec:s1:alternative:abcd1234") == "spec:s1"
    assert ces._base_artifact_ref("spec:s1:assumption:deadbeef") == "spec:s1"
    assert ces._base_artifact_ref(f"bug:{u}") == f"card:{u}"
    assert ces._base_artifact_ref(f"card:bug:{u}:learning:xyz") == f"card:{u}"
    assert ces._base_artifact_ref(f"card:{u}") == f"card:{u}"


@pytest.mark.asyncio
async def test_dlq_entry_has_contract_fields(tmp_path, db_factory):
    """Finding #1: DLQ entries carry artifact_type + dead_letter_ids +
    edge_counts + remediation, not just a bool."""
    board = "rkg01-fields"
    await _board(db_factory, board)
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-f1", board_id=board, artifact_type="spec", artifact_id=DLQ_SPECS[0],
            original_queue_id="qf1", attempts=5,
            errors=[{"attempt": 1, "error_type": "KGPrimitiveError", "message": "connectivity guard"}],
        ))
        await db.commit()
    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available")
    entry = next(a for a in snap["artifacts"] if a["state"] == "dlq")
    assert entry["artifact_type"] == "spec"
    assert entry["dead_letter_ids"] == ["dlq-f1"]
    assert "edge_counts" in entry
    assert entry["remediation"]
    assert entry["next_action"]


@pytest.mark.asyncio
async def test_invalid_artifact_id_shape_raises_400(tmp_path, db_factory):
    """Finding #2: an artifact_id that is not a <type>:<id> ref is rejected."""
    board = "rkg01-badfilter"
    await _board(db_factory, board)
    async with db_factory() as db:
        with pytest.raises(ces.CognitiveEffectivenessError) as ei:
            await ces.build_cognitive_effectiveness_inventory(
                db, board, store=_store(tmp_path), metric_status="available",
                artifact_id="not-a-ref")
    assert ei.value.http_status == 400
    assert ei.value.code == "invalid_artifact_filter"


@pytest.mark.asyncio
async def test_done_spec_is_artifact_and_alt_assum_keyed_to_spec(tmp_path, db_factory, monkeypatch):
    """Findings #3 + #4: a done SPEC is a first-class cognitive artifact; with a
    persisted Alternative keyed spec:<id>:alternative:<hash> it is
    persisted_or_consolidated (NOT a false extractor_triggered)."""
    board = "rkg01-spec"
    spec_id = "spec-aaa"
    await _board(db_factory, board)
    async with db_factory() as db:
        db.add(Spec(id=spec_id, board_id=board, title="s", created_by="u", status=SpecStatus.DONE))
        await db.commit()

    # First: no persisted node -> spec is extractor_triggered (inferred), proving
    # the spec is in the universe and eligible for Alternative/Assumption.
    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available")
    entry = next(a for a in snap["artifacts"] if a["artifact_ref"] == f"spec:{spec_id}")
    assert entry["artifact_type"] == "spec"
    assert entry["state"] == "extractor_triggered_but_not_persisted"
    assert "alternative_assumption" in entry["inference_reason"]

    # Then: an Alternative node keyed under the spec base ref -> persisted.
    monkeypatch.setattr(ces, "_project_cognitive_graph", lambda *_a, **_k: {
        "persisted_refs": {f"spec:{spec_id}": {"Alternative": 2}},
        "edge_refs": {f"spec:{spec_id}": {"relates_to": 2}},
        "node_type_counts": {"Decision": 0, "Alternative": 2, "Assumption": 0, "Learning": 0},
        "edge_type_counts": {e: (2 if e == "relates_to" else 0) for e in ces.COGNITIVE_EDGE_TYPES},
        "available": True,
    })
    async with db_factory() as db:
        snap2 = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available")
    entry2 = next(a for a in snap2["artifacts"] if a["artifact_ref"] == f"spec:{spec_id}")
    assert entry2["state"] == "persisted_or_consolidated"
    assert entry2["node_counts"] == {"Alternative": 2}


@pytest.mark.asyncio
async def test_effectiveness_blockers_persistence_gap(tmp_path, db_factory):
    """Finding #5: a persistence gap (extractor_triggered_but_not_persisted)
    blocks full effectiveness explicitly, not just DLQ."""
    board = "rkg01-blockers"
    await _board(db_factory, board)
    async with db_factory() as db:
        db.add(_done_card(UUID_E, board, card_type=CardType.BUG, action_plan="x" * 80))
        await db.commit()
    async with db_factory() as db:
        snap = await ces.build_cognitive_effectiveness_inventory(
            db, board, store=_store(tmp_path), metric_status="available")
    cats = {b["category"] for b in snap["effectiveness_blockers"]}
    assert "persistence_gap" in cats
    assert snap["cognitively_effective"] is False
