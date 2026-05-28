"""Tests for KG-03A.7 — Rebuild dedupe / reopen behaviour.

Coverage (per spec card 44426e5c):

* `CognitiveConsolidationItem` carries ``content_hash`` end-to-end.
* Rebuild with UNCHANGED content_hash does NOT recreate a pending item;
  the prior terminal row is carried into the new generation.
* Rebuild with CHANGED content_hash creates a fresh PENDING for the
  same source_ref AND emits one bounded
  ``kg_cognitive_pending_reopen_total`` sample with
  reason_code=content_changed.
* First-deploy / no-prior generation: no reopen sample is emitted —
  the row is just a normal PENDING.
* Counter labels are bounded per ``or_029bd920``:
  (entity_type, outcome, reason_code) — no board_id, source_ref or
  content_hash leaks into the metric stream.
* Multi-generation drift: the most-recent terminal per source_ref wins
  during the cross-generation scan.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingMarker,
    CognitivePendingOutcomeType,
    CognitivePendingReopenOutcome,
    CognitivePendingReopenReasonCode,
    CONSOLIDABLE_ARTIFACT_TYPES,
    get_reopen_counter_labels,
    get_reopen_event_count,
    get_reopen_samples,
    reset_reopen_counter,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-kg03a7"
AGENT = "agent-kg03a7"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg-03a-7"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counter() -> Iterator[None]:
    reset_reopen_counter()
    yield
    reset_reopen_counter()


def _row(
    source_ref: str = "spec:abc",
    *,
    artifact_type: str = "spec",
    content_hash: str = "h1",
) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": source_ref.split(":", 1)[-1],
        "source_ref": source_ref,
        "content_hash": content_hash,
    }


def _mark(
    base_dir: Path,
    *,
    rows: list[dict],
    generation_id: str | None = None,
    event_ref: str = "evt_kg03a7",
) -> str:
    gen = generation_id or generate_kg_generation_id()
    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=rows,
        event_ref=event_ref,
    )
    return gen


def _consolidate(
    store: CognitiveConsolidationItemStore,
    *,
    board_id: str,
    kg_generation_id: str,
    source_ref: str,
):
    items = store.list_items(board_id, kg_generation_id, limit=10)
    target = next(it for it in items if it.source_ref == source_ref)
    return store.update_item(
        board_id=board_id,
        kg_generation_id=kg_generation_id,
        item_id=target.item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id=AGENT,
        consolidation_session_id="sess_kg03a7",
        outcome_type=CognitivePendingOutcomeType.RELATION_CREATED.value,
    )


# ---------------------------------------------------------------------------
# Schema — content_hash persists
# ---------------------------------------------------------------------------


def test_content_hash_round_trips_through_to_dict_and_from_dict() -> None:
    item = CognitiveConsolidationItem(
        item_id="cogn_test",
        board_id=BOARD,
        kg_generation_id="gen1",
        source_ref="spec:abc",
        artifact_type="spec",
        status=CognitiveItemStatus.PENDING.value,
        recorded_at="2026-05-27T00:00:00Z",
        content_hash="hashA",
    )
    assert item.to_dict()["content_hash"] == "hashA"
    restored = CognitiveConsolidationItem.from_dict(item.to_dict())
    assert restored.content_hash == "hashA"


def test_marker_persists_content_hash_on_new_pending_rows(
    base_dir: Path,
) -> None:
    gen = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen, limit=10)
    assert items[0].content_hash == "hashA"
    assert items[0].status == CognitiveItemStatus.PENDING.value


# ---------------------------------------------------------------------------
# First deploy — no reopen sample
# ---------------------------------------------------------------------------


def test_first_deploy_does_not_emit_reopen_sample(base_dir: Path) -> None:
    _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])
    assert get_reopen_event_count() == 0
    assert get_reopen_samples() == []


# ---------------------------------------------------------------------------
# Dedupe — unchanged content_hash carries the terminal row forward
# ---------------------------------------------------------------------------


def test_rebuild_with_unchanged_content_does_not_recreate_pending(
    base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=base_dir)

    gen1 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])
    _consolidate(
        store,
        board_id=BOARD,
        kg_generation_id=gen1,
        source_ref="spec:abc",
    )

    gen2 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])

    items = store.list_items(BOARD, gen2, limit=10)
    assert len(items) == 1
    carried = items[0]
    assert carried.source_ref == "spec:abc"
    assert carried.status == CognitiveItemStatus.CONSOLIDATED.value
    # The carried row keeps the ORIGINAL generation id so the item id
    # remains stable across rebuilds (deterministic replay invariant).
    assert carried.kg_generation_id == gen1

    # No reopen sample was emitted.
    assert get_reopen_event_count() == 0


# ---------------------------------------------------------------------------
# Reopen — content_hash change opens a fresh PENDING
# ---------------------------------------------------------------------------


def test_rebuild_with_changed_content_reopens_pending_and_emits_counter(
    base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=base_dir)

    gen1 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])
    _consolidate(
        store,
        board_id=BOARD,
        kg_generation_id=gen1,
        source_ref="spec:abc",
    )

    gen2 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashB")])

    items = store.list_items(BOARD, gen2, limit=10)
    assert len(items) == 1
    reopened = items[0]
    assert reopened.source_ref == "spec:abc"
    assert reopened.status == CognitiveItemStatus.PENDING.value
    assert reopened.kg_generation_id == gen2
    assert reopened.content_hash == "hashB"

    # Exactly one reopen sample emitted with the bounded labels.
    assert get_reopen_event_count(
        entity_type="spec",
        outcome=CognitivePendingReopenOutcome.SUCCESS.value,
        reason_code=(
            CognitivePendingReopenReasonCode.CONTENT_CHANGED.value
        ),
    ) == 1


def test_reopen_only_fires_for_changed_source_refs(
    base_dir: Path,
) -> None:
    """A multi-row rebuild emits one reopen sample per changed source_ref
    and zero for the unchanged ones."""

    store = CognitiveConsolidationItemStore(base_dir=base_dir)

    gen1 = _mark(
        base_dir,
        rows=[
            _row("spec:a", content_hash="hash_a1"),
            _row("spec:b", content_hash="hash_b1"),
            _row("spec:c", content_hash="hash_c1"),
        ],
    )
    for ref in ("spec:a", "spec:b", "spec:c"):
        _consolidate(
            store,
            board_id=BOARD,
            kg_generation_id=gen1,
            source_ref=ref,
        )

    _mark(
        base_dir,
        rows=[
            _row("spec:a", content_hash="hash_a1"),  # unchanged
            _row("spec:b", content_hash="hash_b2"),  # changed
            _row("spec:c", content_hash="hash_c1"),  # unchanged
        ],
    )

    assert get_reopen_event_count(entity_type="spec") == 1
    assert get_reopen_event_count(
        entity_type="spec",
        outcome=CognitivePendingReopenOutcome.SUCCESS.value,
        reason_code=(
            CognitivePendingReopenReasonCode.CONTENT_CHANGED.value
        ),
    ) == 1


# ---------------------------------------------------------------------------
# Multi-generation drift — most recent terminal wins
# ---------------------------------------------------------------------------


def test_cross_generation_lookup_uses_most_recent_terminal(
    base_dir: Path,
) -> None:
    """If multiple prior generations carry terminal rows for the same
    source_ref, the most recent one's content_hash governs the
    dedupe/reopen decision."""

    store = CognitiveConsolidationItemStore(base_dir=base_dir)

    gen1 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])
    _consolidate(
        store, board_id=BOARD, kg_generation_id=gen1, source_ref="spec:abc",
    )
    gen2 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashB")])
    _consolidate(
        store, board_id=BOARD, kg_generation_id=gen2, source_ref="spec:abc",
    )
    # gen3 with the same content as gen2's MOST RECENT terminal → dedupe.
    gen3 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashB")])
    items = store.list_items(BOARD, gen3, limit=10)
    assert items[0].status == CognitiveItemStatus.CONSOLIDATED.value
    # gen3 carried forward from gen2 (the most recent terminal), so
    # the kg_generation_id should be gen2, not gen1.
    assert items[0].kg_generation_id == gen2

    # No reopen sample emitted for this rebuild (gen2->gen3 has no change).
    # The reopen sample from gen1->gen2 happened during _mark for gen2.
    assert get_reopen_event_count(
        reason_code=(
            CognitivePendingReopenReasonCode.CONTENT_CHANGED.value
        ),
    ) == 1


# ---------------------------------------------------------------------------
# Counter labels — bounded
# ---------------------------------------------------------------------------


def test_reopen_counter_labels_are_bounded_per_or_029bd920() -> None:
    """The reopen counter labels must match the observability contract
    exactly: (entity_type, outcome, reason_code). board_id, source_ref
    and content_hash are high-cardinality and MUST NOT appear."""

    assert get_reopen_counter_labels() == (
        "entity_type", "outcome", "reason_code",
    )


def test_reopen_samples_only_carry_bounded_fields(base_dir: Path) -> None:
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    gen1 = _mark(base_dir, rows=[_row("spec:abc", content_hash="hashA")])
    _consolidate(
        store, board_id=BOARD, kg_generation_id=gen1, source_ref="spec:abc",
    )
    _mark(base_dir, rows=[_row("spec:abc", content_hash="hashB")])
    samples = get_reopen_samples()
    assert samples

    forbidden_keys = {
        "board_id", "board_id_hash", "source_ref", "content_hash",
    }
    bounded_artifact_types = set(CONSOLIDABLE_ARTIFACT_TYPES)
    bounded_outcomes = {o.value for o in CognitivePendingReopenOutcome}
    bounded_reason_codes = {
        r.value for r in CognitivePendingReopenReasonCode
    }

    for sample in samples:
        assert set(sample.keys()) == {
            "entity_type", "outcome", "reason_code",
        }, sample
        assert not forbidden_keys & set(sample.keys()), sample
        assert sample["entity_type"] in bounded_artifact_types, sample
        assert sample["outcome"] in bounded_outcomes, sample
        assert sample["reason_code"] in bounded_reason_codes, sample


def test_reopen_emits_entity_type_spec_for_changed_spec(
    base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    gen1 = _mark(
        base_dir,
        rows=[_row("spec:abc", artifact_type="spec", content_hash="h1")],
    )
    _consolidate(
        store, board_id=BOARD, kg_generation_id=gen1, source_ref="spec:abc",
    )
    _mark(
        base_dir,
        rows=[_row("spec:abc", artifact_type="spec", content_hash="h2")],
    )
    assert get_reopen_event_count(entity_type="spec") == 1
    assert get_reopen_event_count(entity_type="decision") == 0


def test_reopen_emits_entity_type_decision_for_changed_decision(
    base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    gen1 = _mark(
        base_dir,
        rows=[
            _row(
                "decision:spec1:dec_a",
                artifact_type="decision",
                content_hash="h1",
            ),
        ],
    )
    _consolidate(
        store,
        board_id=BOARD,
        kg_generation_id=gen1,
        source_ref="decision:spec1:dec_a",
    )
    _mark(
        base_dir,
        rows=[
            _row(
                "decision:spec1:dec_a",
                artifact_type="decision",
                content_hash="h2",
            ),
        ],
    )
    assert get_reopen_event_count(entity_type="decision") == 1
    assert get_reopen_event_count(entity_type="spec") == 0
