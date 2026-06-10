"""Tests for KG-03.1 — CognitiveConsolidationItemStore + Marker per-item ledger.

Covers:
* AC1 — Given 3 consolidable + 1 non-consolidable sources, marker writes 3
  pending item rows and aggregate pending_count=3.
* AC7 — Legacy aggregate-only record (no items[]) loads OK via compatibility
  adapter.
* FR1 — Marker materializes per-item records preserving KG-02 aggregate.
* FR8 — Rebuild/Marker can ONLY write pending status (never consolidated).
* TR tr_39e76c26 — Deterministic item_id via SHA256.
* TR tr_746090f6 — Atomic temp-then-replace write.
* TR tr_a7b391e0 — Bounded status enum.
* BR br_84194faf — Rebuild only creates pending items.
* BR br_3d985533 — Legacy aggregate records remain readable.
* BR br_d510e1e3 — Latest generation fallback is deterministic.
* BR br_858a0859 — Ledger payload is safe metadata only.
* OR or_3b71e3c1 — kg_cognitive_pending_items_materialized_total counter.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from okto_pulse.core.kg.rebuild_audit import (
    ACTIVE_ITEM_STATUSES,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitiveMaterializeOutcome,
    CognitivePendingMarker,
    CognitivePendingStatus,
    MaterializeFromMarkerResult,
    compute_cognitive_item_id,
    get_materialized_count,
    get_materialized_counter_labels,
    get_materialized_event_count,
    get_materialized_samples,
    reset_materialized_counter,
    reset_pending_counter,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-kg03-1"


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg-03-1"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_materialized_counter()
    reset_pending_counter()
    yield
    reset_materialized_counter()
    reset_pending_counter()


def _row(artifact_type: str, id_: str) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"{artifact_type}:{id_}",
        "source_version": "1",
        "content_hash": "h" * 64,
        "created_at": "2026-05-26T00:00:00Z",
    }


# -------- tr_39e76c26 — deterministic item_id ----------------------------


def test_compute_cognitive_item_id_is_deterministic() -> None:
    """tr_39e76c26: same (board, gen, source_ref) always yields the same
    item_id across process restarts."""

    gen = generate_kg_generation_id()
    a = compute_cognitive_item_id(BOARD, gen, "spec:1")
    b = compute_cognitive_item_id(BOARD, gen, "spec:1")
    assert a == b
    assert a.startswith("cogn_")


def test_compute_cognitive_item_id_differs_per_source() -> None:
    gen = generate_kg_generation_id()
    a = compute_cognitive_item_id(BOARD, gen, "spec:1")
    b = compute_cognitive_item_id(BOARD, gen, "spec:2")
    assert a != b


def test_compute_cognitive_item_id_differs_per_generation() -> None:
    a = compute_cognitive_item_id(BOARD, generate_kg_generation_id(), "spec:1")
    b = compute_cognitive_item_id(BOARD, generate_kg_generation_id(), "spec:1")
    assert a != b


# -------- AC1 — per-item ledger materialization -------------------------


def test_ac1_marker_materializes_three_pending_items_from_four_sources(
    base_dir: Path,
) -> None:
    """AC1: Given 3 consolidable + 1 non-consolidable sources, when
    CognitivePendingMarker persists, then the item ledger contains
    exactly three pending item rows and aggregate pending_count=3."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    source_set = [
        _row("spec", "s1"),
        _row("refinement", "r1"),
        _row("task", "t1"),
        _row("comment", "c1"),  # NOT consolidable
    ]
    result = marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=source_set,
        event_ref="evt_test",
    )
    assert result.status == CognitivePendingStatus.PENDING_MARKED.value
    assert result.pending_count == 3
    assert sorted(result.pending_refs) == ["refinement:r1", "spec:s1", "task:t1"]

    # Inspect the persisted ledger file
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    assert len(items) == 3
    for item in items:
        assert item.status == CognitiveItemStatus.PENDING.value
        assert item.board_id == BOARD
        assert item.kg_generation_id == gen
        assert item.event_ref == "evt_test"
        assert item.item_id.startswith("cogn_")
        # tr_39e76c26 invariant: id reproducible
        assert item.item_id == compute_cognitive_item_id(BOARD, gen, item.source_ref)


def test_ac1_aggregate_pending_count_and_refs_preserved(base_dir: Path) -> None:
    """KG-02 aggregate fields (pending_count, pending_refs) remain after
    item materialization."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "a"), _row("refinement", "b")],
        event_ref="evt_test",
    )

    record_path = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD / f"{gen}.json"
    )
    assert record_path.exists()
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    # KG-02.7 aggregate fields still present
    assert payload["pending_count"] == 2
    assert sorted(payload["pending_refs"]) == ["refinement:b", "spec:a"]
    assert payload["status"] == CognitivePendingStatus.PENDING_MARKED.value
    # KG-03.1 new field
    assert isinstance(payload["items"], list)
    assert len(payload["items"]) == 2


# -------- AC7 — legacy aggregate-only compat -----------------------------


def test_ac7_legacy_aggregate_only_record_loads_via_compatibility_adapter(
    base_dir: Path,
) -> None:
    """AC7: Legacy KG-02 aggregate record (pending_refs + no items array)
    must load via compatibility adapter without crashing."""

    gen = generate_kg_generation_id()
    record_dir = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD
    )
    record_dir.mkdir(parents=True, exist_ok=True)
    legacy = {
        "board_id": BOARD,
        "kg_generation_id": gen,
        "event_ref": "evt_legacy",
        "pending_count": 2,
        "pending_refs": ["spec:legacy1", "refinement:legacy2"],
        "status": "pending_marked",
        "recorded_at": "2026-05-25T00:00:00+00:00",
        # NOTE: no "items" key — pure KG-02.7 aggregate format
    }
    (record_dir / f"{gen}.json").write_text(json.dumps(legacy), encoding="utf-8")

    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    # Synthesized from pending_refs
    assert len(items) == 2
    assert {i.source_ref for i in items} == {"spec:legacy1", "refinement:legacy2"}
    for i in items:
        assert i.status == CognitiveItemStatus.PENDING.value
        assert i.event_ref == "evt_legacy"
        # artifact_type recovered from "spec:..." prefix
        if i.source_ref.startswith("spec:"):
            assert i.artifact_type == "spec"
        if i.source_ref.startswith("refinement:"):
            assert i.artifact_type == "refinement"


# -------- TR tr_746090f6 — atomic write ----------------------------------


def test_tr_746090f6_atomic_write_uses_temp_then_replace(
    base_dir: Path,
) -> None:
    """Verify the .json.tmp pattern is used by checking no .tmp file is
    left behind after a successful write (replace removes it)."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "x")],
        event_ref="evt_atomic",
    )
    record_dir = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD
    )
    # No .json.tmp left behind
    assert list(record_dir.glob("*.json.tmp")) == []
    # Real file exists
    assert (record_dir / f"{gen}.json").exists()


# -------- FR8 / br_84194faf — rebuild only pending ----------------------


def test_marker_only_writes_pending_status_never_terminal(
    base_dir: Path,
) -> None:
    """br_84194faf + br_f79887a3 + tr_9521d8fd: deterministic write path
    can ONLY persist pending. The marker must never set consolidated /
    skipped / failed on items even if called repeatedly."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    for _ in range(3):
        marker.mark_for_generation(
            board_id=BOARD,
            kg_generation_id=gen,
            source_set=[_row("spec", "s1"), _row("refinement", "r1")],
            event_ref="evt_loop",
        )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    assert len(items) == 2
    for item in items:
        assert item.status == CognitiveItemStatus.PENDING.value
        assert item.consolidation_session_id is None
        assert item.reason is None


# -------- BR br_d510e1e3 — latest generation fallback -------------------


def test_latest_generation_returns_most_recent_recorded_at(
    base_dir: Path,
) -> None:
    """br_d510e1e3: latest generation deterministic by recorded_at."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen_a = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen_a,
        source_set=[_row("spec", "a")],
        event_ref="evt_a",
    )
    import time
    time.sleep(0.01)
    gen_b = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen_b,
        source_set=[_row("spec", "b")],
        event_ref="evt_b",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    assert store.latest_generation(BOARD) == gen_b


def test_latest_generation_returns_none_for_empty_board(
    base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    assert store.latest_generation("nonexistent-board") is None


# -------- TR tr_a7b391e0 — bounded status enum --------------------------


def test_bounded_item_status_enum_values() -> None:
    values = {s.value for s in CognitiveItemStatus}
    assert values == {"pending", "in_progress", "consolidated", "skipped", "failed"}
    # ACTIVE excludes terminal
    assert ACTIVE_ITEM_STATUSES == frozenset({"pending", "in_progress", "failed"})


# -------- OR or_3b71e3c1 — one event per marker, item_count as value ----


def test_or_3b71e3c1_emits_exactly_one_sample_per_marker_with_item_count_as_value(
    base_dir: Path,
) -> None:
    """or_3b71e3c1 (Codex audit val_036cb81e):
    ``kg_cognitive_pending_items_materialized_total`` must emit EXACTLY
    one sample per successful marker write, with ``item_count`` as the
    sample VALUE — never one sample per item."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            _row("spec", "s1"),
            _row("spec", "s2"),
            _row("refinement", "r1"),
            _row("bug", "b1"),
            _row("comment", "c1"),  # NOT consolidable
        ],
        event_ref="evt_count",
    )
    # One marker -> exactly one event sample on the counter.
    assert get_materialized_event_count(BOARD) == 1
    # The sample VALUE is the item_count (4 consolidable sources).
    assert get_materialized_count(BOARD) == 4
    # The single emitted outcome is ``materialized``.
    assert (
        get_materialized_count(
            BOARD, outcome=CognitiveMaterializeOutcome.MATERIALIZED.value
        )
        == 4
    )
    assert (
        get_materialized_event_count(
            BOARD, outcome=CognitiveMaterializeOutcome.MATERIALIZED.value
        )
        == 1
    )


def test_or_3b71e3c1_labels_exclude_artifact_type_source_ref_item_id_actor(
    base_dir: Path,
) -> None:
    """or_3b71e3c1 (Codex audit val_036cb81e):
    label set MUST be exactly ``(board_id, outcome)``. The forbidden
    dimensions ``artifact_type``, ``source_ref``, ``item_id`` and
    ``actor`` MUST NOT leak as labels."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            _row("spec", "s1"),
            _row("refinement", "r1"),
            _row("test", "tc1"),
        ],
        event_ref="evt_labels",
    )

    # Label set is exactly (board_id, outcome).
    assert get_materialized_counter_labels() == ("board_id", "outcome")

    # Sample shape carries only label keys + item_count value.
    samples = get_materialized_samples()
    assert samples, "expected at least one materialization sample"
    forbidden = {"artifact_type", "source_ref", "item_id", "actor"}
    for sample in samples:
        sample_keys = set(sample.keys())
        # Must contain ONLY label dims + item_count value.
        assert sample_keys == {"board_id", "outcome", "item_count"}, (
            f"unexpected keys in sample {sample!r}; expected exactly "
            "(board_id, outcome, item_count)"
        )
        leaked = forbidden.intersection(sample_keys)
        assert not leaked, (
            f"sample leaks forbidden label dimensions: {leaked}"
        )


def test_or_3b71e3c1_empty_marker_emits_one_sample_with_empty_outcome_and_zero_value(
    base_dir: Path,
) -> None:
    """A marker call with zero consolidable sources still produces one
    sample for traceability, but with ``outcome=empty`` and
    ``item_count=0`` — never per-item."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            _row("comment", "c1"),  # not consolidable
            _row("card", "k1"),     # not consolidable
        ],
        event_ref="evt_empty",
    )
    assert get_materialized_event_count(BOARD) == 1
    assert (
        get_materialized_event_count(
            BOARD, outcome=CognitiveMaterializeOutcome.EMPTY.value
        )
        == 1
    )
    assert get_materialized_count(BOARD) == 0


def test_or_3b71e3c1_update_item_does_not_emit_materialization_sample(
    base_dir: Path,
) -> None:
    """Codex audit val_036cb81e: ``update_item`` MUST NOT increment
    ``kg_cognitive_pending_items_materialized_total`` — update-side
    metrics live on KG-03.3 (``kg_cognitive_item_update_total``)."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "s1"), _row("refinement", "r1")],
        event_ref="evt_no_update_bump",
    )
    # One sample from the marker, item_count = 2.
    assert get_materialized_event_count(BOARD) == 1
    assert get_materialized_count(BOARD) == 2

    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1",
    )
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[1].item_id,
        new_status=CognitiveItemStatus.SKIPPED.value,
        updated_by_agent_id="agent-1",
        reason="not applicable",
    )
    # Updates produced ZERO new materialization samples.
    assert get_materialized_event_count(BOARD) == 1
    assert get_materialized_count(BOARD) == 2


# -------- api_1ae54fa0 — materialize_from_marker contract response ------


def test_materialize_from_marker_returns_contract_shape(
    base_dir: Path,
) -> None:
    """api_1ae54fa0: ``materialize_from_marker(board_id, kg_generation_id,
    event_ref, source_set)`` MUST return ``record_ref``, ``item_count``
    and ``counts``."""

    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    gen = generate_kg_generation_id()
    result = store.materialize_from_marker(
        board_id=BOARD,
        kg_generation_id=gen,
        event_ref="evt_contract",
        source_set=[
            _row("spec", "s1"),
            _row("spec", "s2"),
            _row("refinement", "r1"),
            _row("bug", "b1"),
            _row("comment", "c1"),  # filtered out
        ],
    )

    assert isinstance(result, MaterializeFromMarkerResult)
    # record_ref points to the on-disk ledger file.
    expected_path = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD / f"{gen}.json"
    )
    assert result.record_ref == str(expected_path)
    assert expected_path.exists()

    # item_count = total items stored = 4 consolidable rows.
    assert result.item_count == 4

    # counts is a per-artifact_type breakdown (response payload, NOT labels).
    assert isinstance(result.counts, dict)
    assert result.counts == {"spec": 2, "refinement": 1, "bug": 1}

    # outcome echoes the metric label emitted.
    assert result.outcome == CognitiveMaterializeOutcome.MATERIALIZED.value


def test_materialize_from_marker_drives_marker_pipeline(base_dir: Path) -> None:
    """``CognitivePendingMarker.mark_for_generation`` must be wired
    through the contract operation ``materialize_from_marker`` (not the
    private helper). Verified by inspecting that the marker's
    ``record_ref`` matches the public operation's ``record_ref``."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    gen = generate_kg_generation_id()
    result = marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "s1")],
        event_ref="evt_pipeline",
    )
    # The marker returns the same record_ref the public operation does.
    expected_path = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD / f"{gen}.json"
    )
    assert result.record_ref == str(expected_path)
    # And exactly one sample was emitted on the materialization counter.
    assert get_materialized_event_count(BOARD) == 1
    # Store sees the file as a real materialized record.
    assert len(store.list_items(BOARD, gen)) == 1


# -------- Deterministic replay safety: terminal MCP state never lost ----


def test_deterministic_replay_preserves_terminal_mcp_owned_status(
    base_dir: Path,
) -> None:
    """Authoritative model regression (Codex audit val_036cb81e):
    deterministic code may create/refresh ``pending`` items, but a
    same-generation marker REPLAY must NEVER erase an MCP-owned terminal
    status (``consolidated`` / ``skipped``) by rebuilding the row back
    to ``pending``."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    # First materialize: 3 pending items.
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            _row("spec", "s1"),
            _row("refinement", "r1"),
            _row("test", "tc1"),
        ],
        event_ref="evt_initial",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    consolidated_item = next(i for i in items if i.source_ref == "spec:s1")
    skipped_item = next(i for i in items if i.source_ref == "refinement:r1")
    # MCP moves two items to terminal statuses.
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=consolidated_item.item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1",
    )
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=skipped_item.item_id,
        new_status=CognitiveItemStatus.SKIPPED.value,
        updated_by_agent_id="agent-1",
        reason="not applicable",
    )

    # Replay marker with the SAME generation and source_set.
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            _row("spec", "s1"),
            _row("refinement", "r1"),
            _row("test", "tc1"),
        ],
        event_ref="evt_replay",
    )

    replayed = {i.source_ref: i for i in store.list_items(BOARD, gen)}
    # Terminal MCP-owned statuses preserved.
    assert (
        replayed["spec:s1"].status == CognitiveItemStatus.CONSOLIDATED.value
    ), "consolidated terminal state must survive deterministic replay"
    assert (
        replayed["refinement:r1"].status == CognitiveItemStatus.SKIPPED.value
    ), "skipped terminal state must survive deterministic replay"
    # The previously-pending row stays pending.
    assert (
        replayed["test:tc1"].status == CognitiveItemStatus.PENDING.value
    )


# -------- BR br_858a0859 — safe payload (no raw artifact body) ----------


def test_br_858a0859_ledger_has_no_raw_artifact_body(base_dir: Path) -> None:
    """br_858a0859: Ledger payload is safe metadata only. Even if the
    source rows carry extra fields, the ledger must NOT persist them."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    # Source rows with potentially-sensitive extra fields
    rich_rows = [
        {
            **_row("spec", "s1"),
            "title": "Sensitive Spec Title",
            "description": "This should NOT leak into ledger",
            "secret_token": "leak-bait",
        }
    ]
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=rich_rows,
        event_ref="evt_safe",
    )
    record_path = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD / f"{gen}.json"
    )
    body = record_path.read_text(encoding="utf-8")
    assert "Sensitive Spec Title" not in body
    assert "This should NOT leak" not in body
    assert "secret_token" not in body
    assert "leak-bait" not in body


# -------- BR br_3d985533 / FR10 — backward compat with KG-02 readers ----


def test_br_3d985533_kg02_aggregate_readers_can_still_consume(
    base_dir: Path,
) -> None:
    """KG-02.7 aggregate readers expect pending_count + pending_refs +
    status + recorded_at. These must remain at the top level even after
    the KG-03 items[] extension."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "s"), _row("refinement", "r")],
        event_ref="evt_compat",
    )
    record_path = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD / f"{gen}.json"
    )
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    # Required by KG-02.7 readers
    assert "pending_count" in payload
    assert "pending_refs" in payload
    assert "status" in payload
    assert "recorded_at" in payload
    assert "event_ref" in payload
    assert "board_id" in payload
    assert "kg_generation_id" in payload
    # KG-03.1 addition
    assert "items" in payload


# -------- list_items status filter ---------------------------------------


def test_list_items_filters_by_status(base_dir: Path) -> None:
    """KG-03.2 will rely on this filter — tested here at the store level
    since the MCP tool sits on top."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "s1"), _row("refinement", "r1")],
        event_ref="evt_filter",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    pending = store.list_items(BOARD, gen, status_filter="pending")
    assert len(pending) == 2
    consolidated = store.list_items(BOARD, gen, status_filter="consolidated")
    assert consolidated == []


def test_update_item_round_trips_status_and_aggregate(base_dir: Path) -> None:
    """KG-03.3 storage primitive: single-item update preserves others
    and recomputes aggregate counts (br_d544da65)."""

    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "a"), _row("spec", "b"), _row("refinement", "c")],
        event_ref="evt_update",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    assert len(items) == 3
    target = items[0]
    updated = store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=target.item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-xyz",
    )
    assert updated is not None
    assert updated.status == CognitiveItemStatus.CONSOLIDATED.value
    # Others unchanged
    remaining = store.list_items(BOARD, gen)
    others = [i for i in remaining if i.item_id != target.item_id]
    for i in others:
        assert i.status == CognitiveItemStatus.PENDING.value
    # Aggregate count = 2 (the consolidated item dropped out of active)
    record_path = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD / f"{gen}.json"
    )
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert payload["pending_count"] == 2
    assert target.source_ref not in payload["pending_refs"]


def test_update_item_returns_none_for_unknown_item_id(base_dir: Path) -> None:
    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("spec", "a")],
        event_ref="evt_unknown",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    result = store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id="cogn_does_not_exist",
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-xyz",
    )
    assert result is None


def test_update_item_on_legacy_record_synthesizes_then_updates(
    base_dir: Path,
) -> None:
    """If a legacy aggregate-only record exists, update_item must
    synthesize items from pending_refs and then apply the update."""

    gen = generate_kg_generation_id()
    record_dir = (
        base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD
    )
    record_dir.mkdir(parents=True, exist_ok=True)
    legacy = {
        "board_id": BOARD,
        "kg_generation_id": gen,
        "event_ref": "evt_legacy",
        "pending_count": 1,
        "pending_refs": ["spec:l1"],
        "status": "pending_marked",
        "recorded_at": "2026-05-25T00:00:00+00:00",
    }
    (record_dir / f"{gen}.json").write_text(json.dumps(legacy), encoding="utf-8")

    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    target_id = compute_cognitive_item_id(BOARD, gen, "spec:l1")
    updated = store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=target_id,
        new_status=CognitiveItemStatus.SKIPPED.value,
        updated_by_agent_id="agent-1",
        reason="legacy handoff",
    )
    assert updated is not None
    assert updated.status == CognitiveItemStatus.SKIPPED.value
    # File is now item-aware
    payload = json.loads((record_dir / f"{gen}.json").read_text(encoding="utf-8"))
    assert "items" in payload
    assert payload["pending_count"] == 0
    assert payload["pending_refs"] == []
