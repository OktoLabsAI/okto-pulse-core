"""Tests for KG-03.3 — MCP tool ``okto_pulse_kg_update_cognitive_pending_item``.

Aligns with api_525a25f1:

Request:
    board_id (required UUID), kg_generation_id (required UUID),
    item_id (required), status (required, bounded enum),
    consolidation_session_id?, reason?, summary_text?

Response (success):
    board_id, kg_generation_id, updated=True, item, counts

Errors:
    unauthorized | item_not_found | consolidation_session_required |
    reason_required | invalid_status | unsafe_payload + bounded
    validation guards for missing identifiers.

Invariants:
    * br_689bdf14 — consolidated requires consolidation_session_id.
    * br_f9823bad — skipped/failed require non-empty reason.
    * br_858a0859 — unsafe payloads (token shapes, oversized fields)
      rejected before write.
    * br_d544da65 — single-item atomic update; others unchanged; counts
      recomputed from the post-update generation.
    * tr_e6b6356b — existing 7 KG primitives remain registered (sanity).

Counter or_174f18d5 labels:
    (board_id, target_status, outcome, reason_code) — bounded.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pytest

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitiveItemUpdateOutcome,
    CognitiveItemUpdateReasonCode,
    CognitivePendingMarker,
    get_update_counter_labels,
    get_update_event_count,
    get_update_samples,
    reset_update_counter,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.mcp.kg_tools import register_kg_tools


BOARD = "board-kg03-3"


@dataclass
class _FakeAgent:
    id: str = "agent-test-001"


class _MCPRegistryDouble:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[fn.__name__] = fn
            return fn
        return _decorator


@pytest.fixture
def isolated_base_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03-3"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_update_counter()
    yield
    reset_update_counter()


def _register_tools(get_agent_impl: Callable[[], Any]):
    mcp = _MCPRegistryDouble()

    class _NullDb:
        async def __aenter__(self) -> "_NullDb":
            return self

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    def _get_db() -> _NullDb:
        return _NullDb()

    register_kg_tools(mcp, get_agent=get_agent_impl, get_db=_get_db)
    return mcp


@pytest.fixture
def update_tool(isolated_base_dir: Path) -> Callable[..., Any]:
    async def _agent() -> _FakeAgent:
        return _FakeAgent()

    mcp = _register_tools(_agent)
    assert "okto_pulse_kg_update_cognitive_pending_item" in mcp.tools
    # tr_e6b6356b — existing 7 primitives remain registered.
    for primitive in (
        "okto_pulse_kg_begin_consolidation",
        "okto_pulse_kg_add_node_candidate",
        "okto_pulse_kg_add_edge_candidate",
        "okto_pulse_kg_get_similar_nodes",
        "okto_pulse_kg_propose_reconciliation",
        "okto_pulse_kg_commit_consolidation",
        "okto_pulse_kg_abort_consolidation",
    ):
        assert primitive in mcp.tools
    return mcp.tools["okto_pulse_kg_update_cognitive_pending_item"]


@pytest.fixture
def unauthorized_update_tool(
    isolated_base_dir: Path,
) -> Callable[..., Any]:
    async def _agent_none() -> None:
        return None

    mcp = _register_tools(_agent_none)
    return mcp.tools["okto_pulse_kg_update_cognitive_pending_item"]


def _row(artifact_type: str, id_: str) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"{artifact_type}:{id_}",
    }


def _seed(base_dir: Path, sources: list[dict]) -> tuple[str, list]:
    gen = generate_kg_generation_id()
    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=sources,
        event_ref="evt_kg03_3",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(BOARD, gen)
    return gen, items


def _invoke(tool: Callable[..., Any], **kwargs: Any) -> dict[str, Any]:
    raw = asyncio.run(tool(**kwargs))
    return json.loads(raw)


# -------- Happy path -----------------------------------------------------


def test_consolidated_with_session_id_succeeds(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("refinement", "r1"),
    ])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-abc",
        # KG-03A.3 — consolidated now also requires a bounded outcome.
        outcome_type="relation_created",
    )
    assert response["updated"] is True
    assert response["item"]["status"] == "consolidated"
    assert response["item"]["item_id"] == items[0].item_id
    assert response["item"]["updated_by_agent_id"] == "agent-test-001"
    assert response["item"]["consolidation_session_id"] == "sess-abc"
    # counts reflect generation: 1 consolidated + 1 pending = total 2.
    assert response["counts"]["consolidated"] == 1
    assert response["counts"]["pending"] == 1
    assert response["counts"]["total"] == 2


def test_skipped_with_reason_succeeds(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.SKIPPED.value,
        reason="not applicable to current KG schema",
    )
    assert response["updated"] is True
    assert response["item"]["status"] == "skipped"


def test_failed_with_reason_succeeds(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.FAILED.value,
        reason="adapter raised KGPrimitiveError",
    )
    assert response["updated"] is True
    assert response["item"]["status"] == "failed"


def test_in_progress_does_not_require_session_or_reason(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.IN_PROGRESS.value,
    )
    assert response["updated"] is True
    assert response["item"]["status"] == "in_progress"


# -------- AC4 — consolidated requires consolidation_session_id ---------


@pytest.mark.parametrize(
    "missing_session",
    [None, "", "   "],
)
def test_ac4_consolidated_without_session_id_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    missing_session: Any,
) -> None:
    """br_689bdf14: consolidated requires non-empty consolidation_session_id.
    The persisted item must remain unchanged."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id=missing_session,
    )
    assert "error" in response
    assert response["error"]["code"] == "consolidation_session_required"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.CONSOLIDATION_SESSION_REQUIRED.value
    )
    # Persisted item still pending.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].status == CognitiveItemStatus.PENDING.value


# -------- AC5 — skipped/failed require non-empty reason -----------------


@pytest.mark.parametrize(
    "missing_reason",
    [None, "", "   "],
)
@pytest.mark.parametrize(
    "terminal_status",
    [CognitiveItemStatus.SKIPPED.value, CognitiveItemStatus.FAILED.value],
)
def test_ac5_skipped_or_failed_without_reason_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    missing_reason: Any,
    terminal_status: str,
) -> None:
    """br_f9823bad: skipped/failed require non-empty reason."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=terminal_status,
        reason=missing_reason,
    )
    assert "error" in response
    assert response["error"]["code"] == "reason_required"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.REASON_REQUIRED.value
    )
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].status == CognitiveItemStatus.PENDING.value


# -------- AC6 — single-item atomicity + recomputed counts ---------------


def test_ac6_single_item_update_preserves_others(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """br_d544da65: only the targeted item changes; aggregate counts are
    recomputed for the whole generation."""

    gen, items = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("spec", "s2"),
        _row("refinement", "r1"),
    ])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[1].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-abc",
        outcome_type="relation_created",
    )
    assert response["updated"] is True
    # Item 1 (index) flipped; others stay pending.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = {i.item_id: i for i in store.list_items(BOARD, gen)}
    assert persisted[items[0].item_id].status == "pending"
    assert persisted[items[1].item_id].status == "consolidated"
    assert persisted[items[2].item_id].status == "pending"
    # Aggregate counts recomputed.
    assert response["counts"]["pending"] == 2
    assert response["counts"]["consolidated"] == 1
    assert response["counts"]["total"] == 3


def test_counts_returned_match_full_generation_post_update(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("spec", "s2"),
        _row("refinement", "r1"),
    ])
    # Skip one item.
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.SKIPPED.value,
        reason="orphan source_ref",
    )
    # Consolidate another.
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[1].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-2",
        outcome_type="candidate_created",
    )
    assert response["counts"] == {
        "pending": 1,
        "in_progress": 0,
        "consolidated": 1,
        "skipped": 1,
        "failed": 0,
        "total": 3,
    }


# -------- Bounded status enum + invalid status --------------------------


@pytest.mark.parametrize(
    "bad_status",
    ["completed", "PENDING", "any", "", "in-progress"],
)
def test_invalid_status_is_rejected_with_typed_error(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    bad_status: str,
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=bad_status,
    )
    assert "error" in response
    assert response["error"]["code"] == "invalid_status"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.INVALID_STATUS.value
    )


# -------- item_not_found semantics --------------------------------------


def test_unknown_item_id_returns_item_not_found(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, _ = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id="cogn_does_not_exist",
        status=CognitiveItemStatus.IN_PROGRESS.value,
    )
    assert "error" in response
    assert response["error"]["code"] == "item_not_found"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.ITEM_NOT_FOUND.value
    )


def test_unknown_generation_returns_item_not_found(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """A missing generation surfaces as item_not_found (update_item
    returns None in both cases; not a separate error)."""

    unknown_gen = generate_kg_generation_id()
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=unknown_gen,
        item_id="cogn_anything",
        status=CognitiveItemStatus.IN_PROGRESS.value,
    )
    assert response["error"]["code"] == "item_not_found"


# -------- Unsafe payload guard ------------------------------------------


def test_unsafe_payload_long_reason_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """br_858a0859: oversize reason indicates raw artifact body smuggling
    attempt; reject before storage write."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    oversize_reason = "x" * 3000
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.SKIPPED.value,
        reason=oversize_reason,
    )
    assert response["error"]["code"] == "unsafe_payload"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.UNSAFE_PAYLOAD.value
    )
    # Persisted unchanged.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].status == CognitiveItemStatus.PENDING.value


def test_unsafe_payload_token_shape_session_id_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """A token-shape ``consolidation_session_id`` (``conf_<urlsafe>``
    matching the rebuild confirmation token shape) must be rejected so
    raw confirmation tokens never land in the cognitive ledger."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    raw_token_shape = "conf_" + "x" * 32
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id=raw_token_shape,
    )
    assert response["error"]["code"] == "unsafe_payload"


# -------- Authorization gating ------------------------------------------


def test_unauthorized_short_circuits_before_storage_write(
    isolated_base_dir: Path,
    unauthorized_update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        unauthorized_update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.IN_PROGRESS.value,
    )
    assert response["error"]["code"] == "unauthorized"
    # No counter sample fires on auth failure.
    assert get_update_event_count() == 0
    # Persisted unchanged.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].status == CognitiveItemStatus.PENDING.value


# -------- Missing identifier guards --------------------------------------


@pytest.mark.parametrize(
    "kwargs,expected_code",
    [
        ({"board_id": "", "kg_generation_id": "gen", "item_id": "i", "status": "pending"}, "missing_board_id"),
        ({"board_id": "b", "kg_generation_id": "", "item_id": "i", "status": "pending"}, "missing_kg_generation_id"),
        ({"board_id": "b", "kg_generation_id": "gen", "item_id": "", "status": "pending"}, "missing_item_id"),
    ],
)
def test_missing_identifier_returns_typed_error(
    update_tool: Callable[..., Any],
    kwargs: dict[str, Any],
    expected_code: str,
) -> None:
    response = _invoke(update_tool, **kwargs)
    assert response["error"]["code"] == expected_code


# -------- Item projection: reason_code never echoes free-text reason ----


def test_response_item_uses_reason_code_not_free_text_reason(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """br_858a0859: API surface MUST NOT echo the free-text ``reason``
    submitted by the agent; bounded ``reason_code`` is the only narrative
    field exposed."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    free_text_reason = "Manual judgement: source not relevant to current KG"
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.SKIPPED.value,
        reason=free_text_reason,
    )
    assert response["updated"] is True
    item = response["item"]
    # reason_code is present; raw reason is NOT echoed.
    assert "reason_code" in item
    assert "reason" not in item
    asyncio.run(update_tool(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.PENDING.value,
    ))  # Reset to pending for follow-up clarity (not asserted).
    # The original free-text reason must not appear in the success
    # response body (already serialized above).
    assert free_text_reason not in json.dumps(response)


def test_response_item_shape_is_contract_aligned(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.IN_PROGRESS.value,
    )
    item = response["item"]
    # KG-03.3 base shape + KG-03A.3 outcome metadata projection.
    expected = {
        "item_id",
        "source_ref",
        "artifact_type",
        "status",
        "updated_at",
        "updated_by_agent_id",
        "consolidation_session_id",
        "reason_code",
        "outcome_type",
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    }
    assert set(item.keys()) == expected


# -------- or_174f18d5 — counter contract --------------------------------


def test_counter_label_set_is_bounded(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.IN_PROGRESS.value,
    )
    assert get_update_counter_labels() == (
        "board_id",
        "target_status",
        "outcome",
        "reason_code",
    )
    samples = get_update_samples()
    assert len(samples) == 1
    sample = samples[0]
    assert set(sample.keys()) == {
        "board_id",
        "target_status",
        "outcome",
        "reason_code",
    }
    forbidden = {
        "source_ref",
        "item_id",
        "actor",
        "agent_id",
        "reason",
        "consolidation_session_id",
        "summary_text",
    }
    assert not forbidden.intersection(sample.keys())


def test_counter_distinguishes_missing_session_vs_missing_reason(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """or_174f18d5 threshold: rejections for missing
    consolidation_session_id and missing reason are distinguishable via
    reason_code."""

    gen, items = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("spec", "s2"),
    ])
    # Missing consolidation_session_id.
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
    )
    # Missing reason.
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[1].item_id,
        status=CognitiveItemStatus.SKIPPED.value,
    )

    assert get_update_event_count(
        reason_code=CognitiveItemUpdateReasonCode.CONSOLIDATION_SESSION_REQUIRED.value,
        outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
    ) == 1
    assert get_update_event_count(
        reason_code=CognitiveItemUpdateReasonCode.REASON_REQUIRED.value,
        outcome=CognitiveItemUpdateReasonCode.REASON_REQUIRED.value if False else CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
    ) == 1


def test_counter_emits_one_sample_per_call(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    for _ in range(4):
        _invoke(
            update_tool,
            board_id=BOARD,
            kg_generation_id=gen,
            item_id=items[0].item_id,
            status=CognitiveItemStatus.IN_PROGRESS.value,
        )
    assert get_update_event_count(
        outcome=CognitiveItemUpdateOutcome.UPDATED.value
    ) == 4


def test_counter_target_status_is_bounded_even_on_invalid_input(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """Even when the caller submits a status outside the bounded enum,
    the counter ``target_status`` label collapses to ``invalid`` so the
    label space remains finite."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status="something_wild",
    )
    samples = get_update_samples()
    assert len(samples) == 1
    assert samples[0]["target_status"] == "invalid"
