"""Tests for KG-03.2 — MCP tool ``okto_pulse_kg_list_cognitive_pending_items``.

Aligns with api_ae3a932a contract (Codex audit val_ead80fbd rework):

Request:
    board_id (required), kg_generation_id?, status?, limit?, offset?

Response (success):
    board_id, selected_kg_generation_id, legacy_mode, counts, items

Errors:
    unauthorized | invalid_status | generation_not_found
    plus typed validation guards for missing_board_id / invalid_limit /
    invalid_offset.

Item projection MUST use ``reason_code`` (bounded), not ``reason``
(free-text). counts MUST contain {pending, in_progress, consolidated,
skipped, failed, total} and counts.total MUST equal len(items returned).
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
    CognitiveItemListOutcome,
    CognitiveItemListReasonCode,
    CognitiveItemListSurface,
    CognitiveItemStatus,
    CognitivePendingMarker,
    get_list_counter_labels,
    get_list_event_count,
    get_list_samples,
    reset_list_counter,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.mcp.kg_tools import register_kg_tools


BOARD = "board-kg03-2"


@dataclass
class _FakeAgent:
    id: str = "agent-test-001"


class _MCPRegistryDouble:
    """Tiny in-memory double of FastMCP capturing @mcp.tool() closures."""

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
    target = tmp_path / "kg-03-2"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_list_counter()
    yield
    reset_list_counter()


def _register_tool(get_agent_impl: Callable[[], Any]) -> Callable[..., Any]:
    mcp = _MCPRegistryDouble()

    class _NullDb:
        async def __aenter__(self) -> "_NullDb":
            return self

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    def _get_db() -> _NullDb:
        return _NullDb()

    register_kg_tools(mcp, get_agent=get_agent_impl, get_uow=_get_db)
    return mcp.tools["okto_pulse_kg_list_cognitive_pending_items"], mcp.tools


@pytest.fixture
def list_tool(isolated_base_dir: Path) -> Callable[..., Any]:
    async def _get_agent() -> _FakeAgent:
        return _FakeAgent()

    tool, tools = _register_tool(_get_agent)
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
        assert primitive in tools
    return tool


@pytest.fixture
def unauthorized_list_tool(
    isolated_base_dir: Path,
) -> Callable[..., Any]:
    async def _get_agent_none() -> None:
        return None

    tool, _ = _register_tool(_get_agent_none)
    return tool


def _row(artifact_type: str, id_: str) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"{artifact_type}:{id_}",
    }


def _materialize(base_dir: Path, gen: str, sources: list[dict]) -> None:
    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=sources,
        event_ref="evt_kg03_2",
    )


def _invoke(tool: Callable[..., Any], **kwargs: Any) -> dict[str, Any]:
    raw = asyncio.run(tool(**kwargs))
    return json.loads(raw)


# -------- api_ae3a932a — response shape ---------------------------------


def test_response_includes_all_required_contract_keys(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [
        _row("spec", "s1"),
        _row("refinement", "r1"),
    ])
    response = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    required = {
        "board_id",
        "selected_kg_generation_id",
        "items",
        "counts",
        "legacy_mode",
    }
    assert required.issubset(response.keys())
    # Storage-name leaks must NOT be present.
    assert "kg_generation_id" not in response
    assert "status_filter" not in response
    assert "reason_code" not in response


def test_counts_have_all_status_buckets_plus_total(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [
        _row("spec", "s1"),
        _row("spec", "s2"),
        _row("refinement", "r1"),
    ])
    response = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    counts = response["counts"]
    assert set(counts.keys()) == {
        "pending",
        "in_progress",
        "consolidated",
        "skipped",
        "failed",
        "total",
    }
    # All bucket values are integers.
    for key, val in counts.items():
        assert isinstance(val, int), f"{key} must be int, got {type(val)}"
    # All 3 items are pending, total reflects items returned.
    assert counts["pending"] == 3
    assert counts["in_progress"] == 0
    assert counts["consolidated"] == 0
    assert counts["skipped"] == 0
    assert counts["failed"] == 0
    assert counts["total"] == 3


def test_counts_total_equals_len_items_after_pagination(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [
        _row("spec", f"s{i}") for i in range(7)
    ])
    response = _invoke(
        list_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        limit=3,
        offset=2,
    )
    assert len(response["items"]) == 3
    assert response["counts"]["total"] == 3
    assert response["counts"]["pending"] == 3


def test_legacy_mode_true_for_aggregate_only_record(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    record_dir = (
        isolated_base_dir / "rebuild" / "audit" / "cognitive_pending" / BOARD
    )
    record_dir.mkdir(parents=True, exist_ok=True)
    legacy = {
        "board_id": BOARD,
        "kg_generation_id": gen,
        "event_ref": "evt_legacy",
        "pending_count": 2,
        "pending_refs": ["spec:l1", "refinement:l2"],
        "status": "pending_marked",
        "recorded_at": "2026-05-25T00:00:00+00:00",
    }
    (record_dir / f"{gen}.json").write_text(
        json.dumps(legacy), encoding="utf-8"
    )

    response = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    assert response["legacy_mode"] is True
    assert response["counts"]["total"] == 2
    refs = {it["source_ref"] for it in response["items"]}
    assert refs == {"spec:l1", "refinement:l2"}


def test_legacy_mode_false_for_kg03_record(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    response = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    assert response["legacy_mode"] is False


# -------- Item projection (reason_code, not reason) ---------------------


def test_item_shape_uses_reason_code_not_reason(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    response = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    assert len(response["items"]) == 1
    item = response["items"][0]
    # API field present.
    assert "reason_code" in item
    # Storage field MUST NOT leak.
    assert "reason" not in item
    # Internal-only storage fields hidden.
    assert "board_id" not in item
    assert "kg_generation_id" not in item
    assert "event_ref" not in item


def test_item_shape_exposes_only_contract_fields(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    response = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    expected = {
        "item_id",
        "source_ref",
        "artifact_type",
        "status",
        "recorded_at",
        "updated_at",
        "updated_by_agent_id",
        "consolidation_session_id",
        "reason_code",
    }
    for item in response["items"]:
        assert set(item.keys()) == expected


def test_safe_projection_drops_raw_artifact_body(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    rich = {
        "artifact_type": "spec",
        "id": "s1",
        "source_ref": "spec:s1",
        "title": "Sensitive Spec Title",
        "description": "leaky body",
        "secret_token": "leak-bait",
    }
    _materialize(isolated_base_dir, gen, [rich])

    raw = asyncio.run(list_tool(board_id=BOARD, kg_generation_id=gen))
    assert "Sensitive Spec Title" not in raw
    assert "leaky body" not in raw
    assert "secret_token" not in raw
    assert "leak-bait" not in raw


# -------- Status (contract param) ----------------------------------------


@pytest.mark.parametrize(
    "valid_status",
    ["pending", "in_progress", "consolidated", "skipped", "failed"],
)
def test_status_param_accepts_bounded_enum(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
    valid_status: str,
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    response = _invoke(
        list_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        status=valid_status,
    )
    assert "error" not in response


@pytest.mark.parametrize(
    "bad_status",
    ["completed", "PENDING", "any", "in-progress", ""],
)
def test_invalid_status_returns_typed_error(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
    bad_status: str,
) -> None:
    response = _invoke(list_tool, board_id=BOARD, status=bad_status)
    assert "error" in response
    assert response["error"]["code"] == "invalid_status"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemListReasonCode.INVALID_STATUS.value
    )


def test_status_param_filters_items(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [
        _row("spec", "s1"),
        _row("spec", "s2"),
        _row("refinement", "r1"),
    ])
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    items = store.list_items(BOARD, gen)
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1",
    )

    pending = _invoke(
        list_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        status="pending",
    )
    consolidated = _invoke(
        list_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        status="consolidated",
    )
    assert pending["counts"]["total"] == 2
    assert pending["counts"]["pending"] == 2
    assert consolidated["counts"]["total"] == 1
    assert consolidated["counts"]["consolidated"] == 1


def test_status_filter_compat_alias_still_works(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    """``status_filter`` is the deprecated compatibility alias; passing it
    should still filter correctly when ``status`` is omitted."""

    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    response = _invoke(
        list_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        status_filter="pending",
    )
    assert "error" not in response
    assert response["counts"]["pending"] == 1


# -------- Limit / offset (pagination) -----------------------------------


def test_limit_caps_returned_items(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [
        _row("spec", f"s{i}") for i in range(5)
    ])
    response = _invoke(
        list_tool, board_id=BOARD, kg_generation_id=gen, limit=2
    )
    assert len(response["items"]) == 2


def test_offset_skips_items(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [
        _row("spec", f"s{i}") for i in range(5)
    ])
    full = _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    offset_two = _invoke(
        list_tool, board_id=BOARD, kg_generation_id=gen, offset=2
    )
    assert len(offset_two["items"]) == 3
    assert offset_two["items"][0]["item_id"] == full["items"][2]["item_id"]


@pytest.mark.parametrize(
    "bad_limit",
    [0, -1, 201, "100"],
)
def test_invalid_limit_returns_typed_error(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
    bad_limit: Any,
) -> None:
    response = _invoke(list_tool, board_id=BOARD, limit=bad_limit)
    assert "error" in response
    assert response["error"]["code"] == "invalid_limit"


@pytest.mark.parametrize(
    "bad_offset",
    [-1, "0"],
)
def test_invalid_offset_returns_typed_error(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
    bad_offset: Any,
) -> None:
    response = _invoke(list_tool, board_id=BOARD, offset=bad_offset)
    assert "error" in response
    assert response["error"]["code"] == "invalid_offset"


# -------- generation_not_found semantics --------------------------------


def test_explicit_unknown_generation_returns_generation_not_found(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    """Codex audit val_ead80fbd: explicit kg_generation_id + no record =
    typed ``generation_not_found`` error (not silent empty success)."""

    nonexistent = generate_kg_generation_id()
    response = _invoke(
        list_tool, board_id=BOARD, kg_generation_id=nonexistent
    )
    assert "error" in response
    assert response["error"]["code"] == "generation_not_found"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemListReasonCode.NO_GENERATION_FOUND.value
    )
    assert (
        get_list_event_count(
            outcome=CognitiveItemListOutcome.NOT_FOUND.value,
            reason_code=CognitiveItemListReasonCode.NO_GENERATION_FOUND.value,
        )
        == 1
    )


def test_omitted_generation_with_no_latest_returns_safe_empty(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    """When ``kg_generation_id`` is omitted and the board has no latest,
    return a safe empty success with all bucket counts zeroed — NOT
    ``generation_not_found`` (which is reserved for explicit lookups)."""

    response = _invoke(list_tool, board_id="board-without-any-rebuild")
    assert "error" not in response
    assert response["selected_kg_generation_id"] is None
    assert response["legacy_mode"] is False
    assert response["items"] == []
    assert response["counts"] == {
        "pending": 0,
        "in_progress": 0,
        "consolidated": 0,
        "skipped": 0,
        "failed": 0,
        "total": 0,
    }


# -------- Latest-generation fallback -------------------------------------


def test_omitting_kg_generation_id_uses_latest(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    import time
    gen_a = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen_a, [_row("spec", "a")])
    time.sleep(0.01)
    gen_b = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen_b, [
        _row("spec", "b1"),
        _row("refinement", "b2"),
    ])

    response = _invoke(list_tool, board_id=BOARD)
    assert response["selected_kg_generation_id"] == gen_b
    assert response["counts"]["total"] == 2


# -------- Auth gating ----------------------------------------------------


def test_unauthorized_short_circuits_before_storage_read(
    unauthorized_list_tool: Callable[..., Any],
) -> None:
    response = _invoke(unauthorized_list_tool, board_id=BOARD)
    assert "error" in response
    assert response["error"]["code"] == "unauthorized"
    assert get_list_event_count() == 0


def test_empty_board_id_returns_missing_board_id_error(
    list_tool: Callable[..., Any],
) -> None:
    response = _invoke(list_tool, board_id="")
    assert "error" in response
    assert response["error"]["code"] == "missing_board_id"


# -------- or_c8fff4f5 counter --------------------------------------------


def test_counter_label_set_excludes_high_cardinality_dims(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    assert get_list_counter_labels() == (
        "surface",
        "board_id",
        "outcome",
        "status_filter_present",
        "reason_code",
    )
    samples = get_list_samples()
    assert len(samples) == 1
    sample = samples[0]
    assert set(sample.keys()) == {
        "surface",
        "board_id",
        "outcome",
        "status_filter_present",
        "reason_code",
        "item_count",
    }
    forbidden = {
        "source_ref",
        "item_id",
        "status",
        "status_filter",
        "status_value",
        "actor",
        "reason",
    }
    assert not forbidden.intersection(sample.keys())
    assert sample["surface"] == CognitiveItemListSurface.MCP.value


def test_counter_emits_validation_error_on_invalid_status(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    _invoke(list_tool, board_id=BOARD, status="bogus")
    samples = get_list_samples()
    assert len(samples) == 1
    s = samples[0]
    assert s["outcome"] == CognitiveItemListOutcome.VALIDATION_ERROR.value
    assert s["reason_code"] == CognitiveItemListReasonCode.INVALID_STATUS.value
    assert s["status_filter_present"] == "true"
    assert s["item_count"] == 0


def test_counter_emits_one_sample_per_call_for_repeated_invocations(
    isolated_base_dir: Path,
    list_tool: Callable[..., Any],
) -> None:
    gen = generate_kg_generation_id()
    _materialize(isolated_base_dir, gen, [_row("spec", "s1")])
    for _ in range(5):
        _invoke(list_tool, board_id=BOARD, kg_generation_id=gen)
    assert get_list_event_count(
        outcome=CognitiveItemListOutcome.SUCCESS.value
    ) == 5
