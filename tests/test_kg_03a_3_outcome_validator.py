"""Tests for KG-03A.3 — Cognitive pending outcome validator + MCP tool
extension.

Scope (extend existing ``okto_pulse_kg_update_cognitive_pending_item``):

* ``status=consolidated`` MUST attribute ``outcome_type`` from the
  bounded ``CognitivePendingOutcomeType`` enum
  ({relation_created, candidate_created, formal_decision_promoted,
  existing_decision_linked, contradiction_dismissed,
  no_action_required}).
* ``outcome_type=no_action_required`` MUST carry a non-empty
  ``reason`` (no silent empty consolidations — br_7500e5f9).
* Outcome metadata (``outcome_type``, ``evidence_refs``,
  ``generated_candidate_decision_ids``,
  ``promoted_formal_decision_ids``) persists on the ledger item.
* ``kg_cognitive_item_update_total`` counter is reused (no duplicate
  runtime counter). Reason codes ``outcome_required`` and
  ``invalid_outcome_type`` are bounded values on the existing
  counter.
* Tool registration: only ONE
  ``okto_pulse_kg_update_cognitive_pending_item`` definition exists in
  the code base (no parallel tool).
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
    CognitivePendingOutcomeType,
    get_update_event_count,
    reset_update_counter,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.mcp.kg_tools import register_kg_tools


BOARD = "board-kg03a-3"


@dataclass
class _FakeAgent:
    id: str = "agent-test-03a3"


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
    target = tmp_path / "kg-03a-3"
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


@pytest.fixture
def update_tool(isolated_base_dir: Path) -> Callable[..., Any]:
    mcp = _MCPRegistryDouble()

    async def _agent() -> _FakeAgent:
        return _FakeAgent()

    class _NullDb:
        async def __aenter__(self) -> "_NullDb":
            return self

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    register_kg_tools(mcp, get_agent=_agent, get_uow=lambda: _NullDb())
    return mcp.tools["okto_pulse_kg_update_cognitive_pending_item"]


def _seed(base_dir: Path) -> tuple[str, list]:
    gen = generate_kg_generation_id()
    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            {
                "artifact_type": "spec",
                "id": "s1",
                "source_ref": "spec:s1",
            }
        ],
        event_ref="evt_kg03a_3",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    return gen, store.list_items(BOARD, gen)


def _invoke(tool: Callable[..., Any], **kwargs: Any) -> dict[str, Any]:
    raw = asyncio.run(tool(**kwargs))
    return json.loads(raw)


# -------- Bounded outcome enum -----------------------------------------


def test_outcome_type_enum_is_exactly_six_values() -> None:
    """KG-03A.3 bounded enum — six terminal outcomes; no more, no less."""

    values = {o.value for o in CognitivePendingOutcomeType}
    assert values == {
        "relation_created",
        "candidate_created",
        "formal_decision_promoted",
        "existing_decision_linked",
        "contradiction_dismissed",
        "no_action_required",
    }


# -------- Reject paths --------------------------------------------------


def test_consolidated_without_outcome_type_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """br_7500e5f9: consolidated without outcome_type is rejected with
    ``outcome_required``."""

    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        # outcome_type intentionally absent
    )
    assert "error" in response
    assert response["error"]["code"] == "outcome_required"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.OUTCOME_REQUIRED.value
    )
    # Persisted item still pending.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].status == "pending"


@pytest.mark.parametrize(
    "bad_outcome",
    ["promoted", "fixed_it", "consolidated", "OK", ""],
)
def test_invalid_outcome_type_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    bad_outcome: str,
) -> None:
    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        outcome_type=bad_outcome,
    )
    assert "error" in response
    # Empty string is caught by outcome_required guard first.
    if bad_outcome == "":
        assert response["error"]["code"] == "outcome_required"
    else:
        assert response["error"]["code"] == "invalid_outcome_type"
        assert (
            response["error"]["reason_code"]
            == CognitiveItemUpdateReasonCode.INVALID_OUTCOME_TYPE.value
        )


def test_no_action_required_without_reason_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """outcome_type=no_action_required MUST carry a justifying reason —
    no silent empty consolidations (br_7500e5f9)."""

    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        outcome_type="no_action_required",
        # reason intentionally absent
    )
    assert "error" in response
    assert response["error"]["code"] == "reason_required"
    assert (
        response["error"]["reason_code"]
        == CognitiveItemUpdateReasonCode.REASON_REQUIRED.value
    )


# -------- Accepted terminal outcomes -----------------------------------


@pytest.mark.parametrize(
    "outcome",
    [
        "relation_created",
        "candidate_created",
        "formal_decision_promoted",
        "existing_decision_linked",
        "contradiction_dismissed",
    ],
)
def test_accepted_outcome_persists_metadata(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    outcome: str,
) -> None:
    """Each valid outcome type (except no_action_required which needs
    a reason) consolidates successfully and the metadata persists."""

    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        outcome_type=outcome,
        evidence_refs=["ref-a", "ref-b"],
        generated_candidate_decision_ids=["cand-1"],
        promoted_formal_decision_ids=["dec-1"],
    )
    assert response["updated"] is True
    assert response["item"]["status"] == "consolidated"
    # Verify the ledger persisted outcome metadata.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    item = persisted[0]
    assert item.outcome_type == outcome
    assert item.evidence_refs == ("ref-a", "ref-b")
    assert item.generated_candidate_decision_ids == ("cand-1",)
    assert item.promoted_formal_decision_ids == ("dec-1",)


def test_no_action_required_with_reason_is_accepted(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        outcome_type="no_action_required",
        reason="content already covered by existing decision dec_42",
    )
    assert response["updated"] is True
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].outcome_type == "no_action_required"


# -------- Counter reuse (no duplicate runtime counter) -----------------


def test_outcome_failures_emit_on_existing_counter_with_bounded_reason_codes(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """KG-03A.3 reuses ``kg_cognitive_item_update_total``; the new
    reason codes ``outcome_required`` and ``invalid_outcome_type``
    appear there — no parallel counter."""

    gen, items = _seed(isolated_base_dir)
    # 1) outcome_required
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
    )
    # 2) invalid_outcome_type
    _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        outcome_type="bogus",
    )

    assert get_update_event_count(
        outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
        reason_code=CognitiveItemUpdateReasonCode.OUTCOME_REQUIRED.value,
    ) == 1
    assert get_update_event_count(
        outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
        reason_code=CognitiveItemUpdateReasonCode.INVALID_OUTCOME_TYPE.value,
    ) == 1


def test_counter_label_set_unchanged_after_kg03a3(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """Reuse invariant: the counter label tuple must stay
    ``(board_id, target_status, outcome, reason_code)``. KG-03A.3 must
    not introduce a parallel counter or add labels."""

    from okto_pulse.core.kg.rebuild_audit import get_update_counter_labels
    assert get_update_counter_labels() == (
        "board_id",
        "target_status",
        "outcome",
        "reason_code",
    )


# -------- Tool registration: no parallel tool --------------------------


def test_only_one_update_tool_definition_exists() -> None:
    """Grep the source: only ONE
    ``okto_pulse_kg_update_cognitive_pending_item`` function definition
    must exist (KG-03A handoff: do NOT create parallel MCP tool)."""

    from pathlib import Path
    import re

    src_root = Path(__file__).parents[1] / "src"
    pattern = re.compile(r"def\s+okto_pulse_kg_update_cognitive_pending_item\b")
    matches: list[Path] = []
    for path in src_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if pattern.search(text):
            matches.append(path)
    # Exactly one source file declares the tool.
    assert len(matches) == 1, (
        f"expected exactly one definition; found {len(matches)}: "
        f"{[str(m) for m in matches]}"
    )


# -------- Skipped/failed still require reason (unchanged contract) -----


def test_skipped_still_requires_reason(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """KG-03A.3 must not weaken KG-03.3's skipped/failed reason guard."""

    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.SKIPPED.value,
        # no reason
    )
    assert response["error"]["code"] == "reason_required"


# -------- Rework (val_44b86726) — unsafe metadata fields rejection -----
#
# Codex blocking issue: evidence_refs / generated_candidate_decision_ids /
# promoted_formal_decision_ids bypassed unsafe_payload validation. The
# rework extends detect_unsafe_update_payload to inspect them.


def _consolidated_kwargs(item_id: str, gen: str) -> dict[str, Any]:
    return {
        "board_id": BOARD,
        "kg_generation_id": gen,
        "item_id": item_id,
        "status": CognitiveItemStatus.CONSOLIDATED.value,
        "consolidation_session_id": "sess-1",
        "outcome_type": "relation_created",
    }


@pytest.mark.parametrize(
    "metadata_field",
    [
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    ],
)
def test_oversized_string_entry_in_metadata_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    metadata_field: str,
) -> None:
    """Each new metadata field rejects oversize entries
    (> 200 bytes), preserving the persisted item unchanged."""

    gen, items = _seed(isolated_base_dir)
    kwargs = _consolidated_kwargs(items[0].item_id, gen)
    kwargs[metadata_field] = ["ok-ref", "x" * 5000]
    response = _invoke(update_tool, **kwargs)
    assert response["error"]["code"] == "unsafe_payload"
    assert response["error"]["unsafe_field"] == metadata_field
    # Persisted item still pending.
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    assert persisted[0].status == "pending"
    assert persisted[0].outcome_type is None


@pytest.mark.parametrize(
    "metadata_field",
    [
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    ],
)
def test_token_shape_entry_in_metadata_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    metadata_field: str,
) -> None:
    """Token-shape strings (conf_<urlsafe> / tok_<urlsafe>) MUST never
    land in the ledger via outcome metadata."""

    gen, items = _seed(isolated_base_dir)
    kwargs = _consolidated_kwargs(items[0].item_id, gen)
    kwargs[metadata_field] = ["conf_" + "x" * 32]
    response = _invoke(update_tool, **kwargs)
    assert response["error"]["code"] == "unsafe_payload"
    assert response["error"]["unsafe_field"] == metadata_field


@pytest.mark.parametrize(
    "metadata_field",
    [
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    ],
)
def test_non_string_entry_in_metadata_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    metadata_field: str,
) -> None:
    """Lists must contain bounded strings only. Dicts, ints, nested
    lists and None entries trip the guard."""

    gen, items = _seed(isolated_base_dir)
    for bad_entry in [{"k": "v"}, 42, ["nested"], None]:
        kwargs = _consolidated_kwargs(items[0].item_id, gen)
        kwargs[metadata_field] = ["ok-ref", bad_entry]
        response = _invoke(update_tool, **kwargs)
        assert response["error"]["code"] == "unsafe_payload", (
            f"bad_entry={bad_entry!r} for field={metadata_field} should "
            "trip unsafe_payload but the response was: "
            f"{response.get('error')}"
        )
        assert response["error"]["unsafe_field"] == metadata_field


@pytest.mark.parametrize(
    "metadata_field",
    [
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    ],
)
def test_empty_string_entry_in_metadata_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    metadata_field: str,
) -> None:
    gen, items = _seed(isolated_base_dir)
    kwargs = _consolidated_kwargs(items[0].item_id, gen)
    kwargs[metadata_field] = ["valid-ref", "   "]
    response = _invoke(update_tool, **kwargs)
    assert response["error"]["code"] == "unsafe_payload"


@pytest.mark.parametrize(
    "metadata_field",
    [
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    ],
)
def test_non_list_metadata_value_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    metadata_field: str,
) -> None:
    """Passing a string or dict as the metadata field itself (instead
    of a list) is rejected — preserving the contract shape."""

    gen, items = _seed(isolated_base_dir)
    for bad_value in ["single-string", {"x": 1}, 42]:
        kwargs = _consolidated_kwargs(items[0].item_id, gen)
        kwargs[metadata_field] = bad_value
        response = _invoke(update_tool, **kwargs)
        assert response["error"]["code"] == "unsafe_payload", (
            f"bad_value={bad_value!r} for {metadata_field} should reject"
        )


@pytest.mark.parametrize(
    "metadata_field",
    [
        "evidence_refs",
        "generated_candidate_decision_ids",
        "promoted_formal_decision_ids",
    ],
)
def test_oversized_list_length_is_rejected(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
    metadata_field: str,
) -> None:
    """List bounded to 50 entries — DoS guard against thousands of refs."""

    gen, items = _seed(isolated_base_dir)
    kwargs = _consolidated_kwargs(items[0].item_id, gen)
    kwargs[metadata_field] = [f"ref-{i}" for i in range(51)]
    response = _invoke(update_tool, **kwargs)
    assert response["error"]["code"] == "unsafe_payload"


def test_rejected_metadata_does_not_leak_to_ledger_or_counter(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """End-to-end safety regression: an unsafe metadata field rejected
    by the guard MUST NOT mutate the ledger AND MUST NOT add new counter
    label dimensions."""

    from okto_pulse.core.kg.rebuild_audit import get_update_counter_labels

    gen, items = _seed(isolated_base_dir)
    kwargs = _consolidated_kwargs(items[0].item_id, gen)
    kwargs["evidence_refs"] = ["conf_" + "x" * 50]  # token shape
    response = _invoke(update_tool, **kwargs)
    assert response["error"]["code"] == "unsafe_payload"

    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    persisted = store.list_items(BOARD, gen)
    # Item untouched.
    assert persisted[0].status == "pending"
    assert persisted[0].outcome_type is None
    assert persisted[0].evidence_refs == ()
    # Counter labels unchanged.
    assert get_update_counter_labels() == (
        "board_id",
        "target_status",
        "outcome",
        "reason_code",
    )


# -------- Response projection now echoes outcome metadata --------------


def test_update_response_echoes_outcome_metadata(
    isolated_base_dir: Path,
    update_tool: Callable[..., Any],
) -> None:
    """Codex minor note: the update response now returns the persisted
    outcome metadata so callers can confirm what was stored without an
    extra round-trip."""

    gen, items = _seed(isolated_base_dir)
    response = _invoke(
        update_tool,
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        status=CognitiveItemStatus.CONSOLIDATED.value,
        consolidation_session_id="sess-1",
        outcome_type="formal_decision_promoted",
        evidence_refs=["ev-1", "ev-2"],
        generated_candidate_decision_ids=["cand-1"],
        promoted_formal_decision_ids=["dec-99"],
    )
    item = response["item"]
    assert item["outcome_type"] == "formal_decision_promoted"
    assert item["evidence_refs"] == ["ev-1", "ev-2"]
    assert item["generated_candidate_decision_ids"] == ["cand-1"]
    assert item["promoted_formal_decision_ids"] == ["dec-99"]
