"""R3.2 — Traceability and selected local MCP payload compactors.

Covers spec ``MCP KG Query Safety and Quick-Win Payload Bounds`` test scenarios:

- ``ts_c7804ae6`` — selected card/comment reads compact only semantic-empty
  fields (FR6) + FR8 safe compaction diagnostics (or_f4159e58).
- ``ts_58712f3b`` — traceability compact default omits artifact bodies and
  deduplicates bug cards (FR5/FR7, AC5/AC8).
- ``ts_4db5890f`` — resolved FR text is not duplicated in business-rule and
  API-contract list responses (FR7, AC7).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import logging
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.payload_compaction import (
    COMPACTION_METRIC,
    MCPQuickWinPayloadCompactor,
    compact_and_emit,
    compact_payload,
    compute_compaction_stats,
)
from sqlalchemy_test_models import (
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Comment,
    Spec,
    SpecStatus,
)

USER_ID = "payload-compaction-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["board:read"],
        },
    )()


async def _call(name: str, **kwargs):
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


_SAFE_METRIC_KEYS = {
    "tool_name",
    "profile",
    "omitted_count",
    "deduped_count",
    "truncated",
}


def _only_compaction_record(caplog, tool_name: str):
    """Return the single mcp_quickwin_compaction_total record for ``tool_name``."""
    recs = [
        r
        for r in caplog.records
        if r.name == "okto_pulse.mcp.compaction"
        and getattr(r, "compaction", {}).get("tool_name") == tool_name
    ]
    assert len(recs) == 1, f"expected exactly one metric for {tool_name}, got {len(recs)}"
    rec = recs[0]
    assert rec.message == "mcp_quickwin_compaction_total"
    # Safe shape: only counts + identifiers, all scalar.
    assert set(rec.compaction.keys()) == _SAFE_METRIC_KEYS
    assert all(isinstance(v, (str, int, bool)) for v in rec.compaction.values())
    return rec


def _assert_no_body(rec, *forbidden: str) -> None:
    """Prove no payload body / FR text / artifact content leaked into the metric."""
    blob = " ".join(
        [rec.getMessage(), str(rec.compaction), str(getattr(rec, "args", ""))]
    )
    for needle in forbidden:
        assert needle not in blob


# ---------------------------------------------------------------------------
# ts_c7804ae6 — compactor unit behaviour (semantic-empty only) + FR8 diagnostics
# ---------------------------------------------------------------------------


def test_compactor_drops_only_semantic_empty_fields():
    obj = {
        "id": "card-1",
        "status": "open",  # semantic; kept even though some empties around
        "card_type": "",  # semantic-always → kept despite being empty
        "spec_id": None,  # non-semantic empty → dropped
        "description": "",  # non-semantic empty → dropped
        "labels": [],  # non-semantic empty → dropped
        "attachments": [],  # non-semantic empty → dropped
        "title": "Real title",  # non-empty → kept
        "position": 0,  # falsy but NOT empty → kept
    }
    out = compact_payload(obj)
    assert out["id"] == "card-1"
    assert out["status"] == "open"
    assert out["card_type"] == ""  # semantic-always survives emptiness
    assert out["title"] == "Real title"
    assert out["position"] == 0
    for dropped in ("spec_id", "description", "labels", "attachments"):
        assert dropped not in out


def test_compactor_collapses_updated_at_equal_to_created_at():
    same = "2026-06-01T00:00:00+00:00"
    out = compact_payload(
        {"id": "c", "created_at": same, "updated_at": same, "status": "open"}
    )
    assert out["created_at"] == same
    assert "updated_at" not in out  # collapsed: no edit since creation


def test_compactor_keeps_updated_at_when_edited():
    out = compact_payload(
        {
            "id": "c",
            "created_at": "2026-06-01T00:00:00+00:00",
            "updated_at": "2026-06-02T00:00:00+00:00",
            "status": "open",
        }
    )
    assert out["updated_at"] == "2026-06-02T00:00:00+00:00"


def test_compactor_recurses_into_nested_lists_and_dicts():
    obj = {
        "id": "x",
        "cards": [
            {"id": "a", "status": "open", "assignee_id": None, "due_date": None},
            {"id": "b", "status": "done", "labels": ["urgent"]},
        ],
    }
    out = compact_payload(obj)
    assert "assignee_id" not in out["cards"][0]
    assert "due_date" not in out["cards"][0]
    assert out["cards"][0]["id"] == "a"
    assert out["cards"][1]["labels"] == ["urgent"]


def test_compute_compaction_stats_counts_omitted_and_deduped():
    same = "2026-06-01T00:00:00+00:00"
    _, stats = compute_compaction_stats(
        {
            "id": "c",
            "status": "open",
            "created_at": same,
            "updated_at": same,  # → deduped 1
            "spec_id": None,  # → omitted 1
            "description": "",  # → omitted 1
            "labels": [],  # → omitted 1
        },
        tool_name="okto_pulse_get_card",
    )
    assert stats == {
        "tool_name": "okto_pulse_get_card",
        "profile": "compact",
        "omitted_count": 3,
        "deduped_count": 1,
        "truncated": False,
    }


def test_compact_and_emit_emits_safe_metric_without_payload_body(caplog):
    marker = "SECRET_BODY_MARKER_DO_NOT_LOG"
    with caplog.at_level(logging.INFO, logger="okto_pulse.mcp.compaction"):
        out = compact_and_emit(
            {"id": "c", "status": "open", "title": marker, "spec_id": None},
            tool_name="okto_pulse_get_card",
        )
    # The compacted payload itself is returned intact (title preserved).
    assert out["title"] == marker
    assert "spec_id" not in out

    records = [r for r in caplog.records if r.name == "okto_pulse.mcp.compaction"]
    assert len(records) == 1
    record = records[0]
    assert record.message == COMPACTION_METRIC
    stats = record.compaction
    assert set(stats.keys()) == {
        "tool_name",
        "profile",
        "omitted_count",
        "deduped_count",
        "truncated",
    }
    # Every emitted value is a scalar identifier/count — never a payload body.
    assert all(isinstance(v, (str, int, bool)) for v in stats.values())
    # Hard proof the body never travels through the diagnostic channel.
    blob = " ".join(
        [record.getMessage(), str(stats), str(getattr(record, "args", ""))]
    )
    assert marker not in blob


def test_truncated_flag_is_carried_through():
    _, stats = compute_compaction_stats(
        [{"id": "a", "status": "open"}],
        tool_name="okto_pulse_list_cards_by_status",
        truncated=True,
    )
    assert stats["truncated"] is True
    assert stats["tool_name"] == "okto_pulse_list_cards_by_status"


def test_compactor_class_compact_matches_module_wrapper():
    obj = {"id": "x", "status": "open", "empty": None}
    assert MCPQuickWinPayloadCompactor().compact(obj) == compact_payload(obj)


# ---------------------------------------------------------------------------
# ts_c7804ae6 (integration) — get_card / list_comments compact at runtime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_card_compacts_null_non_semantic_fields():
    db_factory = get_session_factory()
    board_id = _id("compact-board")
    card_id = _id("compact-card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Compact Board", owner_id=USER_ID))
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=None,  # non-semantic empty → omitted
                assignee_id=None,  # non-semantic empty → omitted
                title="Lonely card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        card = await _call("okto_pulse_get_card", board_id=board_id, card_id=card_id)

    # Semantic fields preserved.
    assert card["id"] == card_id
    assert card["status"] == "not_started"
    assert card["title"] == "Lonely card"
    # Null / empty non-semantic fields omitted.
    assert "assignee_id" not in card
    assert "spec_id" not in card
    assert "due_date" not in card
    assert "labels" not in card
    assert "attachments" not in card
    assert "qa_items" not in card
    assert "comments" not in card


@pytest.mark.asyncio
async def test_list_comments_collapses_updated_at_when_unedited():
    db_factory = get_session_factory()
    board_id = _id("comment-board")
    card_id = _id("comment-card")
    comment_id = _id("comment")
    stamp = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Comment Board", owner_id=USER_ID))
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                title="Card with comment",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        db.add(
            Comment(
                id=comment_id,
                card_id=card_id,
                content="First note",
                author_id=USER_ID,
                comment_type="text",
                created_at=stamp,
                updated_at=stamp,  # identical → must collapse
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        comments = await _call(
            "okto_pulse_list_comments", board_id=board_id, card_id=card_id
        )

    assert len(comments) == 1
    item = comments[0]
    assert item["id"] == comment_id
    assert item["content"] == "First note"
    assert item["comment_type"] == "text"
    assert "created_at" in item
    assert "updated_at" not in item  # collapsed (no edit since creation)


# ---------------------------------------------------------------------------
# ts_58712f3b — traceability compact default omits artifact bodies + bug dedup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_traceability_default_omits_artifacts_and_dedups_bug_cards(caplog):
    db_factory = get_session_factory()
    board_id = _id("trace-board")
    spec_id = _id("trace-spec")
    task_id = _id("trace-task")
    bug_card_id = _id("trace-bug")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Trace Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Trace Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_a", "text": "FR A"}],
                acceptance_criteria=["AC A"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
                screen_mockups=[{"id": "spec-mockup", "title": "Spec Mockup"}],
            )
        )
        db.add(
            Card(
                id=task_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Implement feature",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
                screen_mockups=[{"id": "card-mockup", "title": "Card Mockup"}],
            )
        )
        db.add(
            Card(
                id=bug_card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Fix regression",
                status=CardStatus.DONE,
                card_type=CardType.BUG,
                severity=BugSeverity.MAJOR,
                origin_task_id=task_id,
                expected_behavior="works",
                observed_behavior="broken",
                linked_test_task_ids=[],
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None), caplog.at_level(
        logging.INFO, logger="okto_pulse.mcp.compaction"
    ):
        # Default call — no include_artifacts argument.
        report = await _call(
            "okto_pulse_get_traceability_report", board_id=board_id, spec_id=spec_id
        )

    spec = report["orphan_specs"][0] if report.get("orphan_specs") else None
    # The spec is attached to the board but has no ideation lineage → orphan.
    assert spec is not None
    assert spec["id"] == spec_id

    # AC5/ac_c59937d3: default omits artifact BODIES (no `artifacts` block); a
    # compact summary with counts + IDs + drilldown hints is present instead.
    assert "artifacts" not in spec
    summary = spec["artifact_summary"]
    # counts
    assert summary["mockups_count"] == 1
    assert summary["knowledge_bases_count"] == 0
    assert summary["architecture_designs_count"] == 0
    # applicable IDs (no bodies/titles/content)
    assert summary["artifact_ids"]["mockups"] == ["spec-mockup"]
    assert "title" not in summary  # never a body
    # explicit safe full-mode drilldown instruction
    drill = summary["artifact_drilldown"]
    assert drill["available"] is True
    assert drill["tool_name"] == "okto_pulse_get_traceability_report"
    assert drill["include_artifacts"] == "true"
    assert drill["entity_type"] == "spec"
    assert drill["entity_id"] == spec_id

    full_card = next(c for c in spec["cards"] if c["id"] == bug_card_id)
    assert "artifacts" not in full_card  # default compact → no artifact body
    assert "artifact_summary" in full_card

    # AC8: bug appears as FULL body once (in `cards`) and as a slim index in
    # `bugs` — the bug index must NOT carry the heavy artifact bodies.
    bug_index = spec["bugs"][0]
    assert bug_index["id"] == bug_card_id
    assert bug_index["bug"]["severity"] == "major"
    assert "artifacts" not in bug_index
    assert "resolved_artifacts" not in bug_index
    assert "artifact_summary" not in bug_index

    # FR8 / or_f4159e58: the compacted+deduped traceability response emits the
    # safe metric (counts only, no body).
    rec = _only_compaction_record(caplog, "okto_pulse_get_traceability_report")
    assert rec.compaction["profile"] == "compact"
    assert rec.compaction["deduped_count"] >= 1  # the bug card
    assert rec.compaction["omitted_count"] >= 1  # at least the spec artifact body
    _assert_no_body(rec, spec_id)


@pytest.mark.asyncio
async def test_traceability_full_mode_still_expands_artifacts():
    db_factory = get_session_factory()
    board_id = _id("trace-full-board")
    spec_id = _id("trace-full-spec")
    task_id = _id("trace-full-task")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Trace Full Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Trace Full Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_a", "text": "FR A"}],
                acceptance_criteria=["AC A"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=task_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Implement feature",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        report = await _call(
            "okto_pulse_get_traceability_report",
            board_id=board_id,
            spec_id=spec_id,
            include_artifacts="true",
        )

    spec = report["orphan_specs"][0]
    assert "artifacts" in spec  # AC9: explicit full output still available


# ---------------------------------------------------------------------------
# ts_4db5890f — no duplicate FR text in business-rule / API-contract lists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_business_rules_does_not_duplicate_fr_text(caplog):
    db_factory = get_session_factory()
    board_id = _id("br-board")
    spec_id = _id("br-spec")
    fr_text = "User can authenticate with SSO"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="BR Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="BR Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_sso", "text": fr_text}],
                business_rules=[
                    {
                        "id": "br_1",
                        "title": "SSO rule",
                        "status": "active",
                        "linked_requirements": ["0"],
                    }
                ],
                api_contracts=[],
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), caplog.at_level(logging.INFO, logger="okto_pulse.mcp.compaction"):
        raw = await _call_raw(
            "okto_pulse_list_business_rules", board_id=board_id, spec_id=spec_id
        )

    data = json.loads(raw)
    rule = data["business_rules"][0]
    # IMPL-2: projection now emits canonical fr_id, not the re-derived index.
    # Stored value was legacy index "0"; frs[0].id = "fr_sso" → emitted as "fr_sso".
    assert rule["linked_requirements"] == ["fr_sso"]
    # Human text is resolved exactly once, under resolved_requirements.
    assert rule["resolved_requirements"] == [f"[FR-0] {fr_text}"]
    # The full FR text appears exactly once across the whole serialized payload.
    assert raw.count(fr_text) == 1

    # FR8 / or_f4159e58: dedup path emits the safe metric without the FR text.
    rec = _only_compaction_record(caplog, "okto_pulse_list_business_rules")
    assert rec.compaction["deduped_count"] == 1
    _assert_no_body(rec, fr_text)


@pytest.mark.asyncio
async def test_list_api_contracts_does_not_duplicate_fr_text(caplog):
    db_factory = get_session_factory()
    board_id = _id("contract-board")
    spec_id = _id("contract-spec")
    fr_text = "Expose a paginated cards endpoint"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Contract Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Contract Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_cards", "text": fr_text}],
                business_rules=[],
                api_contracts=[
                    {
                        "id": "api_1",
                        "method": "GET",
                        "path": "/cards",
                        "status": "active",
                        "linked_requirements": ["0"],
                        "linked_rules": [],
                    }
                ],
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), caplog.at_level(logging.INFO, logger="okto_pulse.mcp.compaction"):
        raw = await _call_raw(
            "okto_pulse_list_api_contracts", board_id=board_id, spec_id=spec_id
        )

    data = json.loads(raw)
    contract = data["api_contracts"][0]
    # IMPL-2: projection now emits canonical fr_id, not the re-derived index.
    # Stored value was legacy index "0"; frs[0].id = "fr_cards" → emitted as "fr_cards".
    assert contract["linked_requirements"] == ["fr_cards"]
    assert contract["resolved_requirements"] == [f"[FR-0] {fr_text}"]
    assert raw.count(fr_text) == 1

    # FR8 / or_f4159e58: dedup path emits the safe metric without the FR text.
    rec = _only_compaction_record(caplog, "okto_pulse_list_api_contracts")
    assert rec.compaction["deduped_count"] == 1
    _assert_no_body(rec, fr_text)


@pytest.mark.asyncio
async def test_list_business_rules_preserves_unresolved_legacy_refs():
    db_factory = get_session_factory()
    board_id = _id("br-unres-board")
    spec_id = _id("br-unres-spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="BR Unres Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="BR Unres Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_x", "text": "Known FR"}],
                business_rules=[
                    {
                        "id": "br_legacy",
                        "title": "Legacy rule",
                        "status": "active",
                        "linked_requirements": [
                            "0",
                            "dangling-legacy-ref-no-fr",
                        ],
                    }
                ],
                api_contracts=[],
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ):
        data = await _call(
            "okto_pulse_list_business_rules", board_id=board_id, spec_id=spec_id
        )

    rule = data["business_rules"][0]
    # IMPL-2: stored index "0" resolves to frs[0].id = "fr_x" → emitted as "fr_x".
    assert rule["linked_requirements"] == ["fr_x"]
    assert rule["resolved_requirements"] == ["[FR-0] Known FR"]
    # Legacy ref that maps to no FR is preserved, not silently dropped.
    assert rule["unresolved_requirements"] == ["dangling-legacy-ref-no-fr"]


async def _call_raw(name: str, **kwargs) -> str:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    return await tool.fn(**kwargs)
