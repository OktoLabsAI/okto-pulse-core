"""R2.1 — MCPContextProjectionService + profiled get_task_context/get_spec_context.

Covers spec ``MCP High-Frequency Response Projection and Dedup`` scenarios:

- ``ts_2d23335e`` — summary task context deduplicates spec + resolved refs.
- ``ts_ffdb99a6`` — full task context preserves the existing payload shape.
- ``ts_aed726ed`` — compaction omits only semantically-empty nulls.
- ``ts_7fd71d39`` — summary keeps the active card's linked scenario refs.
- ``ts_3721f9a9`` — detail drilldown carries more identifying ref fields.
- ``ts_385e7a75`` — unsupported projection returns the supported profile list.
- ``ts_cb1046ab`` — projected responses use the R5 canonical metadata names.
- ``ts_85b00d36`` — summary spec context deduplicates resolved references.

OWNER DECISION (R2.1 card): summary is CONSERVATIVE/dedup-only — it keeps the
unique content an agent needs (card body, spec requirement texts, this card's
scenarios, validations) and removes only the duplication.
"""

from __future__ import annotations

import copy
import json
import uuid

import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.context_projection import (
    project_spec_context,
    project_task_context,
)
from okto_pulse.core.mcp.projection_envelope import ENVELOPE_METADATA_KEYS
from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)

USER_ID = "context-projection-agent"


def _full_task_result() -> dict:
    return {
        "card": {
            "id": "c1",
            "title": "Build X",
            "description": "what to build",
            "details": "detailed scope the agent needs",
            "status": "in_progress",
            "card_type": "normal",
            "spec_id": "s1",
            "test_scenario_ids": ["ts_a"],
            "assignee_id": None,  # semantic-empty null
            "labels": [],
        },
        "spec": {
            "id": "s1",
            "title": "Spec",
            "status": "in_progress",
            "functional_requirements": [{"id": "fr_0", "text": "FR text the agent needs"}],
            "decisions": [{"id": "d1", "title": "decision"}],
        },
        "my_test_scenarios": [{"id": "ts_a", "title": "my scenario"}],
        "resolved_references": {
            "knowledge_bases": [
                {
                    "id": "kb1",
                    "title": "KB",
                    "content": "HUGE BODY " * 30,
                    "description": "kb prose description",
                    "source_type": "manual",
                    "source_ref": "kb:src",
                }
            ],
            "functional_requirements": [
                {"index": 0, "text": "FR text the agent needs", "referenced_by_task": True}
            ],
        },
        "validations": [{"id": "v1", "outcome": "failed"}],
    }


def _arch_design(did: str, parent_type: str, parent_id: str, *, with_body: bool = True) -> dict:
    """An architecture design as serialized into context sections — identifying
    fields + (optionally) the heavy bodies R2.2 deduplicates."""
    d = {
        "id": did,
        "title": f"Arch {did}",
        "parent_type": parent_type,
        "parent_id": parent_id,
        "version": 2,
        "source_ref": f"arch:{did}",
        "source_design_id": None,
        "source_version": None,
    }
    if with_body:
        d["global_description"] = "BIG architecture prose " * 20
        d["entities"] = [{"name": "User"}, {"name": "Order"}]
        d["interfaces"] = [{"name": "REST"}]
        d["diagrams"] = [{"type": "c4", "body": "DIAGRAM " * 50}]
    return d


def _task_result_with_arch_md() -> dict:
    """``_full_task_result`` plus the two big R2.2 blocks: ``decisions_markdown``
    (the redundant third rendering) and architecture bodies repeated across the
    card, spec, and resolved_references sections."""
    r = _full_task_result()
    r["card"]["architecture_designs"] = [_arch_design("ad_card", "card", "c1")]
    r["spec"]["architecture_designs"] = [_arch_design("ad_spec", "spec", "s1")]
    r["spec"]["decisions_markdown"] = "## Decisions\n\n- d1: decision body\n" * 5
    r["resolved_references"]["architecture_designs"] = [
        {**_arch_design("ad_card", "card", "c1"), "source_type": "card", "source_id": "c1"},
        {
            **_arch_design("ad_spec", "spec", "s1"),
            "source_type": "spec",
            "source_id": "s1",
            "reference_type": "parent_spec",
        },
    ]
    return r


# ---------------------------------------------------------------------------
# ts_ffdb99a6 — full/legacy preserve the assembled payload exactly
# ---------------------------------------------------------------------------


def test_full_and_legacy_preserve_payload_unchanged():
    src = _full_task_result()
    snapshot = copy.deepcopy(src)
    for profile in ("full", "legacy"):
        out = project_task_context(src, card_id="c1", profile=profile)
        assert out == snapshot  # byte-for-byte the prior payload
        assert "projection" not in out  # no envelope injected in full/legacy
    assert src == snapshot  # input never mutated


# ---------------------------------------------------------------------------
# ts_2d23335e — summary dedups spec + resolved references (no body duplication)
# ---------------------------------------------------------------------------


def test_summary_dedups_resolved_references_keeping_unique_content():
    src = _full_task_result()
    snapshot = copy.deepcopy(src)
    out = project_task_context(src, card_id="c1", profile="summary")

    # Unique content the agent needs is preserved.
    assert out["card"]["description"] == "what to build"
    assert out["card"]["details"] == "detailed scope the agent needs"
    assert out["spec"]["functional_requirements"][0]["text"] == "FR text the agent needs"
    assert out["validations"] == [{"id": "v1", "outcome": "failed"}]

    # The DUPLICATION is removed: resolved_references no longer carry the bodies
    # already present in spec — only id/ref/link fields.
    kb_ref = out["resolved_references"]["knowledge_bases"][0]
    assert "content" not in kb_ref
    assert kb_ref["id"] == "kb1" and kb_ref["title"] == "KB"
    fr_ref = out["resolved_references"]["functional_requirements"][0]
    assert "text" not in fr_ref  # the FR text is in spec, not duplicated here
    assert fr_ref["index"] == 0 and fr_ref["referenced_by_task"] is True

    assert src == snapshot  # input untouched


# ---------------------------------------------------------------------------
# ts_aed726ed — compaction omits ONLY semantically-empty nulls
# ---------------------------------------------------------------------------


def test_summary_omits_only_semantic_nulls():
    out = project_task_context(_full_task_result(), card_id="c1", profile="summary")
    # null assignee_id + empty labels dropped...
    assert "assignee_id" not in out["card"]
    assert "labels" not in out["card"]
    # ...but status / id / card_type kept (semantic).
    assert out["card"]["status"] == "in_progress"
    assert out["card"]["id"] == "c1"
    assert out["card"]["card_type"] == "normal"


# ---------------------------------------------------------------------------
# ts_7fd71d39 — summary keeps the active card's linked scenarios
# ---------------------------------------------------------------------------


def test_summary_keeps_active_card_linked_scenarios():
    out = project_task_context(_full_task_result(), card_id="c1", profile="summary")
    assert out["my_test_scenarios"] == [{"id": "ts_a", "title": "my scenario"}]


# ---------------------------------------------------------------------------
# ts_3721f9a9 — detail carries more identifying ref fields than summary
# ---------------------------------------------------------------------------


def test_detail_enriches_refs_without_full_bodies():
    src = _full_task_result()
    summary = project_task_context(copy.deepcopy(src), card_id="c1", profile="summary")
    detail = project_task_context(copy.deepcopy(src), card_id="c1", profile="detail")

    s_kb = summary["resolved_references"]["knowledge_bases"][0]
    d_kb = detail["resolved_references"]["knowledge_bases"][0]
    # Identifying fields are KEPT in BOTH (denylist — no semantic loss):
    assert s_kb.get("source_type") == "manual" and s_kb.get("source_ref") == "kb:src"
    assert s_kb.get("id") == "kb1" and s_kb.get("title") == "KB"
    # detail keeps the prose `description` that summary drops...
    assert "description" not in s_kb
    assert d_kb.get("description") == "kb prose description"
    # ...but NEITHER carries the full `content` body (always deduped).
    assert "content" not in s_kb and "content" not in d_kb
    assert detail["projection"]["profile"] == "detail"


# ---------------------------------------------------------------------------
# ts_385e7a75 — unsupported profile → structured error with supported list
# ---------------------------------------------------------------------------


def test_unsupported_profile_returns_supported_list():
    out = project_task_context(_full_task_result(), card_id="c1", profile="verbose")
    assert out["error_code"] == "unsupported_projection"
    assert out["supported_profiles"] == ["summary", "detail", "full", "legacy"]
    # spec context too
    out2 = project_spec_context({"id": "s1"}, profile="weird")
    assert out2["error_code"] == "unsupported_projection"


# ---------------------------------------------------------------------------
# ts_cb1046ab — R5 canonical projection metadata names
# ---------------------------------------------------------------------------


def test_projection_metadata_uses_r5_canonical_names():
    out = project_task_context(_full_task_result(), card_id="c1", profile="summary")
    meta = out["projection"]
    for key in ENVELOPE_METADATA_KEYS:  # profile/payload_bytes/truncated/omitted/deduped/follow_up
        assert key in meta
    assert meta["profile"] == "summary"
    assert isinstance(meta["payload_bytes"], int) and meta["payload_bytes"] > 0
    assert meta["follow_up"] == [
        {"rel": "read_full_context", "target_ref": "okto_pulse_get_task_context"}
    ]


# ---------------------------------------------------------------------------
# ts_85b00d36 — summary spec context deduplicates resolved references
# ---------------------------------------------------------------------------


def test_summary_spec_context_dedups_resolved_references():
    spec_result = {
        "id": "s1",
        "title": "Spec",
        "functional_requirements": [{"id": "fr_0", "text": "FR"}],
        "assignee_id": None,
        "resolved_references": {
            "knowledge_bases": [{"id": "kb1", "title": "KB", "content": "BODY " * 40}],
        },
    }
    out = project_spec_context(spec_result, profile="summary")
    assert out["functional_requirements"][0]["text"] == "FR"  # unique content kept
    assert "content" not in out["resolved_references"]["knowledge_bases"][0]  # dedup
    assert "assignee_id" not in out  # null omitted
    assert out["projection"]["profile"] == "summary"


# ---------------------------------------------------------------------------
# Handler integration — default summary, full passthrough, include flags
# ---------------------------------------------------------------------------


def _id(p: str) -> str:
    return f"{p}-{uuid.uuid4()}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": USER_ID, "board_id": board_id, "permissions": ["board:read"]},
    )()


async def _call(name: str, **kwargs) -> dict:
    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


@pytest.mark.asyncio
async def test_get_task_context_default_summary_and_full_passthrough():
    from unittest.mock import AsyncMock, patch

    db_factory = get_session_factory()
    board_id = _id("ctxproj-board")
    spec_id = _id("ctxproj-spec")
    card_id = _id("ctxproj-card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Ctx Proj", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_0", "text": "FR text"}],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Card",
                status=CardStatus.IN_PROGRESS,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        # default → summary
        default = await _call("okto_pulse_get_task_context", board_id=board_id, card_id=card_id)
        full = await _call(
            "okto_pulse_get_task_context", board_id=board_id, card_id=card_id, profile="full"
        )
        bad = await _call(
            "okto_pulse_get_task_context", board_id=board_id, card_id=card_id, profile="nope"
        )
        # include flag still respected under projection
        no_kb = await _call(
            "okto_pulse_get_task_context",
            board_id=board_id,
            card_id=card_id,
            profile="full",
            include_knowledge="false",
        )

    # default is summary with R5 canonical metadata
    assert default["projection"]["profile"] == "summary"
    for key in ENVELOPE_METADATA_KEYS:
        assert key in default["projection"]
    # summary keeps the card body (unique content the agent needs)
    assert default["card"]["title"] == "Card"

    # full preserves the prior payload shape (no projection envelope injected)
    assert "projection" not in full
    assert full["spec"]["functional_requirements"][0]["text"] == "FR text"

    # unsupported profile → structured error, no silent fallback
    assert bad["error_code"] == "unsupported_projection"

    # include_knowledge=false honored even under the full profile
    assert no_kb["resolved_references"].get("knowledge_bases") == []


# ===========================================================================
# R2.2 — decisions_markdown gating + architecture body dedup
# (ts_0fb8a3f4, ts_cfdf1aa3, ts_85b00d36, ts_cb1046ab)
# ===========================================================================


# ---------------------------------------------------------------------------
# ts_0fb8a3f4 — summary gates decisions_markdown; full/legacy preserve it
# ---------------------------------------------------------------------------


def test_summary_gates_decisions_markdown_with_render_follow_up():
    src = _task_result_with_arch_md()
    snapshot = copy.deepcopy(src)
    out = project_task_context(src, card_id="c1", profile="summary")

    # The redundant third rendering is gone...
    assert "decisions_markdown" not in out["spec"]
    # ...but the structured decisions + stats (unique content) stay.
    assert out["spec"]["decisions"] == [{"id": "d1", "title": "decision"}]
    # A compact render reference takes its place in follow_up.
    assert {
        "rel": "render_decisions_markdown",
        "target_ref": "spec:s1:decisions_markdown",
    } in out["projection"]["follow_up"]

    assert src == snapshot  # input untouched


def test_full_and_legacy_preserve_decisions_markdown_and_architecture_bodies():
    src = _task_result_with_arch_md()
    snapshot = copy.deepcopy(src)
    for profile in ("full", "legacy"):
        out = project_task_context(src, card_id="c1", profile=profile)
        assert out == snapshot  # byte-for-byte
        assert out["spec"]["decisions_markdown"]  # markdown preserved
        assert out["card"]["architecture_designs"][0]["entities"]  # full body kept
        assert out["spec"]["architecture_designs"][0]["global_description"]
        assert "projection" not in out
    assert src == snapshot


# ---------------------------------------------------------------------------
# ts_cfdf1aa3 — summary emits one summarized arch set, no repeated full bodies
# ---------------------------------------------------------------------------


def test_summary_dedups_architecture_bodies_across_sections():
    src = _task_result_with_arch_md()
    snapshot = copy.deepcopy(src)
    out = project_task_context(src, card_id="c1", profile="summary")

    sections = [
        out["card"]["architecture_designs"][0],
        out["spec"]["architecture_designs"][0],
        *out["resolved_references"]["architecture_designs"],
    ]
    for sec in sections:
        # No full architecture body repeated anywhere under summary.
        assert "entities" not in sec
        assert "interfaces" not in sec
        assert "diagrams" not in sec
        assert "global_description" not in sec
        # Stable design ref: identifying fields + version + source_ref KEPT.
        assert sec["id"] in ("ad_card", "ad_spec")
        assert sec["parent_type"] in ("card", "spec")
        assert sec["version"] == 2
        assert sec["source_ref"].startswith("arch:")
        # Counts drilldown hint present.
        assert sec["counts"]["entities"] == 2
        assert sec["counts"]["interfaces"] == 1
        assert sec["counts"]["diagrams"] == 1
        assert sec["counts"]["has_global_description"] is True

    # deduped_count totals every body-carrying occurrence removed:
    # card(1) + spec(1) + 2 resolved arch(2) + markdown(1) + R2.1 kb/fr refs(2).
    assert out["projection"]["deduped_count"] >= 5
    # Architecture drilldown follow_up emitted once.
    assert {
        "rel": "read_full_architecture",
        "target_ref": "okto_pulse_get_task_context",
    } in out["projection"]["follow_up"]

    assert src == snapshot  # input untouched


def test_summary_does_not_reintroduce_architecture_when_excluded():
    # include_architecture=false upstream => sections empty/absent.
    src = _full_task_result()  # card/spec carry no architecture_designs key
    src["resolved_references"]["architecture_designs"] = []
    out = project_task_context(src, card_id="c1", profile="summary")

    assert "architecture_designs" not in out["card"]
    assert "architecture_designs" not in out["spec"]
    # Empty list never resurrected into bodies by the projection layer.
    assert not out["resolved_references"].get("architecture_designs")
    # No architecture drilldown when nothing was summarized.
    assert all(
        f["rel"] != "read_full_architecture" for f in out["projection"]["follow_up"]
    )
    assert out["projection"]["profile"] == "summary"


# ---------------------------------------------------------------------------
# ts_85b00d36 — spec context dedups architecture too (one shared path)
# ---------------------------------------------------------------------------


def test_spec_context_dedups_architecture_and_uses_r5_metadata():
    spec_result = {
        "id": "s1",
        "title": "Spec",
        "functional_requirements": [{"id": "fr_0", "text": "FR"}],
        "architecture_designs": [_arch_design("ad_spec", "spec", "s1")],
        "resolved_references": {
            "architecture_designs": [
                {**_arch_design("ad_spec", "spec", "s1"), "source_type": "spec"}
            ],
        },
        "decisions_markdown": "## Decisions\n- d1\n",
        "decisions": [{"id": "d1", "title": "decision"}],
    }
    snapshot = copy.deepcopy(spec_result)
    out = project_spec_context(spec_result, profile="summary")

    # Unique content kept; architecture bodies summarized in BOTH places.
    assert out["functional_requirements"][0]["text"] == "FR"
    assert "entities" not in out["architecture_designs"][0]
    assert out["architecture_designs"][0]["counts"]["entities"] == 2
    assert "global_description" not in out["resolved_references"]["architecture_designs"][0]
    # decisions_markdown gated at the top level; structured decisions kept.
    assert "decisions_markdown" not in out
    assert out["decisions"] == [{"id": "d1", "title": "decision"}]
    assert {
        "rel": "render_decisions_markdown",
        "target_ref": "spec:s1:decisions_markdown",
    } in out["projection"]["follow_up"]
    # Canonical R5 metadata.
    for key in ENVELOPE_METADATA_KEYS:
        assert key in out["projection"]
    assert out["projection"]["deduped_count"] >= 2

    assert spec_result == snapshot  # input untouched


# ---------------------------------------------------------------------------
# ts_cb1046ab — observability: usage + payload_bytes metrics (counts only)
# ---------------------------------------------------------------------------


def test_context_projection_emits_usage_and_bytes_metrics(caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="okto_pulse.mcp.context_projection"):
        project_task_context(_task_result_with_arch_md(), card_id="c1", profile="summary")
        project_task_context(_full_task_result(), card_id="c1", profile="full")

    messages = [r.getMessage() for r in caplog.records]
    # Emitted once per projected response — summary AND full/legacy passthrough.
    assert messages.count("mcp_context_projection_usage_total") == 2
    assert messages.count("mcp_context_projection_payload_bytes") == 2

    rec = next(
        r for r in caplog.records
        if r.getMessage() == "mcp_context_projection_usage_total"
    )
    labels = rec.context_projection
    assert isinstance(labels["payload_bytes"], int) and labels["payload_bytes"] > 0
    assert labels["profile"] in ("summary", "full")
    # Labels carry ONLY counts + identifiers — never a body field.
    assert set(labels) <= {
        "tool_name", "profile", "outcome",
        "payload_bytes", "omitted_count", "deduped_count", "truncated",
    }


def test_telemetry_sink_records_context_projection_fail_closed():
    from okto_pulse.core.mcp.payload_budget import (
        METRIC_CONTEXT_PROJECTION_BYTES,
        METRIC_CONTEXT_PROJECTION_USAGE,
        MCPProjectionTelemetrySink,
    )

    out = project_spec_context(
        {"id": "s1", "architecture_designs": [_arch_design("a", "spec", "s1")]},
        profile="summary",
    )
    sink = MCPProjectionTelemetrySink()
    res = sink.record_context_projection_envelope(
        out["projection"], tool_name="okto_pulse_get_spec_context"
    )
    assert res.accepted
    assert sink.metrics[METRIC_CONTEXT_PROJECTION_USAGE] == 1
    assert sink.metrics[METRIC_CONTEXT_PROJECTION_BYTES] == out["projection"]["payload_bytes"]

    # A forbidden label (a body field) is rejected fail-closed.
    bad = sink.record_context_projection({"profile": "summary", "title": "leaky body"})
    assert not bad.accepted


# ---------------------------------------------------------------------------
# Handler integration — get_spec_context (Codex ressalva: explicit handler test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_spec_context_default_summary_full_and_unsupported():
    from unittest.mock import AsyncMock, patch

    db_factory = get_session_factory()
    board_id = _id("ctxproj-spec-board")
    spec_id = _id("ctxproj-spec-spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Ctx Proj Spec", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                functional_requirements=[{"id": "fr_0", "text": "FR text"}],
            )
        )
        await db.commit()

    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        default = await _call("okto_pulse_get_spec_context", board_id=board_id, spec_id=spec_id)
        full = await _call(
            "okto_pulse_get_spec_context", board_id=board_id, spec_id=spec_id, profile="full"
        )
        bad = await _call(
            "okto_pulse_get_spec_context", board_id=board_id, spec_id=spec_id, profile="nope"
        )

    # default → summary with R5 canonical metadata + unique requirement content kept
    assert default["projection"]["profile"] == "summary"
    for key in ENVELOPE_METADATA_KEYS:
        assert key in default["projection"]
    assert default["functional_requirements"][0]["text"] == "FR text"

    # full preserves the prior payload shape (no projection envelope injected)
    assert "projection" not in full
    assert full["functional_requirements"][0]["text"] == "FR text"

    # unsupported profile → structured error, no silent fallback
    assert bad["error_code"] == "unsupported_projection"


# ---------------------------------------------------------------------------
# spec 9e0bf979 / b4e89fcc point 1 — projection must NOT omit the re-executable
# evidence fields, in summary OR full, so a validator sees them in context.
# ---------------------------------------------------------------------------

_NEW_EVIDENCE_FIELDS = (
    "evidence_class",
    "replay_command",
    "expected_output_snapshot",
    "non_replayable_justification",
)


def _result_with_evidence() -> dict:
    evidence = {
        "evidence_class": "replay_command",
        "replay_command": "pytest tests/test_x.py::test_y",
        "expected_output_snapshot": "1 passed",
        "non_replayable_justification": "n/a",
        "test_file_path": "tests/test_x.py",
        "test_function": "test_y",
        "last_run_at": "2026-06-19T00:00:00",
        "output_snippet": "1 passed",
    }
    scenario = {"id": "ts_a", "title": "my scenario", "status": "passed", "evidence": dict(evidence)}
    return {
        "card": {
            "id": "c1",
            "title": "X",
            "status": "in_progress",
            "card_type": "normal",
            "spec_id": "s1",
            "test_scenario_ids": ["ts_a"],
        },
        "spec": {
            "id": "s1",
            "title": "Spec",
            "status": "in_progress",
            "test_scenarios": [dict(scenario)],
        },
        "my_test_scenarios": [dict(scenario)],
    }


@pytest.mark.parametrize("profile", ["summary", "full"])
def test_projection_preserves_re_executable_evidence_fields(profile):
    projected = project_task_context(_result_with_evidence(), card_id="c1", profile=profile)

    scenarios = projected.get("my_test_scenarios") or []
    assert scenarios, f"my_test_scenarios dropped in {profile} projection"
    evidence = scenarios[0].get("evidence") or {}
    for field in _NEW_EVIDENCE_FIELDS:
        assert evidence.get(field), f"{field} omitted from my_test_scenarios evidence ({profile})"

    spec_scenarios = (projected.get("spec") or {}).get("test_scenarios") or []
    assert spec_scenarios, f"spec.test_scenarios dropped in {profile} projection"
    spec_evidence = spec_scenarios[0].get("evidence") or {}
    for field in _NEW_EVIDENCE_FIELDS:
        assert spec_evidence.get(field), f"{field} omitted from spec scenario evidence ({profile})"
