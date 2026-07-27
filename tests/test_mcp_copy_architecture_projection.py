"""R2.3 — compact copy_architecture_to_card default response.

Covers spec ``MCP High-Frequency Response Projection and Dedup`` card R2.3
(FR9 / api_c163fb7d):

- ``ts_a7e8204e`` — default copy response is copy metadata + ids, no bodies;
  full/legacy preserve the prior bodies; invalid profile errors structurally.
- ``ts_cb1046ab`` — projected response uses the R5 canonical metadata names.

The copy tools support a 3-value profile set (``summary``/``full``/``legacy``) —
``detail`` is intentionally NOT supported here.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import logging
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.copy_projection import (
    COPY_SUPPORTED_PROFILES,
    project_copy_architecture_response,
    resolve_copy_profile,
)
from okto_pulse.core.mcp.projection_envelope import (
    ENVELOPE_METADATA_KEYS,
    _stable_payload_bytes,
)
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)

USER_ID = "copy-arch-agent"


def _full_design(did: str) -> dict:
    """A fully-serialized Architecture Design with the heavy bodies the summary
    response must NOT echo."""
    return {
        "id": did,
        "title": f"Arch {did}",
        "parent_type": "card",
        "parent_id": "card-1",
        "version": 1,
        "source_design_id": f"src_{did}",
        "source_ref": f"arch:{did}",
        "global_description": "HUGE GLOBAL DESCRIPTION " * 50,
        "entities": [{"name": "Entity", "responsibility": "R " * 40}],
        "interfaces": [{"name": "REST", "contract": "C " * 40}],
        "diagrams": [{"id": "d1", "adapter_payload": {"elements": ["x"] * 200}}],
    }


# ---------------------------------------------------------------------------
# Unit — project_copy_architecture_response (pure, no DB)
# ---------------------------------------------------------------------------


def test_summary_returns_metadata_and_ids_without_bodies():
    designs = [_full_design("ad1"), _full_design("ad2")]
    out = project_copy_architecture_response(designs, total_on_card=2, profile="summary")

    assert out["copied"] == 2
    assert out["design_ids"] == ["ad1", "ad2"]
    assert out["total_on_card"] == 2
    assert out["success"] is True
    # No architecture bodies echoed under summary.
    assert "architecture_designs" not in out

    meta = out["projection"]
    for key in ENVELOPE_METADATA_KEYS:
        assert key in meta
    assert meta["profile"] == "summary"
    assert meta["outcome"] == "ok"
    assert meta["truncated"] is False
    assert meta["deduped_count"] == 2
    assert meta["follow_up"] == [
        {"rel": "read_full_architecture", "target_ref": "okto_pulse_copy_architecture_to_card"}
    ]


def test_summary_is_the_default_profile():
    for profile in (None, "", "summary"):
        out = project_copy_architecture_response(
            [_full_design("a")], total_on_card=1, profile=profile
        )
        assert out["projection"]["profile"] == "summary"
        assert "architecture_designs" not in out


def test_summary_payload_far_smaller_than_full():
    designs = [_full_design("ad1"), _full_design("ad2")]
    summary = project_copy_architecture_response(designs, total_on_card=2, profile="summary")
    full = project_copy_architecture_response(designs, total_on_card=2, profile="full")

    full_bytes = _stable_payload_bytes(full)
    summary_bytes = _stable_payload_bytes(summary)
    # The compacted summary is a small fraction of the full body echo.
    assert summary_bytes < full_bytes / 5
    # payload_bytes measures the projected (metadata-only) response.
    assert 0 < summary["projection"]["payload_bytes"] < full_bytes


def test_summary_payload_bytes_measures_final_projected_response():
    # R2.3: payload_bytes is the serialized size AFTER projection — the FINAL
    # response including the projection envelope, not just the bare body.
    designs = [_full_design("ad1"), _full_design("ad2")]
    out = project_copy_architecture_response(designs, total_on_card=2, profile="summary")
    assert out["projection"]["payload_bytes"] == _stable_payload_bytes(out)
    # ...and that genuinely accounts for the envelope (more than the bare body).
    bare = {k: v for k, v in out.items() if k != "projection"}
    assert out["projection"]["payload_bytes"] > _stable_payload_bytes(bare)


def test_full_and_legacy_preserve_prior_shape():
    designs = [_full_design("ad1")]
    for profile in ("full", "legacy"):
        out = project_copy_architecture_response(designs, total_on_card=1, profile=profile)
        assert out["success"] is True
        assert out["copied"] == 1
        assert out["architecture_designs"][0]["global_description"]  # full body kept
        assert out["architecture_designs"][0]["diagrams"]
        assert "projection" not in out  # no envelope injected (back-compat)


def test_unsupported_profile_returns_structured_error():
    for bad in ("detail", "verbose", "slim"):
        out = project_copy_architecture_response(
            [_full_design("a")], total_on_card=1, profile=bad
        )
        assert out["outcome"] == "error"
        assert out["error_code"] == "unsupported_projection"
        assert out["supported_profiles"] == ["summary", "full", "legacy"]
    # detail is explicitly NOT a supported copy profile (unlike the context tools).
    assert resolve_copy_profile("detail") is None
    assert resolve_copy_profile("summary") == "summary"
    assert list(COPY_SUPPORTED_PROFILES) == ["summary", "full", "legacy"]


def test_copy_architecture_emits_safe_metrics(caplog):
    with caplog.at_level(logging.INFO, logger="okto_pulse.mcp.copy_projection"):
        project_copy_architecture_response([_full_design("a")], total_on_card=1, profile="summary")
        project_copy_architecture_response([_full_design("a")], total_on_card=1, profile="full")

    messages = [r.getMessage() for r in caplog.records]
    assert messages.count("mcp_copy_architecture_response_total") == 2
    assert messages.count("mcp_copy_architecture_response_bytes") == 2

    rec = next(
        r for r in caplog.records
        if r.getMessage() == "mcp_copy_architecture_response_bytes"
    )
    labels = rec.copy_architecture
    assert isinstance(labels["payload_bytes"], int) and labels["payload_bytes"] > 0
    assert labels["copied"] == 1
    # Counts + identifiers only — never a body.
    assert set(labels) <= {"tool_name", "profile", "payload_bytes", "copied"}


# ---------------------------------------------------------------------------
# Handler integration — default summary, full passthrough, unsupported
# ---------------------------------------------------------------------------


def _id(p: str) -> str:
    return f"{p}-{uuid.uuid4()}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["board:read", "cards:update", "specs:update"],
        },
    )()


async def _call(name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


@pytest.mark.asyncio
async def test_copy_architecture_handler_summary_full_and_unsupported():
    db_factory = get_session_factory()
    board_id = _id("ca-board")
    spec_id = _id("ca-spec")
    card_a = _id("ca-card-a")
    card_b = _id("ca-card-b")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Copy Arch", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
            )
        )
        for cid in (card_a, card_b):
            db.add(
                Card(
                    id=cid,
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
        # Seed two architecture designs on the spec.
        for i in (1, 2):
            created = await _call(
                "okto_pulse_add_architecture_design",
                board_id=board_id,
                parent_type="spec",
                parent_id=spec_id,
                title=f"Arch {i}",
                global_description="A substantial architecture body " * 30,
            )
            assert created.get("success") is True, created

        # Default → summary (card_a); full → card_b (avoid double-copy ambiguity).
        default = await _call(
            "okto_pulse_copy_architecture_to_card",
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_a,
        )
        full = await _call(
            "okto_pulse_copy_architecture_to_card",
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_b,
            profile="full",
        )
        bad = await _call(
            "okto_pulse_copy_architecture_to_card",
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_a,
            profile="detail",  # not supported for the copy tool
        )

    # default summary: copy metadata only, no bodies.
    assert default["copied"] == 2
    assert len(default["design_ids"]) == 2
    assert default["total_on_card"] == 2
    assert "architecture_designs" not in default
    assert default["projection"]["profile"] == "summary"
    assert default["projection"]["deduped_count"] == 2
    for key in ENVELOPE_METADATA_KEYS:
        assert key in default["projection"]

    # full: prior payload shape with full bodies, no envelope injected.
    assert full["success"] is True
    assert full["copied"] == 2
    assert full["architecture_designs"][0]["global_description"]
    assert "projection" not in full

    # unsupported profile: structured error, no mutation, no silent fallback.
    assert bad["error_code"] == "unsupported_projection"
    assert bad["supported_profiles"] == ["summary", "full", "legacy"]


# ---------------------------------------------------------------------------
# Resource doc — the lazy tool-doc must reflect the R2.3 contract (anti-regression)
# ---------------------------------------------------------------------------


_ARCH_TOOL_DOC = (
    Path(__file__).parent.parent
    / "src" / "okto_pulse" / "core" / "mcp" / "resources"
    / "reference" / "tool-docs" / "architecture.md"
)


def test_copy_architecture_tool_doc_documents_profile_contract():
    text = _ARCH_TOOL_DOC.read_text(encoding="utf-8")
    # Isolate the copy_architecture_to_card section.
    marker = "## `okto_pulse_copy_architecture_to_card`"
    assert marker in text
    section = text.split(marker, 1)[1].split("\n## ", 1)[0]

    # The new contract must be documented...
    assert "profile" in section
    assert "summary" in section and "full" in section and "legacy" in section
    assert "total_on_card" in section
    assert "design_ids" in section
    assert "unsupported_projection" in section
    assert "supported_profiles=[summary, full, legacy]" in section
    # ...and the stale "returns full bodies by default" wording must be gone.
    assert "JSON with copied Architecture Designs." not in section
