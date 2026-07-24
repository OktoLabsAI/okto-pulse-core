"""Regression coverage for pagination offsets at the SQLite int64 boundary."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.ports.application_persistence import PAGE_OFFSET_MAX


_OPERATIONAL_OFFSET_TOOLS = (
    "okto_pulse_kg_canonical_debt_list",
    "okto_pulse_kg_canonical_partition_integrity_list",
    "okto_pulse_kg_digest_layer_mismatch_list",
    "okto_pulse_kg_originates_from_contract_audit",
    "okto_pulse_kg_stale_canonical_parity_list",
    "okto_pulse_kg_list_cognitive_dlq",
    "okto_pulse_kg_dead_letter_list",
    "okto_pulse_list_architecture_propagation_legacy",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", _OPERATIONAL_OFFSET_TOOLS)
async def test_mcp_operational_lists_reject_offset_above_sqlite_int64(
    tool_name: str,
) -> None:
    ctx = SimpleNamespace(
        agent_id="offset-boundary-agent",
        agent_name="offset-boundary-agent",
        permissions=None,
    )
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(
            mcp_server,
            "_mcp_check_architecture_permission",
            return_value=None,
        ),
    ):
        tool = await mcp_server.mcp.get_tool(tool_name)
        payload = json.loads(
            await tool.fn(board_id="board-offset-boundary", offset=PAGE_OFFSET_MAX + 1)
        )

    assert payload["error"] == "invalid_pagination"


@pytest.mark.asyncio
async def test_dead_letter_service_uses_native_page_without_materializing_prefix() -> (
    None
):
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    row = SimpleNamespace(
        id="dlq-1",
        board_id="board-offset-boundary",
        artifact_type="spec",
        artifact_id="spec-1",
        original_queue_id="queue-1",
        attempts=3,
        errors=[{"message": "boom"}],
        dead_lettered_at=None,
    )
    page_reader = SimpleNamespace(
        list_dead_letter_page=AsyncMock(return_value=(123, [row]))
    )
    db = object()
    with patch(
        "okto_pulse.core.services.dead_letter_inspector_service."
        "get_kg_worker_queue_port",
        return_value=page_reader,
    ):
        result = await list_dead_letter_rows(
            db,
            "board-offset-boundary",
            limit=2,
            offset=100,
        )

    page_reader.list_dead_letter_page.assert_awaited_once_with(
        db,
        board_id="board-offset-boundary",
        limit=2,
        offset=100,
    )
    assert result["total"] == 123
    assert [item["id"] for item in result["rows"]] == ["dlq-1"]


@pytest.mark.asyncio
async def test_services_reject_offset_above_sqlite_int64_before_storage() -> None:
    from okto_pulse.core.services.architecture_propagation_legacy import (
        build_propagation_legacy_report,
    )
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    too_large = PAGE_OFFSET_MAX + 1
    with (
        patch(
            "okto_pulse.core.services.dead_letter_inspector_service."
            "get_kg_worker_queue_port"
        ) as dlq_reader,
        patch(
            "okto_pulse.core.services.architecture_propagation_legacy."
            "get_architecture_legacy_snapshot_read_port"
        ) as architecture_reader,
    ):
        with pytest.raises(ValueError, match="offset exceeds"):
            await list_dead_letter_rows(
                object(),
                "board-offset-boundary",
                offset=too_large,
            )
        with pytest.raises(ValueError, match="offset exceeds"):
            await build_propagation_legacy_report(
                object(),
                board_id="board-offset-boundary",
                offset=too_large,
            )

    dlq_reader.assert_not_called()
    architecture_reader.assert_not_called()
