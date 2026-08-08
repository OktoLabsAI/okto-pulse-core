"""Authorization gates for KG operations orchestrated directly by MCP."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases.authorize_operation import (
    AuthorizeOperationUseCase,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.kg_power_tools import register_kg_power_tools


BOARD_ID = "board-inline-kg-operation"
_DENIAL = json.dumps({"error": "denied-before-effect"})


def _permission_set(*paths: str) -> PermissionSet:
    document: dict[str, Any] = {}
    for path in paths:
        cursor = document
        parts = path.split(".")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = True
    return PermissionSet(document)


@pytest.mark.asyncio
async def test_server_authorization_bridge_accepts_canonical_and_historical_authority() -> None:
    operation = "kg.operations.tick.run"
    legacy = "kg.admin.settings_write"
    canonical_actor = ActorContext(
        "canonical-operator",
        "mcp",
        board_id=BOARD_ID,
        permissions=_permission_set(operation, legacy),
    )
    historical_actor = ActorContext(
        "historical-operator",
        "mcp",
        board_id=BOARD_ID,
        permissions=[legacy],
    )

    assert (
        await server._authorize_kg_operation(
            canonical_actor,
            operation=operation,
            legacy_operation=legacy,
            board_id=BOARD_ID,
        )
        is None
    )
    assert (
        await server._authorize_kg_operation(
            historical_actor,
            operation=operation,
            legacy_operation=legacy,
            board_id=BOARD_ID,
        )
        is None
    )


@pytest.mark.asyncio
async def test_server_authorization_bridge_reports_the_canonical_denial() -> None:
    payload = json.loads(
        await server._authorize_kg_operation(
            ActorContext(
                "partial-operator",
                "mcp",
                board_id=BOARD_ID,
                permissions=_permission_set("kg.operations.tick.run"),
            ),
            operation="kg.operations.tick.run",
            legacy_operation="kg.admin.settings_write",
            board_id=BOARD_ID,
        )
        or "{}"
    )

    assert payload["error"] == "permission_denied"
    assert payload["required_permission"] == "kg.operations.tick.run"


_INLINE_OPERATION_CASES = (
    (
        "okto_pulse_kg_takedown_status",
        {"board_id": BOARD_ID, "delete_event_id": "delete-event-1"},
        "kg.operations.audit.read",
        "kg.admin.settings_read",
        BOARD_ID,
    ),
    (
        "okto_pulse_kg_orphan_report",
        {"board_id": BOARD_ID},
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
        BOARD_ID,
    ),
    (
        "okto_pulse_kg_orphan_backfill",
        {"board_id": BOARD_ID, "dry_run": False},
        "kg.operations.integrity.backfill",
        "kg.admin.settings_write",
        BOARD_ID,
    ),
    (
        "okto_pulse_kg_migrate_schema",
        {"board_id": BOARD_ID},
        "kg.operations.schema.migrate",
        "kg.admin.settings_write",
        BOARD_ID,
    ),
    (
        "okto_pulse_kg_tick_run_now",
        {"board_id": BOARD_ID},
        "kg.operations.tick.run",
        "kg.admin.settings_write",
        BOARD_ID,
    ),
    (
        "okto_pulse_kg_global_outbox_dead_letter_list",
        {},
        "kg.operations.global_outbox.read",
        "kg.admin.settings_read",
        None,
    ),
    (
        "okto_pulse_kg_global_outbox_dead_letter_reprocess",
        {"dead_letter_ids": ["dlq-1"], "reason": "operator_retry"},
        "kg.operations.global_outbox.reprocess",
        "kg.admin.settings_write",
        None,
    ),
    (
        "okto_pulse_kg_global_outbox_dead_letter_verify",
        {"dead_letter_ids": ["dlq-1"]},
        "kg.operations.global_outbox.verify",
        "kg.admin.settings_read",
        None,
    ),
    (
        "okto_pulse_kg_global_discovery_recovery_preflight",
        {},
        "kg.operations.global_recovery.preflight",
        "kg.admin.settings_read",
        None,
    ),
    (
        "okto_pulse_kg_global_discovery_recovery_confirm",
        {
            "run_id": "run-1",
            "manifest_ref": "manifest-1",
            "preflight_hash": "hash-1",
        },
        "kg.operations.global_recovery.confirm",
        "kg.admin.settings_write",
        None,
    ),
    (
        "okto_pulse_kg_global_discovery_recovery_status",
        {"run_id": "run-1"},
        "kg.operations.global_recovery.read",
        "kg.admin.settings_read",
        None,
    ),
    (
        "okto_pulse_kg_global_discovery_recovery_cancel",
        {"run_id": "run-1", "expected_epoch": 1},
        "kg.operations.global_recovery.cancel",
        "kg.admin.settings_write",
        None,
    ),
    (
        "okto_pulse_kg_global_discovery_recovery_resume",
        {"run_id": "run-1", "expected_epoch": 1},
        "kg.operations.global_recovery.resume",
        "kg.admin.settings_write",
        None,
    ),
    (
        "okto_pulse_kg_global_discovery_recovery_run",
        {
            "confirmation_id": "confirmation-1",
            "manifest_ref": "manifest-1",
            "preflight_hash": "hash-1",
            "reason": "operator recovery",
        },
        "kg.operations.global_recovery.run",
        "kg.admin.settings_write",
        None,
    ),
    (
        "okto_pulse_kg_rebuild_confirm",
        {
            "board_id": BOARD_ID,
            "operation": "rebuild",
            "preflight_hash": "0" * 64,
            "manifest_ref": "manifest-1",
        },
        "kg.operations.rebuild.confirm",
        "kg.admin.settings_write",
        BOARD_ID,
    ),
    (
        "okto_pulse_kg_quarantine_restore",
        {"quarantine_id": "quarantine-1", "apply": True},
        "kg.operations.quarantine.restore",
        "kg.admin.settings_write",
        None,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "kwargs", "operation", "legacy", "board_id"),
    _INLINE_OPERATION_CASES,
)
async def test_inline_kg_operations_deny_before_adapter_owned_effects(
    tool_name: str,
    kwargs: dict[str, Any],
    operation: str,
    legacy: str,
    board_id: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, str, str | None]] = []

    async def _board_context(requested_board_id: str):
        return server.AgentContext(
            "inline-operator",
            "Inline Operator",
            requested_board_id,
            ["board.read"],
        )

    async def _global_context():
        return server.AgentContext(
            "inline-operator",
            "Inline Operator",
            "",
            [],
        )

    async def _deny(
        _actor: ActorContext,
        *,
        operation: str,
        legacy_operation: str,
        board_id: str | None = None,
    ) -> str:
        captured.append((operation, legacy_operation, board_id))
        return _DENIAL

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(server, "_authorize_kg_operation", _deny)

    tool = getattr(server, tool_name)
    assert await tool.fn(**kwargs) == _DENIAL
    assert captured == [(operation, legacy, board_id)]


@pytest.mark.asyncio
async def test_provenance_drift_denies_before_graph_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Mcp:
        def __init__(self) -> None:
            self.tools: dict[str, Any] = {}

        def tool(self):
            def _register(function):
                self.tools[function.__name__] = function
                return function

            return _register

    principal = SimpleNamespace(
        id="provenance-reader",
        agent_id="provenance-reader",
        agent_name="Provenance Reader",
        realm_id=None,
        permissions=["board.read"],
    )

    async def _agent():
        return principal

    async def _board_agent(_board_id: str):
        return principal

    captured = []

    async def _deny(_self, command, **_kwargs):
        captured.append(command)
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(AuthorizeOperationUseCase, "execute", _deny)
    mcp = _Mcp()
    register_kg_power_tools(
        mcp,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    payload = json.loads(
        await mcp.tools["okto_pulse_kg_provenance_drift"](BOARD_ID)
    )

    assert payload["error"]["code"] == "permission_denied"
    assert payload["error"]["required_permission"] == "kg.operations.audit.read"
    assert captured[0].operation == "kg.operations.audit.read"
    assert captured[0].legacy_operation == "kg.admin.settings_read"
    assert captured[0].board_id == BOARD_ID
