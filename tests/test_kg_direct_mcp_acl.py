from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.mcp.kg_authorization import kg_permission_error


BOARD_ID = "board-direct-kg-acl"


def test_kg_permission_error_accepts_authenticated_mcp_wildcard() -> None:
    context = _context(["*"])

    assert kg_permission_error(context, "kg.operations.integrity.read") is None


def _context(permissions) -> SimpleNamespace:
    return SimpleNamespace(
        agent_id="agent-direct-kg-acl",
        agent_name="Direct KG ACL",
        board_id=BOARD_ID,
        realm_id="realm-direct-kg-acl",
        permissions=permissions,
    )


def _permission_set(*operations: str) -> PermissionSet:
    flags: dict[str, object] = {}
    for operation in operations:
        cursor = flags
        *parents, leaf = operation.split(".")
        for part in parents:
            child = cursor.setdefault(part, {})
            assert isinstance(child, dict)
            cursor = child
        cursor[leaf] = True
    return PermissionSet(flags)


def _tool(server, name: str):
    return getattr(server, name).fn


READ_CASES = (
    ("okto_pulse_kg_health", {}),
    ("okto_pulse_kg_health_readiness", {}),
    ("okto_pulse_kg_canonical_debt_list", {}),
    ("okto_pulse_kg_canonical_partition_integrity_list", {}),
    ("okto_pulse_kg_digest_layer_mismatch_list", {}),
    ("okto_pulse_kg_stale_canonical_parity_list", {}),
    ("okto_pulse_kg_originates_from_contract_audit", {}),
    ("okto_pulse_kg_takedown_status", {}),
    (
        "okto_pulse_kg_evaluate_bug_cognitive_closure",
        {"bug_id": "bug-direct-acl"},
    ),
    ("okto_pulse_kg_list_cognitive_readiness_items", {}),
    (
        "okto_pulse_kg_evaluate_cognitive_readiness",
        {"source_ref": "spec:spec-direct-acl"},
    ),
    ("okto_pulse_kg_list_cognitive_dlq", {}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("tool_name", "kwargs"), READ_CASES)
async def test_direct_kg_reads_deny_effective_board_override_before_io(
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    kwargs: dict[str, object],
) -> None:
    from okto_pulse.core.kg import orphan_integrity
    from okto_pulse.core.mcp import server

    events: list[str] = []

    async def _board_context(board_id: str):
        events.append(f"acl:{board_id}")
        return _context(PermissionSet({"board": {"read": False}}))

    def _forbidden_uow():
        raise AssertionError("permission-denied read opened a UnitOfWork")

    class _ForbiddenScanner:
        def __init__(self, *_args, **_kwargs) -> None:
            raise AssertionError("permission-denied read resolved the graph provider")

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", _forbidden_uow)
    monkeypatch.setattr(orphan_integrity, "OrphanNodeScanner", _ForbiddenScanner)

    raw = await _tool(server, tool_name)(board_id=BOARD_ID, **kwargs)

    assert "permission" in raw.lower()
    assert "board.read" in raw
    assert events == [f"acl:{BOARD_ID}"]


CANONICALIZED_EXISTING_READ_CASES = (
    (
        "okto_pulse_kg_originates_from_contract_audit",
        {},
        "kg.operations.audit.read",
    ),
    (
        "okto_pulse_kg_takedown_status",
        {"delete_event_id": "delete-event-authorized"},
        "kg.operations.audit.read",
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permission_form",
    ("legacy", "canonical"),
)
@pytest.mark.parametrize(
    ("tool_name", "kwargs", "operation"),
    CANONICALIZED_EXISTING_READ_CASES,
)
async def test_existing_direct_reads_accept_exact_operation_authority(
    monkeypatch: pytest.MonkeyPatch,
    permission_form: str,
    tool_name: str,
    kwargs: dict[str, object],
    operation: str,
) -> None:
    from okto_pulse.core.mcp import server

    class _ReachedAuthorizedIo(RuntimeError):
        pass

    async def _board_context(_board_id: str):
        permissions = (
            ["board:read", "kg.admin.settings_read"]
            if permission_form == "legacy"
            else _permission_set(
                "board.read",
                operation,
                "kg.admin.settings_read",
            )
        )
        return _context(permissions)

    def _authorized_uow_boundary():
        raise _ReachedAuthorizedIo

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        _authorized_uow_boundary,
    )

    with pytest.raises(_ReachedAuthorizedIo):
        await _tool(server, tool_name)(board_id=BOARD_ID, **kwargs)
