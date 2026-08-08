from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.kg.guarded_write import GuardedWriteError
from okto_pulse.core.kg.orphan_integrity import (
    OrphanBackfillReconciler,
    OrphanBackfillResult,
)
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    get_barrier_mode,
    get_unguarded_count,
    require_write_token,
    reset_unguarded_counter,
    set_barrier_mode,
    under_safe_write,
)
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
    ("okto_pulse_kg_orphan_report", {}),
    ("okto_pulse_kg_dead_letter_list", {}),
    ("okto_pulse_kg_queue_drilldown", {}),
    ("okto_pulse_kg_connectivity_dlq_diagnose", {}),
    ("okto_pulse_kg_connectivity_dlq_verify", {}),
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permissions",
    (
        ["board:read", "kg.admin.settings_read"],
        _permission_set(
            "board.read",
            "kg.operations.integrity.read",
            "kg.admin.settings_read",
        ),
    ),
    ids=("legacy-settings-read", "canonical-integrity-read"),
)
async def test_orphan_report_accepts_legacy_and_canonical_integrity_read(
    monkeypatch: pytest.MonkeyPatch,
    permissions,
) -> None:
    from okto_pulse.core.kg import orphan_integrity
    from okto_pulse.core.mcp import server

    async def _board_context(_board_id: str):
        return _context(permissions)

    class _Report:
        correlation_id = "scan-authorized"

        def to_safe_dict(self):
            return {
                "board_id": BOARD_ID,
                "generation_id": None,
                "orphan_count": 0,
                "orphan_count_by_type": {},
                "orphan_count_by_writer_path": {},
                "samples": [],
                "unresolved_reasons": {},
                "allowlisted_root_count": 0,
                "correlation_id": self.correlation_id,
            }

    class _Scanner:
        def scan(self, **_kwargs):
            return _Report()

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(orphan_integrity, "OrphanNodeScanner", _Scanner)

    payload = json.loads(
        await _tool(server, "okto_pulse_kg_orphan_report")(board_id=BOARD_ID)
    )

    assert payload["board_id"] == BOARD_ID
    assert payload["correlation_id"] == "scan-authorized"


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
    ("okto_pulse_kg_queue_drilldown", {}, "kg.operations.queue.read"),
    (
        "okto_pulse_kg_connectivity_dlq_diagnose",
        {},
        "kg.operations.queue.read",
    ),
    (
        "okto_pulse_kg_connectivity_dlq_verify",
        {},
        "kg.operations.queue.read",
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


@pytest.mark.asyncio
async def test_orphan_backfill_requires_exact_capability_for_dry_run_and_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write, orphan_integrity
    from okto_pulse.core.mcp import server

    events: list[str] = []
    current_permissions = ["board:read"]

    async def _board_context(_board_id: str):
        return _context(current_permissions)

    async def _health(_board_id: str):
        events.append("health")
        return {"error": "test_stop"}

    def _forbidden_guard(*_args, **_kwargs):
        raise AssertionError("permission-denied apply acquired a writer lock")

    class _ForbiddenReconciler:
        def __init__(self, *_args, **_kwargs) -> None:
            raise AssertionError("permission-denied apply resolved a graph provider")

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(server, "_kg_orphan_backfill_health_refusal", _health)
    monkeypatch.setattr(guarded_write, "guarded_board_write", _forbidden_guard)
    monkeypatch.setattr(
        orphan_integrity,
        "OrphanBackfillReconciler",
        _ForbiddenReconciler,
    )

    denied_apply = await _tool(server, "okto_pulse_kg_orphan_backfill")(
        board_id=BOARD_ID, dry_run=False
    )
    denied_dry_run = await _tool(server, "okto_pulse_kg_orphan_backfill")(
        board_id=BOARD_ID, dry_run=True
    )
    assert "kg.operations.integrity.backfill" in denied_apply
    assert "kg.operations.integrity.backfill" in denied_dry_run
    assert events == []

    current_permissions = _permission_set(
        "kg.operations.integrity.backfill",
        "kg.admin.settings_write",
    )
    dry_run = json.loads(
        await _tool(server, "okto_pulse_kg_orphan_backfill")(
            board_id=BOARD_ID,
            dry_run=True,
        )
    )
    assert dry_run == {"error": "test_stop"}
    assert events == ["health"]


@pytest.mark.asyncio
async def test_global_tick_requires_global_effective_admin_before_lease_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server
    from okto_pulse.core.ports import coordination

    calls = {"raw": 0, "global": 0, "lease": 0}

    async def _raw_agent():
        calls["raw"] += 1
        return SimpleNamespace(id="raw-agent", permissions=None)

    async def _global_context():
        calls["global"] += 1
        return _context(
            PermissionSet(
                {
                    "kg": {
                        "operations": {"tick": {"run": True}},
                        "admin": {"settings_write": False},
                    }
                }
            )
        )

    def _lease_provider():
        calls["lease"] += 1
        raise AssertionError("permission-denied tick resolved the lease provider")

    monkeypatch.setattr(server, "_get_authenticated_agent", _raw_agent)
    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(coordination, "get_lease_provider", _lease_provider)

    raw = await _tool(server, "okto_pulse_kg_tick_run_now")()

    assert "kg.operations.tick.run" in raw
    assert "kg.admin.settings_write" in raw
    assert calls == {"raw": 0, "global": 1, "lease": 0}


@pytest.mark.asyncio
async def test_global_tick_accepts_explicit_admin_and_reaches_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server
    from okto_pulse.core.ports import coordination

    class _LeaseProvider:
        async def try_acquire(self, *_args, **_kwargs):
            return None

    async def _global_context():
        return _context(
            _permission_set(
                "kg.operations.tick.run",
                "kg.admin.settings_write",
            )
        )

    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(coordination, "get_lease_provider", _LeaseProvider)

    payload = json.loads(await _tool(server, "okto_pulse_kg_tick_run_now")())

    assert payload["error"] == "tick_already_running"


@pytest.mark.asyncio
async def test_board_tick_honors_effective_admin_override_before_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server
    from okto_pulse.core.ports import coordination

    async def _board_context(_board_id: str):
        return _context(
            PermissionSet(
                {
                    "kg": {
                        "operations": {"tick": {"run": True}},
                        "admin": {"settings_write": False},
                    }
                }
            )
        )

    def _lease_provider():
        raise AssertionError("board permission denial resolved lease provider")

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(coordination, "get_lease_provider", _lease_provider)

    raw = await _tool(server, "okto_pulse_kg_tick_run_now")(board_id=BOARD_ID)

    assert "kg.operations.tick.run" in raw
    assert "kg.admin.settings_write" in raw


REBUILD_CASES = (
    (
        "okto_pulse_kg_rebuild_preflight",
        {},
        "kg.operations.rebuild.preflight",
        "kg.admin.settings_read",
        True,
    ),
    (
        "okto_pulse_kg_rebuild_confirm",
        {
            "operation": "rebuild",
            "preflight_hash": "invalid-is-never-validated",
            "manifest_ref": "manifest-never-loaded",
        },
        "kg.operations.rebuild.confirm",
        "kg.admin.settings_write",
        False,
    ),
    (
        "okto_pulse_kg_rebuild_run",
        {
            "confirmation_id": "confirmation-never-consumed",
            "operation": "rebuild",
            "preflight_hash": "hash-never-checked",
            "manifest_ref": "manifest-never-loaded",
            "reason": "denied before provider access",
        },
        "kg.operations.rebuild.run",
        "kg.admin.settings_write",
        True,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "kwargs", "operation", "historical_authority", "opens_uow"),
    REBUILD_CASES,
)
async def test_rebuild_family_denies_exact_capability_before_service_access(
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    kwargs: dict[str, object],
    operation: str,
    historical_authority: str,
    opens_uow: bool,
) -> None:
    from okto_pulse.core.mcp import server

    events: list[str] = []

    async def _board_context(_board_id: str):
        return _context(_permission_set(historical_authority))

    class _DeniedUow:
        @property
        def services(self):
            raise AssertionError("permission-denied rebuild touched UoW services")

    class _UowContext:
        async def __aenter__(self):
            events.append("uow_enter")
            return _DeniedUow()

        async def __aexit__(self, *_args):
            events.append("uow_exit")

    def _uow_factory(**_kwargs):
        return _UowContext()

    def _uow_provider():
        return _uow_factory

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", _uow_provider)
    monkeypatch.setattr(server, "get_scheduler_control_for_mcp", lambda: None)

    raw = await _tool(server, tool_name)(board_id=BOARD_ID, **kwargs)

    assert operation in raw
    assert events == (["uow_enter", "uow_exit"] if opens_uow else [])


@pytest.mark.asyncio
async def test_quarantine_restore_checks_global_then_resolved_board_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import interfaces
    from okto_pulse.core.mcp import server

    events: list[str] = []

    class _Plan:
        board_id = BOARD_ID

        def to_payload(self):
            return {"board_id": BOARD_ID, "board_dir": "must-not-leak"}

    class _Restore:
        def plan(self, quarantine_id: str):
            events.append(f"plan:{quarantine_id}")
            return _Plan()

        def apply(self, _quarantine_id: str):
            raise AssertionError("board override denial applied the restore")

    class _Registry:
        def require_quarantine_restore(self):
            events.append("provider")
            return _Restore()

    async def _global_context():
        events.append("global_acl")
        return _context(
            _permission_set(
                "kg.operations.quarantine.restore",
                "kg.admin.settings_write",
            )
        )

    async def _board_context(board_id: str):
        events.append(f"board_acl:{board_id}")
        return _context(
            PermissionSet(
                {
                    "kg": {
                        "operations": {"quarantine": {"restore": True}},
                        "admin": {"settings_write": False},
                    }
                }
            )
        )

    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: _Registry())

    raw = await _tool(server, "okto_pulse_kg_quarantine_restore")(
        quarantine_id="quarantine-direct-acl",
        apply=False,
    )

    assert "kg.operations.quarantine.restore" in raw
    assert "kg.admin.settings_write" in raw
    assert "must-not-leak" not in raw
    assert events == [
        "global_acl",
        "provider",
        "plan:quarantine-direct-acl",
        f"board_acl:{BOARD_ID}",
    ]


@pytest.mark.asyncio
async def test_quarantine_restore_global_denial_does_not_resolve_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import interfaces
    from okto_pulse.core.mcp import server

    async def _global_context():
        return _context(_permission_set("kg.admin.settings_write"))

    def _provider():
        raise AssertionError("global permission denial resolved restore provider")

    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(interfaces, "get_kg_registry", _provider)

    raw = await _tool(server, "okto_pulse_kg_quarantine_restore")(
        quarantine_id="quarantine-never-resolved",
    )

    assert "kg.operations.quarantine.restore" in raw


class _OneEdgeReconciler(OrphanBackfillReconciler):
    def _candidate_rows(self, *_args, **_kwargs):
        from okto_pulse.core.kg.orphan_integrity import _NodeRow

        return (
            _NodeRow(
                node_id="requirement-orphan",
                node_type="Requirement",
                source_artifact_ref="spec:parent:fr:0",
                writer_path="historical",
            ),
        )


class _EdgeScope:
    def __init__(self, board_id: str, events: list[str]) -> None:
        self.board_id = board_id
        self.events = events

    def edge_exists(self, *_args, **_kwargs) -> bool:
        return False

    def execute(self, *_args, **_kwargs):
        from okto_pulse.core.kg.interfaces.graph_transaction import (
            GraphStatementResult,
        )

        return GraphStatementResult()

    def create_node(self, *_args, **_kwargs) -> None:
        raise AssertionError("test only expects an edge write")

    def update_node(self, *_args, **_kwargs) -> None:
        raise AssertionError("test only expects an edge write")

    def mark_superseded(self, *_args, **_kwargs) -> None:
        raise AssertionError("test only expects an edge write")

    def create_edge(self, *_args, **_kwargs) -> bool:
        require_write_token(self.board_id)
        self.events.append("edge_write")
        return True

    def reconcile_spec_lineage_parent(self, *_args, **_kwargs):
        raise AssertionError("test does not use Spec lineage")

    def compensate_spec_lineage_parent(self, *_args, **_kwargs) -> None:
        raise AssertionError("test does not use Spec lineage")

    def clear_spec_lineage_parent(self, *_args, **_kwargs):
        raise AssertionError("test does not use Spec lineage")

    def find_node_types(self, _node_id: str):
        return ()

    def delete_edges_by_session(self, *_args, **_kwargs) -> None:
        return None

    def delete_edges_by_session_preserving_spec_lineage(
        self, *_args, **_kwargs
    ) -> None:
        return None

    def delete_nodes_by_session(self, *_args, **_kwargs):
        return ()

    def increment_attestation(self, *_args, **_kwargs) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None


class _Lease:
    def __init__(
        self,
        events: list[str],
        *,
        lifecycle_error: GuardedWriteError | None = None,
    ) -> None:
        self.events = events
        self.lifecycle_error = lifecycle_error

    def ensure_durable(self, **_kwargs) -> None:
        self.events.append("durability")
        if self.lifecycle_error is not None:
            raise self.lifecycle_error


@contextmanager
def _strict_barrier():
    previous = get_barrier_mode()
    reset_unguarded_counter()
    set_barrier_mode(BarrierMode.STRICT)
    try:
        yield
    finally:
        set_barrier_mode(previous)
        reset_unguarded_counter()


def _guard_factory(events: list[str], lifecycle_error=None):
    @contextmanager
    def _guard(board_id: str, *, operation: str, **_kwargs):
        events.append("guard_enter")
        with under_safe_write(board_id, "orphan-writer-token", operation):
            yield _Lease(events, lifecycle_error=lifecycle_error)
        events.append("guard_exit")

    return _guard


def test_orphan_apply_has_one_outer_guard_and_zero_strict_violations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write, orphan_integrity

    events: list[str] = []
    monkeypatch.setattr(
        guarded_write,
        "guarded_board_write",
        _guard_factory(events),
    )
    monkeypatch.setattr(orphan_integrity, "_node_degree", lambda *_args: 0)
    monkeypatch.setattr(
        orphan_integrity,
        "_resolve_entity_target",
        lambda *_args: ("entity-parent",),
    )
    monkeypatch.setattr(orphan_integrity, "_edge_count", lambda *_args: 0)

    with _strict_barrier():
        result = _OneEdgeReconciler().run(
            board_id=BOARD_ID,
            connection=_EdgeScope(BOARD_ID, events),
        )
        assert get_unguarded_count(BOARD_ID) == 0

    assert result.connected == 1
    assert events == [
        "guard_enter",
        "edge_write",
        "durability",
        "guard_exit",
    ]


def test_orphan_dry_run_does_not_acquire_writer_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write, orphan_integrity

    events: list[str] = []

    def _forbidden_guard(*_args, **_kwargs):
        raise AssertionError("dry-run acquired the writer guard")

    monkeypatch.setattr(guarded_write, "guarded_board_write", _forbidden_guard)
    monkeypatch.setattr(orphan_integrity, "_node_degree", lambda *_args: 0)
    monkeypatch.setattr(
        orphan_integrity,
        "_resolve_entity_target",
        lambda *_args: ("entity-parent",),
    )
    monkeypatch.setattr(orphan_integrity, "_edge_count", lambda *_args: 0)

    result = _OneEdgeReconciler().run(
        board_id=BOARD_ID,
        dry_run=True,
        connection=_EdgeScope(BOARD_ID, events),
    )

    assert result.connected == 1
    assert events == []


def test_orphan_partial_failure_runs_durability_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write

    events: list[str] = []

    class _PartialFailureReconciler(OrphanBackfillReconciler):
        def _run_with_connection(self, *_args, **_kwargs):
            require_write_token(BOARD_ID)
            events.append("partial_write")
            raise RuntimeError("later row failed")

    monkeypatch.setattr(
        guarded_write,
        "guarded_board_write",
        _guard_factory(events),
    )

    with _strict_barrier(), pytest.raises(RuntimeError, match="later row failed"):
        _PartialFailureReconciler().run(
            board_id=BOARD_ID,
            connection=object(),
        )

    assert events == ["guard_enter", "partial_write", "durability"]


def test_orphan_lifecycle_failure_never_returns_success_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import guarded_write

    events: list[str] = []
    lifecycle_error = GuardedWriteError(
        "safe_lifecycle_failed",
        "forced lifecycle failure",
        retryable=True,
    )

    class _SuccessfulBody(OrphanBackfillReconciler):
        def _run_with_connection(self, *_args, **_kwargs):
            return OrphanBackfillResult(
                detected=0,
                connected=0,
                noop=0,
                unresolved=0,
                ambiguous=0,
                semantic_pending=0,
                samples=(),
                correlation_id="body-would-have-succeeded",
            )

    monkeypatch.setattr(
        guarded_write,
        "guarded_board_write",
        _guard_factory(events, lifecycle_error),
    )

    with pytest.raises(GuardedWriteError) as caught:
        _SuccessfulBody().run(board_id=BOARD_ID, connection=object())

    assert caught.value.code == "safe_lifecycle_failed"
    assert events == ["guard_enter", "durability"]
