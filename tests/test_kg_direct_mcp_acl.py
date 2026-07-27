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


BOARD_ID = "board-direct-kg-acl"


def _context(permissions) -> SimpleNamespace:
    return SimpleNamespace(
        agent_id="agent-direct-kg-acl",
        agent_name="Direct KG ACL",
        board_id=BOARD_ID,
        realm_id="realm-direct-kg-acl",
        permissions=permissions,
    )


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
        ["board:read"],
        PermissionSet({"board": {"read": True}}),
    ),
    ids=("legacy-board-read", "canonical-effective-board-read"),
)
async def test_orphan_report_accepts_legacy_and_canonical_board_read(
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
    ("okto_pulse_kg_originates_from_contract_audit", {}),
    (
        "okto_pulse_kg_takedown_status",
        {"delete_event_id": "delete-event-authorized"},
    ),
    ("okto_pulse_kg_queue_drilldown", {}),
    ("okto_pulse_kg_connectivity_dlq_diagnose", {}),
    ("okto_pulse_kg_connectivity_dlq_verify", {}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permissions",
    (
        ["board:read"],
        PermissionSet({"board": {"read": True}}),
    ),
    ids=("legacy-board-read", "canonical-effective-board-read"),
)
@pytest.mark.parametrize(
    ("tool_name", "kwargs"),
    CANONICALIZED_EXISTING_READ_CASES,
)
async def test_existing_direct_reads_accept_legacy_and_canonical_board_read(
    monkeypatch: pytest.MonkeyPatch,
    permissions,
    tool_name: str,
    kwargs: dict[str, object],
) -> None:
    from okto_pulse.core.mcp import server

    class _ReachedAuthorizedIo(RuntimeError):
        pass

    async def _board_context(_board_id: str):
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
async def test_orphan_backfill_acl_is_read_for_dry_run_and_admin_for_apply(
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

    denied = await _tool(server, "okto_pulse_kg_orphan_backfill")(
        board_id=BOARD_ID,
        dry_run=False,
    )
    assert "kg.admin.historical_consolidation" in denied
    assert events == []

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
                {"kg": {"admin": {"historical_consolidation": False}}}
            )
        )

    def _lease_provider():
        calls["lease"] += 1
        raise AssertionError("permission-denied tick resolved the lease provider")

    monkeypatch.setattr(server, "_get_authenticated_agent", _raw_agent)
    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(coordination, "get_lease_provider", _lease_provider)

    raw = await _tool(server, "okto_pulse_kg_tick_run_now")()

    assert "kg.admin.historical_consolidation" in raw
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
        return _context(["kg.admin.historical_consolidation"])

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
                {"kg": {"admin": {"historical_consolidation": False}}}
            )
        )

    def _lease_provider():
        raise AssertionError("board permission denial resolved lease provider")

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(coordination, "get_lease_provider", _lease_provider)

    raw = await _tool(server, "okto_pulse_kg_tick_run_now")(
        board_id=BOARD_ID
    )

    assert "kg.admin.historical_consolidation" in raw


REBUILD_CASES = (
    ("okto_pulse_kg_rebuild_preflight", {}),
    (
        "okto_pulse_kg_rebuild_confirm",
        {
            "operation": "rebuild",
            "preflight_hash": "invalid-is-never-validated",
            "manifest_ref": "manifest-never-loaded",
        },
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
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("tool_name", "kwargs"), REBUILD_CASES)
async def test_rebuild_family_requires_wipe_board_before_uow_or_provider(
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    kwargs: dict[str, object],
) -> None:
    from okto_pulse.core.mcp import server

    async def _board_context(_board_id: str):
        return _context(
            PermissionSet({"kg": {"admin": {"wipe_board": False}}})
        )

    def _forbidden_uow():
        raise AssertionError("permission-denied rebuild opened a UnitOfWork")

    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", _forbidden_uow)

    raw = await _tool(server, tool_name)(board_id=BOARD_ID, **kwargs)

    assert "kg.admin.wipe_board" in raw


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
        return _context(PermissionSet({"kg": {"admin": {"wipe_board": True}}}))

    async def _board_context(board_id: str):
        events.append(f"board_acl:{board_id}")
        return _context(PermissionSet({"kg": {"admin": {"wipe_board": False}}}))

    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(server, "_get_agent_ctx", _board_context)
    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: _Registry())

    raw = await _tool(server, "okto_pulse_kg_quarantine_restore")(
        quarantine_id="quarantine-direct-acl",
        apply=False,
    )

    assert "kg.admin.wipe_board" in raw
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
        return _context(PermissionSet({"kg": {"admin": {"wipe_board": False}}}))

    def _provider():
        raise AssertionError("global permission denial resolved restore provider")

    monkeypatch.setattr(server, "_get_global_agent_ctx", _global_context)
    monkeypatch.setattr(interfaces, "get_kg_registry", _provider)

    raw = await _tool(server, "okto_pulse_kg_quarantine_restore")(
        quarantine_id="quarantine-never-resolved",
    )

    assert "kg.admin.wipe_board" in raw


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
