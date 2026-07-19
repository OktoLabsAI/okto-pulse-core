"""KG-01.3.1 — Wiring tests for the write barrier.

Sub-card derived from KG-01.3 (val_e341620d). Validator approved a SOFT
default rollout with require_write_token wired in commit_consolidation.
This file proves the wiring in the remaining write paths:

* `core.application.kg_tick.reset_last_recomputed_at` (force_full_rebuild)
* `outbox_worker._apply_event` (discovery.lbug)
* `clustering.board_delete_cascade` (per-board + global cascade)

Each call site is exercised in STRICT mode without a guard active: the
test passes when the barrier raises `WriteLifecycleViolation` before any
storage mutation begins. This is a static-style proof (the barrier is
the first thing each path does) — full end-to-end execution with real
LadybugDB is out of scope for these tests.
"""

from __future__ import annotations

import inspect

import pytest

from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    GLOBAL_DISCOVERY_BOARD_SENTINEL,
    WriteLifecycleViolation,
    get_barrier_mode,
    require_global_write_token,
    require_write_token,
    set_barrier_mode,
    under_global_safe_write,
    under_safe_write,
)


@pytest.fixture(autouse=True)
def _force_strict_mode():
    prior = get_barrier_mode()
    set_barrier_mode(BarrierMode.STRICT)
    yield
    set_barrier_mode(prior)


# --- Static-source wiring proofs ----------------------------------------------


def test_kg_tick_reset_calls_require_write_token():
    from okto_pulse.core.application import kg_tick

    src = inspect.getsource(kg_tick.reset_last_recomputed_at)
    assert "require_write_token" in src
    assert "require_write_token(current_board_id)" in src


def test_outbox_worker_apply_event_calls_require_global_write_token():
    # _apply_event writes to the GLOBAL discovery meta-graph (discovery.lbug),
    # so it must hold the GLOBAL guard — not the per-board one. Commit 7fd84e0
    # wired require_global_write_token() here; the old assertion pinned the
    # per-board require_write_token(board_id) by copy-paste and went stale.
    # (The sibling global-write tests below already use the global token.)
    from okto_pulse.core.application.processors import global_outbox as outbox_worker

    src = inspect.getsource(outbox_worker.GlobalOutboxProcessor._apply_event)
    assert "require_global_write_token" in src
    assert "require_global_write_token()" in src


def test_board_delete_cascade_calls_require_write_token():
    from okto_pulse.core.kg.global_discovery import clustering

    src = inspect.getsource(clustering.board_delete_cascade)
    assert "require_write_token" in src
    assert "require_write_token(board_id)" in src


# --- STRICT-mode behavioural assertion ----------------------------------------


def test_strict_mode_blocks_unguarded_call_to_each_barrier_point():
    """The barrier itself MUST raise in STRICT mode when no guard is
    active. This is the invariant each wired call site relies on."""
    with pytest.raises(WriteLifecycleViolation):
        require_write_token("any-board")


def test_under_safe_write_unblocks_each_barrier_point():
    """Inside under_safe_write the guard is active and require_write_token
    returns the guard rather than raising. Each wired call site uses this
    path in production via KGSafeWriteLifecycle.apply."""
    with under_safe_write("any-board", "token-x", "consolidate"):
        guard = require_write_token("any-board")
    assert guard is not None
    assert guard.owner_token == "token-x"
    assert guard.operation == "consolidate"


# --- Global discovery barrier wiring (KG-01.3.1 rework val_441ad311) ---------


def test_bootstrap_global_discovery_helper_owns_durable_writer_lease(monkeypatch):
    """The shared fixture enters the same durable writer lane as production."""
    from pathlib import Path

    from okto_pulse.core.composition import RuntimeProviderMissing
    from okto_pulse.core.kg import interfaces as interfaces_pkg
    from okto_pulse.core.kg.global_discovery_writer import (
        assert_global_discovery_writer_fence,
    )
    import global_graph_testing as schema
    from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry
    from okto_pulse.core.kg.write_barrier import require_global_write_token

    calls: list[str] = []

    class _Runtime:
        def bootstrap(self) -> Path:
            assert_global_discovery_writer_fence()
            require_global_write_token()
            calls.append("bootstrap")
            return Path("global-discovery.lbug")

    reg = KGProviderRegistry(global_discovery_runtime=_Runtime())
    monkeypatch.setattr(interfaces_pkg, "get_kg_registry", lambda: reg, raising=True)
    assert schema.bootstrap_global_discovery() == Path("global-discovery.lbug")
    assert calls == ["bootstrap"]

    empty = KGProviderRegistry()
    monkeypatch.setattr(
        interfaces_pkg,
        "get_kg_registry",
        lambda: empty,
        raising=True,
    )
    with pytest.raises(RuntimeProviderMissing) as excinfo:
        schema.bootstrap_global_discovery()
    assert excinfo.value.provider_key == "global_discovery_runtime"
    assert calls == ["bootstrap"]  # the failed call never reached a runtime


def test_purge_global_discovery_helper_owns_durable_writer_lease(monkeypatch):
    """The test purge helper owns a durable lease and preserves its reason."""
    from okto_pulse.core.composition import RuntimeProviderMissing
    from okto_pulse.core.kg import interfaces as interfaces_pkg
    from okto_pulse.core.kg.global_discovery_writer import (
        assert_global_discovery_writer_fence,
    )
    import global_graph_testing as schema
    from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry
    from okto_pulse.core.kg.write_barrier import require_global_write_token

    calls: list[tuple[str, str]] = []

    class _Runtime:
        def purge(self, *, reason: str = "manual") -> list[str]:
            assert_global_discovery_writer_fence()
            require_global_write_token()
            calls.append(("purge", reason))
            return ["quarantined-a"]

    reg = KGProviderRegistry(global_discovery_runtime=_Runtime())
    monkeypatch.setattr(interfaces_pkg, "get_kg_registry", lambda: reg, raising=True)
    assert schema.purge_global_discovery_storage(reason="test") == ["quarantined-a"]
    assert calls == [("purge", "test")]

    empty = KGProviderRegistry()
    monkeypatch.setattr(
        interfaces_pkg,
        "get_kg_registry",
        lambda: empty,
        raising=True,
    )
    with pytest.raises(RuntimeProviderMissing) as excinfo:
        schema.purge_global_discovery_storage(reason="test")
    assert excinfo.value.provider_key == "global_discovery_runtime"
    assert calls == [("purge", "test")]


def test_execute_global_write_helper_owns_durable_writer_lease(monkeypatch):
    from okto_pulse.core.kg import interfaces as interfaces_pkg
    from okto_pulse.core.kg.global_discovery_writer import (
        assert_global_discovery_writer_fence,
    )
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
    from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry

    import global_graph_testing as schema

    calls: list[tuple[str, dict | None]] = []

    class _Runtime:
        def execute(self, statement: str, params=None) -> GraphStatementResult:
            assert_global_discovery_writer_fence()
            require_global_write_token()
            calls.append((statement, params))
            return GraphStatementResult.from_rows([[1]])

    reg = KGProviderRegistry(global_discovery_runtime=_Runtime())
    monkeypatch.setattr(interfaces_pkg, "get_kg_registry", lambda: reg, raising=True)

    result = schema.execute_global_write(
        "CREATE (:Fixture {id: $id})",
        {"id": "fixture-a"},
        operation="test_fixture_write",
    )

    assert result.rows == ((1,),)
    assert calls == [("CREATE (:Fixture {id: $id})", {"id": "fixture-a"})]


def test_gc_orphans_uses_shared_durable_writer_scope():
    from okto_pulse.core.kg.global_discovery import clustering

    src = inspect.getsource(clustering.gc_orphans)
    assert "global_discovery_writer_scope" in src
    assert "require_global_write_token()" not in src


def test_require_global_write_token_strict_raises_without_guard():
    with pytest.raises(WriteLifecycleViolation) as excinfo:
        require_global_write_token()
    assert excinfo.value.board_id == GLOBAL_DISCOVERY_BOARD_SENTINEL


def test_under_global_safe_write_unblocks_require_global():
    with under_global_safe_write("global-token", "bootstrap"):
        guard = require_global_write_token()
    assert guard is not None
    assert guard.board_id == GLOBAL_DISCOVERY_BOARD_SENTINEL
    assert guard.owner_token == "global-token"
    assert guard.operation == "bootstrap"


def test_per_board_guard_does_not_grant_global_access():
    """A per-board guard MUST NOT satisfy require_global_write_token —
    the global discovery write paths need an explicit global guard."""
    with under_safe_write("some-board", "tok-1", "consolidate"):
        with pytest.raises(WriteLifecycleViolation):
            require_global_write_token()


def test_global_guard_does_not_grant_board_access():
    """Conversely a global guard MUST NOT satisfy a per-board check."""
    with under_global_safe_write("global-token", "gc"):
        with pytest.raises(WriteLifecycleViolation):
            require_write_token("some-board")


def test_raw_global_discovery_bootstrap_blocked_without_durable_lease():
    """The adapter still fails closed when a caller bypasses the safe helper."""
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterFenceLost,
    )
    import global_graph_testing as schema

    with pytest.raises(GlobalDiscoveryWriterFenceLost):
        schema._runtime().bootstrap()


def test_raw_global_discovery_purge_blocked_without_durable_lease():
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterFenceLost,
    )
    import global_graph_testing as schema

    with pytest.raises(GlobalDiscoveryWriterFenceLost):
        schema._runtime().purge(reason="test")


@pytest.mark.parametrize(("dry_run", "expected_calls"), [(True, 2), (False, 4)])
def test_gc_orphans_acquires_its_own_durable_global_guard(
    monkeypatch,
    dry_run,
    expected_calls,
):
    from okto_pulse.core.kg import interfaces as interfaces_pkg
    from okto_pulse.core.kg.global_discovery import clustering
    from okto_pulse.core.kg.global_discovery_writer import (
        assert_global_discovery_writer_fence,
    )
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
    from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry

    calls: list[str] = []

    class _Runtime:
        def execute(self, statement: str, params=None) -> GraphStatementResult:
            del params
            assert_global_discovery_writer_fence()
            require_global_write_token()
            calls.append(statement)
            if "RETURN count(t)" in statement:
                return GraphStatementResult.from_rows([[2]])
            if "RETURN count(e)" in statement:
                return GraphStatementResult.from_rows([[3]])
            return GraphStatementResult()

    reg = KGProviderRegistry(global_discovery_runtime=_Runtime())
    monkeypatch.setattr(interfaces_pkg, "get_kg_registry", lambda: reg, raising=True)

    result = clustering.gc_orphans(dry_run=dry_run)

    assert result == {
        "topics_removed": 2,
        "entities_removed": 3,
        "dry_run": dry_run,
    }
    assert len(calls) == expected_calls
