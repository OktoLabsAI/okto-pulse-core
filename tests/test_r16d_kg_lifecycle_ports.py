"""R16-D — migrate the KG lifecycle in core/app.py onto the spec #06 ports.

Authoritative scenario mapping (1:1 with the spec/card titles):

  ts_8077c637 — AST/import gate: core/app.py no longer imports the 5 kg.schema
                lifecycle symbols and wires the #06 ports instead (tr_86e8567d).
  ts_6ce72610 — Sweep NC-10 via ports: existing board -> ensure_bootstrapped;
                missing board -> skip; soft-fail per board; resolver error
                propagates; count + migration_swept/_failed logs; off-loop
                offload (tr_94686e17 / tr_7ea5e6d8).
  ts_f967116f — Shutdown: GraphLifecycle.close(None) replaces
                close_all_connections; a KG-close failure is logged
                (kg.shutdown.close_connections_failed) and close_db() STILL runs
                (tr_5c727d9b).
  ts_a627315d — Registry conformance: the test registry provides in-memory
                graph_schema_manager / graph_lifecycle / graph_path_resolver that
                satisfy their runtime_checkable Protocols (ac_03d8a102).
  ts_960cc3bf — Regression #06/#03b green: the golden lifespan replay passes with
                no unexpected delta and the default-only exclusions are unchanged
                (tr_06adc038 / tr_ae79d1f2).
  ts_f83ad3db — Scope / register-before-remove: startup helpers and the core
                registry do not import kg.schema / embedded Kuzu runtime, while
                test-only graph fakes remain available.

Async port calls are driven via ``asyncio.run`` in sync tests (no pytest-asyncio
dependency).
"""

from __future__ import annotations

import ast
import asyncio
import threading
from pathlib import Path

import pytest

import okto_pulse.community.app as _app_mod
import okto_pulse.core.infra.startup_schema_sweep as _startup_sweep_mod
import okto_pulse.core.kg.startup_schema_sweep as _sweep_mod
from okto_pulse.community.app import shutdown_kg_then_db
from okto_pulse.core.kg.interfaces import (
    get_kg_registry,
    reset_registry_for_tests,
)
from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphLifecycle
from okto_pulse.core.kg.interfaces.graph_runtime_store import GraphRuntimeStore
from okto_pulse.core.kg.interfaces.graph_schema_manager import GraphSchemaManager
from okto_pulse.core.kg.startup_schema_sweep import sweep_board_schemas

APP_PY = Path(_app_mod.__file__)
SWEEP_PY = Path(_sweep_mod.__file__)

_FORBIDDEN_KG_SCHEMA_SYMBOLS = {
    "board_kuzu_path",
    "open_board_connection",
    "ensure_board_graph_bootstrapped",
    "migrate_schema_for_board",
    "close_all_connections",
}


# ---------------------------------------------------------------------------
# Fakes + helpers
# ---------------------------------------------------------------------------
class _FakeRuntimeStore:
    def __init__(self, existing, *, raise_on=None):
        self.existing = set(existing)
        self.raise_on = raise_on
        self.checked: list[str] = []

    def exists(self, board_id: str) -> bool:
        self.checked.append(board_id)
        if self.raise_on is not None and board_id == self.raise_on:
            raise RuntimeError(f"resolver_boom:{board_id}")
        return board_id in self.existing


class _FakeSchemaManager:
    def __init__(self, *, fail_on=()):
        self.fail_on = set(fail_on)
        self.ensured: list[str] = []
        self.threads: list[int] = []

    async def ensure_bootstrapped(self, board_id: str) -> None:
        self.threads.append(threading.get_ident())
        self.ensured.append(board_id)
        if board_id in self.fail_on:
            raise RuntimeError(f"ensure_boom:{board_id}")


class _RecLogger:
    """Minimal logger double capturing (level, event, extra)."""

    def __init__(self):
        self.records: list[tuple[str, str, dict]] = []

    def _rec(self, level, extra):
        self.records.append((level, (extra or {}).get("event", ""), extra or {}))

    def warning(self, *args, **kw):
        self._rec("warning", kw.get("extra"))

    def info(self, *args, **kw):
        self._rec("info", kw.get("extra"))

    def debug(self, *args, **kw):
        self._rec("debug", kw.get("extra"))

    def events(self, level=None):
        return [e for lvl, e, _ in self.records if level is None or lvl == level]


async def _direct(coro_factory):
    """Inline runner — awaits the port coroutine on the test loop (deterministic)."""
    return await coro_factory()


def _kg_schema_named_imports(tree) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "okto_pulse.core.kg.schema":
            names.update(a.name for a in node.names)
    return names


def _module_imports(tree) -> set[str]:
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module)
    return mods


# ===========================================================================
# ts_8077c637 — AST/import gate over the real core/app.py.
# ===========================================================================
def test_ts_8077c637_app_py_drops_kg_schema_lifecycle_imports():
    tree = ast.parse(APP_PY.read_text(encoding="utf-8"))
    module_schema_imported = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "okto_pulse.core.kg":
            if any(a.name == "schema" for a in node.names):
                module_schema_imported = True
        elif isinstance(node, ast.Import):
            if any(a.name == "okto_pulse.core.kg.schema" for a in node.names):
                module_schema_imported = True

    leaked = _kg_schema_named_imports(tree) & _FORBIDDEN_KG_SCHEMA_SYMBOLS
    assert not leaked, f"app.py still imports kg.schema symbols: {sorted(leaked)}"
    assert not module_schema_imported, "app.py imports the kg.schema module wholesale"


def test_ts_8077c637_app_py_wires_the_06_ports():
    src = APP_PY.read_text(encoding="utf-8")
    assert "resolve_graph_lifecycle" in src
    assert "run_startup_schema_sweep" in src
    sweep_wiring_src = Path(_startup_sweep_mod.__file__).read_text(encoding="utf-8")
    assert "sweep_board_schemas" in sweep_wiring_src
    assert "graph_runtime_store" in sweep_wiring_src
    assert "graph_runtime_store=kg_registry.graph_runtime_store" in sweep_wiring_src
    assert "graph_schema_manager" in sweep_wiring_src
    assert "get_kg_registry" in sweep_wiring_src


# ===========================================================================
# ts_6ce72610 — Sweep NC-10 via ports.
# ===========================================================================
def test_ts_6ce72610_sweep_ensures_existing_skips_missing_and_counts():
    resolver = _FakeRuntimeStore(existing={"b1", "b3"})
    manager = _FakeSchemaManager()
    logger = _RecLogger()

    migrated = asyncio.run(
        sweep_board_schemas(
            ["b1", "b2", "b3"],
            graph_runtime_store=resolver,
            graph_schema_manager=manager,
            logger=logger,
            run_blocking=_direct,
        )
    )

    assert migrated == 2
    assert resolver.checked == ["b1", "b2", "b3"]  # every board probed
    assert manager.ensured == ["b1", "b3"]  # b2 skipped (missing), order preserved
    swept = [r for r in logger.records if r[1] == "kg.schema.migration_swept"]
    assert len(swept) == 1 and swept[0][2]["boards_swept"] == 2
    assert "kg.schema.migration_failed" not in logger.events()


def test_ts_6ce72610_empty_or_all_missing_logs_zero():
    resolver = _FakeRuntimeStore(existing=set())
    manager = _FakeSchemaManager()
    logger = _RecLogger()
    migrated = asyncio.run(
        sweep_board_schemas(
            ["b1", "b2"],
            graph_runtime_store=resolver,
            graph_schema_manager=manager,
            logger=logger,
            run_blocking=_direct,
        )
    )
    assert migrated == 0
    assert manager.ensured == []
    swept = [r for r in logger.records if r[1] == "kg.schema.migration_swept"]
    assert swept and swept[0][2]["boards_swept"] == 0


def test_ts_6ce72610_one_board_fails_logs_and_continues():
    resolver = _FakeRuntimeStore(existing={"b1", "b2", "b3"})
    manager = _FakeSchemaManager(fail_on={"b2"})
    logger = _RecLogger()

    migrated = asyncio.run(
        sweep_board_schemas(
            ["b1", "b2", "b3"],
            graph_runtime_store=resolver,
            graph_schema_manager=manager,
            logger=logger,
            run_blocking=_direct,
        )
    )

    # b2 failed but the loop continued to b3; count excludes the failure.
    assert migrated == 2
    assert manager.ensured == ["b1", "b2", "b3"]  # b2 attempted, then b3
    failed = [r for r in logger.records if r[1] == "kg.schema.migration_failed"]
    assert len(failed) == 1
    assert failed[0][2]["board_id"] == "b2"
    assert "ensure_boom:b2" in failed[0][2]["error"]
    swept = [r for r in logger.records if r[1] == "kg.schema.migration_swept"]
    assert swept and swept[0][2]["boards_swept"] == 2


def test_ts_6ce72610_resolver_error_propagates_to_caller_skipped():
    # A resolver exception is NOT soft-caught -> it propagates so the app.py
    # outer guard degrades to kg.schema.migration_skipped (old semantics).
    resolver = _FakeRuntimeStore(existing={"b1"}, raise_on="b2")
    manager = _FakeSchemaManager()
    logger = _RecLogger()
    with pytest.raises(RuntimeError, match="resolver_boom:b2"):
        asyncio.run(
            sweep_board_schemas(
                ["b1", "b2", "b3"],
                graph_runtime_store=resolver,
                graph_schema_manager=manager,
                logger=logger,
                run_blocking=_direct,
            )
        )
    # b1 ensured before the resolver blew up on b2; swept log NOT reached.
    assert manager.ensured == ["b1"]
    assert "kg.schema.migration_swept" not in logger.events()


def test_ts_6ce72610_default_runner_offloads_off_the_event_loop():
    resolver = _FakeRuntimeStore(existing={"b1"})
    manager = _FakeSchemaManager()
    logger = _RecLogger()

    # No run_blocking -> the default _run_off_loop (asyncio.to_thread) is used.
    migrated = asyncio.run(
        sweep_board_schemas(
            ["b1"],
            graph_runtime_store=resolver,
            graph_schema_manager=manager,
            logger=logger,
        )
    )
    assert migrated == 1
    assert manager.ensured == ["b1"]
    # ensure_bootstrapped ran in a worker thread, not the main thread.
    assert manager.threads and manager.threads[0] != threading.get_ident()


# ===========================================================================
# ts_f967116f — Shutdown: GraphLifecycle.close(None) + close_db ALWAYS runs.
# ===========================================================================
def test_ts_f967116f_shutdown_uses_graph_lifecycle_close():
    src = APP_PY.read_text(encoding="utf-8")
    # The shutdown path closes via the port (helper), never the removed symbol.
    assert "graph_lifecycle.close(None)" in src
    assert "shutdown_kg_then_db" in src
    assert "close_all_connections" not in _kg_schema_named_imports(ast.parse(src))


def test_ts_f967116f_happy_path_closes_kg_then_db():
    calls = {"close_arg": "UNSET", "db": 0}

    class _OkLifecycle:
        async def close(self, board_id=None):
            calls["close_arg"] = board_id

    async def _close_db():
        calls["db"] += 1

    logger = _RecLogger()
    asyncio.run(
        shutdown_kg_then_db(
            _close_db,
            logger=logger,
            graph_lifecycle_provider=lambda: _OkLifecycle(),
            run_blocking=_direct,
        )
    )
    assert calls["close_arg"] is None  # close(None) closes the global + all boards
    assert calls["db"] == 1
    assert "kg.shutdown.close_connections_failed" not in logger.events()


def test_ts_f967116f_close_db_continues_when_kg_close_fails():
    class _BoomLifecycle:
        async def close(self, board_id=None):
            raise RuntimeError("close_boom")

    closed = {"db": 0}

    async def _close_db():
        closed["db"] += 1

    logger = _RecLogger()
    asyncio.run(
        shutdown_kg_then_db(
            _close_db,
            logger=logger,
            graph_lifecycle_provider=lambda: _BoomLifecycle(),
            run_blocking=_direct,
        )
    )
    # KG close failed -> logged -> but close_db STILL ran (never blocked).
    failed = [
        r for r in logger.records if r[1] == "kg.shutdown.close_connections_failed"
    ]
    assert len(failed) == 1
    assert "close_boom" in failed[0][2]["error"]
    assert closed["db"] == 1


# ===========================================================================
# ts_a627315d — Registry conformance (runtime_checkable + test fakes).
# ===========================================================================
def test_ts_a627315d_registry_providers_conform_and_are_memory_fakes():
    reset_registry_for_tests()
    configure_test_kg_registry(graph_provider="inmemory")
    try:
        reg = get_kg_registry()
        assert isinstance(reg.graph_schema_manager, GraphSchemaManager)
        assert isinstance(reg.graph_lifecycle, GraphLifecycle)
        assert isinstance(reg.graph_runtime_store, GraphRuntimeStore)

        assert reg.graph_schema_manager.__class__.__name__ == "InMemoryGraphSchemaManager"
        assert reg.graph_lifecycle.__class__.__name__ == "InMemoryGraphLifecycle"
        assert reg.graph_runtime_store.__class__.__name__ == "InMemoryGraphRuntimeStore"
        for provider in (
            reg.graph_schema_manager,
            reg.graph_lifecycle,
            reg.graph_runtime_store,
        ):
            assert provider.__class__.__module__.startswith(
                "okto_pulse.core.kg.providers.testing"
            )
        # The port methods the migrated callers use are coroutine functions.
        assert asyncio.iscoroutinefunction(reg.graph_schema_manager.ensure_bootstrapped)
        assert asyncio.iscoroutinefunction(reg.graph_lifecycle.close)
    finally:
        reset_registry_for_tests()


# ===========================================================================
# ts_960cc3bf — Regression #06/#03b green + replay without delta.
# ===========================================================================
_STARTUP_EVENTS = [
    "init_db", "seed_community_defaults", "backfill_qa_answered_at",
    "_afg_backfill_task", "_preload_embedding_model", "event_dispatcher",
    "consolidation_worker", "cleanup_worker", "outbox_worker", "settings_service",
    "scheduler", "mcp_runtime", "frontend_served", "_metrics_beacon_loop",
]


def test_ts_960cc3bf_default_only_exclusions_unchanged():
    from okto_pulse.core.application.boundary import DEFAULT_ONLY_EXCLUSIONS

    assert set(DEFAULT_ONLY_EXCLUSIONS) == {
        "kg_migration_sweep", "shutdown_kg_events_hub", "close_all_connections"
    }


def test_ts_960cc3bf_golden_replay_has_no_unexpected_delta():
    from okto_pulse.core.application.boundary import (
        REQUIRED_LIFECYCLE_EVENTS,
        CommunityLifespanReplay,
        CommunityLifespanReplayInput,
        NamedLifecycleHook,
    )
    from okto_pulse.core.composition import RuntimeComposition

    hooks = [NamedLifecycleHook(startup_event=e) for e in _STARTUP_EVENTS]
    hooks.append(NamedLifecycleHook(shutdown_event="close_db"))
    comp = RuntimeComposition(
            settings_provider=1, auth_provider=1, storage_provider=1,
            event_bus=1, uow_factory=1,
            lifecycle_hooks=tuple(hooks),
        scheduler_control=1,
    )
    report = asyncio.run(
        CommunityLifespanReplay().run(
            CommunityLifespanReplayInput(composition=comp, include_frontend=True)
        )
    )
    assert report.status == "passed", report.as_dict()
    assert report.unexpected_deltas == []
    assert set(REQUIRED_LIFECYCLE_EVENTS) <= (
        set(report.startup_events) | set(report.shutdown_events)
    )


# ===========================================================================
# ts_f83ad3db — Scope / register-before-remove.
# ===========================================================================
def test_ts_f83ad3db_scope_limited_register_before_remove():
    # 1) The extracted sweep helper imports NO kg.schema (it uses only the ports).
    sweep_imports = _module_imports(ast.parse(SWEEP_PY.read_text(encoding="utf-8")))
    assert not any("kg.schema" in mod for mod in sweep_imports), (
        f"startup_schema_sweep must not import kg.schema; saw {sorted(sweep_imports)}"
    )

    # 2) The core registry no longer imports the runtime schema/Kuzu providers.
    from okto_pulse.core.kg.interfaces import registry as _registry_mod

    registry_imports = _module_imports(
        ast.parse(Path(_registry_mod.__file__).read_text(encoding="utf-8"))
    )
    forbidden = [
        mod
        for mod in registry_imports
        if "core.kg.schema" in mod or "core.kg.providers.embedded.kuzu" in mod
    ]
    assert forbidden == []

    # 3) The sanctioned core graph implementations are test-only fakes.
    from okto_pulse.core.kg.providers.testing.memory_graph_store import (
        InMemoryGraphLifecycle,
        InMemoryGraphRuntimeStore,
        InMemoryGraphSchemaManager,
        InMemoryGraphTransaction,
        InMemoryGraphStore,
    )

    assert all((
        InMemoryGraphStore,
        InMemoryGraphTransaction,
        InMemoryGraphSchemaManager,
        InMemoryGraphLifecycle,
        InMemoryGraphRuntimeStore,
    ))
