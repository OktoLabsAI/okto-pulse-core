"""Spec #06 — KG storage ports.

Core exposes graph ports and test-only in-memory providers. Concrete
Kuzu/Ladybug runtime behavior is owned by the Community adapter package.
"""

from __future__ import annotations

import inspect
import os

import pytest

from okto_pulse.core.kg.interfaces import (
    GraphHandle,
    GraphLifecycle,
    GraphSchemaManager,
    GraphStatementResult,
    GraphTransaction,
    PurgeReport,
    RebuildReport,
    SchemaValidationResult,
    SemanticGraphStore,
    get_kg_registry,
    reset_registry_for_tests,
)
from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.schema_contract import SCHEMA_VERSION


def _bid(tag: str) -> str:
    return f"board-ports06-{tag}-{os.urandom(3).hex()}"


@pytest.fixture
def board():
    bid = _bid("main")
    yield bid


@pytest.fixture
def registry():
    reset_registry_for_tests()
    configure_test_kg_registry(graph_provider="inmemory")
    yield get_kg_registry()
    reset_registry_for_tests()


# --------------------------------------------------------------------------- #
# Ports are registered as core test fakes and satisfy their Protocols
# --------------------------------------------------------------------------- #


def test_onda4_ports_registered_as_memory_fakes(registry):
    assert isinstance(registry.graph_transaction, GraphTransaction)
    assert isinstance(registry.graph_schema_manager, GraphSchemaManager)
    assert isinstance(registry.graph_lifecycle, GraphLifecycle)
    assert type(registry.graph_transaction).__name__ == "InMemoryGraphTransaction"


def test_semantic_graph_store_is_a_live_sync_port(registry):
    # api_9630ab67: SemanticGraphStore is synchronous; no fictitious async.
    assert isinstance(registry.graph_store, SemanticGraphStore)
    for name in ("find_by_topic", "create_node", "bootstrap", "get_schema_version"):
        method = getattr(SemanticGraphStore, name)
        assert not inspect.iscoroutinefunction(method), f"{name} must stay sync"


# --------------------------------------------------------------------------- #
# GraphRuntimeStore exposes backend-neutral storage state
# --------------------------------------------------------------------------- #


def test_runtime_store_exposes_memory_storage_state(registry, board):
    runtime_store = registry.graph_runtime_store
    assert runtime_store.exists(board) is False
    state = runtime_store.graph_state(board)
    assert state.board_id == board
    assert state.storage_ref.token == f"board:{board}"
    assert state.exists is False
    assert state.backend == "logical_memory"
    assert isinstance(state.locked, bool)
    assert state.quarantined is False


# --------------------------------------------------------------------------- #
# GraphSchemaManager exposes schema lifecycle behind a port
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_schema_manager_bootstrap_migrate_validate(registry, board):
    mgr = registry.graph_schema_manager
    # idempotent ensure (board already bootstrapped) — must not raise
    await mgr.ensure_bootstrapped(board)
    version = await mgr.current_version(board)
    assert isinstance(version, str) and version
    assert version == SCHEMA_VERSION
    result = await mgr.validate(board)
    assert isinstance(result, SchemaValidationResult)
    assert result.expected_version == SCHEMA_VERSION
    assert result.valid is True
    assert result.current_version == SCHEMA_VERSION
    assert result.issues == ()
    summary = await mgr.migrate(board)
    assert summary["board_id"] == board
    assert "columns_added" in summary and "errors" in summary


# --------------------------------------------------------------------------- #
# GraphLifecycle exposes lifecycle reports without raw runtime calls
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_lifecycle_open_then_query_then_close(registry, board):
    lifecycle = registry.graph_lifecycle
    handle = await lifecycle.open(board)
    assert isinstance(handle, GraphHandle)
    assert handle.board_id == board
    assert handle.opened is True
    assert handle.storage_ref.namespace == "memory_graph"
    assert isinstance(handle.locked, bool) and isinstance(handle.quarantined, bool)
    await lifecycle.close(board)


@pytest.mark.asyncio
async def test_lifecycle_rebuild_returns_structured_report(registry, board):
    report = await registry.graph_lifecycle.rebuild(board)
    assert isinstance(report, RebuildReport)
    assert report.board_id == board
    assert report.status == "rebuilt"
    assert report.steps == ("memory",)


@pytest.mark.asyncio
async def test_lifecycle_purge_returns_structured_report(registry):
    bid = _bid("purge")
    report = await registry.graph_lifecycle.purge(bid, reason="ports06-test")
    assert isinstance(report, PurgeReport)
    assert report.board_id == bid
    assert report.status == "noop"
    assert report.reason == "ports06-test"
    assert report.quarantined is False
    assert report.affected_storage_refs == ()


# --------------------------------------------------------------------------- #
# GraphTransaction scope hides backend-specific connections
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_transaction_scope_executes_and_closes(registry, board):
    query = "MATCH (m:BoardMeta) RETURN count(m)"
    scope = await registry.graph_transaction.begin(board)
    async with scope:
        result = scope.execute(query)
        assert isinstance(result, GraphStatementResult)
        assert result.rows == ()


@pytest.mark.asyncio
async def test_transaction_commit_and_rollback_close_cleanly(registry, board):
    txn = registry.graph_transaction
    # explicit commit path
    scope = await txn.begin(board)
    scope.execute("MATCH (m:BoardMeta) RETURN count(m)")
    await scope.commit()
    # best-effort rollback path (documented embedded auto-commit) — must not raise
    scope2 = await txn.begin(board)
    scope2.execute("MATCH (m:BoardMeta) RETURN count(m)")
    await scope2.rollback()


# --------------------------------------------------------------------------- #
# SemanticGraphStore read path unchanged (queries observably equivalent)
# --------------------------------------------------------------------------- #


def test_graph_store_schema_version_matches_direct(registry, board):
    store = registry.graph_store
    store.bootstrap(board)
    assert store.get_schema_version(board) == SCHEMA_VERSION


# --------------------------------------------------------------------------- #
# GraphSchemaManager fails CLOSED: a version-read error is never masked as valid
# --------------------------------------------------------------------------- #


class _RaisingGraphStore:
    def get_schema_version(self, board_id: str) -> str | None:
        raise RuntimeError("boom: schema read failed")


class _NoVersionGraphStore:
    def get_schema_version(self, board_id: str) -> str | None:
        return None


@pytest.mark.asyncio
async def test_schema_validate_fails_closed_on_read_error():
    reset_registry_for_tests()
    try:
        configure_test_kg_registry(graph_store=_RaisingGraphStore())
        result = await get_kg_registry().graph_schema_manager.validate("any-board")
        assert result.valid is False
        assert result.current_version is None
        assert any("read failed" in issue for issue in result.issues)
    finally:
        reset_registry_for_tests()


@pytest.mark.asyncio
async def test_schema_validate_invalid_when_no_version_recorded():
    reset_registry_for_tests()
    try:
        configure_test_kg_registry(graph_store=_NoVersionGraphStore())
        result = await get_kg_registry().graph_schema_manager.validate("any-board")
        assert result.valid is False
        assert result.current_version is None
        assert result.issues
    finally:
        reset_registry_for_tests()


@pytest.mark.asyncio
async def test_schema_current_version_does_not_mask_read_error():
    reset_registry_for_tests()
    try:
        configure_test_kg_registry(graph_store=_RaisingGraphStore())
        mgr = get_kg_registry().graph_schema_manager
        with pytest.raises(RuntimeError):
            await mgr.current_version("any-board")  # NOT masked to SCHEMA_VERSION
    finally:
        reset_registry_for_tests()
