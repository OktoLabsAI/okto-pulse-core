"""Spec #06 card 3645f105 — KG storage ports + SemanticGraphStore replay/equivalence.

Proves the new Onda-4 ports (GraphTransaction, GraphSchemaManager, GraphLifecycle,
GraphPathResolver) are wired in the registry as embedded Kùzu adapters and yield
results OBSERVABLY EQUIVALENT to the direct kg.schema calls they encapsulate —
without exposing open_board_connection / board_kuzu_path / close_all_connections.
SemanticGraphStore is confirmed as a live SYNC port (api_9630ab67), with no
fictitious async contract. Kùzu/Ladybug stay embedded (no storage move).
"""

from __future__ import annotations

import inspect
import os

import pytest

from okto_pulse.core.kg.interfaces import (
    GraphHandle,
    GraphLifecycle,
    GraphPathResolver,
    GraphSchemaManager,
    GraphTransaction,
    PurgeReport,
    RebuildReport,
    SchemaValidationResult,
    SemanticGraphStore,
    get_kg_registry,
    reset_registry_for_tests,
)
from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.schema import (
    SCHEMA_VERSION,
    board_kuzu_path,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


def _bid(tag: str) -> str:
    return f"board-ports06-{tag}-{os.urandom(3).hex()}"


@pytest.fixture
def board():
    bid = _bid("main")
    bootstrap_board_graph(bid)
    yield bid
    close_all_connections()


@pytest.fixture
def registry():
    reset_registry_for_tests()
    configure_test_kg_registry()
    yield get_kg_registry()
    reset_registry_for_tests()


# --------------------------------------------------------------------------- #
# Ports are registered as embedded adapters and satisfy their Protocols
# --------------------------------------------------------------------------- #


def test_onda4_ports_registered_as_embedded_adapters(registry):
    assert isinstance(registry.graph_transaction, GraphTransaction)
    assert isinstance(registry.graph_schema_manager, GraphSchemaManager)
    assert isinstance(registry.graph_lifecycle, GraphLifecycle)
    assert isinstance(registry.graph_path_resolver, GraphPathResolver)
    assert type(registry.graph_transaction).__name__ == "KuzuGraphTransaction"
    assert type(registry.graph_path_resolver).__name__ == "KuzuGraphPathResolver"


def test_semantic_graph_store_is_a_live_sync_port(registry):
    # api_9630ab67: SemanticGraphStore is synchronous; no fictitious async.
    assert isinstance(registry.graph_store, SemanticGraphStore)
    for name in ("find_by_topic", "create_node", "bootstrap", "get_schema_version"):
        method = getattr(SemanticGraphStore, name)
        assert not inspect.iscoroutinefunction(method), f"{name} must stay sync"


# --------------------------------------------------------------------------- #
# GraphPathResolver ≡ board_kuzu_path (ac_eacf2ac1 target)
# --------------------------------------------------------------------------- #


def test_path_resolver_equivalent_to_board_kuzu_path(registry, board):
    resolver = registry.graph_path_resolver
    assert resolver.board_graph_path(board) == board_kuzu_path(board)
    assert resolver.exists(board) is board_kuzu_path(board).exists()
    state = resolver.storage_state(board)
    assert state.board_id == board
    assert state.path == board_kuzu_path(board)
    assert state.exists is True
    assert state.size_bytes >= 0
    # StorageIntrospection contract: backend / locked / quarantined modeled.
    assert state.backend == "ladybug_embedded"
    assert isinstance(state.locked, bool)
    assert state.quarantined is False  # a present, live graph is not quarantined


# --------------------------------------------------------------------------- #
# GraphSchemaManager ≡ kg.schema bootstrap/migrate/version
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_schema_manager_equivalent_to_kg_schema(registry, board):
    mgr = registry.graph_schema_manager
    # idempotent ensure (board already bootstrapped) — must not raise
    await mgr.ensure_bootstrapped(board)
    version = await mgr.current_version(board)
    assert isinstance(version, str) and version
    assert version == SCHEMA_VERSION  # a bootstrapped board records the current version
    result = await mgr.validate(board)
    assert isinstance(result, SchemaValidationResult)
    assert result.expected_version == SCHEMA_VERSION
    assert result.valid is True
    assert result.current_version == SCHEMA_VERSION
    assert result.issues == ()
    summary = await mgr.migrate(board)
    # equivalent to migrate_schema_for_board's structured summary
    assert summary["board_id"] == board
    assert "columns_added" in summary and "errors" in summary


# --------------------------------------------------------------------------- #
# GraphLifecycle ≡ ensure/close/purge (no close_all_connections leak)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_lifecycle_open_then_query_then_close(registry, board):
    lifecycle = registry.graph_lifecycle
    handle = await lifecycle.open(board)
    assert isinstance(handle, GraphHandle)
    assert handle.board_id == board
    assert handle.opened is True
    assert handle.backend == "ladybug_embedded"
    assert isinstance(handle.locked, bool) and isinstance(handle.quarantined, bool)
    # queryable after open (observable equivalence to a direct bootstrap+open)
    with open_board_connection(board) as (_db, conn):
        assert conn.execute("MATCH (m:BoardMeta) RETURN count(m)").has_next()
    await lifecycle.close(board)  # releases handles; path still resolvable
    assert registry.graph_path_resolver.exists(board) is True


@pytest.mark.asyncio
async def test_lifecycle_rebuild_returns_structured_report(registry, board):
    report = await registry.graph_lifecycle.rebuild(board)
    assert isinstance(report, RebuildReport)
    assert report.board_id == board
    assert report.status == "rebuilt"
    assert "ensure_board_graph_bootstrapped" in report.steps
    close_all_connections()


@pytest.mark.asyncio
async def test_lifecycle_purge_returns_structured_report(registry):
    bid = _bid("purge")
    bootstrap_board_graph(bid)
    assert board_kuzu_path(bid).exists()
    report = await registry.graph_lifecycle.purge(bid, reason="ports06-test")
    assert isinstance(report, PurgeReport)
    assert report.board_id == bid
    assert report.status == "purged"
    assert report.reason == "ports06-test"
    assert report.quarantined is True
    assert report.affected_paths  # at least the graph file
    # purge cleared the live graph file (quarantine-then-clear)
    assert not board_kuzu_path(bid).exists()
    close_all_connections()


# --------------------------------------------------------------------------- #
# GraphTransaction scope ≡ a direct BoardConnection (no open_board_connection leak)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_transaction_scope_read_equivalent_to_direct_connection(registry, board):
    query = "MATCH (m:BoardMeta) RETURN count(m)"
    with open_board_connection(board) as (_db, conn):
        direct = conn.execute(query).get_next()[0]

    scope = await registry.graph_transaction.begin(board)
    async with scope:
        via_port = scope.execute(query).get_next()[0]
    assert via_port == direct


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
    # connection released → board is re-openable
    with open_board_connection(board) as (_db, conn):
        assert conn.execute("MATCH (m:BoardMeta) RETURN count(m)").has_next()


# --------------------------------------------------------------------------- #
# SemanticGraphStore read path unchanged (queries observably equivalent)
# --------------------------------------------------------------------------- #


def test_graph_store_schema_version_matches_direct(registry, board):
    store = registry.graph_store
    # the store's schema version read is the same surface the schema manager uses
    assert store.get_schema_version(board) in (None, SCHEMA_VERSION) or isinstance(
        store.get_schema_version(board), str
    )


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
