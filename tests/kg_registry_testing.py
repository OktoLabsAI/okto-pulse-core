"""R-P2-03 — test-only KG registry configuration helper.

The KG registry no longer lazy-builds implicit Onda A defaults (R-P2-03
fail-closed): ``get_kg_registry`` raises until the composition configures it, and
``configure_kg_registry`` has no implicit ``_build_defaults`` fallback. Tests
configure the embedded fakes EXPLICITLY through this helper — the SANCTIONED
test/fake route (``defaults_factory=_build_defaults``). Real runtime must supply a
Community ``base_registry`` instead; this module lives under ``tests/`` and is
NEVER imported by production code (audited by the R-P2-03 conformance test).
"""

from __future__ import annotations

from typing import Any, Literal

from okto_pulse.core.kg.interfaces.registry import (
    _build_defaults,
    configure_kg_registry,
)

_GRAPH_PROVIDER_KEYS = {
    "graph_store",
    "cypher_executor",
    "graph_transaction",
    "graph_schema_manager",
    "graph_lifecycle",
    "graph_path_resolver",
    "safe_write_step_adapter",
    "board_graph_runtime",
    "global_discovery_runtime",
}


def _community_source_reader() -> dict[str, Any]:
    from okto_pulse.community.adapters.board_source_reader import (
        CommunityBoardSourceReader,
        resolve_pulse_db_path,
    )

    return {
        "board_source_reader": CommunityBoardSourceReader(
            db_path_provider=resolve_pulse_db_path
        )
    }


def _community_rebuild_ingestion() -> dict[str, Any]:
    from okto_pulse.community.adapters.board_rebuild_ingestion import (
        CommunityBoardRebuildIngestionAdapter,
    )
    from okto_pulse.community.adapters.board_source_reader import resolve_pulse_db_path

    return {
        "rebuild_ingestion_port": CommunityBoardRebuildIngestionAdapter(
            db_path_provider=resolve_pulse_db_path
        )
    }


def _community_graph_providers() -> dict[str, Any]:
    from okto_pulse.community.adapters.board_graph_runtime import (
        CommunityBoardGraphRuntime,
    )
    from okto_pulse.community.adapters.kg import build_community_graph_providers
    from okto_pulse.community.adapters.kg_runtime import apply_ladybug_lifecycle_step

    return {
        **build_community_graph_providers(),
        "safe_write_step_adapter": apply_ladybug_lifecycle_step,
        "board_graph_runtime": CommunityBoardGraphRuntime(),
    }


def _community_board_graph_runtime() -> dict[str, Any]:
    from okto_pulse.community.adapters.board_graph_runtime import (
        CommunityBoardGraphRuntime,
    )
    from okto_pulse.community.adapters.global_discovery_runtime import (
        CommunityGlobalDiscoveryRuntime,
    )

    return {
        "board_graph_runtime": CommunityBoardGraphRuntime(),
        "global_discovery_runtime": CommunityGlobalDiscoveryRuntime(),
    }


def configure_test_kg_registry(
    *,
    graph_provider: Literal["real_if_available", "real", "inmemory"] = "real_if_available",
    **overrides: Any,
) -> None:
    """Configure the KG registry for tests.

    A thin, EXPLICIT wrapper over
    ``configure_kg_registry(defaults_factory=_build_defaults, **overrides)`` so the
    test intent is literal and greppable.

    By default, when the Community repo is on ``sys.path``, graph providers use
    the real Community Ladybug adapters. Most KG integration tests seed/read the
    real board graph through legacy helpers, so the registry must observe the
    same graph. Pure unit tests that need in-memory graph isolation should pass
    ``graph_provider="inmemory"`` or explicit graph-slot overrides.

    R-P2-02: ``event_bus`` and ``audit_repo`` are REQUIRED composition slots (the
    core no longer auto-wires the SqliteOutboxEventBus / SqlAlchemyAuditRepository
    relational fallback). The embedded ``_build_defaults`` does NOT supply them, so
    this helper injects the in-memory test fakes by default — a test can override
    either (e.g. to exercise prefer-provided or a raising fake). Pass any other
    provider overrides exactly as you would to ``configure_kg_registry``.
    """
    from okto_pulse.core.kg.providers.testing.memory_audit_repo import (
        InMemoryAuditRepository,
    )
    from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus

    defaults: dict[str, Any] = {
        "event_bus": InMemoryEventBus(),
        "audit_repo": InMemoryAuditRepository(),
    }

    graph_overridden = bool(_GRAPH_PROVIDER_KEYS.intersection(overrides))
    if graph_provider != "inmemory" and not graph_overridden:
        try:
            defaults.update(_community_graph_providers())
        except ModuleNotFoundError:
            if graph_provider == "real":
                raise
            # Some pure-core boundary tests intentionally run without the Community
            # repo on sys.path. They keep the in-memory graph fakes.
    else:
        try:
            defaults.update(_community_board_graph_runtime())
        except ModuleNotFoundError:
            pass

    if "board_source_reader" not in overrides:
        try:
            defaults.update(_community_source_reader())
        except ModuleNotFoundError:
            pass
    if "rebuild_ingestion_port" not in overrides:
        try:
            defaults.update(_community_rebuild_ingestion())
        except ModuleNotFoundError:
            pass

    defaults.update(overrides)
    configure_kg_registry(defaults_factory=_build_defaults, **defaults)


def configure_real_graph_test_kg_registry(**overrides: Any) -> None:
    """Configure test registry fakes plus real Community graph providers.

    Use this only in integration tests that seed/read a real Ladybug board graph.
    The default autouse test registry stays in-memory so pure unit tests keep
    isolation and speed.
    """
    configure_test_kg_registry(graph_provider="real", **overrides)


def configure_real_graph_and_data_test_kg_registry(
    session_factory: Any,
    **overrides: Any,
) -> None:
    """Configure real Community graph + relational data providers for tests.

    Use this for integration tests that assert SQL audit/outbox side effects.
    It keeps the dependency explicit in test code and avoids reintroducing the
    retired core relational fallback.
    """
    from okto_pulse.community.adapters.data import build_community_data_providers

    real_data = build_community_data_providers(session_factory)
    real_data.update(overrides)
    configure_test_kg_registry(
        graph_provider="real",
        session_factory=session_factory,
        **real_data,
    )


class RealBoardCypherExecutorForTests:
    """Test-only bridge for legacy integration tests that seed Ladybug directly.

    Production code must receive the board-graph executor from the composition
    root. Some older core integration tests still call ``bootstrap_board_graph``
    / ``open_board_connection`` directly; this bridge keeps those tests coherent
    while the production registry remains adapter-owned.
    """

    def execute_read_only(
        self,
        board_id: str,
        cypher: str,
        params: dict[str, Any] | None = None,
        *,
        max_rows: int = 1000,
    ) -> dict:
        from okto_pulse.community.adapters.kg_runtime import open_board_connection

        rows: list[list[Any]] = []
        with open_board_connection(board_id) as (_db, conn):
            result = conn.execute(cypher, params or {})
            try:
                while result.has_next() and len(rows) < max_rows:
                    rows.append(list(result.get_next()))
            finally:
                if hasattr(result, "close"):
                    result.close()
        return {
            "rows": rows,
            "row_count": len(rows),
            "truncated": len(rows) >= max_rows,
        }

    def is_supported(self) -> bool:
        return True


class _RealBoardGraphTransactionScopeForTests:
    def __init__(self, board_id: str) -> None:
        from okto_pulse.community.adapters.kg_runtime import open_board_connection

        self._connection = open_board_connection(board_id)

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Any:
        if params:
            return self._connection.conn.execute(cypher, params)
        return self._connection.conn.execute(cypher)

    async def commit(self) -> None:
        self._connection.close()

    async def rollback(self) -> None:
        self._connection.close()

    async def __aenter__(self) -> "_RealBoardGraphTransactionScopeForTests":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()


class RealBoardGraphTransactionForTests:
    """Test-only GraphTransaction bridge over the legacy real board graph."""

    async def begin(self, board_id: str) -> _RealBoardGraphTransactionScopeForTests:
        return _RealBoardGraphTransactionScopeForTests(board_id)


class RealBoardGraphPathResolverForTests:
    """Test-only GraphPathResolver bridge over the legacy real board path."""

    def board_graph_path(self, board_id: str):
        from okto_pulse.community.adapters.kg_runtime import board_kuzu_path

        return board_kuzu_path(board_id)

    def exists(self, board_id: str) -> bool:
        return self.board_graph_path(board_id).exists()

    def storage_state(self, board_id: str):
        from okto_pulse.core.kg.interfaces.graph_path_resolver import (
            GraphStorageState,
        )

        path = self.board_graph_path(board_id)
        exists = path.exists()
        sidecars: tuple[str, ...] = ()
        if path.parent.exists():
            sidecars = tuple(
                sorted(
                    p.name for p in path.parent.glob(path.name + "*")
                    if p.name != path.name
                )
            )
        return GraphStorageState(
            board_id=board_id,
            path=path,
            exists=exists,
            size_bytes=path.stat().st_size if exists else 0,
            backend="ladybug_embedded_test",
            locked=(path.parent / (path.name + ".wal")).exists(),
            quarantined=path.parent.exists() and not exists,
            sidecars=sidecars,
        )


class RealBoardGraphLifecycleForTests:
    """Test-only GraphLifecycle bridge over the Community Ladybug adapter."""

    def __init__(self) -> None:
        from okto_pulse.community.adapters.kuzu_graph_lifecycle import (
            CommunityKuzuGraphLifecycle,
        )

        self._delegate = CommunityKuzuGraphLifecycle()

    async def open(self, board_id: str):
        return await self._delegate.open(board_id)

    async def close(self, board_id: str | None = None) -> None:
        await self._delegate.close(board_id)

    async def rebuild(self, board_id: str):
        return await self._delegate.rebuild(board_id)

    async def purge(self, board_id: str, *, reason: str):
        return await self._delegate.purge(board_id, reason=reason)
