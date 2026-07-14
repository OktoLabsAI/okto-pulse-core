"""R-P2-03 — test-only KG registry configuration helper.

The KG registry no longer lazy-builds implicit Onda A defaults (R-P2-03
fail-closed): ``get_kg_registry`` raises until the composition configures it, and
``configure_kg_registry`` has no implicit ``_build_defaults`` fallback. Tests
configure the test fakes EXPLICITLY through this helper — the SANCTIONED
test/fake route (``defaults_factory=_build_defaults``). Real runtime must supply a
Community ``base_registry`` instead; this module lives under ``tests/`` and is
NEVER imported by production code (audited by the R-P2-03 conformance test).
"""

from __future__ import annotations

from typing import Any, Literal

from okto_pulse.core.kg.interfaces.registry import configure_kg_registry
from testing_kg_registry import build_testing_kg_registry


def _test_relational_database_path():
    """Resolve the SQLite path through the explicitly composed test runtime."""

    from okto_pulse.core.ports.relational_runtime import resolve_database_runtime

    path = resolve_database_runtime().local_database_path()
    if path is None:
        raise RuntimeError("test relational runtime has no local SQLite path")
    return path


def _community_source_reader() -> dict[str, Any]:
    from okto_pulse.community.adapters.board_source_reader import (
        CommunityBoardSourceReader,
    )

    return {
        "board_source_reader": CommunityBoardSourceReader(
            db_path_provider=_test_relational_database_path
        )
    }


def _community_rebuild_ingestion() -> dict[str, Any]:
    from okto_pulse.community.adapters.board_rebuild_ingestion import (
        CommunityBoardRebuildIngestionAdapter,
    )

    return {
        "rebuild_ingestion_port": CommunityBoardRebuildIngestionAdapter(
            db_path_provider=_test_relational_database_path
        )
    }


def _community_rebuild_artifact_scope_resolver() -> dict[str, Any]:
    from okto_pulse.community.adapters.rebuild_audit_storage import (
        CommunityRebuildAuditArtifactStoreResolver,
    )

    return {
        "rebuild_audit_artifact_store_resolver": (
            CommunityRebuildAuditArtifactStoreResolver()
        )
    }


def _community_graph_providers() -> dict[str, Any]:
    from okto_pulse.community.adapters.kg import build_community_graph_providers
    from okto_pulse.community.adapters.data import CommunityKGConfig
    from okto_pulse.community.config import CommunitySettings

    providers = build_community_graph_providers()
    providers["config"] = CommunityKGConfig(CommunitySettings())
    return providers


def _skip_missing_community_integration(exc: ModuleNotFoundError) -> None:
    import pytest

    pytest.skip(
        "Community KG integration adapter is unavailable; this test explicitly "
        f"requires the real Community runtime ({exc.name or exc}).",
        allow_module_level=False,
    )


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
    the real Community Ladybug adapters. When it is not available, the default
    mode stays in explicit core contract-fake mode through ``_build_defaults``.
    Tests that truly exercise the Community runtime must pass
    ``graph_provider="real"`` (or use ``configure_real_graph_test_kg_registry``);
    that path skips with an explicit reason instead of silently falling back to
    in-memory fakes.

    R-P2-02: ``event_bus`` and ``audit_repo`` are REQUIRED composition slots (the
    core no longer auto-wires the SqliteOutboxEventBus / SqlAlchemyAuditRepository
relational fallback). The test-only ``_build_defaults`` does NOT supply them, so
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

    if graph_provider != "inmemory":
        try:
            defaults.update(_community_graph_providers())
        except ModuleNotFoundError as exc:
            if graph_provider == "real":
                _skip_missing_community_integration(exc)
            # Some pure-core boundary tests intentionally run without the Community
            # repo on sys.path. They keep the explicit in-memory graph fakes from
            # _build_defaults, including InMemoryBoardGraphRuntime.

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
    if "rebuild_audit_artifact_store_resolver" not in overrides:
        try:
            defaults.update(_community_rebuild_artifact_scope_resolver())
        except ModuleNotFoundError:
            pass

    defaults.update(overrides)
    configure_kg_registry(defaults_factory=build_testing_kg_registry, **defaults)

    from okto_pulse.core.services.application_kg import (
        configure_commit_coordinator,
        configure_write_barrier,
    )

    configure_commit_coordinator()
    configure_write_barrier("soft")

    # Spec MKG-A-S1 (FR4): cognitive commits append to the durable
    # CognitiveSourceStore fail-closed. Register a fresh in-memory store per
    # configuration so cognitive commits work in tests; tests exercising the
    # fail-closed path register their own broken store explicitly and reset.
    from okto_pulse.core.ports.kg_cognitive_source import (
        register_cognitive_source_store,
    )

    register_cognitive_source_store(_InMemoryCognitiveSourceStore())

    # Spec MKG-C-S1 (FR1): curation merges append to the EquivalenceLedger
    # fail-closed; a fresh in-memory ledger per configuration keeps dedup
    # and fold tests self-contained.
    from okto_pulse.core.ports.kg_equivalence_ledger import (
        register_equivalence_ledger,
    )

    register_equivalence_ledger(_InMemoryEquivalenceLedger())

    from okto_pulse.core.ports.kg_curation_proposals import (
        register_curation_proposal_store,
    )

    register_curation_proposal_store(_InMemoryCurationProposalStore())

    from okto_pulse.core.ports.kg_subtype_registry import (
        register_node_subtype_registry,
    )

    register_node_subtype_registry(_InMemoryNodeSubtypeRegistry())


class _InMemoryNodeSubtypeRegistry:
    """Test-only subtype registry (MKG-E FR2/FR3) — same pure rules."""

    def __init__(self) -> None:
        self.declarations: list[Any] = []

    async def declare(self, declaration: Any) -> Any:
        from dataclasses import replace
        from datetime import datetime, timezone

        from okto_pulse.core.ports.kg_subtype_registry import (
            validate_subtype_declaration,
        )

        validate_subtype_declaration(declaration, tuple(self.declarations))
        stored = replace(
            declaration,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self.declarations.append(stored)
        return stored

    async def get(self, node_type: str, kind_of: str) -> Any:
        from okto_pulse.core.ports.kg_subtype_registry import normalize_kind_of

        normalized = normalize_kind_of(kind_of)
        for declaration in self.declarations:
            if (
                declaration.node_type == node_type
                and normalize_kind_of(declaration.kind_of) == normalized
            ):
                return declaration
        return None

    async def list_all(self) -> tuple:
        return tuple(
            sorted(self.declarations, key=lambda d: (d.node_type, d.kind_of))
        )


class _InMemoryCurationProposalStore:
    """Test-only proposal store (MKG-C FR7)."""

    def __init__(self) -> None:
        self.proposals: dict[str, Any] = {}

    async def append(self, proposal: Any) -> str:
        self.proposals.setdefault(proposal.proposal_id, proposal)
        return proposal.proposal_id

    async def get(self, proposal_id: str) -> Any:
        return self.proposals.get(proposal_id)

    async def resolve(self, proposal_id: str, status: str) -> Any:
        from dataclasses import replace
        from datetime import datetime, timezone

        from okto_pulse.core.ports.kg_curation_proposals import (
            CurationProposalError,
        )

        proposal = self.proposals.get(proposal_id)
        if proposal is None:
            raise CurationProposalError(
                "curation_proposal_not_found", proposal_id=proposal_id
            )
        updated = replace(
            proposal,
            status=status,
            resolved_at=datetime.now(timezone.utc).isoformat(),
        )
        self.proposals[proposal_id] = updated
        return updated

    async def pending_for_board(self, board_id: str) -> tuple:
        rows = [
            pr for pr in self.proposals.values()
            if pr.board_id == board_id and pr.status == "pending"
        ]
        rows.sort(key=lambda pr: (pr.created_at or "", pr.proposal_id))
        return tuple(rows)


class _InMemoryEquivalenceLedger:
    """Test-only equivalence ledger: append-only, revoke stamps revoked_at."""

    def __init__(self) -> None:
        self.records: dict[str, Any] = {}

    async def append(self, record: Any) -> str:
        if record.record_id in self.records:
            return record.record_id
        self.records[record.record_id] = record
        return record.record_id

    async def revoke(self, record_id: str, reason: str) -> Any:
        from dataclasses import replace
        from datetime import datetime, timezone

        from okto_pulse.core.ports.kg_equivalence_ledger import (
            EquivalenceLedgerError,
        )

        record = self.records.get(record_id)
        if record is None:
            raise EquivalenceLedgerError(
                "equivalence_record_not_found", record_id=record_id
            )
        if record.revoked_at is not None:
            return record
        updated = replace(
            record,
            revoked_at=datetime.now(timezone.utc).isoformat(),
            revoke_reason=reason,
        )
        self.records[record_id] = updated
        return updated

    async def get(self, record_id: str) -> Any:
        return self.records.get(record_id)

    async def active_for_board(self, board_id: str) -> tuple:
        rows = [
            r for r in self.records.values()
            if r.board_id == board_id and r.revoked_at is None
        ]
        rows.sort(key=lambda r: (r.created_at or "", r.record_id))
        return tuple(rows)


class _InMemoryCognitiveSourceStore:
    """Test-only durable store: append-only, per-configuration lifetime."""

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def append(self, record: Any) -> str:
        for existing in self.records:
            if (
                existing.node_id == record.node_id
                and existing.generation == record.generation
            ):
                return existing.node_id
        self.records.append(record)
        return record.node_id

    async def enumerate(self, board_id: str):
        return tuple(
            sorted(
                (r for r in self.records if r.board_id == board_id),
                key=lambda r: (r.committed_at or "", r.node_id, r.generation),
            )
        )


def configure_real_graph_test_kg_registry(**overrides: Any) -> None:
    """Configure test registry fakes plus real Community graph providers.

    Use this only in integration tests that seed/read a real Ladybug board graph.
    The default autouse test registry stays in-memory so pure unit tests keep
    isolation and speed.
    """
    from okto_pulse.community.config import CommunitySettings
    from okto_pulse.core.infra.config import configure_settings

    configure_settings(CommunitySettings())
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
    try:
        from okto_pulse.community.adapters.data import build_community_data_providers
        from okto_pulse.community.config import CommunitySettings
    except ModuleNotFoundError as exc:
        _skip_missing_community_integration(exc)

    from okto_pulse.core.infra.config import configure_settings

    configure_settings(CommunitySettings())
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
        from okto_pulse.community.adapters.kuzu_graph_transaction import (
            _KuzuTransactionScope,
        )

        self._delegate = _KuzuTransactionScope(board_id)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Any:
        return self._delegate.execute(cypher, params)

    async def commit(self) -> None:
        await self._delegate.commit()

    async def rollback(self) -> None:
        await self._delegate.rollback()

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

    def apply_step(self, board_id: str, graph_type: str, step: str):
        return self._delegate.apply_step(board_id, graph_type, step)
