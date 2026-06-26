"""Adapter Readiness Inventory + fail-closed readiness gate (spec R05-A, 0fb5b4ce).

A pure, executable LEDGER/GATE that catalogues the technical adapters embedded in
the core and decides — DETERMINISTICALLY and FAIL-CLOSED — whether each is
``ready`` / ``blocked`` / ``deferred`` to be moved out, WITHOUT moving anything.

R05-A does NOT move/remove adapters, change defaults/deps, or touch
runtime/CLI/seed/MCP/REST/KG/storage/embedding/telemetry/scheduler. It only
records the inventory and the verifiable removal criteria, and provides the gate
that blocks any future removal lacking the required evidence.

PURE (tr_4e074dcf): the DTOs + builders import ONLY stdlib (``ast`` /
``dataclasses`` / ``pathlib`` / ``typing``). They reference every concrete
adapter by STRING (module path / package), and NEVER import Kuzu / Ladybug /
sentence-transformers / SQLAlchemy / ``okto_pulse.community`` / any concrete
provider. The reconciliation gate lazy-imports the (pure) composition gate only
when CALLED, so importing this module in isolation stays clean (tr_f… / ts_9dbe515a).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# ---------------------------------------------------------------------------
# Predecessor axes (tr_68514f22) — the spec(s)/refinement(s) that must close the
# port(s) this adapter depends on (PLURAL: an adapter may be gated by several
# axes — e.g. a relational adapter on both #04 repository/UoW AND #16 schema
# migrations).
# ---------------------------------------------------------------------------
PredecessorAxis = Literal[
    "#03_lifecycle_composition",
    "#04_repository_uow",
    "#06_kg_ports",
    "#08_mcp_auth",
    "#10_telemetry",
    "#11_mcp_resources",
    "#13_llm_embedding_rerank",
    "#15_settings_conformance",
    "#16_schema_migrations",
]
PREDECESSOR_AXES: tuple[PredecessorAxis, ...] = (
    "#03_lifecycle_composition",
    "#04_repository_uow",
    "#06_kg_ports",
    "#08_mcp_auth",
    "#10_telemetry",
    "#11_mcp_resources",
    "#13_llm_embedding_rerank",
    "#15_settings_conformance",
    "#16_schema_migrations",
)

AdapterStatus = Literal["ready", "blocked", "deferred"]

#: The bounded evidence fields required before an adapter can be ``ready``.
REQUIRED_EVIDENCE: tuple[str, ...] = (
    "port_closed",
    "community_registered",
    "oracle_passed",
    "import_audit_passed",
    "dependency_audit_passed",
    "register_before_remove_passed",
)


@dataclass(frozen=True)
class AdapterInventoryEntry:
    """One technical adapter in the readiness ledger. Declarative only — every
    concrete reference (module / packages) is a STRING; nothing is imported."""

    adapter_key: str
    owner: str
    current_module: str  # STRING path, NOT an import
    port_ref: str
    wave: str
    predecessor_refs: tuple[PredecessorAxis, ...]  # PLURAL — all gating axes
    target_destination: str
    packages: tuple[str, ...]  # STRING dependency names
    oracles_required: tuple[str, ...]
    removal_criterion: str
    status: AdapterStatus
    #: Set when this adapter is a ``deferred_to_05`` subject (provider key the
    #: composition/singleton markers defer to spec #05); None otherwise.
    deferred_provider_key: str | None = None
    #: Immutable metadata — a tuple of (key, value) pairs, NOT a mutable dict
    #: (a dict inside a frozen dataclass would still be mutable in place).
    metadata: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class AdapterEvidence:
    """Bounded evidence for an adapter (tr_55db518c). Each field is tri-state:
    ``True`` (present/passed), ``False`` (failed), ``None`` (absent)."""

    port_closed: bool | None = None
    community_registered: bool | None = None
    oracle_passed: bool | None = None
    import_audit_passed: bool | None = None
    dependency_audit_passed: bool | None = None
    register_before_remove_passed: bool | None = None

    def as_map(self) -> dict[str, bool | None]:
        return {
            "port_closed": self.port_closed,
            "community_registered": self.community_registered,
            "oracle_passed": self.oracle_passed,
            "import_audit_passed": self.import_audit_passed,
            "dependency_audit_passed": self.dependency_audit_passed,
            "register_before_remove_passed": self.register_before_remove_passed,
        }


@dataclass(frozen=True)
class AdapterReadinessVerdict:
    """Deterministic outcome of evaluating one adapter's readiness."""

    adapter_key: str
    status: AdapterStatus
    reasons: tuple[str, ...]
    missing_evidence: tuple[str, ...]
    failed_evidence: tuple[str, ...]
    oracles_required: tuple[str, ...]
    #: PLURAL — every spec that must close before this adapter can advance (never
    #: collapses multiple un-closed predecessors into one).
    next_specs: tuple[str, ...]

    @property
    def is_ready(self) -> bool:
        return self.status == "ready"


# ===========================================================================
# Canonical inventory (tr_dcff34a1).
# ===========================================================================
#: The canonical set of adapter_keys this ledger MUST cover (exactly once).
REQUIRED_ADAPTER_KEYS: frozenset[str] = frozenset(
    {
        "filesystem_storage_provider",
        "sentence_transformer_embedding_provider",
        "stub_embedding_provider",
        "cross_encoder_reranker",
        "inmemory_cache_backend",
        "inmemory_token_bucket_rate_limiter",
        "inmemory_session_store",
        "mcp_auth_context",
        "kuzu_graph_store",
        "kuzu_cypher_executor",
        "kuzu_graph_schema_manager",
        "kuzu_graph_lifecycle",
        "kuzu_graph_path_resolver",
        "kuzu_graph_transaction",
        "global_discovery_db",
        "sqlite_outbox_event_bus",
        "sqlalchemy_audit_repository",
        "settings_kg_config",
        "singleton_scheduler_control",
        "local_telemetry_store",
        "asyncpg_postgres_driver",
    }
)


def _entry(**kw) -> AdapterInventoryEntry:
    return AdapterInventoryEntry(**kw)


def build_adapter_inventory() -> tuple[AdapterInventoryEntry, ...]:
    """Build the canonical, immutable adapter readiness ledger. Declarative —
    every adapter is referenced by STRING (no import)."""
    entries: list[AdapterInventoryEntry] = [
        # --- storage (#03 composition owns storage_provider wiring) ---
        _entry(
            adapter_key="filesystem_storage_provider",
            owner="okto-pulse-core/infra",
            current_module="okto_pulse/core/infra/storage.py",
            port_ref="StorageProvider",
            wave="R05-STORAGE",
            predecessor_refs=("#03_lifecycle_composition",),
            target_destination="community/adapters (composition storage_provider)",
            packages=("stdlib(pathlib,shutil)",),
            oracles_required=("storage_roundtrip", "storage_provider_conformance"),
            removal_criterion=(
                "Community registers a StorageProvider via composition and its "
                "oracle passes; remove the core default only after "
                "register-before-remove."
            ),
            status="blocked",
        ),
        # --- embedding / rerank (#13) ---
        _entry(
            adapter_key="sentence_transformer_embedding_provider",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/embedding.py",
            port_ref="EmbeddingProvider",
            wave="R05-EMBED",
            predecessor_refs=("#13_llm_embedding_rerank",),
            target_destination="community/adapters (embedding_provider)",
            packages=("sentence-transformers", "torch", "huggingface_hub"),
            oracles_required=("embedding_dimension_parity", "describe_provider_metadata"),
            removal_criterion=(
                "Community wires the embedding provider via composition with the "
                "EmbeddingProvider port + dimension-parity oracle; the Stub stays "
                "as the core fallback until then."
            ),
            status="blocked",
        ),
        _entry(
            adapter_key="stub_embedding_provider",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/embedding.py",
            port_ref="EmbeddingProvider",
            wave="R05-EMBED",
            predecessor_refs=("#13_llm_embedding_rerank",),
            target_destination="core fallback (stays as last-resort default)",
            packages=("stdlib",),
            oracles_required=("embedding_dimension_parity",),
            removal_criterion=(
                "The deterministic stub is the offline fallback; it is NOT removed "
                "in R05 — kept until a composition-owned default exists."
            ),
            status="deferred",
        ),
        _entry(
            adapter_key="cross_encoder_reranker",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/rerank/cross_encoder.py",
            port_ref="Reranker",
            wave="R05-EMBED",
            predecessor_refs=("#13_llm_embedding_rerank",),
            target_destination="community/adapters (reranker)",
            packages=("sentence-transformers",),
            oracles_required=("rerank_order_parity", "lazy_import_to_token_overlap"),
            removal_criterion=(
                "Community wires the cross-encoder via composition; the lazy "
                "import->token_overlap fallback (R13-C) stays until then."
            ),
            status="blocked",
        ),
        # --- KG registry in-memory providers (#06) ---
        _entry(
            adapter_key="inmemory_cache_backend",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/memory_cache.py",
            port_ref="CacheBackend",
            wave="R05-REGISTRY",
            predecessor_refs=("#06_kg_ports",),
            target_destination="composition-owned cache provider",
            packages=("stdlib",),
            oracles_required=("cache_get_set_roundtrip",),
            removal_criterion=(
                "A composition cache provider is registered with the CacheBackend "
                "port oracle; the in-memory default stays until then."
            ),
            status="blocked",
        ),
        _entry(
            adapter_key="inmemory_token_bucket_rate_limiter",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/memory_rate_limiter.py",
            port_ref="RateLimiter",
            wave="R05-REGISTRY",
            predecessor_refs=("#06_kg_ports",),
            target_destination="composition-owned rate limiter provider",
            packages=("stdlib",),
            oracles_required=("rate_limit_token_consumption",),
            removal_criterion=(
                "A composition rate limiter is registered with the RateLimiter "
                "port oracle; the in-memory bucket stays until then."
            ),
            status="blocked",
        ),
        _entry(
            adapter_key="inmemory_session_store",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/memory_session_store.py",
            port_ref="SessionStore",
            wave="R05-REGISTRY",
            predecessor_refs=("#06_kg_ports",),
            target_destination="composition-owned session store provider",
            packages=("stdlib",),
            oracles_required=("session_ttl_expiry",),
            removal_criterion=(
                "A composition session store is registered with the SessionStore "
                "port oracle; the in-memory store stays until then."
            ),
            status="blocked",
        ),
        # --- MCP auth context (#08 — R08-A + R08-B done; R08-C pending) ---
        _entry(
            adapter_key="mcp_auth_context",
            owner="okto-pulse-core/inbound-mcp",
            current_module="okto_pulse/core/kg/providers/embedded/mcp_auth_context.py",
            port_ref="AuthContext / McpAuthenticator",
            wave="R05-MCPAUTH",
            predecessor_refs=("#08_mcp_auth", "#16_schema_migrations"),
            target_destination="community/adapters (mcp authenticator)",
            packages=("stdlib",),
            oracles_required=("mcp_auth_resolve_agent", "boards_acl_resolution"),
            removal_criterion=(
                "R08-A (McpAuthenticator port + Community adapter) and R08-B (the "
                "AuthSession<->AuthContext bridge reusing MCPAuthContext + the "
                "Community composition registering auth_context_factory pass-through "
                "so the KG query tools resolve agent_id/boards via the port) are "
                "DONE. MCPAuthContext stays as a fail-closed in-transition embedded "
                "exception: REMOVE only after R08-C provides a request_scope_provider "
                "so the per-request identity no longer rides the _active_api_key "
                "ContextVar (it is the register-HALF — nothing removed here)."
            ),
            status="deferred",
        ),
        # --- KG / Kuzu / Ladybug adapters (#06, deferred_to_05 subjects) ---
        _entry(
            adapter_key="kuzu_graph_store",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/kuzu_graph_store.py",
            port_ref="SemanticGraphStore",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="community/adapters (kuzu graph store)",
            packages=("ladybug(embedded)",),
            oracles_required=("graph_store_conformance", "vector_search_parity"),
            removal_criterion=(
                "deferred_to_05: the kuzu_store provider is composition-owned and "
                "the SemanticGraphStore port oracle passes; remove after the move."
            ),
            status="blocked",
            deferred_provider_key="kuzu_store",
        ),
        _entry(
            adapter_key="kuzu_cypher_executor",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/kuzu_cypher_executor.py",
            port_ref="CypherExecutor",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="community/adapters (kuzu cypher executor)",
            packages=("ladybug(embedded)",),
            oracles_required=("cypher_executor_conformance",),
            removal_criterion=(
                "deferred_to_05: composition owns the kuzu_store provider with the "
                "CypherExecutor port oracle; remove after the move."
            ),
            status="blocked",
            deferred_provider_key="kuzu_store",
        ),
        _entry(
            adapter_key="kuzu_graph_schema_manager",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/kuzu_graph_schema_manager.py",
            port_ref="GraphSchemaManager",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="community/adapters (kuzu schema manager)",
            packages=("ladybug(embedded)",),
            oracles_required=("schema_bootstrap_idempotent",),
            removal_criterion=(
                "deferred_to_05: the kg_registry provider is composition-owned; "
                "remove the embedded schema manager after the registry move."
            ),
            status="blocked",
            deferred_provider_key="kg_registry",
        ),
        _entry(
            adapter_key="kuzu_graph_lifecycle",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/kuzu_graph_lifecycle.py",
            port_ref="GraphLifecycle",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="community/adapters (kuzu lifecycle)",
            packages=("ladybug(embedded)",),
            oracles_required=("lifecycle_close_releases_handles",),
            removal_criterion=(
                "deferred_to_05: the kg_registry provider is composition-owned; "
                "remove the embedded lifecycle after the registry move."
            ),
            status="blocked",
            deferred_provider_key="kg_registry",
        ),
        _entry(
            adapter_key="kuzu_graph_path_resolver",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/kuzu_graph_path_resolver.py",
            port_ref="GraphPathResolver",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="community/adapters (kuzu path resolver)",
            packages=("stdlib",),
            oracles_required=("path_resolver_exists_parity",),
            removal_criterion=(
                "deferred_to_05: the kg_registry provider is composition-owned; "
                "remove the embedded path resolver after the registry move."
            ),
            status="blocked",
            deferred_provider_key="kg_registry",
        ),
        _entry(
            adapter_key="kuzu_graph_transaction",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/kuzu_graph_transaction.py",
            port_ref="GraphTransaction",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="community/adapters (kuzu transaction)",
            packages=("ladybug(embedded)",),
            oracles_required=("transaction_open_board_connection",),
            removal_criterion=(
                "deferred_to_05: the kg_registry provider is composition-owned; "
                "remove the embedded transaction after the registry move."
            ),
            status="blocked",
            deferred_provider_key="kg_registry",
        ),
        _entry(
            adapter_key="global_discovery_db",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/global_discovery/schema.py",
            port_ref="kg_registry (global-discovery DB handle)",
            wave="R05-KG",
            predecessor_refs=("#06_kg_ports",),
            target_destination="composition-owned GlobalDiscoveryDb handle",
            packages=("ladybug(embedded)",),
            oracles_required=("global_discovery_db_singleton_replaced",),
            removal_criterion=(
                "deferred_to_05 (singleton ledger _global_db): the composition "
                "root owns the global-discovery DB handle; remove the module "
                "global only after the provider is wired."
            ),
            status="blocked",
            deferred_provider_key="kg_registry",
        ),
        # --- infra config providers ---
        _entry(
            adapter_key="sqlite_outbox_event_bus",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/sqlite_outbox_event_bus.py",
            port_ref="EventBus",
            wave="R05-INFRA",
            predecessor_refs=("#04_repository_uow", "#16_schema_migrations"),
            target_destination="community/adapters (event_bus)",
            packages=("SQLAlchemy",),
            oracles_required=("outbox_event_roundtrip",),
            removal_criterion=(
                "Community registers an EventBus via composition (already wired by "
                "configure_kg_registry); remove the core default after "
                "register-before-remove."
            ),
            status="blocked",
        ),
        _entry(
            adapter_key="sqlalchemy_audit_repository",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/sqlalchemy_audit_repo.py",
            port_ref="AuditRepository",
            wave="R05-INFRA",
            predecessor_refs=("#04_repository_uow", "#16_schema_migrations"),
            target_destination="community/adapters (audit_repo)",
            packages=("SQLAlchemy",),
            oracles_required=("audit_append_query_parity",),
            removal_criterion=(
                "Community registers an AuditRepository via composition; remove "
                "the core default after register-before-remove."
            ),
            status="blocked",
        ),
        _entry(
            adapter_key="settings_kg_config",
            owner="okto-pulse-core/kg",
            current_module="okto_pulse/core/kg/providers/embedded/settings_config.py",
            port_ref="KGConfig",
            wave="R05-INFRA",
            predecessor_refs=("#15_settings_conformance",),
            target_destination="composition-owned KGConfig provider",
            packages=("stdlib",),
            oracles_required=("kg_config_settings_parity",),
            removal_criterion=(
                "A composition KGConfig provider is registered with the conformance "
                "oracle; the settings-backed default stays until then."
            ),
            status="blocked",
        ),
        # --- runtime / scheduler (#03) ---
        _entry(
            adapter_key="singleton_scheduler_control",
            owner="okto-pulse-core/runtime",
            current_module="okto_pulse/core/services/scheduler_control_adapter.py",
            port_ref="SchedulerControl",
            wave="R05-RUNTIME",
            predecessor_refs=("#03_lifecycle_composition",),
            target_destination="composition-owned SchedulerControl provider",
            packages=("APScheduler",),
            oracles_required=("scheduler_reschedule_signal",),
            removal_criterion=(
                "Composition root owns the SchedulerControl provider; remove the "
                "singleton bridge once settings/lifespan resolve it from "
                "composition."
            ),
            status="blocked",
        ),
        # --- telemetry (#10 — ledger item, NOT a functional impl) ---
        _entry(
            adapter_key="local_telemetry_store",
            owner="okto-pulse-core/telemetry",
            current_module="okto_pulse/core/telemetry/store.py",
            port_ref="(governed by #10)",
            wave="R05-TELEMETRY",
            predecessor_refs=("#10_telemetry",),
            target_destination="governed by #10 telemetry boundary",
            packages=("stdlib",),
            oracles_required=("telemetry_event_persisted",),
            removal_criterion=(
                "Ledger item governed by #10: NOT a functional implementation in "
                "R05. Deferred until the #10 telemetry boundary defines the port."
            ),
            status="deferred",
        ),
        # --- dependency ledger: asyncpg postgres driver ---
        _entry(
            adapter_key="asyncpg_postgres_driver",
            owner="okto-pulse-core/infra",
            current_module="okto_pulse/core/infra/database.py",
            port_ref="(relational driver dependency)",
            wave="R05-E",
            predecessor_refs=("#04_repository_uow", "#16_schema_migrations"),
            target_destination="optional Postgres extra (community-owned)",
            packages=("asyncpg",),
            oracles_required=("postgres_engine_optional", "sqlite_default_unaffected"),
            removal_criterion=(
                "R05-E: remove the asyncpg Postgres driver dependency only after "
                "the relational adapter boundary (R05-E) closes and no core default "
                "requires it."
            ),
            status="deferred",
        ),
    ]
    return tuple(entries)


# ===========================================================================
# Deterministic readiness evaluator (tr_55db518c) — FAIL-CLOSED.
# ===========================================================================
def evaluate_adapter_readiness(
    entry: AdapterInventoryEntry,
    evidence: AdapterEvidence,
) -> AdapterReadinessVerdict:
    """Deterministically map an adapter + its bounded evidence to a verdict.

    Rules (fail-closed):
      * ``port_closed`` is the gateway. If it is not ``True`` (absent/failed) the
        adapter is ``deferred`` — its predecessor spec must close the port first.
      * ``port_closed=True`` but ANY of the remaining required evidences absent or
        failed -> ``blocked`` (actionable, but evidence incomplete).
      * ALL required evidences present and passing -> ``ready``.
    """
    ev = evidence.as_map()
    missing = tuple(name for name in REQUIRED_EVIDENCE if ev[name] is None)
    failed = tuple(name for name in REQUIRED_EVIDENCE if ev[name] is False)

    # Gateway: the port boundary(ies) must be closed (upstream specs) before this
    # adapter is even actionable -> otherwise deferred to ALL its predecessors
    # (multiple un-closed predecessors are listed, never collapsed into one).
    if ev["port_closed"] is not True:
        return AdapterReadinessVerdict(
            adapter_key=entry.adapter_key,
            status="deferred",
            reasons=("port_not_closed",)
            + tuple(f"awaiting:{ref}" for ref in entry.predecessor_refs),
            missing_evidence=missing,
            failed_evidence=failed,
            oracles_required=entry.oracles_required,
            next_specs=entry.predecessor_refs,
        )

    # Port closed but evidence incomplete -> blocked (fail-closed).
    if missing or failed:
        return AdapterReadinessVerdict(
            adapter_key=entry.adapter_key,
            status="blocked",
            reasons=("incomplete_evidence",) + tuple(f"failed:{n}" for n in failed),
            missing_evidence=missing,
            failed_evidence=failed,
            oracles_required=entry.oracles_required,
            next_specs=(entry.wave,),
        )

    # Everything present and passing.
    return AdapterReadinessVerdict(
        adapter_key=entry.adapter_key,
        status="ready",
        reasons=("all_evidence_present",),
        missing_evidence=(),
        failed_evidence=(),
        oracles_required=entry.oracles_required,
        next_specs=(entry.wave,),
    )


def evaluate_removal(
    entry: AdapterInventoryEntry,
    evidence: AdapterEvidence,
) -> AdapterReadinessVerdict:
    """Anti-removal gate (tr_17226e07): a removal attempt is only permitted when
    the adapter evaluates to ``ready``. Otherwise the verdict is the fail-closed
    ``blocked``/``deferred`` with stable diagnostic reasons. This is the same
    deterministic evaluator — removal NEVER bypasses the evidence requirement."""
    return evaluate_adapter_readiness(entry, evidence)


# ===========================================================================
# deferred_to_05 reconciliation gate (tr_3a6b50ff) — 1:1.
# ===========================================================================
@dataclass(frozen=True)
class DeferredReconciliationReport:
    ok: bool
    marker_files: tuple[str, ...]
    canonical_sources: tuple[str, ...]
    marker_provider_keys: tuple[str, ...]
    inventory_deferred_keys: tuple[str, ...]
    uncovered_markers: tuple[str, ...]
    phantom_inventory_keys: tuple[str, ...]
    entries_missing_criterion: tuple[str, ...]


def _default_source_root() -> Path:
    # src/okto_pulse/core/application/boundary/adapter_readiness_inventory.py -> src/
    return Path(__file__).resolve().parents[4]


#: This ledger's own posix path (relative to src/) — EXCLUDED from the marker
#: scan so the gate never reconciles against the file it just created.
_SELF_REL = "okto_pulse/core/application/boundary/adapter_readiness_inventory.py"


def run_deferred_reconciliation_gate(
    source_root: Path | None = None,
) -> DeferredReconciliationReport:
    """Walk the REAL ``deferred_to_05`` markers — EXCLUDING this ledger itself —
    and reconcile them 1:1 with the inventory.

    The authoritative deferred provider keys are AGGREGATED from the existing
    canonical sources (not re-derived): ``composition.DEFERRED_TO_05_PROVIDERS``,
    ``composition_gate.DEFERRED_SYMBOLS`` and the ``_global_db`` debt in
    ``singleton_gate.SINGLETON_LEDGER``. Every key must have an inventory entry
    (with a verifiable, non-empty removal criterion), and no inventory entry may
    claim a deferred key absent from those sources. All three are PURE modules,
    lazy-imported only when this gate is CALLED.
    """
    root = source_root or _default_source_root()
    core = root / "okto_pulse" / "core"

    marker_files: list[str] = []
    for py in sorted(core.rglob("*.py")):
        rel = py.relative_to(root).as_posix()
        if rel == _SELF_REL:
            continue  # never self-reference the ledger we just created
        try:
            text = py.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if "deferred_to_05" in text:
            marker_files.append(rel)

    # Aggregate the canonical deferred provider keys from the real sources.
    # Lazy imports keep THIS module importable in isolation; all three are pure.
    from okto_pulse.core.application.boundary.composition_gate import (
        DEFERRED_SYMBOLS,
    )
    from okto_pulse.core.application.boundary.singleton_gate import (
        SINGLETON_LEDGER,
    )
    from okto_pulse.core.composition import DEFERRED_TO_05_PROVIDERS

    marker_provider_keys: set[str] = set()
    marker_provider_keys |= set(DEFERRED_TO_05_PROVIDERS)
    marker_provider_keys |= set(DEFERRED_SYMBOLS.values())
    _gd = SINGLETON_LEDGER.get("_global_db", {})
    if "deferred_to_05" in str(_gd.get("retirement_criterion", "")):
        target = _gd.get("target_provider")
        if target:
            marker_provider_keys.add(target)
    canonical_sources = (
        "okto_pulse.core.composition.DEFERRED_TO_05_PROVIDERS",
        "okto_pulse.core.application.boundary.composition_gate.DEFERRED_SYMBOLS",
        "okto_pulse.core.application.boundary.singleton_gate.SINGLETON_LEDGER[_global_db]",
    )

    inventory = build_adapter_inventory()
    inventory_deferred = {
        e.deferred_provider_key for e in inventory if e.deferred_provider_key
    }
    entries_missing_criterion = tuple(
        e.adapter_key
        for e in inventory
        if e.deferred_provider_key and not e.removal_criterion.strip()
    )

    uncovered = marker_provider_keys - inventory_deferred
    phantom = inventory_deferred - marker_provider_keys

    ok = (
        bool(marker_files)
        and not uncovered
        and not phantom
        and not entries_missing_criterion
    )
    return DeferredReconciliationReport(
        ok=ok,
        marker_files=tuple(marker_files),
        canonical_sources=canonical_sources,
        marker_provider_keys=tuple(sorted(marker_provider_keys)),
        inventory_deferred_keys=tuple(sorted(inventory_deferred)),
        uncovered_markers=tuple(sorted(uncovered)),
        phantom_inventory_keys=tuple(sorted(phantom)),
        entries_missing_criterion=entries_missing_criterion,
    )


__all__ = [
    "PREDECESSOR_AXES",
    "PredecessorAxis",
    "AdapterStatus",
    "REQUIRED_EVIDENCE",
    "REQUIRED_ADAPTER_KEYS",
    "AdapterInventoryEntry",
    "AdapterEvidence",
    "AdapterReadinessVerdict",
    "build_adapter_inventory",
    "evaluate_adapter_readiness",
    "evaluate_removal",
    "DeferredReconciliationReport",
    "run_deferred_reconciliation_gate",
]
