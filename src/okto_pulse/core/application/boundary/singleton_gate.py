"""AntiSingletonGate — block NEW module-global singletons in core (spec #15).

fr_95d98ef5: no new module-global singleton may be introduced in the core; a new
one detected blocks. fr_531b74f3: the existing singletons live in a
register-before-remove ledger with owner, target provider, expected adapter and
a retirement criterion — headlined by ``_global_db``, ``_scheduler``,
``_mcp_session_factory`` and ``_permission_cache``.

Detection is deterministic and NARROW (AST, no import): a module-global is a
singleton when it is reassigned via a ``global`` statement (mutated process
state) or is a ``ContextVar``. Module constants — ``__all__``, lookup tables,
metric sample buffers — are NOT singletons and are never flagged.

The current inventory of such singletons is frozen in ``BASELINE_SINGLETONS``
(register-before-remove): introducing a NEW one fails the gate until it is
either injected through a composition provider/port or consciously added to the
baseline with justification.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

#: register-before-remove ledger of the HEADLINE core singletons (fr_531b74f3).
SINGLETON_LEDGER: dict[str, dict[str, str]] = {
    "_global_db": {
        "file": "okto_pulse/core/kg/global_discovery/schema.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_registry",
        "expected_adapter": "GlobalDiscoveryDb (composition-owned handle)",
        "retirement_criterion": (
            "Composition root owns the global-discovery DB handle (deferred_to_05); "
            "remove only after the provider is wired."
        ),
    },
    "_scheduler": {
        "file": "okto_pulse/core/kg/scheduler_singleton.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "scheduler_control",
        "expected_adapter": "SingletonSchedulerControl -> composition SchedulerControl",
        "retirement_criterion": (
            "Composition root owns the SchedulerControl provider; remove the global "
            "once settings/lifespan resolve the port from composition."
        ),
    },
    "_mcp_session_factory": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_session_factory",
        "expected_adapter": "RuntimeComposition.mcp_session_factory",
        "retirement_criterion": (
            "Composition root provides the MCP session factory; remove the global "
            "after the inbound MCP server resolves it from composition."
        ),
    },
    "_permission_cache": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "auth",
        "expected_adapter": "Auth provider-owned permission cache",
        "retirement_criterion": (
            "Auth provider owns permission caching; remove the module dict once the "
            "provider exposes a scoped cache."
        ),
    },
    "_factory": {
        "file": "okto_pulse/core/telemetry/event_store_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "TelemetryEventStore factory (R10-B) — the composition root (Community) "
            "registers the concrete adapter behind the port; this is the same "
            "register-before-remove pattern as kg/interfaces/registry.py::_registry."
        ),
        "retirement_criterion": (
            "Remove the module global once the telemetry runtime resolves its "
            "EventStore from RuntimeComposition.telemetry instead of the process-wide "
            "factory registry (and the core LocalTelemetryStore shim is deleted)."
        ),
    },
    "_product_aggregator_factory": {
        "file": "okto_pulse/core/telemetry/product_aggregator_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "ProductAggregationPort factory (R10-D) — the composition root "
            "(Community) registers the concrete sqlite3 aggregator behind the port; "
            "same register-before-remove pattern as the R10-B event-store factory."
        ),
        "retirement_criterion": (
            "Remove the module global (and the core ProductTelemetryAggregator shim) "
            "once every edition composes its ProductAggregationPort in R10-E."
        ),
    },
    "_publish_health_source_provider": {
        "file": "okto_pulse/core/telemetry/publish_health_source_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "PublishHealthSource external-descriptor provider (R10-D) — the "
            "composition root (Community) registers the aws_ingest/report_athena "
            "descriptors (default GAP, never healthy) behind the port."
        ),
        "retirement_criterion": (
            "Remove the module global once the publish-health sources are composed "
            "via RuntimeComposition in R10-E."
        ),
    },
    "_telemetry_sender_factory": {
        "file": "okto_pulse/core/telemetry/sender_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "TelemetrySink factory (R10-C) — the composition root (Community) "
            "registers the concrete beacon sender (requests/HMAC/handshake/usage) "
            "behind the port; same register-before-remove pattern as the R10-B "
            "event-store factory."
        ),
        "retirement_criterion": (
            "Remove the module global (and the core TelemetryBeaconSender shim) "
            "once every edition composes its TelemetrySink in R10-E."
        ),
    },
    "_telemetry_port_factory": {
        "file": "okto_pulse/core/telemetry/telemetry_port_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "TelemetryPort facade factory (R10-E, Stage A) — the composition root "
            "(Community) registers the composed TelemetryService facade behind the "
            "port; same register-before-remove pattern as the R10-B/C/D factories."
        ),
        "retirement_criterion": (
            "Remove the module global once every edition composes its TelemetryPort "
            "via RuntimeComposition and the call-sites stop constructing the facade "
            "directly (R10-E Stage D / IMP03)."
        ),
    },
    "_effective_resource_catalog": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_resource_catalog",
        "expected_adapter": (
            "Effective MCP resource catalog (R11-A) — the composition root injects "
            "edition catalogs behind McpResourceCatalog before the catalog is frozen."
        ),
        "retirement_criterion": (
            "Remove the module global once the inbound MCP server resolves the "
            "effective resource catalog from RuntimeComposition instead of the "
            "process-wide register-before-freeze catalog bridge."
        ),
    },
    "_resource_catalog_frozen": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_resource_catalog",
        "expected_adapter": (
            "Catalog freeze guard (R11-A) — late resource-catalog injection fails "
            "closed after the composition root finishes registering providers."
        ),
        "retirement_criterion": (
            "Remove the module global when resource catalog lifecycle/freeze state is "
            "owned by RuntimeComposition or a scoped inbound MCP lifecycle provider."
        ),
    },
    "_RESOURCE_REGISTRY": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_resource_catalog",
        "expected_adapter": (
            "Read-only legacy projection (R11-A) derived from the effective MCP "
            "resource catalog for compatibility with existing resource consumers."
        ),
        "retirement_criterion": (
            "Remove the projection when all MCP resource consumers read from "
            "effective_resource_catalog() or RuntimeComposition directly."
        ),
    },
    "_worker": {
        "file": "okto_pulse/core/kg/workers/cognitive_closeout.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "cognitive_closeout_worker",
        "expected_adapter": (
            "CognitiveCloseoutWorker runtime handle — started/stopped by app "
            "lifespan while the cognitive closeout worker is being moved behind "
            "composition-owned lifecycle control."
        ),
        "retirement_criterion": (
            "Remove the module global when the cognitive closeout worker is owned "
            "by RuntimeComposition/lifespan provider and resolved through the KG "
            "worker lifecycle port."
        ),
    },
}

#: Frozen inventory (``file::name``) of EXISTING global-mutation / ContextVar
#: singletons in core at spec #15. Anything detected outside this set is NEW and
#: blocks (register-before-remove). The four headline singletons that are
#: global/ContextVar appear here; ``_permission_cache`` is an in-place dict cache
#: tracked by name in SINGLETON_LEDGER, not by this detector.
BASELINE_SINGLETONS: frozenset[str] = frozenset(
    {
        "okto_pulse/core/api/kg_events_hub.py::_hub",
        "okto_pulse/core/events/dispatcher.py::_dispatcher",
        "okto_pulse/core/infra/auth.py::_auth_provider",
        "okto_pulse/core/infra/config.py::_settings_instance",
        "okto_pulse/core/infra/database.py::_engine",
        "okto_pulse/core/infra/database.py::_session_factory",
        "okto_pulse/core/infra/database.py::_last_stale_warn_at",
        "okto_pulse/core/infra/storage.py::_storage_provider",
        "okto_pulse/core/kg/backpressure.py::_default_gate",
        "okto_pulse/core/kg/connection_pool.py::_pool",
        "okto_pulse/core/kg/global_discovery/outbox_worker.py::_singleton",
        "okto_pulse/core/kg/global_discovery/schema.py::_global_db",
        "okto_pulse/core/kg/interfaces/registry.py::_registry",
        "okto_pulse/core/kg/interfaces/registry.py::_configured",
        "okto_pulse/core/kg/kg_service.py::_default_service",
        "okto_pulse/core/kg/primitives.py::_kuzu_executor",
        "okto_pulse/core/kg/scheduler_singleton.py::_scheduler",
        "okto_pulse/core/kg/session_manager.py::_singleton",
        "okto_pulse/core/kg/tier_power.py::_rate_limiter",
        "okto_pulse/core/kg/workers/cleanup.py::_singleton",
        "okto_pulse/core/kg/workers/consolidation.py::_singleton",
        "okto_pulse/core/kg/workers/cognitive_closeout.py::_worker",
        "okto_pulse/core/kg/workers/deterministic_worker.py::_whitelist_cache",
        "okto_pulse/core/kg/write_barrier.py::_current_mode",
        "okto_pulse/core/kg/write_barrier.py::_active_guards",
        "okto_pulse/core/mcp/server.py::_mcp_session_factory",
        "okto_pulse/core/mcp/server.py::_effective_resource_catalog",
        "okto_pulse/core/mcp/server.py::_resource_catalog_frozen",
        "okto_pulse/core/mcp/server.py::_RESOURCE_REGISTRY",
        "okto_pulse/core/mcp/server.py::_XML_SAFETY_DECORATED_COUNT",
        "okto_pulse/core/services/queue_health_service.py::_ALERT_FIRED_TOTAL",
        "okto_pulse/core/telemetry/event_store_registry.py::_factory",
        "okto_pulse/core/telemetry/product_aggregator_registry.py::_product_aggregator_factory",
        "okto_pulse/core/telemetry/publish_health_source_registry.py::_publish_health_source_provider",
        "okto_pulse/core/telemetry/sender_registry.py::_telemetry_sender_factory",
        "okto_pulse/core/telemetry/telemetry_port_registry.py::_telemetry_port_factory",
    }
)


@dataclass(frozen=True)
class SingletonOccurrence:
    """A module-global singleton found by the scanner."""

    name: str
    file: str
    kind: str  # "global_mutation" | "contextvar"

    @property
    def key(self) -> str:
        return f"{self.file}::{self.name}"


@dataclass(frozen=True)
class AntiSingletonGateInput:
    source_root: Path | None = None
    #: extra ``file::name`` keys to treat as already-baselined (tests).
    extra_baseline: tuple[str, ...] = ()
    #: scan only these files (relative posix); () = whole core tree.
    only_files: tuple[str, ...] = ()


def _default_source_root() -> Path:
    # src/okto_pulse/core/application/boundary/singleton_gate.py -> src/
    return Path(__file__).resolve().parents[4]


def _is_contextvar(value: ast.expr | None) -> bool:
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    return (isinstance(func, ast.Name) and func.id == "ContextVar") or (
        isinstance(func, ast.Attribute) and func.attr == "ContextVar"
    )


def _module_level_targets(tree: ast.Module) -> dict[str, ast.expr | None]:
    out: dict[str, ast.expr | None] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id.startswith("_"):
                    out[tgt.id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id.startswith("_"):
                out[node.target.id] = node.value
    return out


def _scan_module(rel: str, tree: ast.Module) -> list[SingletonOccurrence]:
    module_names = _module_level_targets(tree)
    global_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            global_names.update(node.names)
    found: list[SingletonOccurrence] = []
    for name, value in module_names.items():
        if name in global_names:
            kind = "global_mutation"
        elif _is_contextvar(value):
            kind = "contextvar"
        else:
            continue
        found.append(SingletonOccurrence(name=name, file=rel, kind=kind))
    return found


class AntiSingletonGate:
    """Blocks new module-global singletons; ledgers the known ones."""

    gate_id = "anti_singleton"

    def run(self, gate_input: AntiSingletonGateInput | None = None) -> GateReport:
        gate_input = gate_input or AntiSingletonGateInput()
        root = gate_input.source_root or _default_source_root()
        core = root / "okto_pulse" / "core"
        baseline = set(BASELINE_SINGLETONS) | set(gate_input.extra_baseline)

        occurrences: list[SingletonOccurrence] = []
        for py in sorted(core.rglob("*.py")):
            rel = py.relative_to(root).as_posix()
            if gate_input.only_files and rel not in gate_input.only_files:
                continue
            try:
                tree = ast.parse(py.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            occurrences.extend(_scan_module(rel, tree))

        new_singletons = [o for o in occurrences if o.key not in baseline]
        ledger_view = {
            name: {**meta, "status": "ledgered"}
            for name, meta in SINGLETON_LEDGER.items()
        }
        evidence = {
            "ledger": ledger_view,
            "baseline_count": len(baseline),
            "detected_count": len(occurrences),
            "new_singletons": [
                {"name": o.name, "file": o.file, "kind": o.kind}
                for o in sorted(new_singletons, key=lambda o: o.key)
            ],
            "scanned_root": core.relative_to(root).as_posix(),
        }

        if new_singletons:
            return GateReport(
                gate_id=self.gate_id,
                subject="core module-global singletons",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "new_singleton"},
                observed_value=sorted(o.key for o in new_singletons),
                expected_value=[],
                remediation_hint=(
                    "A new module-global singleton was introduced. Inject the "
                    "dependency through a RuntimeComposition provider/port instead. "
                    "If it is unavoidable transitional debt, register it in "
                    "BASELINE_SINGLETONS (and SINGLETON_LEDGER when it owns a runtime "
                    "resource) with owner, target provider and retirement criterion "
                    "(register-before-remove)."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core module-global singletons",
            status="baseline",
            severity="medium",
            owner="okto-pulse-core/architecture",
            evidence={**evidence, "error": "ledgered_singletons"},
            observed_value=sorted(SINGLETON_LEDGER),
            expected_value=sorted(SINGLETON_LEDGER),
            promotion_criteria=(
                "Known singletons tracked register-before-remove; promote each by "
                "wiring its target provider and meeting its retirement criterion."
            ),
            remediation_hint="No new singleton; existing inventory remains baselined.",
        )
