"""F05 service decomposition inventory and zero-relational gate.

The inventory records where every mixed Core module is being split.  The
scanner is intentionally independent from the historical relational baselines:
F05 closes the debt, so its terminal budget is zero rather than a ratchet.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class ServiceFacet(str, Enum):
    RULE = "rule"
    ORCHESTRATION = "orchestration"
    QUERY = "query"
    EFFECT = "effect"


@dataclass(frozen=True, slots=True)
class ServiceSlice:
    name: str
    files: tuple[str, ...]
    core_responsibilities: str
    community_responsibilities: str


@dataclass(frozen=True, slots=True)
class MethodClassification:
    file: str
    symbol: str
    facets: tuple[ServiceFacet, ...]


@dataclass(frozen=True, slots=True)
class RelationalOwnershipViolation:
    file: str
    line: int
    module: str
    slice_name: str | None


F05_SERVICE_SLICES: tuple[ServiceSlice, ...] = (
    ServiceSlice(
        "runtime_processors",
        (
            "application/processors/consolidation.py",
            "application/processors/event_delivery.py",
            "application/processors/global_outbox.py",
        ),
        "claim policy, retry state, ordering and processor orchestration",
        "SQL claims, queue persistence, outbox persistence and transaction mechanics",
    ),
    ServiceSlice(
        "domain_events",
        (
            "events/bus.py",
            "events/handlers/cancellation_decay.py",
            "events/handlers/card_boost_recompute.py",
            "events/handlers/cognitive_extraction.py",
            "events/handlers/consolidation_enqueuer.py",
            "events/handlers/discovery_selector_cache.py",
            "events/handlers/kg_hit_recompute.py",
        ),
        "event meaning, handler ordering and idempotency policy",
        "event rows, handler receipts and ORM-backed side effects",
    ),
    ServiceSlice(
        "amendment_and_regression",
        (
            "services/amendment_revision.py",
            "services/bug_regression_observability.py",
            "services/bug_regression_preview.py",
            "services/bug_regression_scenarios.py",
        ),
        "amendment lineage, eligibility and regression gates",
        "amendment, card and spec queries plus audit persistence",
    ),
    ServiceSlice(
        "analytics_and_discovery",
        (
            "services/analytics_service.py",
            "services/discovery_catalog_reader.py",
            "services/discovery_executor.py",
            "services/discovery_selector_catalog.py",
        ),
        "metric definitions, selector semantics and discovery authorization",
        "aggregate queries, projections, history and saved-search effects",
    ),
    ServiceSlice(
        "architecture_and_resources",
        (
            "services/architecture.py",
            "services/architecture_propagation_legacy.py",
            "services/design_system.py",
            "services/effective_resource_propagation.py",
            "services/spec_resource_propagation.py",
        ),
        "resource gates, propagation policy, version rules and design invariants",
        "architecture, resource, design-system and audit persistence",
    ),
    ServiceSlice(
        "board_workflow",
        (
            "services/board_governance.py",
            "services/critical_context_guard.py",
            "services/main.py",
            "services/persistence_mutation.py",
            "services/resource_gate.py",
            "services/skip_overrides.py",
        ),
        "workflow gates, state transitions, permissions and application orchestration",
        "board artifact queries, ORM mutation, activity logs and transaction effects",
    ),
    ServiceSlice(
        "default_board_configuration",
        (
            "services/default_board_config_api.py",
            "services/default_board_configuration.py",
        ),
        "configuration validation, merge rules and lifecycle policy",
        "configuration, guideline, design-system and audit rows",
    ),
    ServiceSlice(
        "kg_application_services",
        (
            "services/canonical_debt_service.py",
            "services/cognitive_effectiveness_service.py",
            "services/connectivity_dlq_reprocess_service.py",
            "services/dead_letter_inspector_service.py",
            "services/kg_health_readiness_service.py",
            "services/kg_health_service.py",
            "services/queue_health_service.py",
        ),
        "health classification, retry eligibility, readiness and debt policy",
        "queue, debt, audit and health read models plus persistence effects",
    ),
    ServiceSlice(
        "structured_specs",
        ("services/spec_structured_entities.py",),
        "structured requirement identity, validation and migration rules",
        "spec JSON mutation and ORM dirty tracking",
    ),
    ServiceSlice(
        "legacy_materialization_command",
        ("commands/materialize_legacy_fr_ac.py",),
        "legacy-to-structured conversion policy",
        "spec enumeration and persisted JSON mutation",
    ),
    ServiceSlice(
        "kg_relational_workflows",
        (
            "kg/bug_cognitive_closure.py",
            "kg/canonical_debt_replay.py",
            "kg/canonical_demotion_global_sync.py",
            "kg/canonical_learning_partition.py",
            "kg/canonical_partition_integrity.py",
            "kg/canonical_stale_reconciler.py",
            "kg/global_discovery/clustering.py",
            "kg/global_discovery/layer_parity.py",
            "kg/governance.py",
            "kg/parent_doc/parent_doc.py",
            "kg/stale_canonical_parity.py",
            "kg/transaction.py",
            "kg/workers/cognitive_closeout.py",
        ),
        "KG invariants, reconciliation, promotion, debt and integrity decisions",
        "relational lookups, queue/outbox mutation and concrete transaction access",
    ),
)


F05_FILE_TO_SLICE = {
    file: service_slice.name
    for service_slice in F05_SERVICE_SLICES
    for file in service_slice.files
}

_SCAN_DIRECTORIES = ("application", "commands", "domain", "events", "kg", "services")
_FORBIDDEN_MODULES = (
    "sqlalchemy",
    "okto_pulse.core.models.db",
    "okto_pulse.core.repositories.sqlalchemy",
)
_QUERY_SIGNALS = {"execute", "scalar", "scalars", "select", "get"}
_EFFECT_SIGNALS = {
    "add",
    "commit",
    "delete",
    "flag_modified",
    "flush",
    "refresh",
    "rollback",
    "update",
}


def _forbidden_module(module: str) -> bool:
    return any(module == root or module.startswith(f"{root}.") for root in _FORBIDDEN_MODULES)


def scan_f05_relational_violations(
    core_root: str | Path,
) -> tuple[RelationalOwnershipViolation, ...]:
    root = Path(core_root)
    findings: list[RelationalOwnershipViolation] = []
    for directory in _SCAN_DIRECTORIES:
        base = root / directory
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            relative = path.relative_to(root).as_posix()
            if relative.startswith("application/boundary/"):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                modules: tuple[str, ...] = ()
                if isinstance(node, ast.ImportFrom):
                    modules = (node.module or "",)
                elif isinstance(node, ast.Import):
                    modules = tuple(alias.name for alias in node.names)
                for module in modules:
                    if _forbidden_module(module):
                        findings.append(
                            RelationalOwnershipViolation(
                                file=relative,
                                line=node.lineno,
                                module=module,
                                slice_name=F05_FILE_TO_SLICE.get(relative),
                            )
                        )
    return tuple(findings)


def unassigned_f05_files(core_root: str | Path) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                finding.file
                for finding in scan_f05_relational_violations(core_root)
                if finding.slice_name is None
            }
        )
    )


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _classify_function(node: ast.AsyncFunctionDef | ast.FunctionDef) -> tuple[ServiceFacet, ...]:
    descendants = tuple(ast.walk(node))
    calls = {_call_name(item) for item in descendants if isinstance(item, ast.Call)}
    facets: set[ServiceFacet] = set()
    if calls & _QUERY_SIGNALS:
        facets.add(ServiceFacet.QUERY)
    if calls & _EFFECT_SIGNALS:
        facets.add(ServiceFacet.EFFECT)
    if any(isinstance(item, ast.Await) for item in descendants) or len(calls) > 1:
        facets.add(ServiceFacet.ORCHESTRATION)
    if any(isinstance(item, (ast.Compare, ast.If, ast.Raise)) for item in descendants):
        facets.add(ServiceFacet.RULE)
    if not facets:
        facets.add(ServiceFacet.RULE)
    return tuple(sorted(facets, key=lambda value: value.value))


def classify_f05_methods(
    core_root: str | Path,
) -> tuple[MethodClassification, ...]:
    root = Path(core_root)
    classifications: list[MethodClassification] = []
    for relative in sorted(F05_FILE_TO_SLICE):
        path = root / relative
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for parent in tree.body:
            if isinstance(parent, (ast.AsyncFunctionDef, ast.FunctionDef)):
                classifications.append(
                    MethodClassification(relative, parent.name, _classify_function(parent))
                )
            elif isinstance(parent, ast.ClassDef):
                for child in parent.body:
                    if isinstance(child, (ast.AsyncFunctionDef, ast.FunctionDef)):
                        classifications.append(
                            MethodClassification(
                                relative,
                                f"{parent.name}.{child.name}",
                                _classify_function(child),
                            )
                        )
    return tuple(classifications)


__all__ = [
    "F05_FILE_TO_SLICE",
    "F05_SERVICE_SLICES",
    "MethodClassification",
    "RelationalOwnershipViolation",
    "ServiceFacet",
    "ServiceSlice",
    "classify_f05_methods",
    "scan_f05_relational_violations",
    "unassigned_f05_files",
]
