"""AST gate for Community imports in the core test suite.

The core test suite may mention ``okto_pulse.community`` as text when it is
testing boundary behavior, but executable imports belong either in the Community
repo or behind an explicit, reviewed conformance exception. This scanner is
syntax-only: it never imports the Community package.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .report import GateReport, GateStatus

COMMUNITY_PREFIX = "okto_pulse.community"
CORE_TEST_IMPORT_GATE_ID = "core_test_suite_community_imports"

CommunityImportAction = Literal[
    "allowlisted_conformance",
    "community_integration_marker",
    "move_to_community",
    "rewrite_with_core_fake",
]
CoreTestCommunityImportMode = Literal["inventory", "strict"]
CommunityRuntimeDependencyScope = Literal["module", "node"]


@dataclass(frozen=True)
class CoreTestCommunityImportSite:
    file: str
    line: int
    import_kind: str
    imported: str
    action: str | None
    owner: str | None
    justification: str | None
    allowed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "line": self.line,
            "import_kind": self.import_kind,
            "imported": self.imported,
            "action": self.action,
            "owner": self.owner,
            "justification": self.justification,
            "allowed": self.allowed,
        }


@dataclass(frozen=True)
class CoreTestCommunityImportClassification:
    file: str
    import_kind: str
    imported: str
    expected_count: int
    action: CommunityImportAction
    owner: str
    justification: str

    @property
    def key(self) -> tuple[str, str, str]:
        return self.file, self.import_kind, self.imported

    @property
    def allowed(self) -> bool:
        return self.action == "allowlisted_conformance"

    def as_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "import_kind": self.import_kind,
            "imported": self.imported,
            "expected_count": self.expected_count,
            "action": self.action,
            "owner": self.owner,
            "justification": self.justification,
        }


@dataclass(frozen=True)
class CoreTestCommunityRuntimeDependency:
    selector: str
    scope: CommunityRuntimeDependencyScope
    adapter: str
    owner: str
    justification: str

    def matches(self, nodeid: str, file: str) -> bool:
        if self.scope == "module":
            return file == self.selector
        return nodeid == self.selector or nodeid.startswith(f"{self.selector}[")

    def as_dict(self) -> dict[str, object]:
        return {
            "selector": self.selector,
            "scope": self.scope,
            "adapter": self.adapter,
            "owner": self.owner,
            "justification": self.justification,
        }


def _cls(
    file: str,
    import_kind: str,
    imported: str,
    expected_count: int,
    action: CommunityImportAction,
    *,
    owner: str = "okto-pulse-core/architecture",
    justification: str = "AF-04 inventory: migrate adapter coverage to Community or rewrite against a core-owned fake.",
) -> CoreTestCommunityImportClassification:
    return CoreTestCommunityImportClassification(
        file=file,
        import_kind=import_kind,
        imported=imported,
        expected_count=expected_count,
        action=action,
        owner=owner,
        justification=justification,
    )


def _rt(
    selector: str,
    scope: CommunityRuntimeDependencyScope = "module",
    *,
    adapter: str = "okto_pulse.community.adapters.kg_runtime",
    owner: str = "okto-pulse-core/architecture",
    justification: str = (
        "AF-04 inventory: this test exercises a Community-owned graph/runtime "
        "adapter and must skip explicitly in core-only runs."
    ),
) -> CoreTestCommunityRuntimeDependency:
    return CoreTestCommunityRuntimeDependency(
        selector=selector,
        scope=scope,
        adapter=adapter,
        owner=owner,
        justification=justification,
    )


DEFAULT_CORE_TEST_COMMUNITY_IMPORT_CLASSIFICATIONS: tuple[
    CoreTestCommunityImportClassification, ...
] = (
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.board_source_reader",
        2,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.board_rebuild_ingestion",
        1,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.board_graph_runtime",
        2,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.kg",
        1,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.kg_runtime",
        4,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.global_discovery_runtime",
        1,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.data",
        2,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/kg_registry_testing.py",
        "from",
        "okto_pulse.community.adapters.kuzu_graph_lifecycle",
        1,
        "community_integration_marker",
        justification=(
            "Helper must distinguish contract-fake mode from Community integration mode."
        ),
    ),
    _cls(
        "tests/test_amendment_kg_rebuild.py",
        "from",
        "okto_pulse.community.adapters.board_source_reader",
        4,
        "move_to_community",
    ),
    _cls(
        "tests/test_amendment_kg_rebuild.py",
        "from",
        "okto_pulse.community.adapters.board_rebuild_ingestion",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_events.py",
        "import",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_graph_unavailable.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_03a_1_decision_enumeration.py",
        "importorskip",
        "okto_pulse.community.adapters.board_source_reader",
        1,
        "rewrite_with_core_fake",
    ),
    _cls(
        "tests/test_kg_board_rebuild_adapter.py",
        "importorskip",
        "okto_pulse.community.adapters.board_rebuild_ingestion",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_board_rebuild_adapter.py",
        "from",
        "okto_pulse.community.adapters",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_bootstrap_autoheal.py",
        "from",
        "okto_pulse.community.adapters",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_db_cache_lru.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_edge_metadata_migration.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_interrupted_checkpoint_recovery.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_lifecycle_nondestructive.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_migration_self_heal.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_priority_boost_integration.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_quarantine.py",
        "import",
        "okto_pulse.community.adapters.kg_runtime",
        2,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_r2_test2.py",
        "importorskip",
        "okto_pulse.community.adapters.board_rebuild_ingestion",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_r2_test2.py",
        "importorskip",
        "okto_pulse.community.adapters.board_source_reader",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_real_integration.py",
        "importorskip",
        "okto_pulse.community.adapters.embedding",
        2,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_rebuild_sources.py",
        "importorskip",
        "okto_pulse.community.adapters.board_source_reader",
        1,
        "rewrite_with_core_fake",
    ),
    _cls(
        "tests/test_kg_relevance_dynamic.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_relevance_dynamic.py",
        "importorskip",
        "okto_pulse.community.adapters.kuzu_graph_store",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_relevance_score_migration.py",
        "importorskip",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kg_schema_vector_extension.py",
        "import",
        "okto_pulse.community.adapters.kg_runtime",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_kuzu_memory_config.py",
        "import",
        "okto_pulse.community.adapters.kg_runtime",
        2,
        "move_to_community",
    ),
    _cls(
        "tests/test_r08b_auth_context_bridge.py",
        "importorskip",
        "okto_pulse.community.adapters.mcp_auth",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_r08d_auth_replay_gates.py",
        "importorskip",
        "okto_pulse.community.adapters.mcp_auth",
        2,
        "move_to_community",
    ),
    _cls(
        "tests/test_r09_global_discovery_runtime.py",
        "from",
        "okto_pulse.community.adapters.global_discovery_runtime",
        4,
        "move_to_community",
    ),
    _cls(
        "tests/test_r10a_board_source_reader.py",
        "importorskip",
        "okto_pulse.community.adapters.board_source_reader",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_r10b_rebuild_ingestion_port.py",
        "from",
        "okto_pulse.community.adapters.board_rebuild_ingestion",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_r10b_rebuild_ingestion_port.py",
        "from",
        "okto_pulse.community.adapters.composition",
        1,
        "move_to_community",
    ),
    _cls(
        "tests/test_r17_core_settings_defaults_gate.py",
        "importorskip",
        "okto_pulse.community.config",
        1,
        "allowlisted_conformance",
        owner="okto-pulse-core/settings-conformance",
        justification=(
            "R17 sanctioned parity check for CommunitySettings defaults; this is not a generic Community test-suite exception."
        ),
    ),
)

DEFAULT_CORE_TEST_COMMUNITY_RUNTIME_DEPENDENCIES: tuple[
    CoreTestCommunityRuntimeDependency, ...
] = (
    _rt("tests/test_amendment_kg_rebuild.py", adapter="okto_pulse.community.adapters.board_source_reader"),
    _rt("tests/test_discovery_learnings_by_relevance.py"),
    _rt(
        "tests/test_events.py::test_human_curated_migration_helper_exists_and_is_called",
        "node",
    ),
    _rt("tests/test_kg_bootstrap_autoheal.py"),
    _rt("tests/test_kg_confirmed_rebuild_safety.py"),
    _rt("tests/test_kg_consolidation_rejects_criterion_constraint.py"),
    _rt("tests/test_kg_foundation.py"),
    _rt("tests/test_kg_global_discovery.py"),
    _rt("tests/test_kg_global_discovery_health.py"),
    _rt("tests/test_kg_global_discovery_vector_types.py"),
    _rt("tests/test_kg_layer_propagation.py"),
    _rt("tests/test_kg_memory_pressure_proxy.py"),
    _rt("tests/test_kg_metrics_endpoint.py"),
    _rt("tests/test_kg_natural_query_layer.py"),
    _rt("tests/test_kg_orphan_integrity.py"),
    _rt("tests/test_kg_primitives_connectivity_guard.py"),
    _rt("tests/test_kg_quarantine.py"),
    _rt("tests/test_kg_r1_imp1.py"),
    _rt("tests/test_kg_r1_imp2.py"),
    _rt("tests/test_kg_r1_imp3.py"),
    _rt("tests/test_kg_r2_imp1.py"),
    _rt("tests/test_kg_r2_imp2.py"),
    _rt("tests/test_kg_r2_imp3.py"),
    _rt("tests/test_kg_r2_imp4.py"),
    _rt("tests/test_kg_r2_imp5.py"),
    _rt("tests/test_kg_r2_test1.py"),
    _rt("tests/test_kg_r2_test3.py"),
    _rt("tests/test_kg_r2_test4.py"),
    _rt("tests/test_kg_r2_test5.py"),
    _rt("tests/test_kg_r2_test6.py"),
    _rt("tests/test_kg_r2_test7.py"),
    _rt("tests/test_kg_r2_test8.py"),
    _rt("tests/test_kg_r7_imp1.py"),
    _rt("tests/test_kg_r7_imp2.py"),
    _rt("tests/test_kg_r7_imp3.py"),
    _rt("tests/test_kg_r7_imp4.py"),
    _rt("tests/test_kg_r7_imp5.py"),
    _rt("tests/test_kg_rebuild_preflight.py"),
    _rt("tests/test_kg_rebuild_service.py", adapter="okto_pulse.community.adapters.kuzu_graph_lifecycle"),
    _rt("tests/test_kg_s_kg_02_canonical_learning.py"),
    _rt("tests/test_kg_schema_lifecycle.py"),
    _rt("tests/test_kg_schema_vector_extension.py"),
    _rt("tests/test_kg_spec_children_canonicalization.py"),
    _rt("tests/test_kg_spec_draft_canonical_leak.py"),
    _rt(
        "tests/test_rkg04_connectivity_dlq_reprocess.py::test_ts2_connectivity_dlq_drains_to_graph",
        "node",
        owner="okto-pulse-core/rkg04",
        justification=(
            "AF-04 inventory: dual-environment validation showed this queue-to-graph "
            "drain test requires the Community graph runtime to preserve transaction "
            "and MATCH semantics; core-only runs must skip it explicitly."
        ),
    ),
    _rt(
        "tests/test_kg_tier_power.py::TestNLQuery::test_query_exact_fallback_finds_bug_without_vector_index_hit",
        "node",
    ),
    _rt("tests/test_kuzu_memory_config.py"),
    _rt(
        "tests/test_r09_global_discovery_runtime.py::test_community_global_discovery_bootstrap_with_token_runs_schema_ddl",
        "node",
        adapter="okto_pulse.community.adapters.global_discovery_runtime",
    ),
    _rt(
        "tests/test_r09_global_discovery_runtime.py::test_community_global_discovery_purge_with_token_quarantines_targets",
        "node",
        adapter="okto_pulse.community.adapters.global_discovery_runtime",
    ),
    _rt(
        "tests/test_r09_global_discovery_runtime.py::test_community_global_discovery_close_reopen_preserves_query_results",
        "node",
        adapter="okto_pulse.community.adapters.global_discovery_runtime",
    ),
    _rt(
        "tests/test_r09_global_discovery_runtime.py::test_community_global_discovery_purge_without_token_does_not_mutate",
        "node",
        adapter="okto_pulse.community.adapters.global_discovery_runtime",
    ),
    _rt(
        "tests/test_kg_write_barrier_wiring.py::test_bootstrap_global_discovery_blocked_in_strict_without_guard",
        "node",
        adapter="okto_pulse.community.adapters.global_discovery_runtime",
        owner="okto-pulse-core/write-barrier",
        justification=(
            "AF-04 inventory: dynamic write-barrier coverage exercises the Community "
            "global-discovery runtime path, so core-only runs must not fake it."
        ),
    ),
    _rt(
        "tests/test_kg_write_barrier_wiring.py::test_purge_global_discovery_storage_blocked_in_strict_without_guard",
        "node",
        adapter="okto_pulse.community.adapters.global_discovery_runtime",
        owner="okto-pulse-core/write-barrier",
        justification=(
            "AF-04 inventory: dynamic write-barrier coverage exercises the Community "
            "global-discovery runtime path, so core-only runs must not fake it."
        ),
    ),
    _rt(
        "tests/test_r10b_rebuild_ingestion_port.py::test_community_composition_registers_real_rebuild_ingestion_provider",
        "node",
        adapter="okto_pulse.community.adapters.board_rebuild_ingestion",
    ),
    _rt(
        "tests/test_r6_imp3_schema_safe_and_graph_layer_echo.py::test_schema_info_includes_label_properties",
        "node",
    ),
    _rt(
        "tests/test_r6_imp3_schema_safe_and_graph_layer_echo.py::test_query_global_echoes_applied_graph_layer",
        "node",
    ),
    _rt(
        "tests/test_r6_imp3_schema_safe_and_graph_layer_echo.py::test_query_global_invalid_graph_layer_fails_closed",
        "node",
    ),
    _rt(
        "tests/test_r6_imp3_schema_safe_and_graph_layer_echo.py::test_get_related_context_echoes_and_fails_closed",
        "node",
    ),
    _rt(
        "tests/test_r6_imp3_schema_safe_and_graph_layer_echo.py::test_query_global_all_recovers_working_via_linear_fallback",
        "node",
    ),
)


@dataclass(frozen=True)
class CoreTestCommunityImportReport:
    mode: CoreTestCommunityImportMode
    status: GateStatus
    scanned_files: int
    sites: tuple[CoreTestCommunityImportSite, ...]
    blocking: tuple[CoreTestCommunityImportSite, ...]
    count_violations: tuple[dict[str, object], ...]
    retired_expected_entries: tuple[dict[str, object], ...]

    def as_gate_report(self) -> GateReport:
        return GateReport(
            gate_id=CORE_TEST_IMPORT_GATE_ID,
            subject="core test suite Community imports",
            status=self.status,
            severity="high" if self.status in ("blocking", "reject") else "medium",
            owner="okto-pulse-core/architecture" if self.status != "passed" else None,
            evidence={
                "mode": self.mode,
                "scanned_files": self.scanned_files,
                "sites": [site.as_dict() for site in self.sites],
                "blocking": [site.as_dict() for site in self.blocking],
                "count_violations": list(self.count_violations),
                "retired_expected_entries": list(self.retired_expected_entries),
            },
            observed_value=len(self.blocking) + len(self.count_violations),
            expected_value=0,
            promotion_criteria=(
                "Move Community adapter tests to the Community repo or rewrite core tests with core-owned fakes."
            )
            if self.status != "passed"
            else None,
            remediation_hint=(
                "Classify each executable Community import as move_to_community, "
                "rewrite_with_core_fake, community_integration_marker, or "
                "allowlisted_conformance. In strict mode only allowlisted "
                "conformance entries may remain."
            )
            if self.status != "passed"
            else None,
        )


def default_core_tests_root() -> Path:
    return Path(__file__).resolve().parents[5] / "tests"


def find_core_test_community_runtime_dependency(
    nodeid: str,
    *,
    dependencies: tuple[CoreTestCommunityRuntimeDependency, ...] = (
        DEFAULT_CORE_TEST_COMMUNITY_RUNTIME_DEPENDENCIES
    ),
) -> CoreTestCommunityRuntimeDependency | None:
    normalized = nodeid.replace("\\", "/")
    file = normalized.split("::", 1)[0]
    for dependency in dependencies:
        if dependency.matches(normalized, file):
            return dependency
    return None


def community_runtime_dependency_skip_reason(
    nodeid: str,
    *,
    community_available: bool,
    dependencies: tuple[CoreTestCommunityRuntimeDependency, ...] = (
        DEFAULT_CORE_TEST_COMMUNITY_RUNTIME_DEPENDENCIES
    ),
) -> str | None:
    dependency = find_core_test_community_runtime_dependency(
        nodeid,
        dependencies=dependencies,
    )
    if dependency is None or community_available:
        return None
    return (
        "Community KG integration adapter is unavailable; "
        f"{dependency.selector} explicitly requires {dependency.adapter}. "
        "Core-only runs skip this Community-owned adapter coverage instead of "
        "falling back to in-memory fakes."
    )


def scan_core_test_community_imports(
    test_root: str | Path | None = None,
    *,
    mode: CoreTestCommunityImportMode = "inventory",
    classifications: tuple[CoreTestCommunityImportClassification, ...] = (
        DEFAULT_CORE_TEST_COMMUNITY_IMPORT_CLASSIFICATIONS
    ),
) -> CoreTestCommunityImportReport:
    root = Path(test_root) if test_root is not None else default_core_tests_root()
    label_root = root.parent if root.name == "tests" else root
    classification_by_key = {entry.key: entry for entry in classifications}
    expected_counts = {entry.key: entry.expected_count for entry in classifications}
    observed_counts: Counter[tuple[str, str, str]] = Counter()
    sites: list[CoreTestCommunityImportSite] = []
    scanned_files = 0

    for path in sorted(root.rglob("*.py")) if root.exists() else []:
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, ValueError):
            continue
        scanned_files += 1
        rel = path.relative_to(label_root).as_posix()
        for import_kind, lineno, imported in _community_imports_in_tree(tree):
            key = rel, import_kind, imported
            observed_counts[key] += 1
            classification = classification_by_key.get(key)
            allowed = bool(classification and classification.allowed)
            sites.append(
                CoreTestCommunityImportSite(
                    file=rel,
                    line=lineno,
                    import_kind=import_kind,
                    imported=imported,
                    action=classification.action if classification else None,
                    owner=classification.owner if classification else None,
                    justification=classification.justification if classification else None,
                    allowed=allowed,
                )
            )

    count_violations = tuple(
        {
            "file": file,
            "import_kind": import_kind,
            "imported": imported,
            "observed_count": observed,
            "expected_count": expected_counts[key],
            "reason": "observed_count_exceeds_classification",
        }
        for key, observed in sorted(observed_counts.items())
        if key in expected_counts
        and observed > expected_counts[key]
        for file, import_kind, imported in [key]
    )
    retired_expected_entries = tuple(
        {
            "file": file,
            "import_kind": import_kind,
            "imported": imported,
            "observed_count": observed_counts.get(key, 0),
            "expected_count": expected,
            "reason": "classified_import_not_observed",
        }
        for key, expected in sorted(expected_counts.items())
        if observed_counts.get(key, 0) < expected
        for file, import_kind, imported in [key]
    )

    if mode == "strict":
        blocking = tuple(site for site in sites if not site.allowed)
    else:
        blocking = tuple(site for site in sites if site.action is None)

    status: GateStatus
    if blocking or count_violations:
        status = "blocking"
    elif any(not site.allowed for site in sites):
        status = "baseline"
    else:
        status = "passed"

    return CoreTestCommunityImportReport(
        mode=mode,
        status=status,
        scanned_files=scanned_files,
        sites=tuple(sorted(sites, key=lambda site: (site.file, site.line, site.imported))),
        blocking=tuple(sorted(blocking, key=lambda site: (site.file, site.line, site.imported))),
        count_violations=count_violations,
        retired_expected_entries=retired_expected_entries,
    )


def run_core_test_community_import_gate(
    test_root: str | Path | None = None,
    *,
    mode: CoreTestCommunityImportMode = "inventory",
    classifications: tuple[CoreTestCommunityImportClassification, ...] = (
        DEFAULT_CORE_TEST_COMMUNITY_IMPORT_CLASSIFICATIONS
    ),
) -> GateReport:
    return scan_core_test_community_imports(
        test_root,
        mode=mode,
        classifications=classifications,
    ).as_gate_report()


def _community_imports_in_tree(tree: ast.AST) -> tuple[tuple[str, int, str], ...]:
    imports: list[tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_community_module(alias.name):
                    imports.append(("import", node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            module = node.module or ""
            if _is_community_module(module):
                imports.append(("from", node.lineno, module))
        elif isinstance(node, ast.Call):
            imported = _importorskip_module(node)
            if imported and _is_community_module(imported):
                imports.append(("importorskip", node.lineno, imported))
    return tuple(imports)


def _is_community_module(module: str) -> bool:
    return module == COMMUNITY_PREFIX or module.startswith(f"{COMMUNITY_PREFIX}.")


def _importorskip_module(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        name = func.id
    elif isinstance(func, ast.Attribute):
        name = func.attr
    else:
        return None
    if name != "importorskip" or not node.args:
        return None
    arg = node.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    return None


__all__ = [
    "COMMUNITY_PREFIX",
    "CORE_TEST_IMPORT_GATE_ID",
    "DEFAULT_CORE_TEST_COMMUNITY_IMPORT_CLASSIFICATIONS",
    "DEFAULT_CORE_TEST_COMMUNITY_RUNTIME_DEPENDENCIES",
    "CommunityImportAction",
    "CommunityRuntimeDependencyScope",
    "CoreTestCommunityImportClassification",
    "CoreTestCommunityImportMode",
    "CoreTestCommunityImportReport",
    "CoreTestCommunityImportSite",
    "CoreTestCommunityRuntimeDependency",
    "community_runtime_dependency_skip_reason",
    "default_core_tests_root",
    "find_core_test_community_runtime_dependency",
    "run_core_test_community_import_gate",
    "scan_core_test_community_imports",
]
