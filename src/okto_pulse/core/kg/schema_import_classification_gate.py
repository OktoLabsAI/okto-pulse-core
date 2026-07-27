"""Fail-closed inventory for the retired concrete graph schema facade."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

TARGET_MODULE = "okto_pulse.core.kg.schema"
TARGET_PARENT = "okto_pulse.core.kg"  # for `from okto_pulse.core.kg import schema`

# Importers under these locations are legitimate adapter-internal / allowlisted
# consumers of kg.schema and are NOT migration targets this phase.
ALLOWLIST_EMBEDDED_PREFIX = ""
ALLOWLIST_MIGRATION_FILES = frozenset()
#: The `kg migrate-schema` CLI (lives outside core/, documented for completeness).
ALLOWLIST_MIGRATION_CLI = ""

VERDICT_ADAPTER = "adapter_internal_legitimate"
VERDICT_ALLOWLISTED = "migration_allowlisted"
VERDICT_NEEDS_MIGRATION = "needs_migration"

#: (R05-C IMP2) Register-before-remove LEDGER of temporary exceptions: the core
#: production KG consumers that import a forbidden kg.schema direct-storage
#: symbol TODAY and are kept as a documented temporary exception until they are
#: migrated to the #06 ports (or encapsulated) — the embedded Kùzu runtime stays
#: ledgered until R05-E does the physical move + dependency cleanup. A consumer
#: in this ledger is NON-blocking (verdict ``migration_allowlisted``); a NEW
#: consumer NOT in this ledger / allowlist BLOCKS the oracle. Retirement: the
#: file drops out of the ledger when it stops importing the forbidden symbol.
#:
#: R05-C RULING (option 2): surfaces with a DIRECT, behaviour-equivalent #06 port
#: were MIGRATED off this ledger this wave (not ledgered):
#:   - ``services/main.py`` board-create bootstrap → GraphSchemaManager.ensure_bootstrapped;
#:   - the Community CLI/seed bootstrap surfaces (separate package, not scanned here);
#:   - every ASYNC + simple-linear ``open_board_connection`` Cypher call-site
#:     (class A) → GraphTransaction (``async with begin(board_id) as scope:
#:     scope.execute(...)``): get_kg_metrics + boost_node (api/kg_routes.py),
#:     _reset_last_recomputed_at (api/kg_tick.py), and the three
#:     canonical_learning_partition maintenance scans. ``api/kg_routes.py``,
#:     ``api/kg_tick.py`` and ``kg/canonical_learning_partition.py`` thereby
#:     dropped off the ledger. R-P2-05 then removed the direct safe-write/schema
#:     lifecycle imports from rebuild/consolidation/server routes by injecting the
#:     lifecycle step through the KG registry.
#:
#: What REMAINS below are REAL exceptions only (class B/C/D), each with an
#: OBJECTIVE R05-E removal criterion derived from its most-coupled category (see
#: ``_LEDGER_REASON_BY_CATEGORY`` + ``ledger_detail_for``). NB: the GraphTransaction
#: port DOES expose ``GraphTransactionScope.execute`` via ``begin(board_id)`` — the
#: reason the remaining ``open_board_connection`` consumers stay ledgered is NOT a
#: missing port surface, it is that they live in SYNCHRONOUS functions (class C)
#: where consuming the async ``begin`` context manager would change the
#: event-loop boundary, plus sync-context schema/lifecycle ops (async ports), the
#: ``apply_ladybug_lifecycle_step`` per-step callable injected into the rebuild
#: orchestrator (no port primitive), and adapter-internal Kùzu primitives that
#: move with the embedded runtime in R05-E (class D).
LEDGERED_EXCEPTIONS: frozenset[str] = frozenset()

#: R05-C: per-category retirement contract. Each ledgered file is mapped — via its
#: most-coupled kg.schema category — to an OBJECTIVE R05-E removal criterion plus
#: the human-readable reason it could NOT migrate to a #06 port THIS wave. The
#: target port itself comes from ``_SYMBOL_CLASS`` (per importer). This is how the
#: ruling's "owner / motivo / porta-alvo / critério objetivo de remoção" is
#: attached to every exception without a hand-maintained per-file table.
_LEDGER_REASON_BY_CATEGORY: dict[str, tuple[str, str]] = {
    "transaction": (
        "class C (sync→async boundary): consumes open_board_connection/"
        "BoardConnection from a SYNCHRONOUS function. The GraphTransaction port "
        "DOES expose GraphTransactionScope.execute via begin(board_id), but begin "
        "is an async context manager — adopting it here would change the "
        "event-loop boundary of a sync call path. (All ASYNC + simple-execute "
        "call-sites, class A, were already migrated to the port this wave.)",
        "R05-E: make the call-site async (or add a vetted sync bridge) and consume "
        "`async with graph_transaction.begin(board_id) as scope: scope.execute(...)`; "
        "the file then stops importing the symbol and drops off this ledger. "
        "(If a residual site is async but needs inline-query/result-mapping "
        "refactor — class B — migrate the query shape first.)",
    ),
    "ddl_schema": (
        "class C (sync→async boundary): runs schema DDL migration from a "
        "SYNCHRONOUS recovery/guard path; the GraphSchemaManager.migrate port is "
        "async and a sync→async bridge in this worker error path would risk "
        "event-loop conflicts (regression risk)",
        "R05-E: make the call-site async (or add a vetted sync bridge) and consume "
        "GraphSchemaManager.migrate; symbol import then disappears",
    ),
    "lifecycle": (
        "class C/D: performs a lifecycle op (close/bootstrap/purge) from a "
        "SYNCHRONOUS context (async port = boundary change), OR injects the "
        "per-step apply_ladybug_lifecycle_step CALLABLE into the rebuild "
        "orchestrator — GraphLifecycle exposes open/close/rebuild/purge but no "
        "per-step adapter primitive (no mature port for that injection)",
        "R05-E: migrate the call-site to async GraphLifecycle (or expose a "
        "lifecycle-step primitive on the port) and drop the symbol",
    ),
    "path_purge": (
        "resolves the board graph path / purges storage; the path read maps cleanly "
        "to GraphPathResolver (sync) but the file is co-coupled to an async "
        "GraphLifecycle.purge op, so it stays ledgered with its sibling symbol",
        "R05-E: migrate to GraphPathResolver + async GraphLifecycle.purge together",
    ),
    "adapter_internal_kuzu": (
        "adapter-internal Kùzu primitive (_open_kuzu_db/load_vector_extension/"
        "_is_ladybug_corruption_error) — legitimately moves WITH the embedded "
        "runtime, exactly like the providers/embedded/ adapters",
        "R05-E: relocate with the embedded Kùzu runtime to community behind "
        "SemanticGraphStore (physical move + Ladybug/asyncpg dep cleanup)",
    ),
    "schema_metadata": (
        "imports read-only schema metadata/constants (NODE_TYPES/REL_TYPES/"
        "SCHEMA_VERSION/...) — non-blocking formal schema, not a storage coupling",
        "R05-E (low priority): source the constants from GraphSchemaManager metadata",
    ),
    "read_query": (
        "imports a read-only query helper (vector_index_name/resolve_*) — "
        "non-blocking, no storage/transaction coupling",
        "R05-E (low priority): source via the SemanticGraphStore read API",
    ),
    "module_wildcard": (
        "bare module import exposes every kg.schema symbol (wildcard coupling)",
        "R05-E: replace the module import with explicit #06 port consumption",
    ),
}


def ledger_detail_for(importer: "KgSchemaImporter") -> dict:
    """Per-exception R05-E retirement contract: owner, the reason it stayed
    ledgered this wave, the target #06 port, and an OBJECTIVE removal criterion.
    Derived deterministically from the importer's most-coupled category."""
    reason, criterion = _LEDGER_REASON_BY_CATEGORY.get(
        importer.category, _LEDGER_REASON_BY_CATEGORY["read_query"]
    )
    return {
        "file": importer.file,
        "owner": importer.owner,
        "category": importer.category,
        "target_port": importer.target_port,
        "reason": reason,
        "r05e_removal_criterion": criterion,
    }

# symbol -> (category, target_port, blocking)
# blocking=True means a non-adapter/non-allowlisted importer of this symbol is a
# boundary violation (direct storage/transaction/lifecycle/path coupling).
_SYMBOL_CLASS: dict[str, tuple[str, str, bool]] = {
    "open_board_connection": ("transaction", "GraphTransaction", True),
    "BoardConnection": ("transaction", "GraphTransaction", True),
    "close_all_connections": ("lifecycle", "GraphLifecycle", True),
    "bootstrap_board_graph": ("lifecycle", "GraphLifecycle", True),
    "ensure_board_graph_bootstrapped": ("lifecycle", "GraphLifecycle", True),
    "reset_bootstrap_cache_for_tests": ("lifecycle", "GraphLifecycle", True),
    "apply_ladybug_lifecycle_step": ("lifecycle", "GraphLifecycle", True),
    "board_kuzu_path": ("path_purge", "GraphRuntimeStore/StorageRef", True),
    "purge_board_graph_storage": ("path_purge", "GraphLifecycle", True),
    "migrate_schema_for_board": ("ddl_schema", "GraphSchemaManager", True),
    "migrate_edge_metadata": ("ddl_schema", "GraphSchemaManager", True),
    "_open_kuzu_db": ("adapter_internal_kuzu", "SemanticGraphStore", True),
    "load_vector_extension": ("adapter_internal_kuzu", "SemanticGraphStore", True),
    "_is_ladybug_corruption_error": ("adapter_internal_kuzu", "SemanticGraphStore", True),
    # Read-only schema metadata / constants / query helpers — shared, non-blocking
    # (migrate to GraphSchemaManager/SemanticGraphStore later, lower priority).
    "NODE_TYPES": ("schema_metadata", "GraphSchemaManager", False),
    "REL_TYPES": ("schema_metadata", "GraphSchemaManager", False),
    "MULTI_REL_TYPES": ("schema_metadata", "GraphSchemaManager", False),
    "VECTOR_INDEX_TYPES": ("schema_metadata", "GraphSchemaManager", False),
    "EDGE_METADATA_COLUMNS": ("schema_metadata", "GraphSchemaManager", False),
    "STABLE_NODE_PROPERTIES": ("schema_metadata", "GraphSchemaManager", False),
    "EDGE_LAYERS": ("schema_metadata", "GraphSchemaManager", False),
    "SCHEMA_VERSION": ("schema_metadata", "GraphSchemaManager", False),
    "vector_index_name": ("read_query", "SemanticGraphStore", False),
    "resolve_relationship_endpoint_pair": ("read_query", "SemanticGraphStore", False),
    "stable_rel_type_entries": ("read_query", "SemanticGraphStore", False),
}
# A bare module import (`import ...kg.schema` / `from ...kg import schema`) exposes
# every symbol — treated as a wildcard, forbidden outside the allowlist.
_MODULE_WILDCARD = ("module_wildcard", "multiple", True)

# Baselines to reconcile (ac_1c413377): the validator reported 45 importer files;
# the local spec measured 42 files / 77 import statements.
VALIDATOR_BASELINE_IMPORTER_FILES = 45
SPEC_LOCAL_IMPORTER_FILES = 42
SPEC_LOCAL_IMPORT_STATEMENTS = 77

# Direct-storage reference baselines from the spec (recounted live below).
REF_BASELINE = {
    "open_board_connection": 118,
    "board_kuzu_path": 54,
    "close_all_connections": 15,
    "open_kuzu_db": 0,
    "apply_ladybug_lifecycle_step": 0,
    "affected_paths": 0,
    "purge_board_graph_storage": 0,
}

# Category precedence when a file imports several symbols — the most coupled wins.
_CATEGORY_RANK = {
    "transaction": 6,
    "ddl_schema": 5,
    "lifecycle": 4,
    "path_purge": 3,
    "adapter_internal_kuzu": 2,
    "module_wildcard": 7,
    "read_query": 1,
    "schema_metadata": 0,
}


@dataclass(frozen=True)
class KgSchemaImporter:
    file: str
    symbols: tuple[str, ...]
    category: str
    verdict: str
    target_port: str
    owner: str
    rationale: str
    blocking: bool


@dataclass
class KgSchemaClassificationReport:
    ok: bool  # True only when there is no out-of-allowlist blocking importer
    importers: list[KgSchemaImporter] = field(default_factory=list)
    violations: list[KgSchemaImporter] = field(default_factory=list)
    reconciliation: dict = field(default_factory=dict)
    recounted_refs: dict = field(default_factory=dict)
    counts_by_verdict: dict = field(default_factory=dict)
    allowlist: dict = field(default_factory=dict)
    #: (R05-C) the ledgered temporary-exception files actually present in the
    #: scan (deterministic, sorted) — the documented register-before-remove debt.
    ledgered_exceptions: list = field(default_factory=list)
    #: (R05-C B-PARTIAL) one record per ledgered file: owner / reason / target
    #: port / OBJECTIVE R05-E removal criterion (the ruling's per-exception
    #: contract). Sorted by file for determinism.
    ledger_detail: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "importer_count": len(self.importers),
            "violation_count": len(self.violations),
            "counts_by_verdict": self.counts_by_verdict,
            "reconciliation": self.reconciliation,
            "recounted_refs": self.recounted_refs,
            "allowlist": self.allowlist,
            "ledgered_exceptions": list(self.ledgered_exceptions),
            "ledger_detail": list(self.ledger_detail),
            "table": [
                {
                    "file": i.file,
                    "symbols": list(i.symbols),
                    "category": i.category,
                    "verdict": i.verdict,
                    "target_port": i.target_port,
                    "owner": i.owner,
                    "rationale": i.rationale,
                    "blocking": i.blocking,
                }
                for i in self.importers
            ],
        }


def default_package_path() -> Path:
    """Scan the whole ``okto_pulse`` package (core/ + tools/ + ...) so allowlisted
    importers outside ``core/`` — e.g. the ``kg migrate-schema`` CLI in ``tools/``
    — are part of the canonical inventory (spec #06 rework)."""
    return Path(__file__).resolve().parents[2]


def _norm_path(path: Path, base: Path) -> str:
    """Path relative to the package root with a leading ``core/`` stripped, so
    core files read as ``kg/...`` (the spec convention) while siblings keep their
    prefix (``tools/...``)."""
    rel = str(path.relative_to(base)).replace("\\", "/")
    if rel.startswith("core/"):
        rel = rel[len("core/") :]
    return rel


def _owner_for(rel_path: str) -> str:
    head = rel_path.split("/", 1)[0]
    return f"core-{head}" if head else "core"


def _is_allowlisted(rel_path: str) -> str | None:
    if ALLOWLIST_EMBEDDED_PREFIX and rel_path.startswith(ALLOWLIST_EMBEDDED_PREFIX):
        return VERDICT_ADAPTER
    if rel_path in ALLOWLIST_MIGRATION_FILES or rel_path == ALLOWLIST_MIGRATION_CLI:
        return VERDICT_ALLOWLISTED
    if rel_path in LEDGERED_EXCEPTIONS:
        # R05-C ledgered temporary exception — non-blocking (migration_allowlisted
        # verdict so the canonical-verdict contract is preserved).
        return VERDICT_ALLOWLISTED
    return None


def _classify_symbol(symbol: str) -> tuple[str, str, bool]:
    return _SYMBOL_CLASS.get(symbol, ("read_query", "SemanticGraphStore", False))


def _collect_symbols(tree: ast.AST) -> set[str]:
    symbols: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == TARGET_MODULE:
                symbols.update(a.name for a in node.names)
            elif module == TARGET_PARENT:
                for a in node.names:
                    if a.name == "schema":
                        symbols.add("<module:schema>")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == TARGET_MODULE:
                    symbols.add("<module:schema>")
    return symbols


def _classify_file(rel_path: str, symbols: set[str]) -> KgSchemaImporter:
    allow_verdict = _is_allowlisted(rel_path)
    # Pick the most-coupled symbol to drive category/target_port/blocking.
    best = None
    best_rank = -1
    any_blocking = False
    for sym in symbols:
        if sym.startswith("<module"):
            category, target, blocking = _MODULE_WILDCARD
        else:
            category, target, blocking = _classify_symbol(sym)
        any_blocking = any_blocking or blocking
        rank = _CATEGORY_RANK.get(category, 0)
        if rank > best_rank:
            best_rank, best = rank, (sym, category, target, blocking)
    sym_name, category, target, _sym_blocking = best or ("", "read_query", "SemanticGraphStore", False)

    if allow_verdict is not None:
        verdict = allow_verdict
        blocking = False  # allowlisted/adapter/ledgered consumers are not violations
        if allow_verdict == VERDICT_ADAPTER:
            rationale = (
                "adapter-internal embedded provider — legitimate direct kg.schema use"
            )
        elif rel_path in LEDGERED_EXCEPTIONS:
            _reason, _criterion = _LEDGER_REASON_BY_CATEGORY.get(
                category, _LEDGER_REASON_BY_CATEGORY["read_query"]
            )
            rationale = (
                f"R05-C ledgered REAL exception ({category}) — {_reason}. "
                f"{_criterion}"
            )
        else:
            rationale = "allowlisted migration tooling — temporary direct kg.schema use"
    else:
        verdict = VERDICT_NEEDS_MIGRATION
        blocking = any_blocking
        if blocking:
            rationale = (
                f"non-adapter consumer uses forbidden kg.schema symbol "
                f"('{sym_name}', {category}); migrate to {target}"
            )
        else:
            rationale = (
                f"non-adapter consumer imports read-only kg.schema "
                f"{category}; migrate to {target} (low priority, non-blocking)"
            )

    return KgSchemaImporter(
        file=rel_path,
        symbols=tuple(sorted(symbols)),
        category=category,
        verdict=verdict,
        target_port=target,
        owner=_owner_for(rel_path),
        rationale=rationale,
        blocking=blocking,
    )


def _count_refs(core_root: Path) -> dict:
    refs = {k: 0 for k in REF_BASELINE}
    self_file = Path(__file__).resolve()
    for path in core_root.rglob("*.py"):
        if path.resolve() == self_file:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            symbol = None
            if isinstance(node, ast.Name):
                symbol = node.id
            elif isinstance(node, ast.Attribute):
                symbol = node.attr
            if symbol in refs:
                refs[symbol] += 1
    return refs


def run_kg_schema_import_classification_gate(
    root: str | Path | None = None,
) -> KgSchemaClassificationReport:
    """Scan ``root`` (default: the whole ``okto_pulse`` package) and classify
    every importer of ``okto_pulse.core.kg.schema`` — including allowlisted
    importers outside ``core/`` (the ``kg migrate-schema`` CLI in ``tools/``)."""
    base = Path(root) if root is not None else default_package_path()
    importers: list[KgSchemaImporter] = []
    import_statements = 0
    # The source module + this gate are never importers (norm paths).
    _excluded = {"kg/schema.py", "kg/schema_import_classification_gate.py"}

    for path in sorted(base.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        rel = _norm_path(path, base)
        if rel in _excluded:
            continue
        symbols = _collect_symbols(tree)
        if not symbols:
            continue
        # Count import statements (ac_1c413377 reconciliation denominator).
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "") in (
                TARGET_MODULE,
                TARGET_PARENT,
            ):
                if (node.module or "") == TARGET_PARENT and not any(
                    a.name == "schema" for a in node.names
                ):
                    continue
                import_statements += 1
            elif isinstance(node, ast.Import) and any(
                a.name == TARGET_MODULE for a in node.names
            ):
                import_statements += 1
        importers.append(_classify_file(rel, symbols))

    importers.sort(key=lambda i: (not i.blocking, i.file))
    violations = [i for i in importers if i.blocking]
    counts_by_verdict: dict[str, int] = {}
    for i in importers:
        counts_by_verdict[i.verdict] = counts_by_verdict.get(i.verdict, 0) + 1

    current_files = len(importers)
    reconciliation = {
        "validator_baseline_importer_files": VALIDATOR_BASELINE_IMPORTER_FILES,
        "spec_local_importer_files": SPEC_LOCAL_IMPORTER_FILES,
        "spec_local_import_statements": SPEC_LOCAL_IMPORT_STATEMENTS,
        "current_importer_files": current_files,
        "current_import_statements": import_statements,
        "explanation": (
            f"Scanning the whole okto_pulse package (core/ + tools/; the `kg migrate-schema` "
            f"CLI path is scanned and allowlisted when it imports kg.schema), the live recount "
            f"finds {current_files} importer files / {import_statements} "
            f"import statements vs the validator baseline of {VALIDATOR_BASELINE_IMPORTER_FILES} and "
            f"the spec-local {SPEC_LOCAL_IMPORTER_FILES} files / {SPEC_LOCAL_IMPORT_STATEMENTS} "
            f"statements. The historical numbers explain the migration baseline; the closure "
            f"oracle requires both current values and every direct primitive reference to be zero."
        ),
    }

    current_refs = _count_refs(base)
    recounted_refs = {
        "baseline": dict(REF_BASELINE),
        "current": current_refs,
    }

    ledgered_present = sorted(
        i.file for i in importers if i.file in LEDGERED_EXCEPTIONS
    )
    ledger_detail = [
        ledger_detail_for(i)
        for i in sorted(
            (i for i in importers if i.file in LEDGERED_EXCEPTIONS),
            key=lambda i: i.file,
        )
    ]

    return KgSchemaClassificationReport(
        ok=not violations and not any(current_refs.values()),
        importers=importers,
        violations=violations,
        reconciliation=reconciliation,
        recounted_refs=recounted_refs,
        counts_by_verdict=counts_by_verdict,
        allowlist={
            "embedded_prefix": ALLOWLIST_EMBEDDED_PREFIX,
            "migration_files": sorted(ALLOWLIST_MIGRATION_FILES),
            "migration_cli": ALLOWLIST_MIGRATION_CLI,
            "ledgered_exceptions": sorted(LEDGERED_EXCEPTIONS),
        },
        ledgered_exceptions=ledgered_present,
        ledger_detail=ledger_detail,
    )
