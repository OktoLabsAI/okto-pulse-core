"""Cross-surface ORM consumer inventory + split-inventory guard
(SaaS Refactor spec R01C, FR4/FR5 evidence, AC5 ac_0f56b2b2, TR tr_65b25d4d;
negative scenario ``ts_c9653b5c``).

Two evidence artifacts for the relational-ownership cut:

1. **Cross-surface inventory** (:func:`build_orm_consumer_inventory`) — a versioned,
   per-call-site inventory of every core import/consumer of an ORM-definition
   primitive (``Base``/``TypeDecorator``/``declarative_base``/``core.models.db``
   ORM, via the *single-source* :mod:`core_orm_import_gate` detector) AND of the
   relational provider surface (``core.infra.database``). Each record is
   classified by ``surface`` (rest/mcp/service/worker/cli/seed/bootstrap/
   definition/repository), ``group_id``, ``owner`` and ``status``. The two
   categories are kept distinct so the inventory never conflates the **removal
   target** (ORM definition → Community, R01C) with the **preserved provider**
   (``core.infra.database`` engine/session, owned by R01B, kept until the
   definitive provider is registered — ac_af454ee3).

   Anti-baseline-theater: a current ORM-definition coupling is honest only if its
   file is registered in the gate allowlist with an owner + removal date
   (``status == temporary_exception``); a coupling outside the allowlist is
   ``unclassified`` and surfaces as a consistency error
   (:func:`inventory_consistency_errors`). A file claimed ``migrated`` is
   RE-SCANNED to prove it no longer couples (:func:`verify_migrated`) — a claim is
   never taken on faith.

2. **Split-inventory guard** (:func:`guard_orm_batch_removal`) — validates a batch
   ORM-removal declaration BEFORE any move. It FAILS a batch that lumps
   heterogeneous ORM groups together or omits ``group_id`` / ``owner`` /
   register-before-remove order / before-after inventory; it PASSES only small,
   per-cluster groups each carrying that metadata (``ts_c9653b5c``).

Pure static analysis (reuses the gate's ``ast`` detector); no runtime import of
scanned code.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

from okto_pulse.core.repositories.core_orm_import_gate import (
    _ALLOWLIST_CLUSTERS,
    CORE_ORM_IMPORT_ALLOWLIST,
    _label,
    _scan_module,
    allowlist_entry,
    build_module_alias_map,
    orm_definition_names,
    resolve_attribute_module,
    resolve_debt_ref,
)
from okto_pulse.core.repositories.debt import ORM_RETURN_DEBT, SESSION_BRIDGE_DEBT
from okto_pulse.core.repositories.relational_boundary_gate import default_core_path

#: ORM types for which a repository PORT exists — derived from the live
#: ``ORM_RETURN_DEBT`` ledger (the strangler's first-cut aggregates). A
#: ``<session>.get(M, pk)`` call is *port-backed* (routable to ``uow.<agg>.get``)
#: iff ``M`` ∈ this set. Tying it to the ledger makes the drainability rule
#: deterministic AND falsifiable: adding a repository port means registering its
#: ORM return here, which expands the port-backed set automatically.
PORTED_AGGREGATE_ORM_TYPES = frozenset(
    entry.orm_type.rsplit(".", 1)[-1] for entry in ORM_RETURN_DEBT
)

#: The 4 consumers R01C IMP3 physically DRAINED off the ORM via the boards
#: repository port (each previously a pure ``<session>.get(Board, id)`` existence
#: probe). Recorded as the card's audit delta; the report re-proves they carry no
#: ORM-definition coupling now (they are absent from the shrunk allowlist).
ORM_DRAINED_BY_IMP3 = (
    "src/okto_pulse/core/api/cognitive_action_center.py",
    "src/okto_pulse/core/api/kg_rebuild.py",
    "src/okto_pulse/core/services/default_board_config_api.py",
    "src/okto_pulse/core/services/kg_health_readiness_service.py",
)

#: Bump when the record shape or classification semantics change.
ORM_CONSUMER_INVENTORY_SCHEMA_VERSION = 1

_DB_PROVIDER_MODULE = "okto_pulse.core.infra.database"
_DB_PROVIDER_OWNER = "okto-pulse-core/relational-provider (R01B)"

# --- record categories ---------------------------------------------------------
CATEGORY_ORM_DEFINITION = "orm_definition"  # removal target — gated by core_orm_import_gate (R01C)
CATEGORY_DB_PROVIDER = "db_provider"        # core.infra.database — preserved (R01B, ac_af454ee3)

# --- statuses ------------------------------------------------------------------
STATUS_TEMPORARY_EXCEPTION = "temporary_exception"  # allowlisted ORM coupling (owner + removal_date)
STATUS_UNCLASSIFIED = "unclassified"                # ORM coupling NOT allowlisted -> consistency error
STATUS_PRESERVED_R01B = "preserved_r01b"            # db_provider import — kept until R01B provider registered
STATUS_MIGRATED = "migrated"                        # proven coupling-free (re-scan) — used by verify_migrated

# --- surfaces ------------------------------------------------------------------
SURFACE_REST = "rest"
SURFACE_MCP = "mcp"
SURFACE_SERVICE = "service"
SURFACE_WORKER = "worker"
SURFACE_CLI = "cli"
SURFACE_SEED = "seed"
SURFACE_BOOTSTRAP = "bootstrap"
SURFACE_DEFINITION = "definition"
SURFACE_REPOSITORY = "repository"

#: Surfaces TR tr_65b25d4d requires the inventory to span. ``cli``/``seed`` are
#: recognised even when 0 in the core tree (their entrypoints live Community-side);
#: the core-side inventory covers the rest.
ALL_SURFACES = (
    SURFACE_REST, SURFACE_MCP, SURFACE_SERVICE, SURFACE_WORKER,
    SURFACE_CLI, SURFACE_SEED, SURFACE_BOOTSTRAP, SURFACE_DEFINITION, SURFACE_REPOSITORY,
)


def classify_surface(file_label: str) -> str:
    """Map a ``src/okto_pulse/core/...`` label to its functional surface."""
    label = file_label.replace("\\", "/")
    if label in (
        "src/okto_pulse/core/__init__.py",
        "src/okto_pulse/core/infra/__init__.py",
        "src/okto_pulse/core/infra/database.py",
        "src/okto_pulse/core/models/__init__.py",
        "src/okto_pulse/core/models/db.py",
    ):
        return SURFACE_DEFINITION
    if "/core/api/" in label:
        return SURFACE_REST
    if "/core/mcp/" in label:
        return SURFACE_MCP
    if "/core/services/" in label:
        return SURFACE_SERVICE
    if "/core/repositories/" in label:
        return SURFACE_REPOSITORY
    if any(seg in label for seg in ("/core/kg/", "/core/events/", "/core/commands/")):
        return SURFACE_WORKER
    if "/core/cli" in label:
        return SURFACE_CLI
    if "seed" in label:
        return SURFACE_SEED
    return SURFACE_BOOTSTRAP


@dataclass(frozen=True)
class OrmConsumerRecord:
    """One detected ORM / db-provider coupling, classified for the cut evidence."""

    file: str
    line: int
    symbol: str
    kind: str
    category: str
    surface: str
    group_id: str
    owner: str
    status: str

    def as_dict(self) -> dict:
        return {
            "file": self.file, "line": self.line, "symbol": self.symbol,
            "kind": self.kind, "category": self.category, "surface": self.surface,
            "group_id": self.group_id, "owner": self.owner, "status": self.status,
        }


def _scan_db_provider(tree: ast.AST, file_label: str) -> list[tuple[int, str]]:
    """Detect consumers of the relational provider surface (``core.infra.database``)
    across ALL syntactic forms (so no consumer is lost to a module-alias bypass):

    * direct ``from okto_pulse.core.infra.database import get_engine`` (symbol);
    * aliased module attribute — ``import ...database as dbmod; dbmod.get_session_factory``,
      ``from okto_pulse.core.infra import database; database.get_engine``;
    * bare dotted chain — ``okto_pulse.core.infra.database.get_db``.

    ``Base`` is EXCLUDED — it is an ORM-definition coupling owned by the gate scan,
    not a provider consumer.
    """
    out: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()

    def add(line: int, symbol: str) -> None:
        key = (line, symbol)
        if key not in seen:
            seen.add(key)
            out.append(key)

    module_alias = build_module_alias_map(tree)
    prefix = _DB_PROVIDER_MODULE + "."
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == _DB_PROVIDER_MODULE:
            for alias in node.names:
                if alias.name != "Base":  # ORM-definition, owned by the gate scan
                    add(node.lineno, alias.name)
        elif isinstance(node, ast.Attribute):
            resolved = resolve_attribute_module(node, module_alias)
            if resolved is not None and resolved.startswith(prefix):
                attr = resolved[len(prefix):]
                # only direct attribute access (single segment), never the ``Base`` def
                if attr and "." not in attr and attr != "Base":
                    add(node.lineno, attr)
    return out


@dataclass
class OrmConsumerInventory:
    """Versioned cross-surface inventory + roll-ups (AC5 evidence)."""

    schema_version: int
    core_root: str
    scanned_files: int
    records: list[OrmConsumerRecord] = field(default_factory=list)
    by_surface: dict[str, int] = field(default_factory=dict)
    by_category: dict[str, int] = field(default_factory=dict)
    by_group: dict[str, int] = field(default_factory=dict)
    by_status: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.records)

    def as_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "core_root": self.core_root,
            "scanned_files": self.scanned_files,
            "total": self.total,
            "by_surface": dict(self.by_surface),
            "by_category": dict(self.by_category),
            "by_group": dict(self.by_group),
            "by_status": dict(self.by_status),
            "records": [r.as_dict() for r in self.records],
        }


def build_orm_consumer_inventory(core_root: str | Path | None = None) -> OrmConsumerInventory:
    """Build the versioned per-call-site cross-surface ORM-consumer inventory.

    ORM-definition couplings come from the single-source gate detector; provider
    couplings (``core.infra.database``) are tracked as the preserved R01B surface.
    Records are returned in deterministic ``(file, line, symbol)`` order.
    """
    base = Path(core_root) if core_root is not None else default_core_path()
    orm_names = orm_definition_names()
    records: list[OrmConsumerRecord] = []
    files = sorted(base.rglob("*.py")) if base.exists() else []
    scanned = 0
    for path in files:
        if "__pycache__" in path.parts:
            continue
        scanned += 1
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        label = _label(path)
        surface = classify_surface(label)
        # ORM-definition couplings (removal target)
        for occ in _scan_module(tree, label, orm_names):
            cluster = CORE_ORM_IMPORT_ALLOWLIST.get(occ.file)
            if cluster is not None:
                entry = allowlist_entry(occ.file)
                group_id = f"orm:{cluster}"
                owner = entry["owner"]
                status = STATUS_TEMPORARY_EXCEPTION
            else:
                group_id = "orm:unclassified"
                owner = ""
                status = STATUS_UNCLASSIFIED
            records.append(OrmConsumerRecord(
                file=occ.file, line=occ.line, symbol=occ.symbol, kind=occ.kind,
                category=CATEGORY_ORM_DEFINITION, surface=surface,
                group_id=group_id, owner=owner, status=status,
            ))
        # db-provider couplings (preserved R01B surface)
        for line, symbol in _scan_db_provider(tree, label):
            records.append(OrmConsumerRecord(
                file=label, line=line, symbol=symbol, kind="db_provider",
                category=CATEGORY_DB_PROVIDER, surface=surface,
                group_id="db_provider:r01b", owner=_DB_PROVIDER_OWNER,
                status=STATUS_PRESERVED_R01B,
            ))

    records.sort(key=lambda r: (r.file, r.line, r.symbol))
    inv = OrmConsumerInventory(
        schema_version=ORM_CONSUMER_INVENTORY_SCHEMA_VERSION,
        core_root=str(base), scanned_files=scanned, records=records,
    )
    for r in records:
        inv.by_surface[r.surface] = inv.by_surface.get(r.surface, 0) + 1
        inv.by_category[r.category] = inv.by_category.get(r.category, 0) + 1
        inv.by_group[r.group_id] = inv.by_group.get(r.group_id, 0) + 1
        inv.by_status[r.status] = inv.by_status.get(r.status, 0) + 1
    return inv


def inventory_consistency_errors(inventory: OrmConsumerInventory) -> list[str]:
    """Return AC5 consistency errors: every ORM-definition coupling MUST be
    classified (allowlisted with an owner). Any ``unclassified`` record (an ORM
    coupling outside the gate allowlist) is an error — proving "all classified or
    explicitly allowlisted" with no silent bridge. Empty == AC5 holds.
    """
    return [
        f"{r.file}:{r.line} couples ORM '{r.symbol}' but is not classified/allowlisted"
        for r in inventory.records
        if r.category == CATEGORY_ORM_DEFINITION and r.status == STATUS_UNCLASSIFIED
    ]


def verify_migrated(
    previous_allowlist: dict[str, str], core_root: str | Path | None = None
) -> dict[str, list[str]]:
    """Prove migration claims by RE-SCANNING. For every file dropped from
    ``previous_allowlist`` relative to the current frozen allowlist, confirm it no
    longer has any ORM-definition coupling.

    Returns ``{"migrated": [...], "still_coupled": [...]}``. ``still_coupled`` is
    the anti-theater signal: a file claimed migrated (dropped) that STILL imports
    ORM — the migration is not real.
    """
    base = Path(core_root) if core_root is not None else default_core_path()
    orm_names = orm_definition_names()
    dropped = [f for f in previous_allowlist if f not in CORE_ORM_IMPORT_ALLOWLIST]
    coupled_files: set[str] = set()
    files = sorted(base.rglob("*.py")) if base.exists() else []
    for path in files:
        if "__pycache__" in path.parts:
            continue
        label = _label(path)
        if label not in dropped:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        if _scan_module(tree, label, orm_names):
            coupled_files.add(label)
    migrated = sorted(f for f in dropped if f not in coupled_files)
    still_coupled = sorted(coupled_files)
    return {"migrated": migrated, "still_coupled": still_coupled}


# ---------------------------------------------------------------------------
# Split-inventory guard (ts_c9653b5c) — fail a heterogeneous / unclassified batch
# removal BEFORE any move; pass only small, per-cluster, owned, before/after
# groups with register-before-remove order.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrmRemovalGroup:
    """One declared ORM-removal group in a batch move."""

    group_id: str
    owner: str
    files: tuple[str, ...]
    before_count: int | None
    after_count: int | None
    register_before_remove: bool


@dataclass(frozen=True)
class OrmBatchRemovalDeclaration:
    """A declared batch of ORM-removal groups to validate before moving."""

    groups: tuple[OrmRemovalGroup, ...]


@dataclass
class SplitInventoryGuardReport:
    ok: bool
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"ok": self.ok, "failures": list(self.failures)}


def _group_clusters(files: tuple[str, ...]) -> set[str]:
    """Distinct gate clusters the group's files belong to (a homogeneous group
    maps to exactly one)."""
    return {
        CORE_ORM_IMPORT_ALLOWLIST[f]
        for f in files
        if f in CORE_ORM_IMPORT_ALLOWLIST
    }


def guard_orm_batch_removal(
    declaration: OrmBatchRemovalDeclaration,
) -> SplitInventoryGuardReport:
    """Validate a batch ORM-removal declaration. FAILS (before any move) when a
    group omits ``group_id``/``owner``, lumps heterogeneous clusters, lacks
    register-before-remove order, or omits a consistent before/after inventory.

    Passes only when every group is a single registered cluster with an owner, an
    explicit register-before-remove order, and a before/after inventory that
    actually reduces coupling.
    """
    failures: list[str] = []
    if not declaration.groups:
        failures.append("empty batch: no removal groups declared")

    seen_ids: set[str] = set()
    for idx, group in enumerate(declaration.groups):
        tag = group.group_id or f"<group#{idx}>"
        if not group.group_id.strip():
            failures.append(f"{tag}: missing group_id")
        elif group.group_id in seen_ids:
            failures.append(f"{group.group_id}: duplicate group_id in batch")
        else:
            seen_ids.add(group.group_id)
        if not group.owner.strip():
            failures.append(f"{tag}: missing owner")
        if not group.files:
            failures.append(f"{tag}: no files in group")
        # register-before-remove: every file must currently be registered in the
        # allowlist (you can only remove what was registered first).
        unregistered = [f for f in group.files if f not in CORE_ORM_IMPORT_ALLOWLIST]
        if unregistered:
            failures.append(
                f"{tag}: register-before-remove violated — not registered: {unregistered}"
            )
        if not group.register_before_remove:
            failures.append(f"{tag}: register_before_remove not asserted")
        # heterogeneous: a group must map to exactly one cluster.
        clusters = _group_clusters(group.files)
        if len(clusters) > 1:
            failures.append(
                f"{tag}: heterogeneous group spans clusters {sorted(clusters)} — "
                "split into one group per cluster"
            )
        # before/after inventory must be present and actually reduce.
        if group.before_count is None or group.after_count is None:
            failures.append(f"{tag}: missing before/after inventory")
        elif group.after_count >= group.before_count:
            failures.append(
                f"{tag}: before/after inventory does not reduce coupling "
                f"({group.before_count} -> {group.after_count})"
            )

    return SplitInventoryGuardReport(ok=not failures, failures=failures)


# ---------------------------------------------------------------------------
# Deterministic drainability classifier + governance report (R01C IMP3).
#
# Codex IMP3 criterion: auditable, deterministic, re-executable evidence — NOT
# artisanal per-file annotation. The classifier is a pure AST pass; the report
# proves WHAT is drainable now (and what is not, and why) with complete counts
# and an explicit, non-hidden temporary remainder.
# ---------------------------------------------------------------------------

_MODELS_DB_MODULES = ("okto_pulse.core.models.db", "okto_pulse.core.models")


def _imported_orm_locals(tree: ast.AST, orm_names: frozenset[str]) -> dict[str, str]:
    """local_name -> ORM model name for ``from core.models[.db] import X [as Y]``."""
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _MODELS_DB_MODULES:
            for alias in node.names:
                if alias.name in orm_names:
                    out[alias.asname or alias.name] = alias.name
    return out


def drainability_classification(core_root: str | Path | None = None) -> dict:
    """Deterministic, re-executable classification of every ``<sess>.get(M, pk)``
    ORM get-by-id call-site in the core tree.

    Formalized drainability rule (tied to the live ``ORM_RETURN_DEBT`` ledger):
      * a site is ``port_backed`` iff ``M`` ∈ :data:`PORTED_AGGREGATE_ORM_TYPES`
        (a ``uow.<agg>.get(id)`` port exists — derived from the debt ledger);
      * it is ``import_drainable_now`` iff ``port_backed`` AND ``M``'s ONLY use in
        its file is as the first arg of a ``.get(...)`` call (so routing the call
        to the port REMOVES the ``core.models.db`` import — a real coupling drain);
      * a file ``fully_drainable_now`` (would VACATE the allowlist) iff EVERY one of
        its ORM-definition symbols is a port-backed, import-drainable get-model.

    Non-port-backed gets (``M`` has no repository port yet) are NOT drainable —
    blocked by the domain/ORM separation axis. Returns deterministic, sorted data.
    """
    base = Path(core_root) if core_root is not None else default_core_path()
    orm_names = orm_definition_names()
    ported = PORTED_AGGREGATE_ORM_TYPES
    sites: list[dict] = []
    would_vacate: dict[str, list[str]] = {}
    files = sorted(base.rglob("*.py")) if base.exists() else []
    for path in files:
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        label = _label(path)
        occ = _scan_module(tree, label, orm_names)
        if not occ:
            continue
        file_syms = {o.symbol for o in occ}
        alias = build_module_alias_map(tree)
        locals_ = _imported_orm_locals(tree, orm_names)

        drain_arg_ids: set[int] = set()
        get_sites: list[tuple[int, str]] = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
                and node.args
                and isinstance(node.args[0], ast.Name)
            ):
                raw = node.args[0].id
                model = alias.get(raw, raw).rsplit(".", 1)[-1]
                if model in orm_names:
                    drain_arg_ids.add(id(node.args[0]))
                    get_sites.append((node.lineno, model))

        uses: dict[str, list[ast.Name]] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id in locals_:
                uses.setdefault(node.id, []).append(node)
        removable: set[str] = set()
        for local, model in locals_.items():
            u = uses.get(local, [])
            if u and all(id(n) in drain_arg_ids for n in u):
                removable.add(model)

        for line, model in sorted(get_sites):
            port_backed = model in ported
            sites.append({
                "file": label, "line": line, "symbol": model,
                "port_backed": port_backed,
                "import_drainable_now": port_backed and model in removable,
            })
        if file_syms and file_syms <= (removable & ported):
            would_vacate[label] = sorted(file_syms)

    sites.sort(key=lambda s: (s["file"], s["line"], s["symbol"]))
    return {"sites": sites, "would_vacate_files": would_vacate}


def core_orm_governance_report(core_root: str | Path | None = None) -> dict:
    """Deterministic R01C IMP3 governance report (auditable, re-executable).

    Surfaces, with COMPLETE counts and an EXPLICIT (never hidden) temporary
    remainder:
      * the shrunk allowlist size + per-surface / per-category inventory rollups;
      * per-cluster governance (owner, removal_date, withdrawal_criterion,
        blocked_by axis, resolved debt_ref) + temporary status;
      * the deterministic drainability verdict — total get-by-id sites partitioned
        into port-backed vs no-port (blocked by axis), the count import-drainable
        now, and which files (if any) would fully vacate the allowlist now;
      * the 4 consumers this card DRAINED (re-proven coupling-free);
      * the session-bridge debt with an objective reduction/justification;
      * the explicit temporary remainder by surface, with the unclassified count
        (a silent-bridge tripwire — must be 0).
    """
    inv = build_orm_consumer_inventory(core_root)
    drain = drainability_classification(core_root)
    sites = drain["sites"]
    port_backed = [s for s in sites if s["port_backed"]]
    no_port = [s for s in sites if not s["port_backed"]]
    drainable_now = [s for s in sites if s["import_drainable_now"]]
    vacate = drain["would_vacate_files"]

    clusters: dict[str, dict] = {}
    for cluster, meta in _ALLOWLIST_CLUSTERS.items():
        cluster_files = [f for f, c in CORE_ORM_IMPORT_ALLOWLIST.items() if c == cluster]
        occurrences = sum(
            1 for r in inv.records
            if r.category == CATEGORY_ORM_DEFINITION and r.group_id == f"orm:{cluster}"
        )
        debt_target = resolve_debt_ref(meta.get("debt_ref"))
        clusters[cluster] = {
            "files": len(cluster_files),
            "occurrences": occurrences,
            "status": STATUS_TEMPORARY_EXCEPTION,
            "owner": meta["owner"],
            "removal_date": meta["removal_date"],
            "withdrawal_criterion": meta["withdrawal_criterion"],
            "blocked_by": meta["blocked_by"],
            "debt_ref": meta.get("debt_ref"),
            "debt_ref_resolves": debt_target is not None,
        }

    orm_total = inv.by_category.get(CATEGORY_ORM_DEFINITION, 0)
    orm_by_surface: dict[str, int] = {}
    for r in inv.records:
        if r.category == CATEGORY_ORM_DEFINITION:
            orm_by_surface[r.surface] = orm_by_surface.get(r.surface, 0) + 1
    unclassified = len(inventory_consistency_errors(inv))

    return {
        "schema_version": ORM_CONSUMER_INVENTORY_SCHEMA_VERSION,
        "core_root": inv.core_root,
        "allowlisted_files": len(CORE_ORM_IMPORT_ALLOWLIST),
        "inventory": {
            "total": inv.total,
            "by_surface": dict(inv.by_surface),
            "by_category": dict(inv.by_category),
        },
        "clusters": clusters,
        "drainability": {
            "rule": (
                "A <sess>.get(M, pk) site is port-backed iff M is a registered "
                "ORM_RETURN_DEBT aggregate (a uow.<agg>.get port exists); "
                "import-drainable-now iff port-backed AND M is used only as a .get "
                "first-arg (routing to the port removes the core.models.db import). "
                "The port still returns ORM (ORM_RETURN_DEBT not withdrawn), so the "
                "runtime ORM handling is the deferred domain/ORM axis, not this card."
            ),
            "ported_aggregate_orm_types": sorted(PORTED_AGGREGATE_ORM_TYPES),
            "ports_return_orm": True,
            "orm_return_debt_withdrawal": [e.withdrawal_criterion for e in ORM_RETURN_DEBT],
            "get_by_id_sites": {
                "total": len(sites),
                "port_backed": len(port_backed),
                "no_port_blocked_by_axis": len(no_port),
            },
            "import_drainable_now_sites": len(drainable_now),
            "files_fully_drainable_now": vacate,
            "deferred_port_backed_count_only": [
                s for s in drainable_now if s["file"] not in vacate
            ],
        },
        "drained_this_card": {
            "files": list(ORM_DRAINED_BY_IMP3),
            "count": len(ORM_DRAINED_BY_IMP3),
            "still_coupled": [
                f for f in ORM_DRAINED_BY_IMP3 if f in CORE_ORM_IMPORT_ALLOWLIST
            ],
            "mechanism": "resolve_unit_of_work_factory().wrap(session).boards.get(id)",
        },
        "session_bridge": {
            "debt_ref": "session_bridge_debt",
            "location": SESSION_BRIDGE_DEBT.location,
            "withdrawal_criterion": SESSION_BRIDGE_DEBT.withdrawal_criterion,
            "reduction_now": 0,
            "justification": (
                "Functional uow.session reads in the spec #09 use cases are "
                "board-scoped / aggregated / joined queries the thin get(id)/add "
                "ports do not express; draining them requires DTO-returning rich "
                "ports — the domain/ORM separation axis, not IMP3."
            ),
        },
        "remainder": {
            "total_orm_definition_couplings": orm_total,
            "all_temporary": orm_total == inv.by_status.get(STATUS_TEMPORARY_EXCEPTION, 0),
            "hidden": 0,
            "unclassified": unclassified,
            "by_surface": orm_by_surface,
        },
    }
