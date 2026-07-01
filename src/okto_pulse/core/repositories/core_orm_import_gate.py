"""Core-runtime ORM-definition import gate (SaaS Refactor spec R01C, FR4 / AC4 ac_527bc003).

A *delta-aware* AST gate over the ENTIRE core runtime tree (``src/okto_pulse/core``)
that FAILS when a file imports or uses a relational ORM-DEFINITION primitive —
``declarative_base``, ``TypeDecorator``, the declarative ``Base``, or a concrete
ORM model class from ``core.models.db`` — UNLESS that file is registered in the
temporary allowlist :data:`CORE_ORM_IMPORT_ALLOWLIST` with an owner and a
removal date (register-before-remove governance, the same ledger pattern as the
``AntiSingletonGate`` ``SINGLETON_LEDGER`` and the R01B runtime seams).

This is deliberately NOT :mod:`relational_boundary_gate` (which guards the
migrated ``application/use_cases`` slice for session/``select`` coupling) and NOT
the core-wide *debt baseline* (``relational_baseline_report``). It is a distinct,
delta-aware enforcement (negative scenario ``ts_d1ddd3c9`` forbids reusing those
as a substitute): the allowlist is a FROZEN STATIC literal capturing the CURRENT
set of legitimate ORM-definition consumers; any NEW core-runtime consumer that is
absent from the allowlist is blocking. The allowlist may only SHRINK as consumers
migrate behind ports/DTOs (ratchet — :func:`core_orm_allowlist_only_shrinks`),
proving relational ORM ownership is moving to Community (R01C) without new
coupling leaking back into the core.

Crucially the gate does NOT flag the agnostic enums that ``core.models.db``
re-exports from ``core.domain.enums`` (``CardStatus``, ``BugSeverity`` …) —
importing those is enum coupling, not ORM coupling. The ORM-definition name set is
derived structurally from ``models/db.py`` (classes subclassing ``Base`` or
``TypeDecorator``, plus ``Base`` itself) so it can never drift from the source.

Pure static analysis (``ast`` + ``pathlib``); no runtime import of scanned code.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

from okto_pulse.core.repositories.debt import (
    ORM_RETURN_DEBT,
    SESSION_BRIDGE_DEBT,
)
from okto_pulse.core.repositories.relational_boundary_gate import default_core_path

# --- source modules that own / re-export the ORM-definition primitives ---------
_ORM_MODULE = "okto_pulse.core.models.db"
_MODELS_PKG = "okto_pulse.core.models"  # lazy PEP 562 re-export package (anti-bypass)
_DB_BASE_MODULE = "okto_pulse.core.infra.database"  # Base = declarative_base() lives here
_SQLA_DECLBASE_MODULES = ("sqlalchemy.orm", "sqlalchemy.ext.declarative")
_SQLA_TYPEDECORATOR_MODULES = ("sqlalchemy", "sqlalchemy.types")

# --- kinds of ORM-definition coupling AC4 forbids in NEW core runtime code ------
KIND_DECLARATIVE_BASE = "declarative_base"
KIND_TYPE_DECORATOR = "type_decorator"
KIND_BASE = "base"
KIND_ORM_MODEL = "orm_model"

#: Controlled vocabulary for ``blocked_by`` — the registered axis that must be
#: discharged before a cluster's coupling can be withdrawn. R01C IMP3 makes the
#: allowlist a GOVERNED, auditable ledger (not a blanket exception): every cluster
#: must name WHICH axis blocks its removal, from this closed set.
AXIS_DOMAIN_ORM_SEPARATION = "domain_orm_separation_axis"
AXIS_IMP4_COMMUNITY_ADAPTER = "imp4_community_adapter_ownership"
BLOCKED_BY_AXES = frozenset({AXIS_DOMAIN_ORM_SEPARATION, AXIS_IMP4_COMMUNITY_ADAPTER})

#: Debt-ledger anchors a cluster's ``debt_ref`` may point at. Each key resolves to
#: a REAL, live entry in :mod:`okto_pulse.core.repositories.debt` (imported above),
#: so the allowlist governance is UNIFIED with the registered transitional debt —
#: a cluster cannot cite a withdrawal blocker that the debt ledger does not record.
#: ``debt_ref=None`` is reserved for the ``definition`` cluster, whose removal is a
#: card-tracked physical re-home (IMP4), not a port/session debt.
_RESOLVABLE_DEBT_REFS: dict[str, object] = {
    "orm_return_debt": ORM_RETURN_DEBT,
    "session_bridge_debt": SESSION_BRIDGE_DEBT,
}

#: Required register-before-remove metadata fields on every allowlist cluster.
#: R01C IMP3 extends the contract: beyond (owner, removal_date, reason) every
#: cluster must declare a concrete ``withdrawal_criterion`` and the ``blocked_by``
#: axis. ``debt_ref`` is validated separately (must be None or resolve via
#: :data:`_RESOLVABLE_DEBT_REFS`).
REQUIRED_ALLOWLIST_FIELDS = (
    "owner",
    "removal_date",
    "reason",
    "withdrawal_criterion",
    "blocked_by",
)

#: Register-before-remove metadata, shared per migration cluster. Every
#: allowlisted file resolves its governance (owner, removal_date, reason,
#: withdrawal_criterion, blocked_by axis, debt_ref) through its cluster — so the
#: full governance contract holds per file without repeating the prose 65 times.
#: ``removal_date`` is the owner-assigned R01C migration target (revisable as the
#: track replans); the allowlist ratchets (shrinks) as files migrate, never grows.
#: ``withdrawal_criterion`` is the OBJECTIVE condition that lets the cluster leave
#: the allowlist; ``blocked_by`` is the axis (closed vocab) that gates it;
#: ``debt_ref`` links to the live debt-ledger entry that records the blocker.
_ALLOWLIST_CLUSTERS: dict[str, dict[str, object]] = {
    "definition": {
        "owner": "okto-pulse-core/relational-ownership",
        "removal_date": "2026-12-31",
        "reason": (
            "ORM/Base/declarative_base/TypeDecorator definition or package "
            "re-export home; relational ownership moves to Community per R01C FR3 "
            "(RelationalSchemaMigrator/DataBootstrapper). Removed when the ORM "
            "module physically lands in the Community adapter."
        ),
        "withdrawal_criterion": (
            "The ORM definition module (Base / declarative models / TypeDecorators) "
            "is physically re-homed into the Community relational adapter, leaving "
            "the core with no ORM-definition source (R01C FR3, executed by IMP4)."
        ),
        "blocked_by": AXIS_IMP4_COMMUNITY_ADAPTER,
        # The definition is the ORM SOURCE; its removal is a card-tracked physical
        # re-home (IMP4), not a port-return / session-bridge debt — hence None.
        "debt_ref": None,
    },
    "repository": {
        "owner": "core-refactor:repository",
        "removal_date": "2026-12-31",
        "reason": (
            "SQLAlchemy repository adapter / port interface is the legitimate "
            "relational boundary; retained until ORM is re-homed to the Community "
            "adapter (R01C FR3)."
        ),
        "withdrawal_criterion": (
            "Repository ports return domain entities (agnostic DTOs) instead of the "
            "ORM Base, and the SQLAlchemy adapter is re-homed to Community — "
            "withdraws ORM_RETURN_DEBT."
        ),
        "blocked_by": AXIS_DOMAIN_ORM_SEPARATION,
        "debt_ref": "orm_return_debt",
    },
    "rest": {
        "owner": "core-refactor:rest",
        "removal_date": "2026-09-30",
        "reason": (
            "REST router couples to core.models.db ORM; migrate behind application "
            "port / agnostic DTO (R01C FR2/AC5) before ORM removal."
        ),
        "withdrawal_criterion": (
            "REST consumers read/write exclusively via DTO-returning repository "
            "ports — the router never imports or holds a core.models.db ORM type. "
            "Requires ORM_RETURN_DEBT withdrawn (ports return domain entities) and "
            "SESSION_BRIDGE_DEBT drained (persistence flows through the repositories)."
        ),
        "blocked_by": AXIS_DOMAIN_ORM_SEPARATION,
        "debt_ref": "orm_return_debt",
    },
    "mcp": {
        "owner": "core-refactor:mcp",
        "removal_date": "2026-09-30",
        "reason": (
            "MCP server couples to core.models.db ORM; migrate behind application "
            "port / agnostic DTO (R01C FR2/AC5) before ORM removal."
        ),
        "withdrawal_criterion": (
            "MCP tool implementations read/write exclusively via DTO-returning "
            "repository ports (and the uow factory, not raw sessions) — never "
            "importing/holding a core.models.db ORM type. Requires ORM_RETURN_DEBT "
            "withdrawn and SESSION_BRIDGE_DEBT drained."
        ),
        "blocked_by": AXIS_DOMAIN_ORM_SEPARATION,
        "debt_ref": "orm_return_debt",
    },
    "service": {
        "owner": "core-refactor:service",
        "removal_date": "2026-09-30",
        "reason": (
            "Service flow couples to core.models.db ORM; migrate behind application "
            "port / agnostic DTO (R01C FR2/AC5) before ORM removal."
        ),
        "withdrawal_criterion": (
            "Service flows read/write exclusively via DTO-returning repository "
            "ports — never importing/holding a core.models.db ORM type. Requires "
            "ORM_RETURN_DEBT withdrawn and SESSION_BRIDGE_DEBT drained."
        ),
        "blocked_by": AXIS_DOMAIN_ORM_SEPARATION,
        "debt_ref": "orm_return_debt",
    },
    "worker": {
        "owner": "core-refactor:worker",
        "removal_date": "2026-09-30",
        "reason": (
            "Background worker/event/command couples to core.models.db ORM; migrate "
            "behind application port / agnostic DTO (R01C FR2/AC5) before ORM removal."
        ),
        "withdrawal_criterion": (
            "Workers/event handlers/commands read/write exclusively via "
            "DTO-returning repository ports — never importing/holding a "
            "core.models.db ORM type. Requires ORM_RETURN_DEBT withdrawn and "
            "SESSION_BRIDGE_DEBT drained."
        ),
        "blocked_by": AXIS_DOMAIN_ORM_SEPARATION,
        "debt_ref": "orm_return_debt",
    },
    "bootstrap": {
        "owner": "core-refactor:bootstrap",
        "removal_date": "2026-09-30",
        "reason": (
            "Core bootstrap/app wiring touches core.models.db ORM; migrate behind "
            "application port (R01C FR2/AC5) before ORM removal."
        ),
        "withdrawal_criterion": (
            "Bootstrap/app wiring references only DTO-returning ports and the "
            "composition seam — never importing/holding a core.models.db ORM type. "
            "Requires ORM_RETURN_DEBT withdrawn."
        ),
        "blocked_by": AXIS_DOMAIN_ORM_SEPARATION,
        "debt_ref": "orm_return_debt",
    },
}

#: FROZEN file -> cluster ratchet set (the delta-aware teeth). This is a STATIC
#: literal captured at R01C IMP2 time — NOT a live scan — so a NEW core file with
#: an ORM-definition coupling is absent here and therefore blocking. Shrinks only.
CORE_ORM_IMPORT_ALLOWLIST: dict[str, str] = {
    # -- definition (5) --
    "src/okto_pulse/core/__init__.py": "definition",
    "src/okto_pulse/core/infra/__init__.py": "definition",
    "src/okto_pulse/core/infra/database.py": "definition",
    "src/okto_pulse/core/models/__init__.py": "definition",
    "src/okto_pulse/core/models/db.py": "definition",
    # -- repository (2) --
    "src/okto_pulse/core/repositories/interfaces/repositories.py": "repository",
    "src/okto_pulse/core/repositories/sqlalchemy/repositories.py": "repository",
    # -- rest (9) -- (R01C IMP3 drained cognitive_action_center.py + kg_rebuild.py
    # via the boards repository port — they no longer import core.models.db ORM.)
    "src/okto_pulse/core/api/attachments.py": "rest",
    "src/okto_pulse/core/api/bug_cognitive_closure.py": "rest",
    "src/okto_pulse/core/api/comments.py": "rest",
    "src/okto_pulse/core/api/kg_events_hub.py": "rest",
    "src/okto_pulse/core/api/kg_tick.py": "rest",
    "src/okto_pulse/core/api/me.py": "rest",
    "src/okto_pulse/core/api/presets.py": "rest",
    "src/okto_pulse/core/api/qa.py": "rest",
    "src/okto_pulse/core/api/resource_gate.py": "rest",
    # -- mcp (1) --
    "src/okto_pulse/core/mcp/server.py": "mcp",
    # -- service (27) -- (R01C IMP3 drained default_board_config_api.py +
    # kg_health_readiness_service.py via the boards repository port.)
    "src/okto_pulse/core/services/amendment_revision.py": "service",
    "src/okto_pulse/core/services/amendment_revision_api.py": "service",
    "src/okto_pulse/core/services/analytics_service.py": "service",
    "src/okto_pulse/core/services/architecture.py": "service",
    "src/okto_pulse/core/services/architecture_propagation_legacy.py": "service",
    "src/okto_pulse/core/services/board_governance.py": "service",
    "src/okto_pulse/core/services/bug_regression_preview.py": "service",
    "src/okto_pulse/core/services/bug_regression_scenarios.py": "service",
    "src/okto_pulse/core/services/canonical_debt_service.py": "service",
    "src/okto_pulse/core/services/cognitive_effectiveness_service.py": "service",
    "src/okto_pulse/core/services/critical_context_guard.py": "service",
    "src/okto_pulse/core/services/dead_letter_inspector_service.py": "service",
    "src/okto_pulse/core/services/default_board_configuration.py": "service",
    "src/okto_pulse/core/services/design_system.py": "service",
    "src/okto_pulse/core/services/discovery_catalog_reader.py": "service",
    "src/okto_pulse/core/services/discovery_executor.py": "service",
    "src/okto_pulse/core/services/discovery_selector_catalog.py": "service",
    "src/okto_pulse/core/services/effective_resource_propagation.py": "service",
    "src/okto_pulse/core/services/kg_health_service.py": "service",
    "src/okto_pulse/core/services/main.py": "service",
    "src/okto_pulse/core/services/queue_health_service.py": "service",
    "src/okto_pulse/core/services/resource_gate.py": "service",
    "src/okto_pulse/core/services/settings_service.py": "service",
    "src/okto_pulse/core/services/skip_overrides.py": "service",
    "src/okto_pulse/core/services/spec_resource_propagation.py": "service",
    "src/okto_pulse/core/services/spec_structured_entities.py": "service",
    "src/okto_pulse/core/services/traceability.py": "service",
    # -- worker (20) --
    "src/okto_pulse/core/commands/materialize_legacy_fr_ac.py": "worker",
    "src/okto_pulse/core/events/bus.py": "worker",
    "src/okto_pulse/core/events/dispatcher.py": "worker",
    "src/okto_pulse/core/events/handlers/card_boost_recompute.py": "worker",
    "src/okto_pulse/core/events/handlers/cognitive_extraction.py": "worker",
    "src/okto_pulse/core/events/handlers/consolidation_enqueuer.py": "worker",
    "src/okto_pulse/core/events/handlers/kg_decay_tick.py": "worker",
    "src/okto_pulse/core/kg/canonical_demotion_global_sync.py": "worker",
    "src/okto_pulse/core/kg/cognitive_action_center.py": "worker",
    "src/okto_pulse/core/kg/cognitive_readiness.py": "worker",
    "src/okto_pulse/core/kg/dashboard_readers.py": "worker",
    "src/okto_pulse/core/kg/global_discovery/clustering.py": "worker",
    "src/okto_pulse/core/kg/global_discovery/outbox_worker.py": "worker",
    "src/okto_pulse/core/kg/governance.py": "worker",
    "src/okto_pulse/core/kg/health.py": "worker",
    "src/okto_pulse/core/kg/parent_doc/parent_doc.py": "worker",
    "src/okto_pulse/core/kg/workers/cognitive_closeout.py": "worker",
    "src/okto_pulse/core/kg/workers/commit_events.py": "worker",
    "src/okto_pulse/core/kg/workers/consolidation.py": "worker",
    "src/okto_pulse/core/kg/workers/dead_letter.py": "worker",
    # -- bootstrap (1) --
    "src/okto_pulse/core/app.py": "bootstrap",
}


def allowlist_entry(file_label: str) -> dict[str, object] | None:
    """Return the resolved governance entry for ``file_label`` or None.

    The returned dict carries ``cluster`` + the cluster's full governance contract
    (``owner`` / ``removal_date`` / ``reason`` / ``withdrawal_criterion`` /
    ``blocked_by`` / ``debt_ref``) — the register-before-remove ledger.
    """
    cluster = CORE_ORM_IMPORT_ALLOWLIST.get(file_label)
    if cluster is None:
        return None
    return {"cluster": cluster, **_ALLOWLIST_CLUSTERS[cluster]}


def resolve_debt_ref(debt_ref: object) -> object | None:
    """Resolve a cluster ``debt_ref`` to its live debt-ledger entry, or None.

    ``None`` (the ``definition`` cluster's card-tracked re-home) resolves to None;
    any other value must be a key registered in :data:`_RESOLVABLE_DEBT_REFS` —
    unifying the allowlist governance with the real
    :mod:`okto_pulse.core.repositories.debt` ledger (an unknown ref is a
    governance error caught by :func:`validate_allowlist_metadata`).
    """
    if debt_ref is None:
        return None
    return _RESOLVABLE_DEBT_REFS.get(str(debt_ref))


def validate_allowlist_metadata() -> list[str]:
    """Return a list of governance errors (empty == valid). FAIL-CLOSED.

    R01C IMP3 contract — every cluster must carry:
      * all :data:`REQUIRED_ALLOWLIST_FIELDS` (owner, removal_date, reason,
        withdrawal_criterion, blocked_by) as non-empty strings;
      * a ``blocked_by`` axis drawn from the closed :data:`BLOCKED_BY_AXES` vocab;
      * an explicit ``debt_ref`` key — ``None`` (card-tracked re-home) or a key
        that RESOLVES to a live entry in the debt ledger (:func:`resolve_debt_ref`).
    And every allowlisted file must reference a registered cluster. Incomplete or
    dangling governance is an error, so a file can never be silently allowlisted.
    """
    errors: list[str] = []
    for cluster, meta in _ALLOWLIST_CLUSTERS.items():
        for required in REQUIRED_ALLOWLIST_FIELDS:
            if not str(meta.get(required, "")).strip():
                errors.append(f"cluster '{cluster}' missing required field '{required}'")
        axis = meta.get("blocked_by")
        if axis and axis not in BLOCKED_BY_AXES:
            errors.append(
                f"cluster '{cluster}' has unknown blocked_by axis '{axis}' "
                f"(allowed: {sorted(BLOCKED_BY_AXES)})"
            )
        if "debt_ref" not in meta:
            errors.append(
                f"cluster '{cluster}' is missing the explicit 'debt_ref' declaration "
                f"(use None to mark a card-tracked re-home blocker)"
            )
        else:
            ref = meta["debt_ref"]
            if ref is not None:
                if str(ref) not in _RESOLVABLE_DEBT_REFS:
                    errors.append(
                        f"cluster '{cluster}' debt_ref '{ref}' does not resolve to a "
                        f"registered debt-ledger entry"
                    )
                elif not resolve_debt_ref(ref):
                    errors.append(
                        f"cluster '{cluster}' debt_ref '{ref}' resolves to an empty "
                        f"debt-ledger entry"
                    )
    for file_label, cluster in CORE_ORM_IMPORT_ALLOWLIST.items():
        if cluster not in _ALLOWLIST_CLUSTERS:
            errors.append(f"file '{file_label}' references unknown cluster '{cluster}'")
    return errors


def orm_definition_names(models_db_path: str | Path | None = None) -> frozenset[str]:
    """Names exported by ``core.models.db`` that are ORM-DEFINITION primitives.

    The declarative ``Base``, every ORM model (class subclassing ``Base``) and
    every ``TypeDecorator`` subclass — derived structurally from the source so it
    can never drift. The agnostic enums ``models.db`` re-exports from
    ``core.domain.enums`` are EXCLUDED (importing ``CardStatus`` is enum coupling,
    not ORM coupling).
    """
    if models_db_path is not None:
        path = Path(models_db_path)
    else:
        path = default_core_path() / "models" / "db.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names = {"Base"}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                base_name = None
                if isinstance(base, ast.Name):
                    base_name = base.id
                elif isinstance(base, ast.Attribute):
                    base_name = base.attr
                if base_name in ("Base", "TypeDecorator"):
                    names.add(node.name)
    return frozenset(names)


@dataclass(frozen=True)
class OrmImportOccurrence:
    """One detected ORM-definition coupling in a core runtime file."""

    file: str
    line: int
    symbol: str
    kind: str

    def as_dict(self) -> dict:
        return {"file": self.file, "line": self.line, "symbol": self.symbol, "kind": self.kind}


def _dotted_name(node: ast.AST) -> str | None:
    """Flatten an attribute/name chain (``a.b.c``) into a dotted string, else None."""
    parts: list[str] = []
    cur: ast.AST = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
        return ".".join(reversed(parts))
    return None


def build_module_alias_map(tree: ast.AST) -> dict[str, str]:
    """Map every local name to the full module it binds, across all import shapes:

    * ``import a.b.c [as x]``        -> x (or top "a") bound to the module
    * ``from pkg import sub [as x]`` -> x bound to "pkg.sub"

    Shared by the ORM-definition gate and the ``core.infra.database`` provider
    inventory so neither loses a coupling to a module-alias bypass (single-source
    resolution).
    """
    module_alias: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    module_alias[alias.asname] = alias.name
                else:
                    # bare ``import a.b.c`` binds the top package "a"; dotted
                    # access ``a.b.c.X`` then resolves unchanged.
                    top = alias.name.split(".", 1)[0]
                    module_alias.setdefault(top, top)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            for alias in node.names:
                full = f"{base}.{alias.name}" if base else alias.name
                module_alias[alias.asname or alias.name] = full
    return module_alias


def resolve_attribute_module(node: ast.Attribute, module_alias: dict[str, str]) -> str | None:
    """Resolve an attribute access ``<head>.<tail>`` to its canonical dotted path
    using the alias map, or None if it is not a name-rooted chain."""
    dotted = _dotted_name(node)
    if dotted is None:
        return None
    head, _, tail = dotted.partition(".")
    resolved = module_alias.get(head, head)
    return f"{resolved}.{tail}" if tail else resolved


def _match_resolved(resolved: str, orm_names: frozenset[str]) -> tuple[str, str] | None:
    """Classify a CANONICAL (alias-resolved) dotted access path into an
    ORM-definition coupling ``(symbol, kind)`` or None."""
    for module in _SQLA_DECLBASE_MODULES:
        if resolved == module + ".declarative_base":
            return "declarative_base", KIND_DECLARATIVE_BASE
    for module in _SQLA_TYPEDECORATOR_MODULES:
        if resolved == module + ".TypeDecorator":
            return "TypeDecorator", KIND_TYPE_DECORATOR
    if resolved == _DB_BASE_MODULE + ".Base":
        return "Base", KIND_BASE
    for module in (_ORM_MODULE, _MODELS_PKG):
        if resolved.startswith(module + "."):
            attr = resolved.rsplit(".", 1)[-1]
            if attr in orm_names:
                return attr, (KIND_BASE if attr == "Base" else KIND_ORM_MODEL)
    return None


def _scan_module(tree: ast.AST, file_label: str, orm_names: frozenset[str]) -> list[OrmImportOccurrence]:
    """Detect ORM-definition couplings across ALL access shapes:

    * ``from sqlalchemy.orm import declarative_base`` / ``from sqlalchemy import
      TypeDecorator`` / ``from core.infra.database import Base`` / ``from
      core.models.db import <ORM>`` (direct names, incl. the lazy ``core.models``
      package);
    * aliased module imports — ``import sqlalchemy.orm as orm; orm.declarative_base``,
      ``import sqlalchemy as sa; sa.TypeDecorator``, ``import core.infra.database as
      db; db.Base``, ``from core.infra import database; database.Base``, ``from
      core.models import db; db.Card``;
    * no-alias dotted chains — ``okto_pulse.core.models.db.Card``.

    Pass 1 binds every local name to its full module so Pass 2 can resolve any
    dotted access back to a canonical module path — closing module-alias bypasses.
    """
    found: dict[tuple[int, str], OrmImportOccurrence] = {}

    def record(line: int, symbol: str, kind: str) -> None:
        found.setdefault((line, symbol), OrmImportOccurrence(file_label, line, symbol, kind))

    # Pass 1 — bind every local name to its full module (closes alias bypasses).
    module_alias = build_module_alias_map(tree)

    # Pass 2 — direct from-imports + alias-resolved attribute access.
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                hit: tuple[str, str] | None = None
                if module in _SQLA_DECLBASE_MODULES and alias.name == "declarative_base":
                    hit = ("declarative_base", KIND_DECLARATIVE_BASE)
                elif module in _SQLA_TYPEDECORATOR_MODULES and alias.name == "TypeDecorator":
                    hit = ("TypeDecorator", KIND_TYPE_DECORATOR)
                elif module == _DB_BASE_MODULE and alias.name == "Base":
                    hit = ("Base", KIND_BASE)
                elif module in (_ORM_MODULE, _MODELS_PKG) and alias.name in orm_names:
                    hit = (alias.name, KIND_BASE if alias.name == "Base" else KIND_ORM_MODEL)
                if hit is not None:
                    record(node.lineno, hit[0], hit[1])
        elif isinstance(node, ast.Attribute):
            resolved = resolve_attribute_module(node, module_alias)
            if resolved is None:
                continue
            hit = _match_resolved(resolved, orm_names)
            if hit is not None:
                record(node.lineno, hit[0], hit[1])

    return list(found.values())


@dataclass
class CoreOrmImportGateReport:
    """Result of running the delta-aware core ORM-definition import gate."""

    ok: bool
    scanned_files: int
    guarded_path: str
    occurrences: list[OrmImportOccurrence] = field(default_factory=list)
    violations: list[OrmImportOccurrence] = field(default_factory=list)
    allowlisted_files: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "guarded_path": self.guarded_path,
            "occurrences": [o.as_dict() for o in self.occurrences],
            "violations": [o.as_dict() for o in self.violations],
            "allowlisted_files": list(self.allowlisted_files),
        }


def _label(path: Path) -> str:
    """Stable ``src/okto_pulse/core/...`` label, identical for the real tree and a
    mirrored ``tmp/src/okto_pulse/core/...`` fixture tree (so teeth need no real
    tree mutation)."""
    parts = path.parts
    if "src" in parts:
        return "/".join(parts[parts.index("src"):])
    return path.name


def run_core_orm_import_gate(root: str | Path | None = None) -> CoreOrmImportGateReport:
    """Scan every ``*.py`` under ``root`` (default: ``src/okto_pulse/core``) for an
    ORM-definition coupling. ``ok`` is True only when every detected occurrence is
    in a file registered in :data:`CORE_ORM_IMPORT_ALLOWLIST` (delta-aware: a NEW
    consumer outside the allowlist makes ``ok`` False).
    """
    base = Path(root) if root is not None else default_core_path()
    orm_names = orm_definition_names()
    occurrences: list[OrmImportOccurrence] = []
    files = sorted(base.rglob("*.py")) if base.exists() else []
    for path in files:
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        occurrences.extend(_scan_module(tree, _label(path), orm_names))

    violations = [o for o in occurrences if o.file not in CORE_ORM_IMPORT_ALLOWLIST]
    allowlisted_hit = sorted({o.file for o in occurrences if o.file in CORE_ORM_IMPORT_ALLOWLIST})
    return CoreOrmImportGateReport(
        ok=not violations,
        scanned_files=len([p for p in files if "__pycache__" not in p.parts]),
        guarded_path=str(base),
        occurrences=occurrences,
        violations=violations,
        allowlisted_files=allowlisted_hit,
    )


def core_orm_allowlist_only_shrinks(previous: dict[str, str], current: dict[str, str]) -> bool:
    """True iff ``current`` is a ratchet of ``previous``: every file in ``current``
    was already in ``previous`` with the SAME cluster (no new file, no loosened
    cluster). Files may be DROPPED (migrated). Mirrors
    ``relational_boundary_gate.ratchet_baseline_only_shrinks``.
    """
    for file_label, cluster in current.items():
        if file_label not in previous:
            return False  # a new file appeared — not a shrink
        if previous[file_label] != cluster:
            return False  # cluster changed — not an honest ratchet
    return True
