"""Relational consumer inventory (SaaS Refactor spec R01A, FR1 / AC1 ac_6dcd13a6).

A *versioned*, per-call-site inventory of every REST / MCP / service / worker
consumer in the core that couples directly to the relational layer —
``Depends(get_db)``, ``get_db_for_mcp``, ``AsyncSession``, ``select`` or a
concrete ORM model from ``core.models.db``. Each record carries
``file, line, symbol, surface, cluster, owner, status`` (AC1).

This is the strangler work-list: the fan-out cards (R01A IMP2..IMP5) draw their
slices from it, and the baseline-aware ratchet gate (R01A IMP7) reduces its
allowlist only for the symbols actually strangled.

Single source of detection truth: this module **reuses** the AST scanner of
:mod:`relational_boundary_gate` (so the inventory and the gate can never drift
on *what* counts as a relational coupling) and layers the
``cluster``/``owner``/``status`` classification on top. Pure static analysis
(``ast`` + ``pathlib`` via the gate); no runtime import of the scanned code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from okto_pulse.core.repositories.relational_boundary_gate import (
    default_core_path,
    run_relational_boundary_gate,
)

#: Bump when the record shape or classification semantics change so consumers of
#: the inventory (fan-out cards, ratchet gate, dashboards) can pin a contract.
RELATIONAL_CONSUMER_INVENTORY_SCHEMA_VERSION = 1

#: Migration status of a detected call-site.
STATUS_BASELINE = "baseline"  # detected, not yet migrated behind an application port
STATUS_MIGRATED = "migrated"  # moved behind a port/use case; relational coupling removed
STATUS_TEMPORARY_EXCEPTION = "temporary_exception"  # registered debt-ledger exception

#: Functional-cluster families. The cluster is derived from the *file location*
#: (which strangler slice owns it); the ``surface`` stays symbol-derived so a
#: worker that uses ``AsyncSession`` is surface=service but cluster=worker:<name>.
CLUSTER_REST = "rest"
CLUSTER_MCP = "mcp"
CLUSTER_SERVICE = "service"
CLUSTER_WORKER = "worker"
CLUSTER_OTHER = "other"

#: Owner per cluster family — the migration track responsible for strangling it.
_OWNER_BY_FAMILY = {
    CLUSTER_REST: "core-refactor:rest",
    CLUSTER_MCP: "core-refactor:mcp",
    CLUSTER_SERVICE: "core-refactor:service",
    CLUSTER_WORKER: "core-refactor:worker",
    CLUSTER_OTHER: "core-refactor",
}


@dataclass(frozen=True)
class RelationalConsumer:
    """One detected relational call-site (AC1 record shape)."""

    file: str
    line: int
    symbol: str
    surface: str
    cluster: str
    owner: str
    status: str

    def as_dict(self) -> dict:
        return {
            "file": self.file,
            "line": self.line,
            "symbol": self.symbol,
            "surface": self.surface,
            "cluster": self.cluster,
            "owner": self.owner,
            "status": self.status,
        }


def _family_and_subname(file_label: str) -> tuple[str, str]:
    """Map a call-site file path to ``(cluster_family, sub_name)``.

    - ``core/api/<name>.py``                     -> (rest, <name>)     REST router by domain
    - ``core/mcp/<name>.py``                     -> (mcp, <name>)      MCP tool family by module
    - ``core/services/<name>.py``                -> (service, <name>)  service flow
    - ``core/{kg/workers,kg/global_discovery,events/handlers}/<name>.py``
                                                 -> (worker, <name>)   background worker/flow
    - anything else                              -> (other, <top>/<name>)
    """
    pure = PurePosixPath(file_label)
    parts = [p for p in pure.parts if p != "core"]
    name = pure.stem
    if len(parts) >= 2 and parts[0] == "api":
        return CLUSTER_REST, name
    if len(parts) >= 2 and parts[0] == "mcp":
        return CLUSTER_MCP, name
    if len(parts) >= 2 and parts[0] == "services":
        return CLUSTER_SERVICE, name
    worker_dirs = ("workers", "global_discovery", "handlers")
    if any(d in parts for d in worker_dirs):
        return CLUSTER_WORKER, name
    top = parts[0] if parts else "core"
    return CLUSTER_OTHER, f"{top}/{name}"


def _classify(file_label: str) -> tuple[str, str]:
    """Return ``(cluster, owner)`` for a call-site file path."""
    family, sub = _family_and_subname(file_label)
    return f"{family}:{sub}", _OWNER_BY_FAMILY[family]


@dataclass
class RelationalConsumerInventory:
    """The versioned inventory + summary roll-ups (AC1)."""

    schema_version: int
    core_root: str
    scanned_files: int
    consumers: list[RelationalConsumer] = field(default_factory=list)
    by_surface: dict[str, int] = field(default_factory=dict)
    by_cluster: dict[str, int] = field(default_factory=dict)
    by_status: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.consumers)

    def as_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "core_root": self.core_root,
            "scanned_files": self.scanned_files,
            "total": self.total,
            "by_surface": dict(self.by_surface),
            "by_cluster": dict(self.by_cluster),
            "by_status": dict(self.by_status),
            "consumers": [c.as_dict() for c in self.consumers],
        }


def build_relational_consumer_inventory(
    core_root: str | Path | None = None,
) -> RelationalConsumerInventory:
    """Build the versioned per-call-site relational consumer inventory.

    Scans every ``*.py`` under ``core_root`` (default: the core package) via the
    relational boundary gate's AST detector, then classifies each detected
    call-site by functional cluster, owner and migration status (all
    ``baseline`` on a fresh scan — nothing migrated yet). Records are returned in
    a deterministic ``(file, line, symbol)`` order so the inventory is diffable.
    """
    base = Path(core_root) if core_root is not None else default_core_path()
    report = run_relational_boundary_gate(root=base)

    consumers: list[RelationalConsumer] = []
    for v in report.violations:
        # Normalise the path separator so the versioned inventory is identical
        # across OSes (the gate labels with the host separator).
        file_label = v.file.replace("\\", "/")
        cluster, owner = _classify(file_label)
        consumers.append(
            RelationalConsumer(
                file=file_label,
                line=v.line,
                symbol=v.symbol,
                surface=v.surface,
                cluster=cluster,
                owner=owner,
                status=STATUS_BASELINE,
            )
        )
    consumers.sort(key=lambda c: (c.file, c.line, c.symbol))

    by_surface: dict[str, int] = {}
    by_cluster: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for c in consumers:
        by_surface[c.surface] = by_surface.get(c.surface, 0) + 1
        family = c.cluster.split(":", 1)[0]
        by_cluster[family] = by_cluster.get(family, 0) + 1
        by_status[c.status] = by_status.get(c.status, 0) + 1

    return RelationalConsumerInventory(
        schema_version=RELATIONAL_CONSUMER_INVENTORY_SCHEMA_VERSION,
        core_root=str(base),
        scanned_files=report.scanned_files,
        consumers=consumers,
        by_surface=by_surface,
        by_cluster=by_cluster,
        by_status=by_status,
    )
