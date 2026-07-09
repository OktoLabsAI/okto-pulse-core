"""AF17 graph runtime surface gate.

Blocks physical storage details from new core graph runtime contracts while
keeping the reviewed legacy compatibility shims explicit.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from .report import GateReport


@dataclass(frozen=True)
class GraphRuntimeSurfaceGateInput:
    source_root: Path | None = None
    mode: str = "blocking"


@dataclass(frozen=True)
class GraphRuntimeCompatibilityEntry:
    token: str
    legacy_surface: str
    neutral_surface: str
    files: tuple[str, ...]
    owner: str
    reason: str
    removal_criterion: str
    validation_oracle: str

    def as_dict(self) -> dict[str, object]:
        return {
            "token": self.token,
            "legacy_surface": self.legacy_surface,
            "neutral_surface": self.neutral_surface,
            "files": list(self.files),
            "owner": self.owner,
            "reason": self.reason,
            "removal_criterion": self.removal_criterion,
            "validation_oracle": self.validation_oracle,
        }


REQUIRED_COMPATIBILITY_FIELDS: tuple[str, ...] = (
    "token",
    "legacy_surface",
    "neutral_surface",
    "files",
    "owner",
    "reason",
    "removal_criterion",
    "validation_oracle",
)


LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER: tuple[
    GraphRuntimeCompatibilityEntry, ...
] = (
    GraphRuntimeCompatibilityEntry(
        token="board_kuzu_path",
        legacy_surface="BoardGraphRuntime compatibility shim",
        neutral_surface="GraphRuntimeStore.exists/graph_state/footprint",
        files=(
            "okto_pulse/core/kg/interfaces/board_graph_runtime.py",
            "okto_pulse/core/kg/schema.py",
        ),
        owner="okto-pulse-core/kg + okto-pulse-community/adapters",
        reason=(
            "Existing integrations and legacy tests still import the historical "
            "board graph runtime/path shim while new core paths consume "
            "GraphRuntimeStore."
        ),
        removal_criterion=(
            "Remove after Community and test fixtures stop importing board path "
            "symbols and all startup/lifecycle checks consume GraphRuntimeStore "
            "or GraphLifecycle ports."
        ),
        validation_oracle=(
            "GraphRuntimeSurfaceGate, R16D lifecycle ports, AF17 runtime store "
            "tests and kg.schema import classification gate."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="open_kuzu_db",
        legacy_surface="BoardGraphRuntime concrete DB opener",
        neutral_surface="GraphTransaction/GlobalDiscoveryRuntime adapter methods",
        files=("okto_pulse/core/kg/interfaces/board_graph_runtime.py",),
        owner="okto-pulse-community/adapters",
        reason=(
            "Global-discovery Community adapters need a transition hook for the "
            "local Ladybug/Kuzu runtime without reintroducing imports in core."
        ),
        removal_criterion=(
            "Remove when GlobalDiscoveryRuntime exposes every concrete open/load "
            "operation through adapter-owned methods and no core-facing shim uses "
            "the Kuzu name."
        ),
        validation_oracle=(
            "R09 global discovery runtime tests and graph runtime surface gate."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="apply_ladybug_lifecycle_step",
        legacy_surface="safe-write lifecycle callable",
        neutral_surface="GraphLifecycle operation contract",
        files=(
            "okto_pulse/core/kg/interfaces/board_graph_runtime.py",
            "okto_pulse/core/kg/schema.py",
        ),
        owner="okto-pulse-community/adapters",
        reason=(
            "The rebuild/safe-write orchestrator still accepts a per-step "
            "callable while the broader lifecycle surface is represented by "
            "GraphLifecycle."
        ),
        removal_criterion=(
            "Remove after the lifecycle-step primitive is either represented on "
            "GraphLifecycle or the orchestrator no longer needs adapter-specific "
            "per-step callbacks."
        ),
        validation_oracle=(
            "R16D lifecycle ports, global discovery consumer gate and AF37 "
            "compatibility tests."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="KuzuNodeRef",
        legacy_surface="SQLite audit/outbox model name and table",
        neutral_surface="Graph node reference audit ledger",
        files=("okto_pulse/core/models/db.py",),
        owner="okto-pulse-core/kg-governance",
        reason=(
            "The persisted table name and ORM class are a public migration "
            "surface for consolidation audit, undo and global discovery outbox."
        ),
        removal_criterion=(
            "Introduce a database migration plus neutral ORM alias/read model, "
            "then retire the legacy class name after outbox and audit consumers "
            "read the neutral surface."
        ),
        validation_oracle=(
            "KG transaction/governance tests and graph runtime compatibility ledger."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="kuzu_node_id",
        legacy_surface="REST/MCP KG DTO and ORM field",
        neutral_surface="graph_node_id",
        files=(
            "okto_pulse/core/models/db.py",
            "okto_pulse/core/kg/schemas.py",
        ),
        owner="okto-pulse-core/kg-api-compat",
        reason=(
            "Existing clients and persisted rows still use the legacy node-id "
            "field. Renaming it without an additive alias would break REST/MCP "
            "payload compatibility."
        ),
        removal_criterion=(
            "Add graph_node_id as the canonical response/input alias, prove "
            "legacy and neutral fields are equivalent, then deprecate the legacy "
            "field in a versioned API window."
        ),
        validation_oracle=(
            "AF37 public compatibility tests and KG primitive/session tests."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="kg_kuzu_",
        legacy_surface="runtime settings field prefix",
        neutral_surface="graph_runtime_* settings alias",
        files=(
            "okto_pulse/core/infra/config.py",
            "okto_pulse/core/api/settings.py",
            "okto_pulse/core/application/boundary/core_settings_defaults_gate.py",
        ),
        owner="okto-pulse-core/settings + okto-pulse-community/settings",
        reason=(
            "The current public settings API exposes Kuzu-named runtime knobs; "
            "they must remain stable until neutral aliases ship."
        ),
        removal_criterion=(
            "Introduce graph_runtime_* aliases with API/UI parity, keep legacy "
            "env names during migration, then remove legacy names through the "
            "public config stability gate."
        ),
        validation_oracle=(
            "core_settings_defaults_gate, public_config_stability and settings "
            "runtime effect tests."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="kg_connection_pool_size",
        legacy_surface="runtime settings connection-pool field",
        neutral_surface="graph_runtime_connection_pool_size settings alias",
        files=(
            "okto_pulse/core/infra/config.py",
            "okto_pulse/core/api/settings.py",
            "okto_pulse/core/kg/config_guard.py",
            "okto_pulse/core/kg/connection_pool.py",
            "okto_pulse/core/application/boundary/core_settings_defaults_gate.py",
        ),
        owner="okto-pulse-core/settings + okto-pulse-community/settings",
        reason=(
            "The connection-pool knob is part of the public runtime settings "
            "API even though it lacks the kg_kuzu_ prefix; it must remain "
            "stable until a provider-neutral alias ships."
        ),
        removal_criterion=(
            "Introduce graph_runtime_connection_pool_size with API/UI parity, "
            "keep the legacy field during migration, then remove it through "
            "public config stability and connection-pool regression gates."
        ),
        validation_oracle=(
            "core_settings_defaults_gate, KGConfigChangeGuard tests and runtime "
            "settings effect tests."
        ),
    ),
    GraphRuntimeCompatibilityEntry(
        token="graph_lbug_bytes",
        legacy_surface="KG health storage footprint response",
        neutral_surface="storage_footprint_proxy.total_bytes/primary_bytes",
        files=("okto_pulse/core/api/kg_health.py",),
        owner="okto-pulse-core/kg-health",
        reason=(
            "Dashboard clients still read the legacy byte field; the neutral "
            "storage footprint fields are additive and preferred."
        ),
        removal_criterion=(
            "Remove only after REST/MCP/UI consumers read total_bytes/primary_bytes "
            "and a compatibility test proves no client depends on graph_lbug_bytes."
        ),
        validation_oracle="AF37 public compatibility and KG health contract tests.",
    ),
    GraphRuntimeCompatibilityEntry(
        token="kuzu_error",
        legacy_surface="REST/MCP problem type",
        neutral_surface="graph_backend_error",
        files=(
            "okto_pulse/core/api/kg_routes.py",
            "okto_pulse/core/application/use_cases/kg_routes_crud.py",
            "okto_pulse/core/kg/graph_availability.py",
        ),
        owner="okto-pulse-core/kg-api-compat",
        reason=(
            "Problem details are part of the public error contract; callers may "
            "branch on the legacy type."
        ),
        removal_criterion=(
            "Add a neutral graph_backend_error type while preserving the legacy "
            "type mapping for one API window, then retire after client parity."
        ),
        validation_oracle="AF37 public compatibility tests and KG route tests.",
    ),
    GraphRuntimeCompatibilityEntry(
        token="kuzu_lock_retries_5m",
        legacy_surface="queue health retry counter",
        neutral_surface="graph_lock_retries_5m",
        files=(
            "okto_pulse/core/api/queue_health.py",
            "okto_pulse/core/kg/commit_coordinator.py",
        ),
        owner="okto-pulse-core/kg-operations",
        reason=(
            "The operations dashboard polls the legacy counter name while the "
            "lock/retry implementation remains adapter-owned."
        ),
        removal_criterion=(
            "Expose a neutral graph_lock_retries_5m field and prove queue-health "
            "REST/MCP/UI parity before dropping the Kuzu-named field."
        ),
        validation_oracle="Queue health tests and AF37 public compatibility tests.",
    ),
)


class GraphRuntimeSurfaceGate:
    gate_id = "graph_runtime_surface"

    _FORBIDDEN_TERMS: tuple[str, ...] = (
        "Path",
        "graph.lbug",
        "board_kuzu_path",
        "ladybug",
        "neptune",
    )
    _COMPATIBILITY_ALLOWLIST: tuple[str, ...] = (
        "okto_pulse/core/kg/interfaces/board_graph_runtime.py",
        "okto_pulse/core/kg/schema.py",
    )
    _SKIPPED_LEGACY_CONTRACTS: frozenset[str] = frozenset({
        "okto_pulse/core/kg/interfaces/board_graph_runtime.py",
        "okto_pulse/core/kg/schema.py",
    })
    _PATH_TOKEN = re.compile(r"\bPath\b")

    def run(self, data: GraphRuntimeSurfaceGateInput | None = None) -> GateReport:
        data = data or GraphRuntimeSurfaceGateInput()
        root = self._source_root(data.source_root)
        files = self._scan_targets(root)
        violations = []
        for file_path in files:
            rel = self._rel(file_path)
            if rel in self._SKIPPED_LEGACY_CONTRACTS:
                continue
            try:
                lines = file_path.read_text(encoding="utf-8").splitlines()
            except OSError as exc:
                violations.append({
                    "file": rel,
                    "line": 0,
                    "term": "<read_error>",
                    "detail": str(exc),
                })
                continue
            for line_no, line in enumerate(lines, start=1):
                lower = line.lower()
                for term in self._FORBIDDEN_TERMS:
                    matched = bool(self._PATH_TOKEN.search(line)) if term == "Path" else term in lower
                    if matched:
                        violations.append({
                            "file": rel,
                            "line": line_no,
                            "term": term,
                        })

        ledger_findings = self._validate_compatibility_ledger(root)
        evidence = {
            "forbidden_terms": list(self._FORBIDDEN_TERMS),
            "scanned_files": [self._rel(p) for p in files],
            "compatibility_allowlist": list(self._COMPATIBILITY_ALLOWLIST),
            "compatibility_ledger": [
                entry.as_dict()
                for entry in LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER
            ],
            "compatibility_ledger_findings": ledger_findings,
            "violations": violations,
        }
        if violations or ledger_findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="graph runtime core contract surface",
                status="blocking" if data.mode == "blocking" else "xfail_advisory",
                severity="high",
                owner="okto-pulse-core/kg",
                evidence=evidence,
                observed_value=violations + ledger_findings,
                expected_value=[],
                remediation_hint=(
                    "Move physical storage and backend-specific symbols to an "
                    "edition adapter or a reviewed compatibility shim with owner "
                    "and removal criterion."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="graph runtime core contract surface",
            status="passed",
            severity="low",
            owner="okto-pulse-core/kg",
            evidence=evidence,
        )

    def _source_root(self, source_root: Path | None) -> Path:
        root = source_root or Path(__file__).resolve().parents[4]
        root = Path(root)
        if (root / "okto_pulse" / "core").exists():
            return root
        if (root / "src" / "okto_pulse" / "core").exists():
            return root / "src"
        return root

    def _scan_targets(self, source_root: Path) -> list[Path]:
        core_root = source_root / "okto_pulse" / "core"
        targets: list[Path] = []
        interfaces = core_root / "kg" / "interfaces"
        if interfaces.exists():
            targets.extend(sorted(interfaces.glob("*.py")))
        schema_contract = core_root / "kg" / "schema_contract.py"
        if schema_contract.exists():
            targets.append(schema_contract)
        return targets

    def _rel(self, path: Path) -> str:
        parts = path.parts
        if "okto_pulse" in parts:
            idx = parts.index("okto_pulse")
            return Path(*parts[idx:]).as_posix()
        return path.as_posix()

    def _validate_compatibility_ledger(self, source_root: Path) -> list[dict[str, object]]:
        findings: list[dict[str, object]] = []
        for entry in LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER:
            missing = [
                field
                for field in REQUIRED_COMPATIBILITY_FIELDS
                if not getattr(entry, field)
            ]
            if missing:
                findings.append({
                    "token": entry.token or "<missing>",
                    "diagnostic_code": "incomplete_compatibility_entry",
                    "missing_fields": missing,
                })
                continue

            existing_files = [source_root / file for file in entry.files]
            readable_files = [file for file in existing_files if file.exists()]
            if not readable_files:
                continue
            present = False
            for file in readable_files:
                try:
                    if entry.token in file.read_text(encoding="utf-8"):
                        present = True
                        break
                except OSError as exc:
                    findings.append({
                        "token": entry.token,
                        "diagnostic_code": "compatibility_file_read_error",
                        "file": self._rel(file),
                        "detail": str(exc),
                    })
                    present = True
                    break
            if not present:
                findings.append({
                    "token": entry.token,
                    "diagnostic_code": "stale_compatibility_entry",
                    "files": list(entry.files),
                    "owner": entry.owner,
                    "removal_criterion": entry.removal_criterion,
                })
        return findings


__all__ = [
    "GraphRuntimeCompatibilityEntry",
    "GraphRuntimeSurfaceGate",
    "GraphRuntimeSurfaceGateInput",
    "LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER",
    "REQUIRED_COMPATIBILITY_FIELDS",
]
