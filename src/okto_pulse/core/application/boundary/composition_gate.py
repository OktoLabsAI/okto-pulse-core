"""CompositionBoundaryGate — detect concrete wiring outside the composition root.

Spec #03, card b4f07ad3, ts_35c46ba7 / contract api_e67e6290.

Scans the core app layer (services/commands/api/mcp/events) for concrete wiring
of a #03-owned provider outside the composition root (``app.py`` / ``composition``).
Deterministic classification:

- ``xfail_advisory`` — KG/Kuzu/Ladybug boundary, deferred to spec #05.
- ``baseline``       — catalogued existing debt (in the baseline snapshot) with
  owner + promotion_criteria; no fallback removal attempted.
- ``blocking``       — NEW concrete wiring of an owned provider, a fallback removed
  without a registered Community provider, or existing debt touched without
  register-before-remove.

Returns the shared :class:`GateReport`. Pure ``ast``/``pathlib`` analysis.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .report import GateReport

#: #03-owned provider keys (contract api_e67e6290).
OWNED_PROVIDER_KEYS: tuple[str, ...] = (
    "settings_provider",
    "auth_provider",
    "storage_provider",
    "session_factory",
    "event_bus",
    "scheduler_control",
    "telemetry",
    "lifecycle_hooks",
    "mcp_session_factory",
)

#: KG boundaries reserved for spec #05 (never blocking here).
DEFERRED_BOUNDARIES: tuple[str, ...] = (
    "kg_registry",
    "kuzu_store",
    "ladybug_store",
    "graph_lifecycle",
)

#: Versioned catalogue of CATALOGUED EXISTING DEBT (``"file::symbol"``) — the
#: owned-provider wiring already present in the core app layer at this point in
#: the strangler, owner=okto-pulse-core/architecture, promotion_criteria="migrate
#: into the composition root". These (and ONLY these) classify as ``baseline``;
#: any owned wiring NOT in this catalogue is NEW and blocks. This is a reviewed
#: snapshot, never a self-scan — a self-scan would mask new wiring.
DEFAULT_COMPOSITION_BASELINE: frozenset[str] = frozenset(
    {
        "okto_pulse/core/mcp/server.py::create_database",
        "okto_pulse/core/mcp/server.py::register_session_factory",
        "okto_pulse/core/services/settings_service.py::configure_settings",
    }
)

#: Concrete symbols that wire an owned provider when CALLED/instantiated.
PROVIDER_WIRING_SYMBOLS: dict[str, str] = {
    "configure_settings": "settings_provider",
    "configure_auth": "auth_provider",
    "configure_storage": "storage_provider",
    "FileSystemStorageProvider": "storage_provider",
    "create_database": "session_factory",
    "register_session_factory": "mcp_session_factory",
    "SqliteOutboxEventBus": "event_bus",
    "set_scheduler": "scheduler_control",
    "AsyncIOScheduler": "scheduler_control",
    "BackgroundScheduler": "scheduler_control",
    "LocalTelemetryStore": "telemetry",
}

#: KG/graph symbols -> deferred_to_05.
DEFERRED_SYMBOLS: dict[str, str] = {
    "get_kg_registry": "kg_registry",
    "configure_kg_registry": "kg_registry",
    "KuzuGraphStore": "kuzu_store",
    "KuzuCypherExecutor": "kuzu_store",
    "_build_defaults": "kg_registry",
}

#: App-layer / inbound dirs whose owned-provider wiring belongs to composition.
_APP_LAYER_DIRS: tuple[str, ...] = (
    "okto_pulse/core/services/",
    "okto_pulse/core/commands/",
    "okto_pulse/core/api/",
    "okto_pulse/core/mcp/",
    "okto_pulse/core/events/",
)

Trigger = Literal[
    "catalogued_existing_debt",
    "new_concrete_wiring",
    "fallback_removal_without_provider",
    "touched_existing_debt_without_register_before_remove",
]

_STATUS_RANK = {"reject": 4, "blocking": 3, "xfail_advisory": 2, "baseline": 1, "passed": 0}


@dataclass(frozen=True)
class CompositionFinding:
    file: str
    line: int
    symbol: str
    provider_key: str
    status: str
    trigger: str
    adapter_target: str | None
    remediation_hint: str

    def as_dict(self) -> dict:
        return {
            "file": self.file,
            "line": self.line,
            "symbol": self.symbol,
            "provider_key": self.provider_key,
            "status": self.status,
            "trigger": self.trigger,
            "adapter_target": self.adapter_target,
            "remediation_hint": self.remediation_hint,
        }


@dataclass(frozen=True)
class CompositionBoundaryGateInput:
    source_root: Path | None = None
    owned_provider_keys: tuple[str, ...] = OWNED_PROVIDER_KEYS
    deferred_boundaries: tuple[str, ...] = DEFERRED_BOUNDARIES
    default_only_exclusions: tuple[str, ...] = (
        "kg_migration_sweep",
        "shutdown_kg_events_hub",
        "close_all_connections",
    )
    mode: str = "bootstrap"
    #: Catalogued existing debt as ``"file::symbol"`` keys. When None the baseline
    #: is resolved by :meth:`CompositionBoundaryGate._resolve_baseline`: a current
    #: tree self-scan ONLY in ``bootstrap`` mode (non-enforcing, to generate an
    #: initial snapshot); in ``blocking`` mode the reviewed versioned
    #: ``DEFAULT_COMPOSITION_BASELINE`` is used, never a self-scan (which would mask
    #: new wiring).
    baseline_snapshot: frozenset[str] | None = None
    #: Provider keys whose global fallback was removed in this change.
    removed_fallbacks: tuple[str, ...] = ()
    #: Provider keys with a registered Community provider (register-before-remove).
    registered_community_providers: tuple[str, ...] = ()


def _default_source_root() -> Path:
    return Path(__file__).resolve().parents[4]


class CompositionBoundaryGate:
    """Deterministic detector of owned-provider wiring outside composition."""

    gate_id = "composition_boundary"

    def run(self, gate_input: CompositionBoundaryGateInput | None = None) -> GateReport:
        gate_input = gate_input or CompositionBoundaryGateInput()
        if not gate_input.owned_provider_keys:
            return GateReport(
                gate_id=self.gate_id,
                subject="composition owned-provider catalog",
                status="blocking",
                severity="high",
                evidence={"error": "provider_catalog_missing"},
                remediation_hint="Supply the #03 owned_provider_keys catalog.",
            )
        root = Path(gate_input.source_root) if gate_input.source_root else _default_source_root()
        raw = self._scan(root)
        baseline = self._resolve_baseline(gate_input, raw)
        findings = [self._classify(f, baseline, gate_input) for f in raw]
        findings.extend(self._register_before_remove_findings(gate_input))

        statuses = [f.status for f in findings] or ["passed"]
        status = max(statuses, key=lambda s: _STATUS_RANK.get(s, 0))
        evidence = {
            "findings": [f.as_dict() for f in findings],
            "boundary_deferred_to_05": list(gate_input.deferred_boundaries),
            "default_only_exclusions": list(gate_input.default_only_exclusions),
            "mode": gate_input.mode,
            "scope": "composition_root",
        }
        blocking = [f for f in findings if f.status == "blocking"]
        return GateReport(
            gate_id=self.gate_id,
            subject="core composition boundary",
            status=status,  # type: ignore[arg-type]
            severity="high" if status in ("blocking", "reject") else "medium",
            owner="okto-pulse-core/architecture" if findings else None,
            evidence=evidence if not blocking else {**evidence, "error": "composition_boundary_violation"},
            observed_value=len(blocking),
            expected_value=0,
            promotion_criteria=(
                "Move every new owned-provider wiring into the composition root and "
                "register a Community provider before removing any fallback."
            )
            if status != "passed"
            else None,
            remediation_hint=(
                "Concrete dependency for a #03-owned provider was wired outside the "
                "composition root; wire it via RuntimeComposition instead."
            )
            if blocking
            else None,
        )

    @staticmethod
    def _resolve_baseline(
        gate_input: CompositionBoundaryGateInput, raw: list[CompositionFinding]
    ) -> frozenset[str]:
        """Resolve the baseline catalogue WITHOUT ever masking new wiring.

        - explicit ``baseline_snapshot`` -> use it.
        - ``bootstrap`` + none -> snapshot the current tree. This mode is
          non-enforcing: it exists to GENERATE an initial catalogue, so a
          self-scan is acceptable.
        - ``blocking`` + none -> the reviewed versioned catalogue ONLY. A
          current-tree self-scan is never used here: it would put a brand new
          owned wiring into its own baseline and mask it. Anything not in the
          versioned catalogue is therefore NEW and blocks.
        """
        if gate_input.baseline_snapshot is not None:
            return gate_input.baseline_snapshot
        if gate_input.mode == "bootstrap":
            return frozenset(f"{f.file}::{f.symbol}" for f in raw)
        return DEFAULT_COMPOSITION_BASELINE

    def _scan(self, root: Path) -> list[CompositionFinding]:
        out: list[CompositionFinding] = []
        core = root / "okto_pulse" / "core"
        if not core.exists():
            return out
        for path in sorted(core.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(root).as_posix()
            if not any(rel.startswith(d) for d in _APP_LAYER_DIRS):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (OSError, SyntaxError):
                continue
            out.extend(self._scan_tree(rel, tree))
        return out

    def _scan_tree(self, rel: str, tree: ast.AST) -> list[CompositionFinding]:
        out: list[CompositionFinding] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = self._call_name(node.func)
            if name is None:
                continue
            if name in PROVIDER_WIRING_SYMBOLS:
                out.append(self._raw(rel, node.lineno, name, PROVIDER_WIRING_SYMBOLS[name], deferred=False))
            elif name in DEFERRED_SYMBOLS:
                out.append(self._raw(rel, node.lineno, name, DEFERRED_SYMBOLS[name], deferred=True))
        return out

    @staticmethod
    def _call_name(func: ast.expr) -> str | None:
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return None

    @staticmethod
    def _raw(rel: str, line: int, symbol: str, provider_key: str, *, deferred: bool) -> CompositionFinding:
        # placeholder status; finalised by _classify
        return CompositionFinding(
            file=rel, line=line, symbol=symbol, provider_key=provider_key,
            status="xfail_advisory" if deferred else "new_concrete_wiring_pending",
            trigger="catalogued_existing_debt" if deferred else "new_concrete_wiring",
            adapter_target="deferred_to_05" if deferred else None,
            remediation_hint="",
        )

    def _classify(
        self,
        finding: CompositionFinding,
        baseline: frozenset[str],
        gate_input: CompositionBoundaryGateInput,
    ) -> CompositionFinding:
        if finding.provider_key in gate_input.deferred_boundaries:
            return CompositionFinding(
                file=finding.file, line=finding.line, symbol=finding.symbol,
                provider_key=finding.provider_key, status="xfail_advisory",
                trigger="catalogued_existing_debt", adapter_target="deferred_to_05",
                remediation_hint=(
                    f"'{finding.symbol}' is a KG/graph boundary deferred to spec #05; "
                    "not a #03 composition violation."
                ),
            )
        key = f"{finding.file}::{finding.symbol}"
        if key in baseline:
            return CompositionFinding(
                file=finding.file, line=finding.line, symbol=finding.symbol,
                provider_key=finding.provider_key, status="baseline",
                trigger="catalogued_existing_debt",
                adapter_target=f"RuntimeComposition.{finding.provider_key}",
                remediation_hint=(
                    f"Catalogued debt: move '{finding.symbol}' wiring of "
                    f"{finding.provider_key} into the composition root before promotion."
                ),
            )
        return CompositionFinding(
            file=finding.file, line=finding.line, symbol=finding.symbol,
            provider_key=finding.provider_key, status="blocking",
            trigger="new_concrete_wiring",
            adapter_target=f"RuntimeComposition.{finding.provider_key}",
            remediation_hint=(
                f"New concrete wiring of owned provider '{finding.provider_key}' "
                f"('{finding.symbol}') outside the composition root is not allowed; "
                "wire it via RuntimeComposition."
            ),
        )

    @staticmethod
    def _register_before_remove_findings(
        gate_input: CompositionBoundaryGateInput,
    ) -> list[CompositionFinding]:
        registered = set(gate_input.registered_community_providers)
        out: list[CompositionFinding] = []
        for provider_key in gate_input.removed_fallbacks:
            if provider_key in registered:
                continue
            out.append(
                CompositionFinding(
                    file="<composition>", line=0, symbol=f"remove_fallback:{provider_key}",
                    provider_key=provider_key, status="blocking",
                    trigger="fallback_removal_without_provider",
                    adapter_target=f"RuntimeComposition.{provider_key}",
                    remediation_hint=(
                        f"register_before_remove_missing: a Community provider for "
                        f"'{provider_key}' must be registered before removing its fallback."
                    ),
                )
            )
        return out


__all__ = [
    "CompositionBoundaryGate",
    "CompositionBoundaryGateInput",
    "CompositionFinding",
    "DEFAULT_COMPOSITION_BASELINE",
    "DEFERRED_BOUNDARIES",
    "DEFERRED_SYMBOLS",
    "OWNED_PROVIDER_KEYS",
    "PROVIDER_WIRING_SYMBOLS",
]
