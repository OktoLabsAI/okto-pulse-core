"""Telemetry axis-independence dependency audit (spec R10-A, IMP4).

Proves two boundary properties of the telemetry decoupling axis:

  1. The R08 MCP-auth ports/adapters/gates do NOT import the telemetry subsystem
     (``okto_pulse.core.telemetry``) — auth and telemetry are independent axes, so
     R10 can move telemetry to Community without disturbing R08.
  2. ``okto_pulse.core.ports`` is the SINGLE contract entry-point for the
     telemetry ports/DTOs (they are re-exported there), so a consumer depends on
     the pure contract, not the runtime module.

Read-only static analysis (``ast`` + ``pathlib``); imports nothing heavy.
"""

from __future__ import annotations

import ast
from pathlib import Path

TELEMETRY_ROOT = "okto_pulse.core.telemetry"

#: R08 MCP-auth surfaces that MUST stay telemetry-free (axis independence).
R08_AUTH_MODULES: tuple[str, ...] = (
    "ports/mcp_auth.py",
    "kg/providers/embedded/mcp_auth_context.py",
    "application/boundary/mcp_credential_usage_gate.py",
)

#: The telemetry contracts that core.ports MUST re-export (single entry-point).
TELEMETRY_PORT_EXPORTS: tuple[str, ...] = (
    "TelemetryPort",
    "TelemetrySink",
    "TelemetryStateStore",
    "PublishHealthSource",
    "ProductAggregationPort",
    "TelemetryResult",
    "TelemetryState",
    "HealthReport",
    "ProductState",
)


def _default_core_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _imports_telemetry(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == TELEMETRY_ROOT or mod.startswith(TELEMETRY_ROOT + "."):
                return True
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == TELEMETRY_ROOT or a.name.startswith(TELEMETRY_ROOT + "."):
                    return True
    return False


def audit_telemetry_axis_independence(core_root: str | Path | None = None) -> dict:
    """Audit telemetry axis independence + the single-entry-point property."""
    root = Path(core_root) if core_root is not None else _default_core_root()

    auth_importing_telemetry: list[str] = []
    for rel in R08_AUTH_MODULES:
        py = root / rel
        if not py.exists():
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        if _imports_telemetry(tree):
            auth_importing_telemetry.append(rel)

    # core.ports re-exports every telemetry contract (single entry-point).
    from okto_pulse.core import ports as ports_pkg

    missing_exports = [
        name for name in TELEMETRY_PORT_EXPORTS if not hasattr(ports_pkg, name)
    ]

    ok = not auth_importing_telemetry and not missing_exports
    return {
        "ok": ok,
        "r08_auth_imports_telemetry": sorted(auth_importing_telemetry),
        "telemetry_port_single_entry_point": not missing_exports,
        "missing_port_exports": sorted(missing_exports),
        "audited_auth_modules": list(R08_AUTH_MODULES),
    }


__all__ = [
    "TELEMETRY_ROOT",
    "R08_AUTH_MODULES",
    "TELEMETRY_PORT_EXPORTS",
    "audit_telemetry_axis_independence",
]
