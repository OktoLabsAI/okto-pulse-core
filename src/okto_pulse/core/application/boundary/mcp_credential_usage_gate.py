"""McpCredentialUsageGate — block NEW direct MCP auth/ACL concretes in core.

After R08-A the MCP credential flows through the ``McpAuthenticator`` port
(``ports/mcp_auth.py``) + the Community adapter, and R-P2-09 removed the
``_active_api_key`` ContextVar carrier. The MCP server facades
(``_get_agent_ctx`` / ``_get_authenticated_agent``), direct
``AgentService.get_agent_by_key`` calls, and the retired ``_active_api_key``
symbol are restricted: a NEW core consumer that needs MCP identity must go
through the request-scoped shim or the port.

Detection is deterministic, AST-based and ALIAS-AWARE (R05-D lesson — the same
import-as / from-import / qualified-attribute / ASSIGNMENT-CHAIN resolution the
data_provider_ownership_gate uses): ``from x import _get_agent_ctx as f; f()`` or
``g = _get_agent_ctx; g()`` is resolved to the canonical symbol BEFORE the
allowlist check, so an aliased bypass cannot slip through. R06 extends the same
gate to the concrete AuthContext bridge and local board-ACL fallback symbols so a
new core MCP/KG consumer cannot reintroduce the retired relational bridge.
String literals are ``ast.Constant`` and are never flagged.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

#: Raw credential / legacy-facade / concrete AuthContext and local ACL symbols
#: whose direct use is restricted. The retired ``_active_api_key`` name stays
#: listed so a reintroduction or an aliased stale import still fails fast.
SENSITIVE_SYMBOLS: tuple[str, ...] = (
    "_active_api_key",
    "get_agent_by_key",
    "_get_agent_ctx",
    "_get_authenticated_agent",
    "MCPAuthContext",
    "create_mcp_auth_factory",
    "auth_context_from_session",
    "AgentService",
    "list_boards_for_agent",
)

#: Core files allowed to reference the sensitive symbols:
#:  * the MCP server request-scope shim, facades and legacy lookup calls;
#:  * the canonical ``AgentService`` definition;
#:  * pre-existing application use cases that legitimately orchestrate
#:    AgentService outside MCP/KG auth fallback paths.
ALLOWLISTED_FILES: frozenset[str] = frozenset(
    {
        "okto_pulse/core/application/use_cases/agent_crud.py",
        "okto_pulse/core/application/use_cases/boards_crud.py",
        "okto_pulse/core/application/use_cases/list_boards_for_agent.py",
        "okto_pulse/core/application/use_cases/mcp_board_crud.py",
        "okto_pulse/core/application/use_cases/update_agent.py",
        "okto_pulse/core/application/use_cases/update_board_overrides.py",
        "okto_pulse/core/mcp/server.py",
        "okto_pulse/core/services/__init__.py",
        "okto_pulse/core/services/main.py",
    }
)


@dataclass(frozen=True)
class CredentialUsageFinding:
    file: str
    symbol: str
    allowlisted: bool


@dataclass(frozen=True)
class CredentialUsageReport:
    findings: tuple[CredentialUsageFinding, ...]
    violations: tuple[CredentialUsageFinding, ...]
    allowlist: tuple[str, ...]
    scanned_root: str

    @property
    def ok(self) -> bool:
        return not self.violations


def _default_source_root() -> Path:
    # src/okto_pulse/core/application/boundary/mcp_credential_usage_gate.py -> src/
    return Path(__file__).resolve().parents[4]


_SENSITIVE = frozenset(SENSITIVE_SYMBOLS)


def _build_alias_map(tree: ast.Module) -> dict[str, str]:
    """Map every LOCAL name that ultimately refers to a sensitive symbol to its
    canonical name (R05-D lesson). Resolves import renames and assignment chains:
      - ``from x import _get_agent_ctx``            -> {_get_agent_ctx: ...}
      - ``from x import _get_agent_ctx as f``       -> {f: _get_agent_ctx}
      - ``import pkg.mod._active_api_key as Y``     -> {Y: _active_api_key}
      - ``g = _get_agent_ctx`` / ``h = g``          -> transitive (fixpoint)
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name in _SENSITIVE:
                    aliases[a.asname or a.name] = a.name
        elif isinstance(node, ast.Import):
            for a in node.names:
                last = a.name.rsplit(".", 1)[-1]
                if last in _SENSITIVE:
                    aliases[a.asname or last] = last

    def _resolve(name: str) -> str | None:
        return name if name in _SENSITIVE else aliases.get(name)

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
                canonical = _resolve(node.value.id)
                if canonical is None:
                    continue
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name) and aliases.get(tgt.id) != canonical:
                        aliases[tgt.id] = canonical
                        changed = True
    return aliases


def _sensitive_symbols_in(tree: ast.Module) -> set[str]:
    """Return the canonical sensitive symbols referenced in ``tree`` — resolving
    import/assignment ALIASES and qualified attribute access so an aliased bypass
    (``from x import _get_agent_ctx as f; f()``) is detected as the canonical."""
    alias_map = _build_alias_map(tree)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            canonical = node.id if node.id in _SENSITIVE else alias_map.get(node.id)
            if canonical is not None:
                found.add(canonical)
        elif isinstance(node, ast.Attribute) and node.attr in _SENSITIVE:
            # qualified access: ``mod.get_agent_by_key`` / stale ``server._active_api_key``
            found.add(node.attr)
    return found


def run_mcp_credential_usage_gate(
    source_root: Path | None = None,
) -> CredentialUsageReport:
    """Scan ``source_root/okto_pulse/core`` for direct uses of the sensitive raw
    credential symbols; any use outside :data:`ALLOWLISTED_FILES` is a violation.
    """
    root = source_root or _default_source_root()
    core = root / "okto_pulse" / "core"

    findings: list[CredentialUsageFinding] = []
    for py in sorted(core.rglob("*.py")):
        rel = py.relative_to(root).as_posix()
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for symbol in sorted(_sensitive_symbols_in(tree)):
            findings.append(
                CredentialUsageFinding(
                    file=rel,
                    symbol=symbol,
                    allowlisted=rel in ALLOWLISTED_FILES,
                )
            )

    violations = tuple(f for f in findings if not f.allowlisted)
    return CredentialUsageReport(
        findings=tuple(findings),
        violations=violations,
        allowlist=tuple(sorted(ALLOWLISTED_FILES)),
        scanned_root=core.relative_to(root).as_posix(),
    )


__all__ = [
    "SENSITIVE_SYMBOLS",
    "ALLOWLISTED_FILES",
    "CredentialUsageFinding",
    "CredentialUsageReport",
    "run_mcp_credential_usage_gate",
]
