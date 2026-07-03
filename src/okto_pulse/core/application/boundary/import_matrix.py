"""ADR-01 layer import matrix — permit/prohibit rules per resolved layer.

Spec #12, tr_9203da76 + tr_9f229444 + contract ``api_24add273``.

- domain / application / ports (pure): no FastAPI, Starlette, MCP, concrete
  SQLAlchemy, concrete Kuzu/Ladybug, APScheduler, Community repos, or direct
  global-provider symbols. Violations are ``blocking``.
- inbound adapters: no direct DB / concrete KG / global settings outside the
  composition root. Violations are ``blocking``.
- outbound adapters: must not import inbound, API routers or MCP tools.
- legacy application/transitional debt (``services``/``commands``): the same
  prohibitions apply, but violations land as ``baseline`` with owner +
  promotion_criteria (known decoupling debt), never silent green.
- composition / packaging / event-messaging: permissive (wiring & ops).

Pure data; importable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .layer_resolver import (
    APPLICATION,
    COMPOSITION,
    DOMAIN,
    EVENT_RUNTIME_MESSAGING,
    FUTURE_TARGET,
    INBOUND,
    LEGACY_APPLICATION_TRANSITIONAL_DEBT,
    OUTBOUND,
    PACKAGING_OPS_EDITION,
    PORTS,
)
from .report import GateStatus

MATRIX_VERSION = "ADR-01"

#: Transport / scheduling frameworks a pure or inbound layer must not import.
FRAMEWORK_ROOTS: frozenset[str] = frozenset(
    {"fastapi", "starlette", "mcp", "fastmcp", "apscheduler", "uvicorn"}
)
#: Concrete persistence / graph engines (the abstraction must be a port).
CONCRETE_PERSISTENCE_ROOTS: frozenset[str] = frozenset(
    {"sqlalchemy", "ladybug", "kuzu", "aiosqlite", "asyncpg"}
)
#: Transport/auth-coupled symbols that must never appear in a pure layer.
PURE_FORBIDDEN_SYMBOLS: frozenset[str] = frozenset(
    {"Request", "Depends", "ContextVar", "_active_api_key"}
)
#: Internal module prefixes that pure layers must not import (Community repos,
#: concrete adapter packages reached as modules rather than framework roots).
COMMUNITY_PREFIXES: frozenset[str] = frozenset({"okto_pulse.community"})


@dataclass(frozen=True)
class AllowDenyRule:
    """Permit/prohibit rule for one resolved layer."""

    forbidden_import_roots: frozenset[str] = frozenset()
    forbidden_symbols: frozenset[str] = frozenset()
    #: Resolved layers this layer must not import (cross-layer direction guard).
    forbidden_target_layers: frozenset[str] = frozenset()
    #: Dotted internal prefixes this layer must not import.
    forbidden_module_prefixes: frozenset[str] = field(default_factory=frozenset)
    #: Status produced when a rule is violated.
    status_on_violation: GateStatus = "blocking"
    owner: str | None = None
    promotion_criteria: str | None = None


_PURE_RULE = AllowDenyRule(
    forbidden_import_roots=FRAMEWORK_ROOTS | CONCRETE_PERSISTENCE_ROOTS,
    forbidden_symbols=PURE_FORBIDDEN_SYMBOLS,
    forbidden_target_layers=frozenset({INBOUND, OUTBOUND}),
    forbidden_module_prefixes=COMMUNITY_PREFIXES,
    status_on_violation="blocking",
)

_LEGACY_RULE = AllowDenyRule(
    forbidden_import_roots=FRAMEWORK_ROOTS | CONCRETE_PERSISTENCE_ROOTS,
    forbidden_symbols=PURE_FORBIDDEN_SYMBOLS,
    forbidden_target_layers=frozenset(),
    forbidden_module_prefixes=frozenset(),
    # Transitional debt: surfaced but not blocking, with explicit governance.
    status_on_violation="baseline",
    owner="okto-pulse-core/architecture",
    promotion_criteria=(
        "Migrate the legacy service/command into application use cases + ports; "
        "then re-classify under the strict pure-layer rule."
    ),
)

_INBOUND_RULE = AllowDenyRule(
    forbidden_import_roots=CONCRETE_PERSISTENCE_ROOTS,
    forbidden_symbols=frozenset(),
    forbidden_target_layers=frozenset(),
    forbidden_module_prefixes=COMMUNITY_PREFIXES,
    status_on_violation="blocking",
)

_OUTBOUND_RULE = AllowDenyRule(
    forbidden_import_roots=FRAMEWORK_ROOTS,
    forbidden_symbols=frozenset(),
    # Outbound adapters must not depend on inbound (API routers / MCP tools).
    forbidden_target_layers=frozenset({INBOUND}),
    forbidden_module_prefixes=COMMUNITY_PREFIXES,
    status_on_violation="blocking",
)

_EVENT_RULE = AllowDenyRule(
    forbidden_import_roots=FRAMEWORK_ROOTS,
    forbidden_target_layers=frozenset({INBOUND}),
    status_on_violation="blocking",
)

#: Composition root and packaging/ops scripts are intentionally permissive —
#: the composition root wires concrete adapters and ops scripts are edition code.
_PERMISSIVE_RULE = AllowDenyRule(status_on_violation="passed")

LAYER_IMPORT_MATRIX: dict[str, AllowDenyRule] = {
    DOMAIN: _PURE_RULE,
    APPLICATION: _PURE_RULE,
    PORTS: _PURE_RULE,
    FUTURE_TARGET: _PURE_RULE,
    INBOUND: _INBOUND_RULE,
    OUTBOUND: _OUTBOUND_RULE,
    EVENT_RUNTIME_MESSAGING: _EVENT_RULE,
    LEGACY_APPLICATION_TRANSITIONAL_DEBT: _LEGACY_RULE,
    COMPOSITION: _PERMISSIVE_RULE,
    PACKAGING_OPS_EDITION: _PERMISSIVE_RULE,
}


def rule_for(layer: str) -> AllowDenyRule | None:
    """The AllowDenyRule for a resolved layer, or None when unconstrained."""
    return LAYER_IMPORT_MATRIX.get(layer)
