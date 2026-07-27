"""R06 tombstone for the former embedded MCP AuthContext bridge.

R06 moved the concrete MCP auth context implementation to
``okto_pulse.community.adapters.mcp_auth``. Core keeps only the pure contracts:
``okto_pulse.core.kg.interfaces.auth_context.AuthContext`` and
``okto_pulse.core.ports.mcp_auth``. This module remains as an import-light marker
so static audits can prove the old embedded runtime bridge no longer carries any
relational ACL fallback.
"""

from __future__ import annotations

from okto_pulse.core.kg.interfaces.auth_context import AuthContext
from okto_pulse.core.ports.mcp_auth import AuthSession

__all__ = ["AuthContext", "AuthSession"]
