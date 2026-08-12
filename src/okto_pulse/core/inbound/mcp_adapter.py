"""MCPAdapterContract (spec #09, tr_5ba8fbac).

Maps an MCP tool call (its resolved agent context) to an ``ActorContext`` and
use case errors to the MCP tool's JSON error envelope, with NO business rule.
The MCP first-cut tools return ``json.dumps({"error": str(exc)})`` for handled
failures; ``error`` reproduces that. The agent display name flows through
``ActorContext.actor_name`` so the migrated use case keeps the MCP audit name.
Policy Compliance transition rejections use the same structured projection as
REST so clients can compare mutation failure evidence with transition preview.
"""

from __future__ import annotations

import json
from typing import Any

from okto_pulse.core.application.use_cases import ActorContext
from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionRejected,
)
from okto_pulse.core.domain.human_validation_cycle import (
    LifecycleTransitionConflictError,
)
from okto_pulse.core.inbound.policy_transition_error import (
    project_policy_transition_rejection,
)


class MCPAdapterContract:
    """Thin MCP inbound adapter contract."""

    source = "mcp"

    @staticmethod
    def actor(
        ctx: Any,
        *,
        board_id: str | None = None,
        realm_id: str | None = None,
    ) -> ActorContext:
        """Build the transport-neutral actor from the MCP agent context.

        ``ctx`` is the object returned by ``_get_agent_ctx`` (agent_id /
        agent_name / permissions). The MCP-resolved agent_name is carried so the
        use case preserves the tool's existing audit actor name.
        """
        return ActorContext(
            ctx.agent_id,
            "mcp",
            actor_name=getattr(ctx, "agent_name", None),
            actor_kind="agent",
            board_id=board_id,
            realm_id=realm_id or getattr(ctx, "realm_id", None),
            permissions=getattr(ctx, "permissions", None),
        )

    @staticmethod
    def error(exc: Exception) -> str:
        """Map a domain/use case error to the MCP JSON error envelope."""
        if isinstance(exc, PolicyTransitionRejected):
            return json.dumps(project_policy_transition_rejection(exc))
        if isinstance(exc, LifecycleTransitionConflictError):
            return json.dumps(exc.to_error_dict())
        code = getattr(exc, "code", None)
        if isinstance(code, str) and code.strip():
            details = getattr(exc, "details", None)
            payload: dict[str, Any] = {
                "error": code,
                "code": code,
                "detail": str(exc),
            }
            if isinstance(details, dict) and details:
                payload["details"] = details
            return json.dumps(payload)
        return json.dumps({"error": str(exc)})
