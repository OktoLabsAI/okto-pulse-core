"""Typed card workflow errors shared by services and application guards."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowRemediationMessage,
    serialize_bug_workflow_remediation,
)


class CardOperationError(ValueError):
    """Typed card workflow error for API/MCP callers."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        remediation: str | None = None,
        facts: dict[str, Any] | None = None,
        workflow_remediation: BugWorkflowRemediationMessage | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation
        self.facts = facts or {}
        self.workflow_remediation = workflow_remediation

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        if self.facts:
            payload["facts"] = self.facts
        if self.workflow_remediation:
            serialized = serialize_bug_workflow_remediation(self.workflow_remediation)
            if serialized:
                payload["remediation_message"] = serialized
                for key in (
                    "reason_code",
                    "remediation_path",
                    "next_action",
                    "semantic_gap_required",
                    "eligible_scenarios_count",
                    "hotfix_lane_status",
                    "actions",
                ):
                    payload[key] = serialized[key]
        return payload


__all__ = ["CardOperationError"]
