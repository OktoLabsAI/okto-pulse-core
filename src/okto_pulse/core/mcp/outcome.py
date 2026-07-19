"""Transport-neutral MCP tool outcome contract.

The Core owns the semantic result of a tool call, while concrete editions own
the protocol projection (FastMCP, HTTP, stdio, ...).  Historically most Pulse
tools returned ``json.dumps(...)`` for both success and handled failures.  To a
protocol host that is an ordinary successful ``str`` return, which means domain
failures are exposed with ``isError=false`` and the annotated ``-> str`` output
schema double-encodes JSON under ``structuredContent.result``.

``McpToolOutcome`` is the compatibility bridge.  New handlers may return it
directly; edition adapters can also call :func:`coerce_mcp_tool_outcome` for a
legacy result while handlers are migrated incrementally.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


MCP_OUTCOME_CONTRACT_VERSION = "2.0"


class McpOutcomeKind(str, Enum):
    """Semantic outcome classes understood by every MCP transport."""

    SUCCESS = "success"
    ACTION_REQUIRED = "action_required"
    ERROR = "error"


ACTION_REQUIRED_CODES = frozenset(
    {
        "architecture_warning_acknowledgement_required",
        "confirmation_required",
        "impact_ack_required",
        "reviewer_separation_required",
    }
)

_RETRYABLE_CODES = frozenset(
    {
        "conflict",
        "resource_busy",
        "resource_locked",
        "spec_locked",
        "temporarily_unavailable",
        "timeout",
        "version_conflict",
    }
)

_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{2,80}$")


def _json_default(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return str(value)


def _native_payload(raw: Any) -> tuple[Any, str | None]:
    """Decode a legacy JSON string without guessing for ordinary prose."""

    if isinstance(raw, McpToolOutcome):
        return raw.payload, raw.legacy_text
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped and stripped[0] in "[{":
            try:
                return json.loads(stripped), raw
            except json.JSONDecodeError:
                pass
        return raw, raw
    if hasattr(raw, "model_dump"):
        return raw.model_dump(mode="json"), None
    if hasattr(raw, "to_dict"):
        return raw.to_dict(), None
    return raw, None


def _first_text(payload: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _error_code(payload: Mapping[str, Any], message: str | None) -> str:
    explicit = _first_text(payload, "error_code", "code")
    if explicit:
        return explicit
    raw_error = payload.get("error")
    if isinstance(raw_error, Mapping):
        nested = _first_text(raw_error, "error_code", "code")
        if nested:
            return nested
    if isinstance(raw_error, str) and _CODE_RE.fullmatch(raw_error.strip()):
        return raw_error.strip()

    lowered = (message or "").lower()
    if any(token in lowered for token in ("authentication", "api key", "unauthorized")):
        return "authentication_required"
    if "permission" in lowered or "forbidden" in lowered:
        return "permission_denied"
    if "not found" in lowered:
        return "not_found"
    if re.search(r"\blocked\b", lowered):
        return "resource_locked"
    if "conflict" in lowered or "version" in lowered and "expected" in lowered:
        return "version_conflict"
    if "gate" in lowered or "cannot move" in lowered or "blocked" in lowered:
        return "gate_blocked"
    if any(token in lowered for token in ("invalid", "must ", "required", "expected")):
        return "validation_failed"
    return "domain_error"


def _next_action(
    payload: Mapping[str, Any], *, tool_name: str | None, code: str | None
) -> dict[str, Any] | None:
    supplied = payload.get("next_action")
    if isinstance(supplied, Mapping):
        return dict(supplied)
    remediation = payload.get("remediation")
    if isinstance(remediation, str) and remediation.strip():
        return {"hint": remediation.strip()}
    if code not in ACTION_REQUIRED_CODES:
        return None

    arguments: dict[str, Any] = {}
    for key in (
        "ack_token",
        "confirmation_id",
        "expected_spec_version",
        "architecture_warning_acknowledgement",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            arguments[key] = value
    action: dict[str, Any] = {"rel": "retry_with_confirmation"}
    if tool_name:
        action["tool"] = tool_name
    if arguments:
        action["arguments"] = arguments
    return action


@dataclass(frozen=True, slots=True)
class McpToolOutcome:
    """Semantic result produced by a Core MCP command.

    ``payload`` always remains a native Python value.  ``legacy_text`` is kept
    only so an explicitly requested ``profile=legacy`` can preserve the old
    text response without re-serialising it.
    """

    kind: McpOutcomeKind
    payload: Any = None
    code: str | None = None
    message: str | None = None
    retryable: bool = False
    next_action: dict[str, Any] | None = None
    details: dict[str, Any] = field(default_factory=dict)
    legacy_text: str | None = None

    @classmethod
    def success(cls, payload: Any = None, *, legacy_text: str | None = None) -> "McpToolOutcome":
        return cls(McpOutcomeKind.SUCCESS, payload=payload, legacy_text=legacy_text)

    @classmethod
    def action_required(
        cls,
        payload: Any = None,
        *,
        code: str,
        message: str | None = None,
        next_action: dict[str, Any] | None = None,
        legacy_text: str | None = None,
    ) -> "McpToolOutcome":
        return cls(
            McpOutcomeKind.ACTION_REQUIRED,
            payload=payload,
            code=code,
            message=message,
            retryable=True,
            next_action=next_action,
            legacy_text=legacy_text,
        )

    @classmethod
    def error(
        cls,
        *,
        code: str,
        message: str,
        payload: Any = None,
        retryable: bool = False,
        next_action: dict[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
        legacy_text: str | None = None,
    ) -> "McpToolOutcome":
        return cls(
            McpOutcomeKind.ERROR,
            payload=payload,
            code=code,
            message=message,
            retryable=retryable,
            next_action=next_action,
            details=dict(details or {}),
            legacy_text=legacy_text,
        )

    @property
    def is_error(self) -> bool:
        return self.kind is McpOutcomeKind.ERROR

    def structured_content(self, *, tool_name: str | None = None) -> dict[str, Any]:
        """Return the canonical native MCP V2 envelope."""

        next_action = self.next_action
        if (
            next_action is None
            and self.kind is McpOutcomeKind.ACTION_REQUIRED
            and isinstance(self.payload, Mapping)
        ):
            next_action = _next_action(
                self.payload,
                tool_name=tool_name,
                code=self.code,
            )
        result: dict[str, Any] = {
            "outcome": self.kind.value,
            "data": self.payload,
            "error_code": self.code,
            "message": self.message,
            "retryable": bool(self.retryable),
            "next_action": next_action,
            "meta": {
                "contract": "okto-pulse.mcp-tool-outcome",
                "contract_version": MCP_OUTCOME_CONTRACT_VERSION,
            },
        }
        if tool_name:
            result["meta"]["tool"] = tool_name
        if self.details:
            result["details"] = self.details
        return result

    def text_content(self) -> str:
        """Compact JSON mirror for clients that do not read structuredContent."""

        return json.dumps(self.structured_content(), default=_json_default, separators=(",", ":"))

    def legacy_content(self) -> str:
        if self.legacy_text is not None:
            return self.legacy_text
        if isinstance(self.payload, str):
            return self.payload
        return json.dumps(self.payload, default=_json_default)


def coerce_mcp_tool_outcome(raw: Any, *, tool_name: str | None = None) -> McpToolOutcome:
    """Classify a native V2 result or one of the legacy handler return shapes."""

    if isinstance(raw, McpToolOutcome):
        return raw

    payload, legacy_text = _native_payload(raw)
    if not isinstance(payload, Mapping):
        return McpToolOutcome.success(payload, legacy_text=legacy_text)

    body = dict(payload)
    explicit_kind = body.get("outcome")
    if explicit_kind in {kind.value for kind in McpOutcomeKind}:
        kind = McpOutcomeKind(str(explicit_kind))
        if kind is McpOutcomeKind.SUCCESS:
            return McpToolOutcome.success(body.get("data", body), legacy_text=legacy_text)
        code = _first_text(body, "error_code", "code") or "domain_error"
        message = _first_text(body, "message", "error", "detail", "reason")
        if kind is McpOutcomeKind.ACTION_REQUIRED:
            return McpToolOutcome.action_required(
                body.get("data", body),
                code=code,
                message=message,
                next_action=_next_action(body, tool_name=tool_name, code=code),
                legacy_text=legacy_text,
            )
        return McpToolOutcome.error(
            code=code,
            message=message or code,
            payload=body.get("data", body),
            retryable=bool(body.get("retryable", code in _RETRYABLE_CODES)),
            next_action=_next_action(body, tool_name=tool_name, code=code),
            details=body.get("details") if isinstance(body.get("details"), Mapping) else None,
            legacy_text=legacy_text,
        )

    message = _first_text(body, "message", "detail", "reason")
    raw_error = body.get("error")
    if isinstance(raw_error, str) and raw_error.strip():
        message = message or raw_error.strip()
    elif isinstance(raw_error, Mapping):
        message = message or _first_text(
            raw_error,
            "message",
            "detail",
            "reason",
        )
    elif raw_error not in (None, False, ""):
        message = message or str(raw_error)
    code = _error_code(body, message)

    if code in ACTION_REQUIRED_CODES:
        return McpToolOutcome.action_required(
            body,
            code=code,
            message=message,
            next_action=_next_action(body, tool_name=tool_name, code=code),
            legacy_text=legacy_text,
        )

    failure_signal = (
        raw_error not in (None, False, "")
        or body.get("success") is False
        or body.get("blocked") is True
        or body.get("error_code") not in (None, "")
    )
    if not failure_signal:
        return McpToolOutcome.success(body, legacy_text=legacy_text)

    code_parts = frozenset(code.split("_"))
    retryable = bool(body.get("retryable")) or code in _RETRYABLE_CODES or bool(
        code_parts & {"conflict", "locked", "timeout", "unavailable", "busy"}
    )
    return McpToolOutcome.error(
        code=code,
        message=message or code,
        payload=body,
        retryable=retryable,
        next_action=_next_action(body, tool_name=tool_name, code=code),
        details=body,
        legacy_text=legacy_text,
    )


__all__ = [
    "ACTION_REQUIRED_CODES",
    "MCP_OUTCOME_CONTRACT_VERSION",
    "McpOutcomeKind",
    "McpToolOutcome",
    "coerce_mcp_tool_outcome",
]
