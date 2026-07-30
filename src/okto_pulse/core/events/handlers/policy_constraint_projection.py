"""Stable domain-event handler for policy-constraint projection."""

from __future__ import annotations

import re

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import (
    PolicyAdoptionChanged,
    PolicyBindingMaterialized,
    PolicyConstraintChanged,
    PolicyRetirementChanged,
)
from okto_pulse.core.ports.policy_constraint_projection import (
    PolicyConstraintProjectionResult,
    get_policy_constraint_projection_port,
)


_SAFE_POLICY_CONSTRAINT_CODE = re.compile(
    r"^policy_constraint_[a-z0-9_]{1,80}$"
)
_SAFE_EXCEPTION_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,79}$")


def _safe_projection_failure(error: Exception) -> RuntimeError:
    code = getattr(error, "code", None)
    if isinstance(code, str) and _SAFE_POLICY_CONSTRAINT_CODE.fullmatch(code):
        return RuntimeError(code)
    exception_type = type(error).__name__
    if _SAFE_EXCEPTION_TYPE.fullmatch(exception_type) is None:
        exception_type = "Exception"
    return RuntimeError(
        f"policy_constraint_projection_failed:{exception_type}"
    )


@register_handler(
    PolicyAdoptionChanged.event_type,
    PolicyBindingMaterialized.event_type,
    PolicyRetirementChanged.event_type,
)
class PolicyConstraintProjectionHandler:
    """Apply one immutable event through the edition-owned projection port."""

    async def handle(
        self,
        event: PolicyConstraintChanged,
        session: object,
    ) -> PolicyConstraintProjectionResult:
        if not isinstance(
            event,
            PolicyAdoptionChanged
            | PolicyBindingMaterialized
            | PolicyRetirementChanged,
        ):
            raise TypeError("policy_constraint_projection_event_invalid")
        port = get_policy_constraint_projection_port()
        try:
            result = await port.apply(session, event=event)
        except Exception as exc:
            raise _safe_projection_failure(exc) from None
        if not isinstance(result, PolicyConstraintProjectionResult):
            raise RuntimeError("policy_constraint_projection_result_invalid")
        if (
            result.board_id != event.board_id
            or result.operation != event.operation
            or result.event_id != event.event_id
        ):
            raise RuntimeError("policy_constraint_projection_result_mismatch")
        return result


__all__ = ["PolicyConstraintProjectionHandler"]
