"""Canonical enum-error envelope for the inbound transport layer (card S-LANE-01).

A bounded normalizer that turns a Pydantic enum-validation failure for a mapped
request field into a single canonical envelope shared by REST and MCP, so an
invalid ``lane_type`` never leaks the raw Pydantic surface (no
``errors.pydantic.dev`` URL, no internal class name, no traceback) and both
transports answer with identical semantics.

This is the transport/adapter layer (it may name request-shape concerns); the
service layer stays transport-neutral and never imports it. The mapping is
intentionally bounded — only ``lane_type`` (→ ``SprintLaneType``) is normalized
today; everything else returns ``None`` so the caller keeps the framework's
default validation behavior unchanged.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.enums import SprintLaneType

# Bounded registry of request fields whose enum-validation failure is rendered as
# a canonical envelope. Add a row to extend; an unlisted field returns ``None`` so
# the caller keeps the default behavior. Guarantee is restricted to
# ``SprintCreate``/``SprintUpdate.lane_type`` today.
_MAPPED_ENUM_FIELDS: dict[str, dict[str, Any]] = {
    "lane_type": {
        "code": "invalid_lane_type",
        # Derived from the enum so the accepted set can never drift from the model.
        "accepted_values": [member.value for member in SprintLaneType],
    },
}


def _envelope(field: str, received_value: Any) -> dict[str, Any]:
    spec = _MAPPED_ENUM_FIELDS[field]
    return {
        "code": spec["code"],
        "field": field,
        "received_value": received_value,
        "accepted_values": list(spec["accepted_values"]),
        # The boundary rejects before any service call, so nothing is persisted.
        "mutation_applied": False,
    }


def canonical_enum_error(errors: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the canonical envelope for a mapped enum field, else ``None``.

    ``errors`` is a Pydantic ``ValidationError.errors()`` list (equivalently a
    FastAPI ``RequestValidationError.errors()`` list). Only an ``enum``-typed
    failure on a mapped field (currently ``lane_type``) is normalized; the value
    echoed in ``received_value`` is the offending input. Any other error shape
    returns ``None`` so the caller falls back to default handling — this keeps the
    guarantee bounded and never reshapes unrelated validations.
    """
    for err in errors:
        loc = err.get("loc") or ()
        if not loc:
            continue
        # The field is the last loc segment so this matches both the MCP shape
        # ``("lane_type",)`` and the REST body shape ``("body", "lane_type")``.
        field = str(loc[-1])
        if field not in _MAPPED_ENUM_FIELDS:
            continue
        if err.get("type") != "enum":
            continue
        return _envelope(field, err.get("input"))
    return None
