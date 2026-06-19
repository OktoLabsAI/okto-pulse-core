"""Single source of truth for the test_scenario evidence rule and lifecycle
guards (spec 6f1e75bf, item #1).

Leaf module — stdlib + domain types only. It does NOT import ``SpecService``
nor ``okto_pulse.core.mcp.server`` (mirrors ``spec_entity_canonicalization``
from item #4). Dependency direction is one-way: ``mcp/server.py``,
``api/specs.py`` and ``services/main.py`` import from here; never the reverse.

Before this module the NC-9 (test-theater prevention) rule lived duplicated in
two places — ``mcp/server.py`` (``_EVIDENCE_REQUIRED_KEYS`` / ``_validate_evidence``)
and ``services/main.py`` (``_test_scenario_has_required_evidence``). Both are now
re-expressed here so the rule has exactly one definition.
"""

from __future__ import annotations

from typing import Any

# --------------------------------------------------------------------------
# Status vocabulary
# --------------------------------------------------------------------------

#: Statuses that require structured evidence (the NC-9 gated statuses).
GATED_STATUSES: frozenset[str] = frozenset({"automated", "passed", "failed"})

#: Every valid operational status of a test scenario.
VALID_SCENARIO_STATUSES: tuple[str, ...] = (
    "draft",
    "ready",
    "automated",
    "passed",
    "failed",
)

#: Spec statuses in which a scenario's operational status may NOT change by
#: default. Status mutation stays allowed in draft/review/approved/in_progress
#: — notably in_progress, the execution phase where scenarios are legitimately
#: marked passed/automated/failed. ``SpecService`` applies a narrow service-level
#: exception for post-lock regression evidence when the scenario is already
#: linked to an executable test card.
_STATUS_IMMUTABLE_SPEC_STATUSES: frozenset[str] = frozenset({"validated", "done"})

#: Editing any of these fields invalidates a scenario's existing evidence.
SEMANTIC_FIELDS: tuple[str, ...] = (
    "given",
    "when",
    "then",
    "scenario_type",
    "linked_criteria",
)

#: Editing only these fields preserves status and evidence.
COSMETIC_FIELDS: tuple[str, ...] = ("title", "notes")

# --------------------------------------------------------------------------
# Scenario-type vocabulary (spec ac16b3c9 — fail-closed scenario_type)
# --------------------------------------------------------------------------

#: The one authoritative scenario_type taxonomy for this initiative. Every write
#: surface (MCP add/update tools, the REST/spec update path and the service
#: create/update paths) validates against THIS tuple — there is no second
#: allowlist. Adding a new type (e.g. ``negative``) is a deliberate taxonomy
#: change, never a silent fallback.
VALID_SCENARIO_TYPES: tuple[str, ...] = ("unit", "integration", "e2e", "manual")

#: The default applied when a caller OMITS scenario_type entirely. This is a
#: true default, NOT a normalization of an invalid value — an invalid value
#: fails closed (see :func:`validate_scenario_type`).
DEFAULT_SCENARIO_TYPE: str = "integration"


class InvalidScenarioTypeError(ValueError):
    """Raised when a write supplies a scenario_type outside
    :data:`VALID_SCENARIO_TYPES`.

    Fail-closed: the value is NEVER normalized to another valid type, because
    silent normalization hides caller intent — an agent that asked for
    ``negative`` and got ``integration`` believes a negative scenario exists
    (spec ac16b3c9). The message names the allowed values so agents and UI
    clients can correct the request.
    """

    def __init__(self, value: object) -> None:
        self.value = value
        self.allowed = VALID_SCENARIO_TYPES
        super().__init__(
            f"Invalid scenario_type {value!r}. "
            f"Allowed values: {', '.join(VALID_SCENARIO_TYPES)}."
        )


def is_valid_scenario_type(value: object) -> bool:
    """Whether ``value`` is one of the supported scenario types. Read-tolerant —
    used by list/report paths that must keep rendering historical data."""
    return isinstance(value, str) and value in VALID_SCENARIO_TYPES


def validate_scenario_type(value: object) -> str:
    """Fail-closed validator: return ``value`` unchanged when it is a supported
    scenario_type, else raise :class:`InvalidScenarioTypeError`. Never normalizes.
    """
    if not is_valid_scenario_type(value):
        raise InvalidScenarioTypeError(value)
    return value  # type: ignore[return-value]


def validate_scenario_types_for_write(
    new_scenarios: object, old_scenarios: object
) -> None:
    """Whole-list write guard for the spec create/update persistence paths.

    Validates the scenario_type of every scenario that is NEW or whose
    scenario_type CHANGED relative to ``old_scenarios`` (matched by ``id``).
    Scenarios whose scenario_type is unchanged are GRANDFATHERED so historical
    or legacy values keep reading, listing and re-serializing without breaking
    (spec ac16b3c9 FR5/AC5; the rule only forbids accepting an invalid value on
    a *new* write). Raises :class:`InvalidScenarioTypeError` on the first
    new/changed invalid value BEFORE any mutation; never normalizes.
    """
    old_by_id: dict[Any, Any] = {
        s.get("id"): s for s in (old_scenarios or []) if isinstance(s, dict)
    }
    for s in new_scenarios or []:
        if not isinstance(s, dict) or "scenario_type" not in s:
            continue
        new_type = s.get("scenario_type")
        prev = old_by_id.get(s.get("id"))
        if prev is None or prev.get("scenario_type") != new_type:
            validate_scenario_type(new_type)


#: Required evidence keys per gated status. Each rule group is AND; a group with
#: more than one key is one-of (OR). Moved verbatim from mcp/server.py.
EVIDENCE_REQUIRED_KEYS: dict[str, tuple[tuple[str, ...], ...]] = {
    # automated: both keys required (two AND rules)
    "automated": (
        ("test_file_path",),
        ("test_function",),
    ),
    # passed/failed: last_run_at required + (output_snippet OR test_run_id)
    "passed": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of
    ),
    "failed": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of
    ),
}


class StatusNotMutableError(ValueError):
    """Raised when a scenario status mutation is attempted while the spec is in a
    status that forbids it by default (``validated`` or ``done``).

    Intentionally distinct from ``SpecService._require_spec_unlocked`` (the
    content-lock), which triggers on an active *passed validation* rather than on
    status and would therefore wrongly block ``in_progress``. The service layer
    may allow post-lock regression evidence after a linked test card is already
    executable; arbitrary scenario status edits still fail with this error.
    """

    def __init__(self, spec_status: str) -> None:
        self.spec_status = spec_status
        super().__init__(
            f"Cannot change test scenario status while spec is '{spec_status}'. "
            "Scenario status is mutable only in draft/review/approved/in_progress, "
            "unless the target scenario is already linked to an executable test card "
            "for post-lock regression evidence."
        )


def validate_test_scenario_evidence(
    status: str, evidence: dict | None
) -> tuple[bool, list[str]]:
    """Return ``(ok, missing_keys)``. An empty ``missing_keys`` means valid.

    For each rule group, ALL keys in the group must be present (AND logic). When
    a group has multiple keys (one-of), at least one must be present.
    """
    rules = EVIDENCE_REQUIRED_KEYS.get(status)
    if not rules:
        return True, []
    if not evidence:
        # Flatten all required keys for the error message.
        flat: list[str] = []
        for group in rules:
            if len(group) == 1:
                flat.extend(group)
            else:
                flat.append(" or ".join(group))
        return False, flat
    missing: list[str] = []
    for group in rules:
        if len(group) == 1:
            key = group[0]
            if not evidence.get(key):
                missing.append(key)
        else:
            # one-of group — at least one key must be present
            if not any(evidence.get(k) for k in group):
                missing.append(" or ".join(group))
    return (len(missing) == 0, missing)


def scenario_has_required_evidence(scenario: dict[str, Any]) -> bool:
    """Whether a scenario carries the evidence its status requires.

    Reads ``evidence`` (or the legacy ``latest_evidence`` key). Re-expressed on
    top of :func:`validate_test_scenario_evidence` so the rule has one source.
    A non-gated status always passes.
    """
    status = scenario.get("status")
    if status not in GATED_STATUSES:
        return True
    evidence = scenario.get("evidence") or scenario.get("latest_evidence")
    if not isinstance(evidence, dict):
        return False
    ok, _missing = validate_test_scenario_evidence(str(status), evidence)
    return ok


def require_test_scenario_status_mutable(spec_status: str | None) -> None:
    """Raise :class:`StatusNotMutableError` when ``spec_status`` forbids scenario
    status mutation (``validated`` or ``done``); otherwise return ``None``.

    This is a STATUS-based guard for the operational status path. It is NOT the
    content-lock (``_require_spec_unlocked``) — using that here would block
    ``in_progress`` and break marking scenarios passed during execution.

    ``spec_status`` may be a plain string or a ``SpecStatus`` enum; the enum's
    ``.value`` is used for the comparison.
    """
    value = getattr(spec_status, "value", spec_status)
    if value in _STATUS_IMMUTABLE_SPEC_STATUSES:
        raise StatusNotMutableError(str(value))


def evidence_invalidated_by_semantic_edit(changed_fields: object) -> bool:
    """Whether any of ``changed_fields`` is a SEMANTIC field (so existing
    evidence must be reset). Cosmetic-only edits return ``False``.
    """
    if not changed_fields:
        return False
    semantic = set(SEMANTIC_FIELDS)
    return any(field in semantic for field in changed_fields)
