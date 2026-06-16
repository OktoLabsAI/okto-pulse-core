"""Telemetry EventType contract (spec R5A, card R5A-A).

A verifiable registry that prevents a declared telemetry ``EventType`` from
becoming a PHANTOM schema — declared in :data:`schema.TELEMETRY_EVENT_TYPES` but
with no real producer/emitter and no aggregate that ever reaches a delta batch.

Every declared type MUST be classified here:

- ``maintained`` — names its ``producer`` (where it is emitted) and its
  ``aggregate`` (the delta-batch map it feeds), and tracks ``coverage``: ``wired``
  (emitter + aggregate live and tested today) or ``pending:<card>`` (declared, the
  real emitter/aggregate is implemented by a later card — the gap is EXPLICIT, not
  silent).
- ``deprecated`` — still declared during a sunset window, with a justification.
- A REMOVED type is OUT of ``TELEMETRY_EVENT_TYPES`` and listed in
  :data:`REMOVED_EVENT_TYPES` with a justification; a ghost-schema test asserts it
  never silently reappears.

tr_8c6167d8 (contract test fails when a declared type has no emitter/test or
documented removal) / br_62b0f7cc (no orphan declared type) / dec_b74b4d29
(declared events are emitted or removed) / ac_7fb151c8 / ac_66e29ff9.
"""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.telemetry.schema import TELEMETRY_EVENT_TYPES

STATUS_MAINTAINED = "maintained"
STATUS_DEPRECATED = "deprecated"
_VALID_STATUSES = frozenset({STATUS_MAINTAINED, STATUS_DEPRECATED})

COVERAGE_WIRED = "wired"  # emitter + aggregate live AND tested today
_PENDING_PREFIX = "pending:"  # declared; real emitter/aggregate lands in <card>

# The delta-batch aggregate maps the sender (sender.py `_build_delta_batch`)
# materialises TODAY. A ``wired`` contract aggregate MUST be one of these — so a
# "wired" claim can never point at a non-existent (ghost) aggregate. lifecycle /
# pipeline_transition are deliberately ABSENT here: they have no dedicated map yet
# (they fall into the generic bucket and are dropped), which is exactly the gap
# R5A-B closes — the contract marks them pending so the gap is not silent.
LIVE_AGGREGATE_MAPS = frozenset(
    {
        "cli_counts",
        "http_route_template_counts",
        "mcp_tool_counts",
        "kg_operation_counts",
        "guided_help_counts",
    }
)


@dataclass(frozen=True)
class EventTypeContract:
    event_type: str
    status: str
    producer: str  # where it is (or, for pending, will be) emitted
    aggregate: str  # the delta-batch aggregate map it feeds
    coverage: str  # COVERAGE_WIRED | "pending:<card>"
    note: str = ""


# Classification of every declared EventType. Snapshot of reality (R5A-A): http and
# guided_help are wired+tested today; cli/mcp/kg have a live aggregate map but no
# in-core emitter yet; lifecycle/pipeline_transition have neither emitter nor a
# dedicated aggregate. All are MAINTAINED (intended) — the pending wiring is R5A-B
# (emitters/aggregators) and R5A-C (HTTP /mcp /health /docs policy). None is removed
# now (guided_help especially has existing tests — do not remove).
TELEMETRY_EVENT_CONTRACT: dict[str, EventTypeContract] = {
    "http": EventTypeContract(
        event_type="http",
        status=STATUS_MAINTAINED,
        producer="app.py HTTP telemetry middleware (record_event('http'))",
        aggregate="http_route_template_counts",
        coverage=COVERAGE_WIRED,
        note="Emitted for /api/ paths today; /mcp /health /docs policy is R5A-C.",
    ),
    "guided_help": EventTypeContract(
        event_type="guided_help",
        status=STATUS_MAINTAINED,
        producer="api/metrics.py event endpoint (SPA guided-help)",
        aggregate="guided_help_counts",
        coverage=COVERAGE_WIRED,
        note="Closed categorical schema; covered by existing guided_help tests.",
    ),
    "cli": EventTypeContract(
        event_type="cli",
        status=STATUS_MAINTAINED,
        producer="okto-pulse CLI command wrapper (record_event('cli'))",
        aggregate="cli_counts",
        coverage="pending:R5A-B",
        note="Aggregate map exists; the real CLI emitter + emit->aggregate test is R5A-B.",
    ),
    "mcp": EventTypeContract(
        event_type="mcp",
        status=STATUS_MAINTAINED,
        producer="MCP server tool dispatch (record_event('mcp'))",
        aggregate="mcp_tool_counts",
        coverage="pending:R5A-B",
        note="Aggregate map exists; the real MCP emitter + test is R5A-B.",
    ),
    "kg": EventTypeContract(
        event_type="kg",
        status=STATUS_MAINTAINED,
        producer="KG operation hook (record_event('kg'))",
        aggregate="kg_operation_counts",
        coverage="pending:R5A-B",
        note="Aggregate map exists; the real KG emitter + test is R5A-B.",
    ),
    "lifecycle": EventTypeContract(
        event_type="lifecycle",
        status=STATUS_MAINTAINED,
        producer="board/spec/card lifecycle hook (record_event('lifecycle'))",
        aggregate="lifecycle_counts",
        coverage="pending:R5A-B",
        note="NO dedicated aggregate map yet: lifecycle events fall into the generic "
        "bucket and are DROPPED from the delta batch today. R5A-B adds lifecycle_counts "
        "(and the emitter) so this stops being a phantom schema.",
    ),
    "pipeline_transition": EventTypeContract(
        event_type="pipeline_transition",
        status=STATUS_MAINTAINED,
        producer="pipeline phase-transition hook (record_event('pipeline_transition'))",
        aggregate="pipeline_transition_counts",
        coverage="pending:R5A-B",
        note="NO dedicated aggregate map yet: dropped from the delta batch today. "
        "R5A-B adds pipeline_transition_counts (and the emitter).",
    ),
}

# Types intentionally removed (and thus absent from TELEMETRY_EVENT_TYPES). Each
# entry is a justification; the ghost-schema guard asserts none silently reappears
# as a declared type. Empty today — nothing is removed in R5A-A.
REMOVED_EVENT_TYPES: dict[str, str] = {}


def is_pending(coverage: str) -> bool:
    return coverage.startswith(_PENDING_PREFIX)


def _contract_violations(
    event_types: tuple[str, ...],
    contract: dict[str, EventTypeContract],
    removed: dict[str, str],
) -> list[str]:
    """Pure rule engine (testable with synthetic inputs). Returns violations."""
    problems: list[str] = []
    declared = set(event_types)
    entries = set(contract)

    for missing in sorted(declared - entries):
        problems.append(f"declared EventType {missing!r} has no contract entry (phantom schema)")
    for extra in sorted(entries - declared):
        problems.append(f"contract entry {extra!r} is not a declared EventType")

    for event_type, entry in contract.items():
        if entry.status not in _VALID_STATUSES:
            problems.append(f"{event_type!r} has invalid status {entry.status!r}")
        if entry.status == STATUS_MAINTAINED:
            if not entry.producer:
                problems.append(f"maintained {event_type!r} declares no producer/emitter")
            if not entry.aggregate:
                problems.append(f"maintained {event_type!r} declares no aggregate")
            if not entry.coverage:
                problems.append(f"maintained {event_type!r} declares no coverage status")
            elif entry.coverage == COVERAGE_WIRED and entry.aggregate not in LIVE_AGGREGATE_MAPS:
                problems.append(
                    f"maintained {event_type!r} claims '{COVERAGE_WIRED}' but aggregate "
                    f"{entry.aggregate!r} is not a live aggregate map (ghost aggregate)"
                )
            elif entry.coverage != COVERAGE_WIRED and not is_pending(entry.coverage):
                problems.append(
                    f"maintained {event_type!r} coverage {entry.coverage!r} must be "
                    f"'{COVERAGE_WIRED}' or 'pending:<card>'"
                )
        elif entry.status == STATUS_DEPRECATED and not entry.note:
            problems.append(f"deprecated {event_type!r} has no deprecation justification")

    for removed_type, justification in removed.items():
        if removed_type in declared:
            problems.append(
                f"removed EventType {removed_type!r} is still declared in TELEMETRY_EVENT_TYPES "
                "(ghost schema reappeared)"
            )
        if not justification:
            problems.append(f"removed EventType {removed_type!r} has no removal justification")
    return problems


def contract_violations() -> list[str]:
    """Violations of the live telemetry EventType contract. Empty == holds."""
    return _contract_violations(TELEMETRY_EVENT_TYPES, TELEMETRY_EVENT_CONTRACT, REMOVED_EVENT_TYPES)


def assert_contract_complete() -> None:
    problems = contract_violations()
    if problems:
        raise AssertionError("telemetry EventType contract violations:\n  " + "\n  ".join(problems))
