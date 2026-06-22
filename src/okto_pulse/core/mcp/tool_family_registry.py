"""ToolFamilyRegistry (spec R4, card R4.1, fr_1f3ab59f / ir_6ca17175 / api_d7542e16).

Executable, testable metadata describing which Okto Pulse MCP tool families are
CONSOLIDATED (eligible) and which are deliberately KEPT SEPARATE (excluded), each
with a concrete ``rejected_reason``.

Owner assertiveness gate (the governing rule): *agent assertiveness > tool count*.
A family is eligible ONLY when consolidation preserves the per-type typed
guidance. FastMCP derives the agent-facing JSON schema from the function
SIGNATURE, not the docstring — so an opaque ``payload: dict`` consolidation hides
the per-type required fields and induces agent errors. Therefore the ONLY allowed
consolidation mode here is ``consolidate_with_dedicated_routing``: a single tool
NAME + a closed ``target_type`` enum + typed sub-params + per-type internal
routing. A flat ``payload: dict`` mode is intentionally NOT representable.

Scope (owner decision after an assertiveness audit): only the two
homogeneous-signature families are consolidated — ``spec_entity_remove`` and
``qa_ask``. The heterogeneous add / update / test_scenario / answer / move
families are kept separate (documented below with reasons) to avoid inducing
agent errors for a marginal tool-count saving. ``kg_query`` and ``move_card`` were
already spec non-goals (fr_d16572df) and are confirmed here.

Established codebase precedent for the dedicated-routing pattern:
``okto_pulse_link_task`` (server.py) — a closed target_type tuple + dispatch to
un-decorated internal helpers. R4 mirrors it but in ADDITIVE-ALIAS mode: the
legacy per-type tools are preserved as aliases (fr_af4b5c6e), not removed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

_LOG = logging.getLogger("okto_pulse.mcp.tool_family")

# --- Telemetry metric names (R4.2: or_4e57890f / or_b0cdae15 / or_984aeeec) -----
# Safe labels only — counts + identifiers, never a payload body / question text.
METRIC_ALIAS_USAGE = "mcp_tool_alias_usage_total"
METRIC_REGISTRY_VIOLATION = "mcp_tool_family_registry_violation_total"
METRIC_SCHEMA_BUDGET_BYTES = "mcp_tool_family_schema_budget_bytes"

# Allowlisted safe label keys for alias-usage telemetry (tr_34b36a9e).
SAFE_ALIAS_LABEL_KEYS = frozenset(
    {"family_id", "alias_kind", "tool_name", "operation", "target_type", "outcome"}
)
# Allowlisted safe label keys for registry-violation telemetry.
SAFE_VIOLATION_LABEL_KEYS = frozenset({"family_id", "reason", "tool_name", "target_type"})


class AliasKind(str, Enum):
    """How a dispatched call entered the consolidated implementation."""

    LEGACY = "legacy"            # a preserved per-type legacy tool name
    CONSOLIDATED = "consolidated"  # the new polymorphic target_type entrypoint
    SHORT = "short"             # an additive short-name alias (no okto_pulse_ prefix)


class ConsolidationMode(str, Enum):
    """The ONLY allowed consolidation mode (owner gate). There is deliberately no
    opaque ``payload_dict`` mode — typed per-type routing must be preserved."""

    DEDICATED_ROUTING = "consolidate_with_dedicated_routing"


# Reasons recorded on the registry-violation metric (or_b0cdae15).
VIOLATION_UNKNOWN_TARGET_TYPE = "unknown_target_type"
VIOLATION_UNKNOWN_ALIAS = "unknown_alias"
VIOLATION_EXCLUDED_FAMILY_ATTEMPT = "excluded_family_attempt"
VIOLATION_ALIAS_COLLISION = "alias_collision"


@dataclass(frozen=True)
class ToolFamily:
    """One MCP tool family — either consolidated (eligible) or kept separate."""

    family_id: str
    eligible: bool
    target_types: tuple[str, ...]
    operations: tuple[str, ...]
    legacy_aliases: tuple[str, ...]
    # Eligible-only fields:
    consolidated_tool: str | None = None
    short_aliases: tuple[str, ...] = ()
    mode: ConsolidationMode | None = None
    routing_notes: str = ""
    # Excluded-only field:
    rejected_reason: str | None = None

    def supports_target_type(self, target_type: str) -> bool:
        return target_type in self.target_types


# ---------------------------------------------------------------------------
# ELIGIBLE families — consolidate WITH dedicated routing (typed params kept)
# ---------------------------------------------------------------------------

ELIGIBLE_FAMILIES: tuple[ToolFamily, ...] = (
    ToolFamily(
        family_id="spec_entity_remove",
        eligible=True,
        consolidated_tool="okto_pulse_remove_spec_entity",
        target_types=("business_rule", "api_contract", "decision"),
        operations=("remove",),
        legacy_aliases=(
            "okto_pulse_remove_business_rule",
            "okto_pulse_remove_api_contract",
            "okto_pulse_remove_decision",
        ),
        short_aliases=("remove_spec_entity",),
        mode=ConsolidationMode.DEDICATED_ROUTING,
        routing_notes=(
            "Homogeneous (board_id, spec_id, <id>) signatures — zero per-type field "
            "schema to lose. DEDICATED ROUTING preserves the behavioral asymmetry: "
            "decision is a SOFT-delete (status=revoked, restorable via "
            "okto_pulse_update_decision), while business_rule/api_contract are hard "
            "removals. The closed enum is SAFER than separate tools (fail-fast on a "
            "wrong target_type vs a silent 'not found' from the wrong legacy tool)."
        ),
    ),
    ToolFamily(
        family_id="qa_ask",
        eligible=True,
        consolidated_tool="okto_pulse_ask",
        target_types=("card", "ideation", "refinement", "spec", "sprint"),
        operations=("ask",),
        legacy_aliases=(
            "okto_pulse_ask_question",
            "okto_pulse_ask_ideation_question",
            "okto_pulse_ask_refinement_question",
            "okto_pulse_ask_spec_question",
            "okto_pulse_ask_sprint_question",
        ),
        short_aliases=("ask",),
        mode=ConsolidationMode.DEDICATED_ROUTING,
        routing_notes=(
            "Identical (board_id, parent_id, question) signatures — only the parent-id "
            "param NAME differed; the closed target_type enum restores that steering. "
            "DEDICATED ROUTING preserves the SPRINT asymmetry: SprintQAService takes a "
            "raw string (no SprintQACreate model), has NO QA_CREATE permission gate and "
            "NO activity-log write, unlike the card/ideation/refinement/spec paths. The "
            "sibling *_choice_question tools are NOT in this family and stay separate."
        ),
    ),
)


# ---------------------------------------------------------------------------
# EXCLUDED families — keep separate (gate-protected non-goals, with reasons)
# ---------------------------------------------------------------------------

EXCLUDED_FAMILIES: tuple[ToolFamily, ...] = (
    ToolFamily(
        family_id="spec_entity_add",
        eligible=False,
        target_types=(
            "business_rule",
            "api_contract",
            "integration_requirement",
            "observability_requirement",
            "decision",
        ),
        operations=("add",),
        legacy_aliases=(
            "okto_pulse_add_business_rule",
            "okto_pulse_add_api_contract",
            "okto_pulse_add_integration_requirement",
            "okto_pulse_add_observability_requirement",
            "okto_pulse_add_decision",
        ),
        rejected_reason=(
            "Heterogeneous REQUIRED fields per type (business_rule: when/then/rule; "
            "api_contract: method/path + 3 nested-JSON blobs; decision: rationale; "
            "integration_requirement/observability_requirement: enum-gated "
            "integration_type/signal_type discriminators). A target_type+payload "
            "signature exposes ZERO per-type schema via FastMCP, inducing "
            "missing-required-field errors and the documented business_rule<->decision "
            "confusion. No homogeneous sub-cluster exists. Decisive codebase precedent: "
            "okto_pulse_update_spec_entity already carves api_contract OUT to a dedicated "
            "typed wrapper 'so the richer payload shape remains explicit'."
        ),
    ),
    ToolFamily(
        family_id="test_scenario",
        eligible=False,
        target_types=("test_scenario",),
        operations=("add", "update_status", "list"),
        legacy_aliases=(
            "okto_pulse_add_test_scenario",
            "okto_pulse_update_test_scenario_status",
            "okto_pulse_list_test_scenarios",
        ),
        rejected_reason=(
            "Heterogeneous OPERATION family (create / update-status / list) with "
            "mutually-exclusive params. The NC-9 test-theater-prevention evidence gate "
            "is STATUS-CONDITIONAL (automated -> test_file_path+test_function; "
            "passed/failed -> explicit evidence_class with its required replayable "
            "fields, or unclassed run-log evidence with last_run_at+"
            "(output_snippet|test_run_id)+expected_output_snapshot+"
            "non_replayable_justification); a generic dict "
            "cannot express status-conditional requiredness in a FastMCP schema, blinding "
            "the agent exactly where test-theater prevention depends on it. Consolidating "
            "would re-merge status mutation into the body-edit path that was deliberately "
            "split to avoid a second NC-9 bypass. TC-R4.4 already mandates dedicated routing."
        ),
    ),
    ToolFamily(
        family_id="qa_answer",
        eligible=False,
        target_types=("card", "ideation", "refinement", "spec", "sprint"),
        operations=("answer",),
        legacy_aliases=(
            "okto_pulse_answer_question",
            "okto_pulse_answer_ideation_question",
            "okto_pulse_answer_refinement_question",
            "okto_pulse_answer_spec_question",
            "okto_pulse_answer_sprint_question",
        ),
        rejected_reason=(
            "Conditional requiredness that one signature cannot express: 'answer' is "
            "REQUIRED for card+sprint but OPTIONAL for the choice-capable "
            "ideation/refinement/spec (where 'selected' substitutes), and 'selected' "
            "exists on only 3 of 5. A flat payload would induce sending 'selected' to a "
            "card/sprint (silently dropped), omitting 'answer' on card/sprint, or sending "
            "free-text to a choice question. (Unlike qa_ask, which IS homogeneous.)"
        ),
    ),
    ToolFamily(
        family_id="move_workitem",
        eligible=False,
        target_types=("ideation", "refinement", "spec", "sprint", "story"),
        operations=("move",),
        legacy_aliases=(
            "okto_pulse_move_ideation",
            "okto_pulse_move_refinement",
            "okto_pulse_move_spec",
            "okto_pulse_move_sprint",
            "okto_pulse_move_story",
        ),
        rejected_reason=(
            "Shape-homogeneous but vocabulary/gate-heterogeneous: five DISTINCT status "
            "enums (ideation 6 values, story 4, refinement 5, sprint 5 with 3 explicit "
            "gates, spec 7) plus distinct server-side transition gates. A generic "
            "move(target_type, status) makes the legal {target_type x status} matrix "
            "unenforceable by any schema and cannot legibly carry five state machines in "
            "one docstring — inducing cross-type status leaks (status='active' on a spec), "
            "story status='converted' misuse, and gate-blind sprint transitions."
        ),
    ),
    ToolFamily(
        family_id="kg_query",
        eligible=False,
        target_types=("natural", "cypher", "global", "reflective"),
        operations=("query",),
        legacy_aliases=(
            "okto_pulse_kg_query_natural",
            "okto_pulse_kg_query_cypher",
            "okto_pulse_kg_query_global",
            "okto_pulse_kg_query_reflective",
        ),
        rejected_reason=(
            "Spec non-goal (fr_d16572df), confirmed in assertiveness terms: the "
            "discriminating payload is a different query LANGUAGE per type — free-text "
            "nl_query (natural/global/reflective) vs a parseable read-only Cypher DSL "
            "(cypher) — with non-overlapping typed knobs (cypher: params/max_rows hard-cap "
            "1000/timeout_ms; natural: min_confidence/since/until; global: cross-board ACL "
            "fan-out with board_id OPTIONAL). A target_type+payload surface strips the cue "
            "telling the agent which query language to emit. Escape-hatch power tier, not a "
            "CRUD family."
        ),
    ),
    ToolFamily(
        family_id="move_card",
        eligible=False,
        target_types=("card",),
        operations=("move",),
        legacy_aliases=("okto_pulse_move_card",),
        rejected_reason=(
            "Spec non-goal, confirmed: move_card carries a STATUS-CONDITIONAL 6-field "
            "execution-report payload (conclusion, completeness 0-100, "
            "completeness_justification, drift 0-100, drift_justification, position) that "
            "is required when moving to validation/done so a reviewer can validate the "
            "claim. Folding into a polymorphic move(target_type, status, payload:dict) "
            "degrades to the 3-arg lowest common denominator + opaque dict, stripping the "
            "typed completeness/drift scoring and eroding the no-test-theater / "
            "reviewer-validation invariant. No homogeneous peer to merge with."
        ),
    ),
)

ALL_FAMILIES: tuple[ToolFamily, ...] = ELIGIBLE_FAMILIES + EXCLUDED_FAMILIES


class ToolFamilyRegistry:
    """Lookup + validation over the family metadata (ir_6ca17175). Pure data — no
    persistence, no MCP registration side-effects."""

    def __init__(self, families: tuple[ToolFamily, ...] = ALL_FAMILIES) -> None:
        self._families = families
        self._by_id = {f.family_id: f for f in families}
        # Map every legacy/short/consolidated name -> (family, alias_kind).
        self._by_name: dict[str, tuple[ToolFamily, AliasKind]] = {}
        for f in families:
            for name in f.legacy_aliases:
                self._by_name[name] = (f, AliasKind.LEGACY)
            if f.eligible:
                if f.consolidated_tool:
                    self._by_name[f.consolidated_tool] = (f, AliasKind.CONSOLIDATED)
                for name in f.short_aliases:
                    self._by_name[name] = (f, AliasKind.SHORT)

    def families(self) -> tuple[ToolFamily, ...]:
        return self._families

    def eligible(self) -> tuple[ToolFamily, ...]:
        return tuple(f for f in self._families if f.eligible)

    def excluded(self) -> tuple[ToolFamily, ...]:
        return tuple(f for f in self._families if not f.eligible)

    def get(self, family_id: str) -> ToolFamily | None:
        return self._by_id.get(family_id)

    def resolve_alias(self, tool_name: str) -> tuple[ToolFamily, AliasKind] | None:
        """Return (family, alias_kind) for a legacy/short/consolidated tool name."""
        return self._by_name.get(tool_name)

    def is_excluded(self, family_id: str) -> bool:
        f = self._by_id.get(family_id)
        return f is not None and not f.eligible

    def validate_target_type(self, family_id: str, target_type: str) -> str | None:
        """Return None if valid, else a structured-error message listing allowed
        values (fr_452f7d7f / ac_c0c8f0f3). Excluded families never validate."""
        f = self._by_id.get(family_id)
        if f is None:
            return f"Unknown tool family '{family_id}'"
        if not f.eligible:
            return (
                f"Tool family '{family_id}' is not consolidated (kept separate by the "
                f"assertiveness gate). Use its dedicated tools: {', '.join(f.legacy_aliases)}"
            )
        if target_type not in f.target_types:
            return (
                f"Unsupported target_type '{target_type}' for {family_id}. "
                f"Allowed: {', '.join(f.target_types)}"
            )
        return None

    def short_alias_collisions(self, existing_names: frozenset[str]) -> tuple[str, ...]:
        """Return short aliases that collide with an existing tool/namespace name
        (tr_524ea8d3). Short aliases must be additive and collision-free."""
        out: list[str] = []
        for f in self.eligible():
            for short in f.short_aliases:
                if short in existing_names:
                    out.append(short)
        return tuple(out)


# Module-level default registry.
REGISTRY = ToolFamilyRegistry()


# ---------------------------------------------------------------------------
# Safe telemetry emission (counts + identifiers only; never a body)
# ---------------------------------------------------------------------------

def _safe_label_value(value: object) -> bool:
    """A label value is safe when it is a short scalar identifier — never a body."""
    if isinstance(value, bool) or value is None:
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return len(value) <= 64 and "\n" not in value
    return False


def emit_alias_usage(
    *,
    family_id: str,
    alias_kind: str,
    tool_name: str,
    operation: str,
    target_type: str,
    outcome: str,
) -> dict[str, object]:
    """Emit the SAFE ``mcp_tool_alias_usage_total`` diagnostic (or_4e57890f). Only
    counts + identifiers — never the question text / payload body. Fail-closed:
    rejects any unsafe label value."""
    labels = {
        "family_id": family_id,
        "alias_kind": alias_kind,
        "tool_name": tool_name,
        "operation": operation,
        "target_type": target_type,
        "outcome": outcome,
    }
    for key, val in labels.items():
        if key not in SAFE_ALIAS_LABEL_KEYS or not _safe_label_value(val):
            _LOG.warning("%s rejected unsafe label %r", METRIC_ALIAS_USAGE, key)
            return {}
    _LOG.info(METRIC_ALIAS_USAGE, extra={"alias_usage": labels})
    return labels


def emit_registry_violation(
    *, family_id: str, reason: str, tool_name: str = "", target_type: str = ""
) -> dict[str, object]:
    """Emit the SAFE ``mcp_tool_family_registry_violation_total`` diagnostic
    (or_b0cdae15) for alias collisions / unknown aliases / excluded-family attempts.
    No mutation should accompany these events."""
    labels = {
        "family_id": family_id,
        "reason": reason,
        "tool_name": tool_name,
        "target_type": target_type,
    }
    for key, val in labels.items():
        if key not in SAFE_VIOLATION_LABEL_KEYS or not _safe_label_value(val):
            _LOG.warning("%s rejected unsafe label %r", METRIC_REGISTRY_VIOLATION, key)
            return {}
    _LOG.info(METRIC_REGISTRY_VIOLATION, extra={"registry_violation": labels})
    return labels
