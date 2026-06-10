---
version: "1.0"
---

# Tool docs — `traceability`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_get_traceability_report`

okto_pulse_get_traceability_report — return a consolidated SDLC traceability report:
ideation → refinement → spec → sprint → card/test/bug → artifacts.

Use this at the end of an E2E flow to verify whether the agent can answer
what was implemented in each flow and whether KBs, mockups, architecture,
tests, bugs, cards, and parent references stayed queryable.

Args:
    board_id: Board ID.
    ideation_id: Optional ideation filter. When provided, returns only
        lineage below that ideation.
    spec_id: Optional spec filter. When provided, returns the spec and its
        parent ideation/refinement lineage when available.
    include_artifacts: defaults to "false" (compact artifact counts) to keep
        the agent-facing report small; pass "true" to expand the full
        KB/mockup/architecture references.

Returns:
    JSON with consolidated lineage, card/test/bug counts, artifacts, and
    orphan_specs that are linked to the selected board but not attached to
    the selected ideation/refinement chain.
