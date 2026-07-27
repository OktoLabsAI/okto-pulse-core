---
version: "1.0"
---

# Tool family: `qa_ask` (R4 consolidation)

Consolidated "ask a question on a work item's Q&A board" through one tool with a
closed `target_type` enum. One of the two assertiveness-gate-eligible families:
all five legacy ask tools have identical `(board_id, <parent_id>, question)`
signatures, so consolidation loses no typed guidance — only the parent-id param
*name*, which the closed enum restores.

## Consolidated tool

`okto_pulse_ask(board_id, target_type, parent_id, question)`

- `target_type` ∈ `card` | `ideation` | `refinement` | `spec` | `sprint`
- An unsupported `target_type` returns a structured error and performs **no
  mutation**.

## Asymmetry (preserved by dedicated routing)

Most targets route through a typed `*QACreate` schema, a `QA_CREATE` permission
gate, and an activity-log write. **`sprint` is asymmetric**: `SprintQAService`
takes a raw string (no `SprintQACreate` model), has **no `QA_CREATE` permission
gate** and **no activity-log** write. Dedicated routing preserves this exactly —
a naive uniform merge would crash on or silently change the sprint path.

The sibling `*_choice_question` tools (which add `options`/`question_type`) are
**not** part of this family and remain separate.

## Legacy aliases (preserved, additive — not removed)

- `okto_pulse_ask_question(board_id, card_id, question)`
- `okto_pulse_ask_ideation_question(board_id, ideation_id, question)`
- `okto_pulse_ask_refinement_question(board_id, refinement_id, question)`
- `okto_pulse_ask_spec_question(board_id, spec_id, question)`
- `okto_pulse_ask_sprint_question(board_id, sprint_id, question)`

Registry-only short name: `ask`. It exists for internal family resolution,
collision checks, and bounded telemetry labels; it is **not** an MCP tool token,
does not appear in `tools/list`, and cannot be invoked remotely. The five
`okto_pulse_*` entries above are the live additive MCP aliases.

## Telemetry

Every dispatch emits `mcp_tool_alias_usage_total` with safe labels
`{family_id, alias_kind, tool_name, operation, target_type, outcome}` — counts
only, never the question text.
