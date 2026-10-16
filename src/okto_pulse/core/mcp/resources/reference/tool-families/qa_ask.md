---
version: "1.0"
---

# Tool family: `qa_ask` (R4 consolidation)

Consolidated "ask a question on a work item's Q&A board" through one tool with a
closed `target_type` enum. One of the two assertiveness-gate-eligible families:
all four targets use `(board_id, parent_id, question)` with target-specific routing.

## Consolidated tool

`okto_pulse_ask(board_id, target_type, parent_id, question)`

- `target_type` ∈ `card` | `ideation` | `refinement` | `spec`
- An unsupported `target_type` returns a structured error and performs **no
  mutation**.

## Authorization and scope

Each target keeps its own Q&A permission, Board scope, content-state guards
and activity log. A permission on one target grants no access to another.

The sibling `*_choice_question` tools (which add `options`/`question_type`) are
**not** part of this family and remain separate.

## Telemetry

Every dispatch emits `mcp_tool_alias_usage_total` with safe labels
`{family_id, alias_kind, tool_name, operation, target_type, outcome}` — counts
only, never the question text.
