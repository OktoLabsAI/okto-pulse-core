---
version: "1.0"
---

# Tool docs — `story`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_archive_story`

Archive a Story without deleting lineage or linked Ideations.

## `okto_pulse_create_story`

Create a lightweight Story before Ideation.

Args:
    board_id: Board ID
    topic_id: Parent Topic ID
    title: Story title
    description: Story description
    actor: The "As a <actor>" role for the user story (optional)
    goal: The "I want <goal>" intent (optional)
    benefit: The "so that <benefit>" outcome (optional)
    status: Story status — draft, triage, or ready (default: draft)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue. (optional)

## `okto_pulse_move_story`

Move a Story through draft, triage, and ready. Converted is set by link/conversion.

## `okto_pulse_restore_story`

Restore an archived Story.

## `okto_pulse_update_story`

Update editable Story fields through MCP.
