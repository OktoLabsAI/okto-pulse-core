---
version: "1.0"
---

# Pre-Flight Checklist (READ FIRST)

Every time you start a session or pick up a new task, follow the matching sequence below.

### Session pre-flight — before any board work

```
1. okto_pulse_get_my_profile()             → know who you are
2. okto_pulse_list_my_boards()             → know what you have access to
3. okto_pulse_get_unseen_summary(board_id) → check mentions + recent activity
4. okto_pulse_get_board_guidelines(board_id) → read rules set by the board owner
```

### Entity context pre-flight — before moving or validating anything

Call the matching `okto_pulse_get_{ideation,refinement,spec,sprint}_context` (or `okto_pulse_get_task_context` for cards) **with `profile="full"`** before any move/validation — the default `summary` profile is for cheap exploration, not for status-changing work (see `okto-pulse://reference/projection-profiles`).

### Card execution pre-flight — before implementation work

```
1. okto_pulse_get_task_context(board_id, card_id, profile="full", include_knowledge=true, include_mockups=true, include_architecture=true, include_qa=true, include_comments=true)
2. okto_pulse_copy_mockups_to_card(board_id, spec_id, card_id)
3. okto_pulse_copy_knowledge_to_card(board_id, spec_id, card_id)
4. okto_pulse_copy_architecture_to_card(board_id, spec_id, card_id)
5. okto_pulse_move_card(status="in_progress")
6. BEGIN WORK
```

**Never skip card execution steps 1 and 5.**

This is an operational protocol rule: the MCP server does not prove that you read context; your audit trail and artifact quality do.

### Resource Gate pre-flight — mandatory before completion

Architecture, Mockup, and Knowledge Base are mandatory Resource Gate types.

| Work item | Resource Gate `entity_type` |
|---|---|
| Ideation | `ideation` |
| Refinement | `refinement` |
| Spec | `spec` |
| Card, task, test, bug | `card` |

Before finalization call `okto_pulse_get_resource_gate_summary(board_id, entity_type, entity_id)` and resolve every `missing` resource by attaching the artifact or marking N/A with `justification`.
