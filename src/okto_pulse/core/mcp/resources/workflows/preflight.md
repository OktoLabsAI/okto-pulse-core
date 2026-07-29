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

Call the matching `okto_pulse_get_{ideation,refinement,spec,sprint}_context`
with `profile="full"` before any move/validation. For cards, call
`okto_pulse_get_task_context(profile="full", context_scope="gate")`: it is the
bounded full gate/readiness slice and includes a content manifest plus
drilldowns. The default `summary` profile is for cheap exploration, not for
status-changing work (see
`okto-pulse://reference/projection-profiles`).

### Card execution pre-flight — before implementation work

```
1. okto_pulse_get_task_context(board_id, card_id, profile="full", context_scope="gate", include_knowledge=true, include_mockups=true, include_architecture=true, include_qa=true, include_comments=true)
2. Attach applicable artifacts — follow the Card-Level Artifact Attachment path in §2.8 of okto-pulse://workflows/cards (the single source: copy tools per artifact, decide per KE/mockup/Architecture Design, and any skip requires a one-line justifying comment)
3. okto_pulse_move_card(status="in_progress")
4. BEGIN WORK
```

**Never skip card execution steps 1 and 3.**

Use `profile="detail"` and follow its drilldowns when implementation/review
needs artifact or requirement bodies. Do not replace the gate-scope call with a
potentially oversized `profile="full", context_scope="all"` response. Mutation
services resolve and fingerprint complete context server-side; the client-side
gate slice is the bounded operational view.

This is an operational protocol rule: the MCP server does not prove that you
read context; your audit trail and artifact quality do.

### Quality Assessment pre-flight — before record/read/gate use

Read `okto-pulse://reference/quality-assessments` before operating on Quality
evidence. For an ambiguity write:

1. Read the subject's full current context.
2. Read its current ambiguity head and retain `head_revision` (use 0 when
   absent).
3. Confirm domain-write authority AND `{subject}.quality.assess`; proposed
   questions also require `{subject}.qa.ask`.
4. Submit a stable idempotency key with the exact
   `expected_subject_version` and `expected_head_revision`.
5. After success, re-read currentness. A head is not proof that the receipt is
   current.

Before executing the Spec checklist, read the full Spec context and
`okto_pulse_get_checklist_binding`; freeze the current Spec version and binding
identity when starting the execution. Submit every immutable item exactly once.
`manual_checklist_ref` is legacy evidence only and never satisfies A3.

### Resource Gate pre-flight — mandatory before completion

Architecture, Mockup, and Knowledge Base are all tracked by Resource Gate, but
their authority differs: **Architecture and Mockup are blocking**;
**Knowledge Base is advisory**. A missing or uncovered KB remains visible in
the summary/workspace and never blocks entity completion, `spec_validation`, or
`spec_done`.

| Work item | Resource Gate `entity_type` |
|---|---|
| Ideation | `ideation` |
| Refinement | `refinement` |
| Spec | `spec` |
| Card, task, test, bug | `card` |

Before finalization call `okto_pulse_get_resource_gate_summary(board_id, entity_type, entity_id)` and resolve every entry in `missing_resources` by attaching the blocking Architecture/Mockup artifact or marking it N/A with `justification`. Review `advisory_missing_resources` for useful KB context, but do not create filler or mark KB N/A solely to satisfy a gate.

The blocking-resource playbook remains reversible and auditable: marking N/A with `justification` records intent, and `okto_pulse_clear_resource_not_applicable` restores the normal presence check when the resource becomes applicable.

### Design System pre-flight — before creating or editing mockups

A board can mandate a **Design System** for its screen mockups. The `MockupDesignSystemGate`
enforces it deterministically on `okto_pulse_add_screen_mockup` / `okto_pulse_update_screen_mockup`
(and the REST twin) **before persistence**. Discover the mandate from the board summary so you
reference the Design System from the start instead of learning it by being rejected:

```
1. okto_pulse_get_board(board_id) → read the `design_system` block:
     { "effective": {design_system_id, title, version, source} | null,
       "gate_mode": "off" | "advisory" | "blocking",
       "mandate": true when there is an effective Design System AND gate_mode == "blocking" }
2. okto_pulse_get_board_design_system(board_id) → the effective Design System identity (id/version/source)
3. okto_pulse_get_design_system(design_system_id) → the full Design System incl. payload (tokens, components, layout/accessibility rules) to actually consume it
```

When `mandate` is true (gate_mode=blocking + an effective Design System), a new/updated mockup MUST carry:
- `design_system_ref` = the board's REAL effective `design_system_id` (synthetic/wrong → rejected),
- `design_system_version` matching the effective version,
- `design_system_evidence` = non-empty proof the screen consumes the Design System.

Otherwise the gate rejects **before persisting** with an actionable, structured error:

| reason code | meaning |
|---|---|
| `design_system_required` | no `design_system_ref` provided |
| `design_system_not_found` | the ref is synthetic, non-existent, or dangling (does not resolve to the real effective Design System) |
| `design_system_version_mismatch` | the version does not match the effective Design System version |
| `design_system_evidence_missing` | no consumption evidence |

The error payload carries `expected_design_system_id` and `expected_design_system_version` — use
them to **self-correct** (set `design_system_ref` / `design_system_version` to the expected values,
add evidence) and retry.

- **advisory**: the mockup is persisted, but the response carries a `design_system_gate` warning and a queryable `DesignSystemGateAudit` row is written.
- **off**, or a board with **no effective Design System**: the gate never blocks.

> Caveat: `mandate=true` can still fail at the gate with `design_system_not_found` if the board's configured Design System is **dangling** (e.g. a board link to a deleted Design System — the resolver still reports it but `exists=false`). Read a `design_system_not_found` under `mandate=true` as "the board's Design System config is broken", not as your ref being wrong.
