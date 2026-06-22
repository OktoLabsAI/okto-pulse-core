---
version: "1.0"
---

# Tool docs — `test-scenario`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_test_scenario`

Add a test scenario to a spec. Test scenarios translate acceptance criteria into
concrete Given/When/Then test plans.

Args:
    board_id: Board ID
    spec_id: Spec ID
    title: Scenario title (e.g. "Valid OAuth2 token grants access")
    given: Precondition (e.g. "User has a valid JWT token")
    when: Action (e.g. "GET /api/v1/boards with Bearer token")
    then: Expected result (e.g. "Returns 200 with board list")
    scenario_type: unit | integration | e2e | manual | negative (default: integration).
        Use ``negative`` for expected denial, validation failure, error-path, or
        abuse-case behavior. STRICT contract: an unsupported value (e.g.
        ``regression``) is rejected
        with a structured ``invalid_scenario_type`` error naming the allowed
        values and NO scenario is appended — it is NEVER silently normalized to
        ``integration``.
    linked_criteria: Multi-value (pipe ``"0|2"`` or JSON-array ``'["0","2"]'``)
        references to the acceptance criteria this scenario validates. Each
        token may be a 0-based index, a structured ``ac_id`` (e.g. ``ac_1a2b``),
        or the EXACT acceptance-criterion text. ``ac_id`` is recommended — it is
        the canonical projection persisted in ``linked_criteria`` (legacy ACs
        without an id fall back to their text). Matching is exact: prefix
        matching is NOT accepted on this write path. Resolution is fail-closed
        and atomic — if ANY token is unresolved the tool returns a structured
        error JSON (listing the failing tokens, the valid index range and the
        available ac_ids) and appends no scenario. Use okto_pulse_get_spec to
        see the acceptance_criteria list, their indices, and their ids.
    notes: Additional notes or edge cases (optional)

Returns:
    JSON with the created scenario; ``linked_criteria`` is always a list of
    canonical ac_id strings (never a dict).

## `okto_pulse_delete_test_scenario`

Delete a test scenario and clean Card.test_scenario_ids in CASCADE.

    Removes the scenario from the spec AND drops its id from every card that
    references it, atomically (all-or-nothing). Does not block on existing links.
    Respects the spec content-lock.

    Args:
        board_id: Board ID.
        spec_id: Spec ID.
        scenario_id: Test scenario ID to delete.

    Returns:
        JSON {success, scenario_id, cards_unlinked} or
        {error: spec_locked|scenario_not_found}.

## `okto_pulse_list_test_scenarios`

List test scenarios for a spec with coverage information. Supports filtering and pagination.

Args:
    board_id: Board ID
    spec_id: Spec ID
    status: Filter by scenario status (optional) — one of: draft, ready, automated, passed, failed
    scenario_type: Filter by type (optional) — one of: unit, integration, e2e, manual, negative
    linked: Filter by task linkage (optional) — "linked" = only scenarios with tasks, "unlinked" = only scenarios without tasks
    offset: Skip first N scenarios (default 0)
    limit: Max scenarios to return (default 50, max 200)

Returns:
    JSON with filtered/paginated scenarios and acceptance criteria coverage status

## `okto_pulse_update_test_scenario`

Edit the BODY of a test scenario (title/given/when/then/scenario_type/
    linked_criteria/notes). Does NOT accept status — status stays exclusive to
    okto_pulse_update_test_scenario_status so no second NC-9 bypass is created.

    Empty-string params mean "leave unchanged". To intentionally CLEAR a field,
    list it in `clear` (pipe-separated); only `notes` and `linked_criteria` are
    clearable. `linked_criteria` is a pipe-separated list of AC index/id/text,
    resolved to AC ids (fail-closed on unresolved tokens).

    Editing a SEMANTIC field (given/when/then/scenario_type/linked_criteria) of a
    scenario that holds evidence invalidates it: status resets to `ready` and the
    evidence is dropped. Cosmetic edits (title/notes) preserve status + evidence.
    Respects the spec content-lock.

    Args:
        board_id: Board ID.
        spec_id: Spec ID.
        scenario_id: Test scenario ID (e.g. "ts_abc123").
        title/given/when/then/scenario_type/notes: New value, or "" to leave as-is.
            scenario_type follows the same STRICT contract as add — valid values
            are unit, integration, e2e, manual, negative. An
            unsupported value is rejected (``invalid_scenario_type``) before any
            mutation, never normalized.
        linked_criteria: Pipe-separated AC index/id/text (resolved to AC ids).
        clear: Pipe-separated field names to empty (notes, linked_criteria).

    Returns:
        JSON {success, scenario_id, updated_fields, evidence_invalidated} or
        {error: spec_locked|scenario_not_found|unresolved_criteria|invalid_scenario_type|invalid_update}.

## `okto_pulse_update_test_scenario_status`

Update the status of a test scenario, optionally attaching structured
evidence that the test really exists/ran.

**Test theater prevention gate (NC-9, spec 873e98cc):**

When the board's `skip_test_evidence_global` setting is False (default),
setting status to one of `automated`, `passed`, or `failed` REQUIRES
structured evidence:
  - `automated`: evidence.test_file_path AND evidence.test_function
  - `passed`/`failed`: either an explicit `evidence_class` with the fields
    listed below, or unclassed run-log evidence with evidence.last_run_at AND
    (evidence.output_snippet OR evidence.test_run_id) AND
    evidence.expected_output_snapshot AND
    evidence.non_replayable_justification
  - `draft`/`ready`: evidence opcional (intent declarado)

When `skip_test_evidence_global=True`, the gate is bypassed — every
status update is accepted without evidence, but a structured audit log
`test_scenario.evidence_gate_skipped` is emitted for forensics.

Evidence is persisted inline within the scenario dict (no DB migration).
Audit log `test_scenario.status_changed` is emitted on every successful
update with `evidence_provided`, `evidence_gate_skipped`, and
`changed_by_agent_id`.

**Re-executable evidence contract (spec 9e0bf979):**

Evidence may declare an explicit `evidence_class` so a validator can rerun or
inspect the artifact instead of trusting a raw log. The six classes and their
minimum fields (on a gated status) are:

  - `automated_test_pointer`: `test_file_path` + `test_function`.
  - `replay_command`: `replay_command` + `expected_output_snapshot`.
  - `mcp_replay_manifest`: `mcp_replay_manifest` + `expected_output_snapshot`.
  - `manual_checklist`: `manual_checklist_ref` + `expected_output_snapshot`.
  - `run_log`: `last_run_at` + (`output_snippet` OR `test_run_id`) +
    `non_replayable_justification` + `expected_output_snapshot`.
  - `non_replayable_justified`: `non_replayable_justification` +
    `expected_output_snapshot`.

An `expected_output_snapshot` (expected output / success criteria) is required
for every class except the direct `automated_test_pointer`. An invalid
`evidence_class` value fails closed (it is never normalized).

**Cheap/existing replay (when run logs are NOT acceptable):** a deterministic
replay is treated as cheap or already-existing — so a run log is the wrong
class — when any of these is present: an existing test (`test_file_path`), an
existing command/script (`replay_command`), or a deterministic MCP replay
manifest writable under bounded setup (`mcp_replay_manifest`). A `run_log` /
`non_replayable_justified` payload is rejected when `replay_should_exist=true`
OR a cheap/existing signal is present — declare a replayable class instead.

**Write vs read:** on a NEW gated write without `evidence_class`, only the
legacy direct test pointer (`test_file_path` + `test_function`) is grandfathered;
a run-log-like payload must carry `expected_output_snapshot` +
`non_replayable_justification` (or declare `evidence_class`). Already-persisted
legacy evidence stays readable and can be upgraded with `evidence_class` without
losing prior fields. The system enforces only the minimum fields; whether a
`non_replayable_justification` is credible remains a validator judgment.

Validated/done specs keep their semantic content lock. The only post-lock
status update allowed here is operational evidence for a scenario that is
already linked to an executable `card_type="test"` card (`started`,
`in_progress`, `validation`, or `done`). If no such test card exists, the call
returns `status_not_mutable`.

Args:
    board_id: Board ID
    spec_id: Spec ID
    scenario_id: Test scenario ID (e.g. "ts_abc123")
    status: New status — one of: draft, ready, automated, passed, failed
    evidence: Optional JSON string. Legacy keys: test_file_path, test_function,
        last_run_at, test_run_id, output_snippet. Re-executable contract keys
        (spec 9e0bf979): evidence_class, replay_command, mcp_replay_manifest,
        manual_checklist_ref, expected_output_snapshot, replay_should_exist,
        non_replayable_justification. Empty string = no evidence.

Returns:
    JSON. On success: {success, scenario_id, old_status, new_status,
    evidence_provided, evidence_gate_skipped}. On gate failure:
    {error: "evidence_required", required: [...], message: "..."}.
