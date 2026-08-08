---
version: "2.0"
---

# Tool docs — `test-scenario`

Card-type and test-governance rules:
`okto-pulse://reference/card_types`.
Executable guideline evaluation of `test_scenario` follows
`okto-pulse://reference/policy-compliance`.

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
        abuse-case behavior. STRICT contract: the FastMCP host rejects an
        unsupported value (e.g. ``regression``) as ``validation_failed`` at its
        closed argument schema, before agent context, permissions, or UoW
        resolution. No scenario is appended and the value is NEVER silently
        normalized to ``integration``. ``invalid_scenario_type`` is the
        defense-in-depth application envelope for a direct handler/use-case
        invocation that bypasses FastMCP validation; it is not the normal
        FastMCP transport response.
    linked_criteria: Multi-value (formats: okto-pulse://reference/multivalue)
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
    canonical ac_id strings (never a dict). Invalid FastMCP arguments do not
    enter this return contract; the host returns ``validation_failed``.

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
    scenario_type: Optional exact-match filter over the raw persisted value.
        Canonical values are unit, integration, e2e, manual, and negative.
        Historical values such as regression may also be supplied here so
        legacy rows remain discoverable. This is a read-only compatibility
        filter and does not make a historical value valid on any write.
    linked: Filter by task linkage (optional) — "linked" = only scenarios with tasks, "unlinked" = only scenarios without tasks
    offset: Skip first N scenarios (default 0)
    limit: Max scenarios to return (default 50, max 200)

Returns:
    JSON with filtered/paginated scenarios and acceptance criteria coverage status

## `okto_pulse_update_test_scenario`

Edit the BODY of a test scenario (title/given/when/then/scenario_type/
    linked_criteria/notes). Does NOT accept status — status stays exclusive to
    okto_pulse_update_test_scenario_status so no second NC-9 bypass is created.

    Omitted params mean "leave unchanged". In particular, omit
    `scenario_type` to preserve the current type; an empty string is not part
    of the closed enum and is rejected by FastMCP as ``validation_failed``
    before agent context, permissions, or UoW resolution. To
    intentionally CLEAR a field, list it in `clear` (pipe-separated); only
    `notes` and `linked_criteria` are clearable. `linked_criteria` is a
    pipe-separated list of AC index/id/text, resolved to AC ids (fail-closed on
    unresolved tokens).

    Editing a SEMANTIC field (given/when/then/scenario_type/linked_criteria) of a
    scenario that holds evidence invalidates it: status resets to `ready` and the
    evidence is dropped. Cosmetic edits (title/notes) preserve status + evidence.
    Respects the spec content-lock.

    Args:
        board_id: Board ID.
        spec_id: Spec ID.
        scenario_id: Test scenario ID (e.g. "ts_abc123").
        title/given/when/then/notes: New value; omit to leave as-is.
        scenario_type: Optional replacement. Omit the argument to preserve the
            current type. Valid values are unit, integration, e2e, manual, and
            negative. Empty string and every unsupported value are rejected by
            the closed FastMCP schema as ``validation_failed`` before context or
            UoW resolution, never normalized.
        linked_criteria: Pipe-separated AC index/id/text (resolved to AC ids).
        clear: Pipe-separated field names to empty (notes, linked_criteria).

    Returns:
        JSON {success, scenario_id, updated_fields, evidence_invalidated} or
        {error: spec_locked|scenario_not_found|unresolved_criteria|invalid_update}.
        ``invalid_scenario_type`` is retained only as a defense-in-depth
        handler/use-case envelope when FastMCP validation is bypassed; normal
        invalid FastMCP input returns ``validation_failed`` before this handler.

## `okto_pulse_execute_test_scenario_evidence`

Execute an installation-owned replay without mutating the scenario. Exactly
one replay source is required:

  - Preferred MCP-only mode: pass `replay` as a JSON object containing an
    optional `description` and required `steps`. The client never needs a
    filesystem path, manifest root or `scenario_sha256`. Community validates
    the non-programmable GET-only steps, adds the exact
    board/spec/scenario/current-semantic-digest bindings, serializes canonical
    JSON and atomically persists it under an installation-owned deterministic
    `inline-<sha256>.json` reference before execution.
  - Legacy/advanced mode: pass `manifest_ref`, a canonical relative `.json`
    path below `<data_dir>/evidence/manifests`. That installation-managed
    manifest must already declare `purpose: test_scenario_evidence`, the exact
    `board_id`, `spec_id`, `scenario_id` and `scenario_sha256`.

Every step is a loopback HTTP GET with `name`, relative `path`,
`expected_status`, and one or more assertions. Assertions are either
`{"name":"...","kind":"json_equals","path":"dotted.path","expected":...}`
or `{"name":"...","kind":"body_contains","expected":"..."}`. Methods,
headers, bodies, scripts, absolute URLs and redirects cannot be supplied.
Absolute/ref-traversal paths, duplicate JSON keys, non-standard JSON values,
oversized payloads, symlinks, junctions and reparse-point components are
rejected. A generic or cross-context legacy manifest is never executed.

Example `replay` value (encode this object as the MCP string argument):

```json
{
  "description": "Installed Community health/version",
  "steps": [
    {
      "name": "health",
      "path": "/health",
      "expected_status": 200,
      "assertions": [
        {
          "name": "version",
          "kind": "json_equals",
          "path": "version",
          "expected": "0.3.0"
        }
      ]
    }
  ]
}
```

The Community adapter calls the live local Pulse HTTP runtime first. Only after
all responses are observed does it authenticate the complete bounded receipt
history and append an immutable receipt to the local ledger with a
per-installation secret. Signed pre-hardening records may establish key
continuity for append, but remain explicitly non-authoritative at every gate.
Receipt files must remain byte-for-byte canonical JSON; reordered/pretty JSON,
duplicate keys and any other byte rewrite are treated as ledger tampering.
Inline manifest names are content-addressed, so an identical replay in the same
scenario context safely reuses identical canonical bytes; a conflicting or
changed target fails closed. The result contains `{success, persisted: false,
scenario_persisted: false, manifest_persisted, evidence, next_tool}`.
`persisted: false` remains the compatibility indicator that scenario state was
not changed; `manifest_persisted` is true for inline mode. Pass `evidence`
unchanged to `okto_pulse_update_test_scenario_status`. A client-authored
binding, `product_runtime_exercised`, public SHA or receipt-like string is never
trusted.

Args:
    board_id: Board ID.
    spec_id: Spec ID.
    scenario_id: Existing scenario ID.
    status: `automated`, `passed`, or `failed`; must match the observed outcome.
    manifest_ref: Legacy/advanced relative path under the installation
        manifest root. Leave empty when using `replay`.
    replay: Preferred MCP-only JSON object with optional `description` and
        required bounded GET-only `steps`. Leave empty when using
        `manifest_ref`.

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
  - `mcp_replay_manifest` (Evidence V2): `manifest_ref` +
    `execution_attestation` + opaque `execution_receipt`, emitted by
    `okto_pulse_execute_test_scenario_evidence`. The old
    `mcp_replay_manifest` string/object is a
    reader-only legacy alias and never satisfies a new gate.
  - `manual_checklist`: `manual_checklist_ref` + `expected_output_snapshot`.
  - `run_log`: `last_run_at` + (`output_snippet` OR `test_run_id`) +
    `non_replayable_justification` + `expected_output_snapshot`.
  - `non_replayable_justified`: `non_replayable_justification` +
    `expected_output_snapshot`.

An `expected_output_snapshot` (expected output / success criteria) is required
for every non-V2 class except the direct `automated_test_pointer`. An invalid
`evidence_class` value fails closed (it is never normalized).

Evidence V2 `execution_attestation` is a typed object with:

  - `schema_version: 2`, `run_id`, timezone-aware `executed_at`, `scenario_id`,
    `scenario_sha256` and `outcome` (`passed` or `failed`);
  - `product_runtime_exercised: true`;
  - `manifest_sha256` and `attestation_sha256` as lowercase
    `sha256:<64 hex>` digests. The attestation digest binds both the exact
    `manifest_ref` and every execution fact;
  - one or more structured assertions with `name`, `expected`, `observed` and
    `status`; a passed assertion is accepted only when expected and observed
    are JSON-identical;
  - provenance with non-empty `producer`, `producer_version`, `adapter` and
    `environment`.

The verifier is semantic and fail-closed. The public digests detect accidental
changes but are not authority: every write also authenticates the immutable
Community ledger receipt against a persistent per-installation HMAC key and
the exact board/spec/scenario/semantic-digest/status/issuing-actor binding.
The digest covers identity, Given/When/Then, scenario type, linked ACs and the
current AC identity/text. Missing/rotated secrets, missing ledger records,
semantic edits, actor substitution, cross-scenario replay and tampering fail
closed at writes; card, sprint and bug-closeout consumers recompute the current
semantic digest and reauthenticate the receipt as well. A `passed` scenario
requires a passed attestation and all assertions to match. A `failed` scenario requires a failed
attestation and at least one genuine mismatch. Runtime=false, contradictory
observed/expected values, a foreign scenario id, missing provenance, malformed
digests or any post-production tampering are rejected even when every old
minimum field is present.

**Cheap/existing replay (when run logs are NOT acceptable):** a deterministic
replay is treated as cheap or already-existing — so a run log is the wrong
class — when any of these is present: an existing test (`test_file_path`), an
existing command/script (`replay_command`), or a deterministic MCP replay
manifest writable under bounded setup (`manifest_ref` or legacy
`mcp_replay_manifest`). A `run_log` /
`non_replayable_justified` payload is rejected when `replay_should_exist=true`
OR a cheap/existing signal is present — declare a replayable class instead.

**Write vs read:** on a NEW gated write without `evidence_class`, only the
legacy direct test pointer (`test_file_path` + `test_function`) is grandfathered;
a run-log-like payload must carry `expected_output_snapshot` +
`non_replayable_justification` (or declare `evidence_class`). Already-persisted
legacy evidence stays readable without losing prior fields. Legacy manifest
strings/free-form objects are explicitly unverified and cannot close the
status, whole-spec, test-card or sprint gate until a Community runtime adapter
produces a complete Evidence V2 attestation. Non-manifest evidence classes keep
their documented structural policy.

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
        (spec 9e0bf979 / Evidence V2): evidence_class, replay_command,
        manifest_ref, execution_attestation, execution_receipt,
        reader-only mcp_replay_manifest,
        manual_checklist_ref, expected_output_snapshot, replay_should_exist,
        non_replayable_justification. Empty string = no evidence.

Returns:
    JSON. On success: {success, scenario_id, old_status, new_status,
    evidence_provided, evidence_gate_skipped, evidence_verification_status}.
    The verification status is `verified`, `bypassed`, or `not_required`. On gate failure:
    {error: "evidence_required", required: [...], message: "..."}.
