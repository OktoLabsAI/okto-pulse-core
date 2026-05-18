# Changelog

All notable changes to `okto-pulse-core` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Fixed — SDLC E2E gate polish (4 issues from end-to-end ceremonial run 2026-05-17)

A complete end-to-end ceremonial run on the `E2E` board (Story → Ideation → Refinement → Spec → Sprint → Card execution → Sprint closeout) exposed four small but recurring issues across spec validation, error messages and tool response shapes. All four were fixed with minimal-surface code/docs changes inside the `Unreleased` cycle and validated in-vivo against the live MCP server:

1. **Spec lock now cannot trap uncovered ACs** — `submit_spec_validation` previously ran the AC → test-scenario coverage gate only at `move_spec → done` time. A spec could pass validation (becoming `validated` and content-locked) and only then discover, at the next status move, that some ACs lacked scenarios. Because the spec was locked, the missing scenarios could not be added without unlocking and resubmitting the entire validation. The new `CardService.check_ac_scenario_coverage` runs as the FIRST coverage pre-check inside `submit_spec_validation`, so the failure surfaces with the spec still in `approved` status. The error message also reminds callers: "Create test scenarios linked to each AC BEFORE submitting validation — once validation passes the spec is locked and scenarios cannot be added."

2. **FR-coverage error message no longer duplicates the label** — The uncovered-FR error string used the format `"FR{i}: {fr_text}"` with a 0-based Python index, but the FR text supplied by authors already starts with a 1-based label such as `"FR1: ..."`. The combination produced confusing strings like `"FR1: FR2: Renderização HTML→PDF ..."` where the prefix and the embedded label disagreed. The formatter now uses a bracket index marker — `"[{i}] {fr_text}"` — consistent with the AC coverage gate at `move_spec → done`. No label collision, same readable position information.

3. **`link_task target_type='decision'` returns the saturation envelope** — Six of the seven `_link_task_to_*_internal` helpers spread `**_saturation_or_coverage(coverage)` into their success JSON; `_link_task_to_decision_internal` was the only outlier, returning just `{success, decision_id, card_id, linked_tasks}`. Agents driving the "continue linking vs submit validation" decision lost the saturation signal whenever the linked target was a Decision. The decision helper now follows the same contract as the others. A parametrised dispatcher test (`test_link_helper_returns_saturation_envelope`) pins the parity so a future eighth helper can't regress this silently.

4. **`okto_pulse_evaluate_ideation` docstring now states the pre-requisite** — The tool requires the ideation to be in `evaluating` status, with the transition flow `draft → review → approved → evaluating → (evaluate) → done`. The error returned by the service layer was already clear, but agents only discovered the requirement by failing first. The MCP tool docstring now explains the pre-requisite, the full flow, and why the tool deliberately does not auto-promote (each transition is an explicit gate decision).

#### Anti-regression tests added

- `tests/test_spec_validation_gate.py::TestAcScenarioPrecheck::test_uncovered_ac_blocks_validation_before_lock` — verifies that submit fails with the AC message AND that the spec remains in `approved` (not `validated`), so the caller can add the missing scenario and retry without unlocking.
- `tests/test_spec_validation_gate.py::TestFrCoverageMessageFormat::test_message_uses_index_marker_not_duplicated_label` — asserts the rendered error message contains `[0]`/`[1]` markers and does NOT contain the old `FR1: FR2:` collision pattern.
- `tests/test_link_task_dispatcher.py::test_link_helper_returns_saturation_envelope` — parametrised over the seven link helpers; each one must reference `_saturation_or_coverage` in its source.
- `tests/.cache/validation_gates_baseline.txt` — `submit_spec_validation` baseline hash bumped to reflect the new coverage call (intentional change; `submit_evaluation` hash unchanged).

### Fixed — P0.B post-release polish (3 bugs from evaluation 2026-05-16)

After the v0.2.1 cut, an empirical evaluation against the E2E board exposed three discrepancies between the spec/API contracts and the actual handler implementations. All three were fixed as docs-or-tiny-code follow-ups in the same `Unreleased` cycle:

1. **Signature alignment (docs)** — The spec P0.B (`ec70c5f7`) and API contracts (`api_c963da56`, `api_ef747629`, `api_0b42caa1`) document idealised shapes that drop `board_id` and use `parent_type/parent_id` for `list_knowledge`. The actual implementations require `board_id` everywhere (needed for ACL/auth) and use a uniform `entity_type/entity_id` naming. The implementation is the source of truth; the affected docstrings are now authoritative. Bug card: `75535b71`.

2. **`filters` accepts JSON string** — `okto_pulse_list_by_board`, `okto_pulse_list_qa`, and `okto_pulse_list_knowledge` now accept `filters` as either a Python dict OR a JSON-encoded string (e.g. `'{"status":"done"}'`). MCP transports often serialise complex params as strings; without this, agents that followed the JSON-RPC convention hit a `dict_type` Pydantic error. The signature is now `filters: dict[str, Any] | str | None = None` and each handler decodes the string with `json.loads` (returning a `structured_error` on `JSONDecodeError`). Bug card: `eb782ae4`.

3. **`limit` default unified to 100** — The consolidated `okto_pulse_list_by_board` handler now owns the list pagination default consistently with `limit=100`. The legacy entity-specific list tools were removed from the MCP registry in this branch, so clients use the consolidated handler directly. Bug card: `717e9915`.

### Authoritative handler signatures (P0.B)

| Handler | Signature |
|---|---|
| `okto_pulse_list_by_board` | `(board_id, entity_type, filters?, limit=100, offset=0)` |
| `okto_pulse_list_qa` | `(board_id, entity_type, entity_id, filters?)` |
| `okto_pulse_list_knowledge` | `(board_id, entity_type, entity_id, filters?)` |
| `okto_pulse_list_snapshots` | `(board_id, entity_type, entity_id)` |

All four return `structured_error` with keys `error`, `error_code`, `supported`, `detail` when invoked with an unsupported `entity_type` or invalid filter.

### Added — P0.B: 4 polymorphic list handlers

Four new consolidated MCP tools that replace 15 entity-specific `list_*` tools. Each handler delegates to the same existing service layer — behavior is preserved while the surface area is reduced.

| New handler | Replaces | Scope |
|---|---|---|
| `okto_pulse_list_by_board` | `list_specs`, `list_ideations`, `list_refinements`, `list_sprints`, `list_stories`, `list_topics` | Top-level board entities |
| `okto_pulse_list_qa` | `list_spec_qa`, `list_ideation_qa`, `list_refinement_qa` | Q&A items per entity |
| `okto_pulse_list_knowledge` | `list_spec_knowledge`, `list_ideation_knowledge`, `list_refinement_knowledge`, `list_card_knowledge` | Knowledge base items per parent (listing only — create/update/delete remain entity-specific) |
| `okto_pulse_list_snapshots` | `list_ideation_snapshots`, `list_refinement_snapshots` | Version snapshots per entity |

Server-side filter validation is enforced via the new `okto_pulse.core.mcp.filters.validate_filters()`; invalid combinations return a structured error with keys `error`, `error_code`, `supported`, `suggested_tool` (preserving the `error` key for backwards compatibility with existing clients).

### Added — P0.A: MCP resources for instructions

Twelve MCP resources (`okto-pulse://workflows/...` and `okto-pulse://reference/...`) now expose workflow and reference material on demand, allowing the root `agent_instructions.md` to shrink from ~3,403 lines to ~271 lines. Clients that support `resources/read` fetch detailed content lazily; clients that do not continue to function unchanged (graceful degradation).

### Added — P2: Runtime schema generation (pilot)

New module `okto_pulse.core.mcp.schema_generator` derives MCP tool input schemas from Pydantic v2 models at server startup via `model.model_json_schema()` + `model.model_fields`. Pilot covers five card CRUD tools (`get_card`, `update_card`, `move_card`, `delete_card`, `list_cards_by_status`); `create_card` is intentionally excluded from the pilot. Coverage of `Field(description=...)` is 100% on the 15 pilot models.

### Removed — 15 entity-specific `list_*` MCP tools

The 15 entity-specific list tools were removed from the MCP server registry in favor of the four consolidated handlers above. Because Pulse is local-first and agents consume the current local server instructions, this is treated as an in-branch surface reduction for v0.2.1 instead of a compatibility window.

**Migration guide**: replace each old call with the corresponding consolidated handler from the table above. The new handlers accept the same logical arguments using board scope, entity discriminator, and optional filters. Server-side filter validation may reject filter shapes that the removed tools silently accepted; when in doubt, omit unsupported filters and re-validate the response shape.

The full mapping plus naming rationale lives in spec `ec70c5f7-1af6-4830-90e6-e9fd9a6e0b63` on the Okto Pulse 0.2.1 board.

---

## [0.2.1] — 2026-05-16

Initial 0.2.1 release. See git tag `v0.2.1` for the full diff against `v0.2.0`.
