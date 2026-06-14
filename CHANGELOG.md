# Changelog

All notable changes to `okto-pulse-core` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.2.5] - 2026-06-13

### Added

- KG working/canonical graph partitioning with source-maturity classification.
- CanonicalDebt ledger and reprocessable retry contracts for failed or deferred canonical promotion.
- Layer-aware rebuild/preflight diagnostics and canonical-only query defaults with explicit working-result opt-in.

### Changed

- KG Health reports canonical-debt diagnostics separately from cognitive-consolidation pending work.
- Rebuild source manifests distinguish maturity skips from cancellation skips.

## [0.2.3] - 2026-06-10

The largest release since the governed-SDLC + Knowledge Graph cut in 0.2.0. Scope is taken from the **53 finalized specs on the Okto Pulse 0.2.3 board** (the platform dogfooded its own SDLC), landing **64 new core modules** across eight subsystems. The throughline is making the embedded Knowledge Graph survivable under real concurrency and rebuildable from canonical sources, while the SDLC layer gained governance context-guards, a bug-regression workflow, structured spec-entity editing, and a token-budget/projection layer for agents.

The MCP surface grows to **215 tools**, the named-gate surface to **17**, and the package to **52 models / 28 services / 33 API modules**. `335 files changed, +103,183 / −4,532` over `v0.2.2`. Every subsystem ships with its pytest suite.

### Added — Knowledge Graph corruption prevention & durability (KG-01, KGDL.01)

The headline. A dedicated set of write-path primitives (`safe_write_lifecycle`, `write_barrier`, `single_writer_lock`, `backpressure`, `quarantine`, `contingency`, `config_guard`) that remove the corruption vectors observed under concurrent reads.

- **Non-destructive durability lifecycle on the hot path** (spec `3d89c192`). `STEP_CHECKPOINT` now runs a real `CHECKPOINT` without closing the cached `Database`; `STEP_FLUSH`/`STEP_FSYNC` became non-destructive; a per-board `_BoardCloseGuard` drains live readers (up to 5s, fail-open + structured `kg.close_guard.timeout`) before any close. This eliminates the use-after-close of the shared `Database` — the probable second corruption vector for `graph.lbug` — where FLUSH/FSYNC closed the handle while the `kg_service` thread pool held live `Connection`s over it (C++ UB). The `close_reopen_probe` leaves the hot path and is now exclusive to the rebuild/recovery lanes.
- **Single-writer enforcement** + write barrier so only one path mutates a board's graph at a time.

### Added — KG recovery, reset & deterministic rebuild (KG-02 + R2a)

Rebuild a board's graph from canonical SQL sources, deterministically and audited.

- `rebuild_service`, `rebuild_preflight`, `rebuild_confirmation`, `rebuild_deterministic`, `rebuild_generation`, `rebuild_sources`, `board_source_store`, `board_rebuild_adapter`, plus `rebuild_audit`/`rebuild_report`.
- REST `api/kg_rebuild` + agent-actionable MCP twins `okto_pulse_kg_rebuild_preflight` / `_confirm` / `_run` (confirmation-token gated; refuses when the board is quarantined). Closes the 2 inherited REST gaps (health-probe + per-board scope).
- **Auto-recovery of interrupted checkpoints** — orphaned sidecars detected and recovered on boot; recovery accepts a non-empty checkpoint shadow.

### Added — KG zero-orphan integrity (KG-ZO-01, KG-ZO-02)

- **Node connectivity pre-commit guard & writer enforcement** (`connectivity_guard`) — refuses to commit nodes that would be orphaned.
- `orphan_integrity` + `global_discovery_reindex` + REST `kg_orphan_integrity` + MCP `okto_pulse_kg_orphan_report` / `okto_pulse_kg_orphan_backfill` — orphan backfill, health reporting and rebuild visibility, with single-flight scanning off the event loop.

### Added — KG cognitive consolidation & source governance (KG-03, KG-03A)

- `cognitive_closeout_gate`, `cognitive_badge_resolver`, `candidate_decision_store`, `refinement_cognitive_guard`, extractor `source_ref` (per-concept `{base}:{type}:{hash8}`).
- REST `kg_cognitive_badges` / `kg_cognitive_candidates` / `kg_cognitive_candidate_commands` / `kg_cognitive_pending` + MCP `okto_pulse_kg_list_cognitive_pending_items` / `okto_pulse_kg_update_cognitive_pending_item` — cognitive item control, candidate-decision promotion and UI feedback.
- **Cognitive dedup granularity, SUPERSEDE wiring & counted merge** — per-concept reference, a dedicated `SUPERSEDE` branch, and counted/audited merges (eliminates the over-dedup that collapsed distinct concepts).

### Added — KG health honesty & degraded-mode resilience (F3, F4, F16, F17, R2c)

- **Health signal clarity** (`health_state`, `kg_health_service`) — distinguishes scheduler/decay debt from corruption; footprint is a file-size proxy, not a corruption signal.
- **Resilient, observable decay tick** — per-board failure isolation, idempotent persistence without retry-storm; the 24h tick honors the last `kg_tick_runs` so it fires on restarting processes.
- **Degraded-KG protocol** — `graph_availability` + `connectivity_guard` fallback gates, a uniform structured `graph_unavailable` envelope across KG read tools, a `kg_health`-first protocol fallback, and a health-aware closeout gate + tick admission control.
- **Real memory-pressure instrumentation** (`memory_pressure` + `memory_pressure_collector`, buffer/flush) + opt-in **DLQ auto-drain** (board setting `dlq_auto_drain_enabled`).
- **KG discovery attribute loading & entity selectors** (`discovery_params_schema`, `discovery_selector_catalog`, `discovery_selector_cache`) — new Global Discovery intents incl. "Key Decisions" (relevance + connectivity) and "Learnings by relevance".

### Added — Governance, lineage & gates (BG-01, RG-01, AFG)

- **`critical_context_guard` (BG-01)** + `board_governance` + `governance_observability` — board agent governance settings; critical mutations resolve and fingerprint full entity context first (board setting `require_full_context_for_critical_actions`).
- **`resource_lineage` resolver (RG-01)** — provenance contract + resource-gate N/A inheritance down the lineage.
- **Architecture Finding Done Gate (AFG)** — `architecture_observability` + a dedicated `validate_or_raise_architecture_findings` on `spec → done`.

### Added — MCP token-optimization & projection (R1–R5)

- **Token-budget & projection layer** — `payload_budget`, `payload_compaction`, `projection_envelope`, `context_projection`, `copy_projection`, `kg_query_safety`, `tool_family_registry`. Compact tools/list description budgets, projection profiles, high-frequency response projection + dedup, and KG-query payload bounds.
- **Schema honesty** — `anyOf array|string` across the full multi-value cluster (refinement/spec/decision/choice) + object/`array|string` on `*_json` params + a uniform `{error, detail}` error envelope.
- **Canonical id/ref references** — positional → canonical id/ref migration for inter-entity spec references (`linked_requirements → FR`, `linked_criteria → AC`) with backward-compat.
- Two consolidated tools (`okto_pulse_ask`, `okto_pulse_remove_spec_entity`) + 3 rebuild twins take the surface to **215 tools**. The pre-flight checklist is now a real `okto-pulse://workflows/preflight` resource with a registry-URI drift guard.

### Added — Bug-regression workflow

- `bug_regression_scenarios`, `bug_regression_preview`, `bug_regression_observability`, `bug_workflow_remediation` + MCP `okto_pulse_resolve_bug_regression_scenarios` — scenario reuse and test-gate remediation, bug-workflow guidance/error-remediation operator UX, and a post-closure hotfix lane in the sprint lifecycle.

### Added — Structured spec entities & API-contract hardening

- **Structured editing for spec entities** (`spec_structured_entities`, `spec_entity_canonicalization`, `test_scenario_lifecycle`) + MCP `okto_pulse_update_spec_entity` / `okto_pulse_update_spec_api_contract` / `okto_pulse_update_test_scenario` / `okto_pulse_delete_test_scenario`. **`test_scenario` CRUD** closes the NC-9 bypass at the service gate. Phased move to structured-only FR/AC writes with an idempotent migrator + the legacy `materialize_legacy_fr_ac` command.
- **ApiContract data-model hardening** — `contract_type` discriminator, HTTP-method enum, documented JSON shapes, canonical construction errors; granular per-requirement `not_applicable` for IR/OR/contract; N/A inheritance parent→child with auto-derive documented as Spec→Card-only.
- **Structured choice fields** (`recommended`/`tradeoff`) + opt-in Q&A role separation per board.

### Added — Analytics & telemetry

- `coverage_calculator` — Analytics IR/OR coverage with a cancelled-card filter; Decision coverage surfaced in the entity-detail analytics with dashboard-row parity.
- Local-first **metrics telemetry** settings/sender/service (beacon-off modes).

### Changed

- **Named governance gates 15 → 17** — **Cognitive Closeout Gate** enforced (blocks `done` while active cognitive-consolidation items remain; board setting `skip_cognitive_consolidation`) and the **Architecture Finding Done Gate** wired into `spec → done`.
- **Buffer hygiene** — periodic `CLOSE` every K commits instead of `CHECKPOINT`; progress-aware rebuild drain keeps cognitive-pending badges correct on large boards.
- Canonical API errors and data-model/doc-drift guards; `kg_health` doc corrected (the model has 38 fields, not 12); `EVENT_TYPES` 21 → 24 (`structured_entity.*`).

### Fixed

- **Graph projection omitted almost every edge** — it required both endpoints on the same page, so cross-page edges vanished; they are now retained.
- **`CHECKPOINT` requires an exclusive window** — fixed a `SIGSEGV` (the 6th observed crash) under concurrent reads.
- **Decay tick never ran in production** — a foreign key on the global event plus an `IntervalTrigger` with no catch-up meant the 24h tick never fired on a restarting process.
- **Native-memory exhaustion** across many boards — LRU cap on the `Database` cache.
- **AFG was inoperative in practice** — lazy evaluation without backfill (83% of board 0.2.3 designs had never had a run), raw diagrams skipped by the critique, and the missing `spec → done` enforcement; fixed with `backfill_architecture_finding_runs` (idempotent sweep) + always-on payload re-hydration + the dedicated spec gate.
- **Q&A inheritance** preserved the answer but not `answered_at`, producing false "Open Q&A" badges on derived refinements/specs.
- **SSE pool leak** root-caused — it hung the server and corrupted data.
- **Structured-spec linking crash** — `add/update business_rule` & `api_contract` stored the FR dict instead of a canonical FR index; now routed through the shared `_parse_linked_requirements` resolver (14 stale contract-tests realigned).
- **Spec evaluation REST/MCP parity** + complexity doc-drift.
- Telemetry beacon usage payload is now WAF-safe; FastAPI 422 deprecation fixed; LadybugDB naming corrected in `commit_coordinator`.

## [0.2.2] - 2026-05-18

Patch release rolling up the post-0.2.1 fixes (SDLC E2E gate polish + the related agent-instructions refactor and reference docs). See the entries below for the full picture.

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
