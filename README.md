# okto-pulse-core

Core engine for [Okto Pulse](https://github.com/OktoLabsAI/okto-pulse) — shared models, services, API routes, and MCP server.

> **Ship with AI. Stay in control.**

> **You probably want to install [`okto-pulse`](https://pypi.org/project/okto-pulse/) instead.**
> This package is the internal engine. The `okto-pulse` package provides the CLI, frontend, and everything you need to get started.

## What's inside

- **52 SQLAlchemy models** — Boards, Cards, Specs, Ideations, Refinements, Sprints, Agents, Knowledge, Mockups, Validations, KG queues, rebuild/cognitive-candidate records, discovery entities and audit/outbox records. (Skills entity dropped in 0.2.0.)
- **28 service classes** — Full business logic with governance rules, board agent governance, resource propagation + lineage, bug-regression workflow, archive/restore, traceability and board-level resource readiness. (Skills service dropped in 0.2.0.)
- **33 API route modules** — FastAPI REST endpoints
- **17 governance gates** — Resource readiness, resource-to-task coverage, spec coverage, validation, evaluation, task completion, cognitive closeout, architecture-findings, evidence, bug traceability and sprint health controls.
- **215 MCP tools** — Complete Model Context Protocol server for AI agent integration, including:
  - Pipeline CRUD (Ideation, Refinement, Spec, Sprint, Card)
  - Q&A and choice questions across every entity
  - Mockups (HTML+Tailwind, sanitised) and Knowledge Bases at spec/refinement/card scope
  - Decisions with supersedence and coverage gates
  - Per-card Knowledge attachment lifecycle (`add_card_knowledge` and friends)
  - 24 Knowledge Graph tools (consolidation, query primary/power, health, dead-letter, schema-migrate, decay tick controllability, rebuild preflight/confirm/run)
  - Community runtime exposure: 215 core MCP tools, 0 community-only MCP tools
- **App factory** — `create_app()` with dependency injection for auth and storage providers
- **Embedded Knowledge Graph** — per-board LadybugDB instance + global discovery meta-graph, deterministic + cognitive workers, 11 node types and 10 relationship types

## Governance Gate Surface

Okto Pulse currently documents and enforces **17 named governance gates**:

| Gate family | Gates |
| --- | --- |
| Resource readiness | Resource readiness; resource-to-task coverage |
| Spec coverage | Scenario/test coverage; functional requirement/business rule coverage; technical requirement/task coverage; API contract/task coverage; active decision/task coverage |
| Validation and evaluation | Spec validation; spec qualitative evaluation; task validation |
| Execution quality | Task start/spec readiness; task conclusion; cognitive closeout; architecture-findings done; test evidence; bug test-first/traceability |
| Sprint health | Sprint closure/evaluation |

The two execution-quality additions introduced in 0.2.3 — **cognitive closeout** (a `done` transition is blocked while active cognitive-consolidation items remain) and the **architecture-findings done gate** (active architecture warnings block `spec`/card `done`) — remain enforced in the current release.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Single Python process  (single Kùzu lock holder)        │
│                                                          │
│   ┌─────────────────┐         ┌──────────────────┐       │
│   │  uvicorn :api   │         │  uvicorn :mcp    │       │
│   │   FastAPI app   │         │  MCP ASGI app    │       │
│   │   /api/v1/*     │         │  /mcp            │       │
│   │   /static, SPA  │         │  streamable-http │       │
│   └────────┬────────┘         └────────┬─────────┘       │
│            │                           │                  │
│            └─────────┬─────────────────┘                  │
│                      │                                    │
│           shared module-level state:                      │
│           - Mongo session factory (init via lifespan)     │
│           - _global_db (Kùzu cache)                       │
│           - _active_api_key (ContextVar)                  │
└─────────────────────────────────────────────────────────┘
```

`build_mcp_asgi_app()` and `mount_mcp(app)` are the two helpers exposed from `okto_pulse.core.mcp` — pick `build_mcp_asgi_app()` to drive a separate uvicorn `Server` (the community edition does this for the `--mcp-port` listener) or `mount_mcp(app)` to mount the MCP sub-app under an arbitrary path on an existing FastAPI app.

## Docker

This repo has **no Dockerfile**. The deployable artifact is a single image built from the sibling [`okto-pulse`](https://github.com/OktoLabsAI/okto-pulse) repo:

- `okto-pulse/Dockerfile` target `local-runtime` builds wheels from this repo and `okto-pulse/` as siblings (used by `okto-pulse/docker-compose.yml` and the release pipeline's smoke build).
- `okto-pulse/Dockerfile` target `pypi-runtime` installs `okto-pulse==<version>` from PyPI, which transitively pulls this package off PyPI per the floor in `okto-pulse/pyproject.toml` (used by `okto-pulse/docker-compose.prod.yml`).

To run the published image:

```bash
docker run -d --name okto-pulse \
  -e HOST=0.0.0.0 -e MCP_HOST=0.0.0.0 \
  -p 8100:8100 -p 8101:8101 \
  -v okto-pulse-data:/data \
  ghcr.io/oktolabsai/okto-pulse:latest
```

See [`okto-pulse/README.md`](https://github.com/OktoLabsAI/okto-pulse#run-with-docker) for the full Docker quickstart, and [`okto-pulse/CLAUDE.md`](https://github.com/OktoLabsAI/okto-pulse/blob/main/CLAUDE.md) for the multi-stage build architecture.

## Release Notes

### 0.2.6 — current

Changeset:

- **Architecture propagation now fails closed everywhere it matters** — active Architecture Design critic findings, missing/unavailable verdicts and in-memory revalidation blockers prevent copy/propagation into downstream artifacts. Acknowledgement remains audit-only and never authorizes propagation.
- **Completion gates consume the same canonical architecture decision** — `ResourceGateService.validate_entity_completion()` and the spec architecture-findings done path now treat `architecture_propagation_blocking=true` as a real blocker, including cases with no persisted active finding rows.
- **REST/MCP error surfaces stay structured** — `ArchitecturePropagationBlocked` now maps to the canonical payload with stable `code`, design/source identifiers, finding keys, verdict status and remediation instead of being flattened into a generic validation string.
- **Card resource snapshots are consistently read-only** — effective-resource read models mark direct card snapshots read-only, matching the write-side 409 behavior and preventing UI/API consumers from advertising edits that cannot succeed.
- **Effective architecture copy avoids duplicate lineage snapshots** — inherited architecture refs now carry source identity (`source_design_id`/`source_ref`) and fallback copy plans de-duplicate by canonical source identity before copying to cards.
- **Regression coverage** — focused tests cover propagation-block completion, structured REST error mapping, read-only card snapshots and deduped effective architecture fallback refs.

### 0.2.5

Scope is taken from the finalized specs on the **Okto Pulse 0.2.5** board and the `feature/0.2.5` branch diff over `feature/0.2.3`: `311 files changed, +59,295 / -1,104`. This release turns the 0.2.3 KG durability base into a governed operating layer for canonical graph maturity, cognitive closeout, board defaults, Design System consumption, Path B amendment remediation and AWS metrics publication health.

- **KG canonical/working partitioning and recovery** — maturity-aware source classification, layer-aware rebuild/preflight diagnostics, canonical-only query defaults with explicit working opt-in, canonical source manifests, global-discovery parity, natural-query layer audits, self-loop/connectivity safeguards, stale-canonical demotion/reconcile paths and `recovery_needed` rebuild safety.
- **Canonical debt and operational visibility** — `CanonicalDebt` tracking for failed/deferred promotion, replay contracts, canonical-partition integrity checks, stale-canonical parity and digest-layer mismatch endpoints, active queue/DLQ separation, lineage/count diagnostics and KG health reporting that distinguishes canonical debt from cognitive closeout work.
- **Cognitive readiness and closeout governance** — shared cognitive closeout store/readiness service, deterministic-only ownership for Criterion/Constraint closeout, human-only skip/no-action ledgers, skip/clear controls, technical-blocker metrics, read-only MCP/API exposure and the Cognitive Action Center read model.
- **Board defaults, guidelines and Design System governance** — versioned `DefaultBoardConfiguration` templates, default global-guideline materialization for new boards, global/inline Design System catalog, board association/default selection, effective Design System surfacing in board context/preflight and `MockupDesignSystemGate` advisory/blocking enforcement with audit.
- **Metrics publication health for AWS ingestion** — explicit telemetry event contracts, real emitters for CLI/MCP/KG/lifecycle/pipeline activity, semantic event eras, delta batching, watermark/retention, HTTP/token policy, local failure-state, redacted publish-health DTOs over API/MCP and a triage runbook that reports AWS/reporting gaps as actionable states instead of healthy.
- **Path B amendment remediation for bug regression** — `AmendmentHotfixRevision` model, service, REST/MCP lifecycle, eligibility policy, artifact association, validator coverage confirmation and KG rebuild handling so post-bug semantic gaps are closed through formal amendment lineage rather than untracked spec edits.
- **Gate and MCP contract hardening from E2E** — docs/schema now surface Design System evidence fields on mockups, direct `okto_pulse_link_task(target_type="fr")` traceability, strict scenario-type behavior, re-executable test evidence classes, 0-100 validation/evaluation scales, test-card scenario caps, direct test-card `not_started -> in_progress` support and the FR coverage dependency on `business_rules[].linked_requirements`.
- **Regression coverage and docs** — new pytest coverage spans telemetry publish-health, default board config, Design System gates, Path B, amendment revisions, cognitive readiness/action-center flows, KG layer/canonical-debt behavior, MCP contract drift and E2E-discovered edge cases. The MCP workflow/reference docs were refreshed in lockstep.

### 0.2.3

The largest release since 0.2.0. Scope is taken from the **53 finalized specs on the Okto Pulse 0.2.3 board** (the platform dogfooded its own SDLC), landing **64 new core modules** across eight subsystems. `335 files changed, +103,183 / −4,532` over `0.2.2`; every subsystem ships with its pytest suite. The package grew to **52 models / 28 services / 33 API modules / 215 MCP tools / 17 named gates**.

- **KG corruption prevention & durability (headline — KG-01, KGDL.01)** — new write-path primitives (`safe_write_lifecycle`, `write_barrier`, `single_writer_lock`, `backpressure`, `quarantine`, `contingency`). A non-destructive durability lifecycle (`STEP_CHECKPOINT` no longer closes the cached `Database`; a per-board `_BoardCloseGuard` drains live readers before any close) eliminates the use-after-close of the shared `Database` — the probable second corruption vector for `graph.lbug`. Spec `3d89c192`.
- **KG recovery, reset & deterministic rebuild (KG-02 + R2a)** — rebuild a board's graph from canonical SQL sources, deterministically and audited: `rebuild_preflight`/`_confirm`/`_run` via REST + agent-actionable MCP twins (confirmation-token gated, quarantine-aware), plus auto-recovery of interrupted checkpoints.
- **KG zero-orphan integrity (KG-ZO-01/02)** — a node-connectivity pre-commit guard that refuses to commit orphans, plus orphan backfill, health reporting and rebuild visibility.
- **KG cognitive consolidation & source governance (KG-03/03A)** — cognitive item control + candidate-decision promotion (`candidate_decision_store`, `cognitive_badge_resolver`), per-concept `source_ref`, and dedup granularity with SUPERSEDE wiring + counted/audited merge.
- **KG health honesty & degraded-mode resilience (F3/F4/F16/F17, R2c)** — signal clarity (scheduler/decay debt ≠ corruption; footprint = file-size proxy), a resilient/observable decay tick, a uniform `graph_unavailable` envelope, a health-aware closeout gate + tick admission, real memory-pressure instrumentation and opt-in DLQ auto-drain.
- **Governance, lineage & gates (BG-01, RG-01, AFG)** — `critical_context_guard` (critical mutations resolve + fingerprint full entity context first), the `resource_lineage` provenance resolver with N/A inheritance, and the Architecture Finding Done Gate wired into `spec → done`. Two gates moved from defined to enforced (15 → 17): **Cognitive Closeout** and **Architecture Findings**.
- **MCP token-optimization & projection (R1–R5)** — `payload_budget`, `payload_compaction`, `projection_envelope`, context/copy projection, `kg_query_safety`, `tool_family_registry`; schema honesty (`anyOf array|string`), positional → canonical id/ref migration (`linked_requirements → FR`, `linked_criteria → AC`), and the pre-flight checklist as a real `okto-pulse://workflows/preflight` resource. Surface grew to **215 tools**.
- **Bug-regression workflow** — scenario reuse + test-gate remediation, operator-facing bug guidance/error remediation, and a post-closure hotfix lane.
- **Structured spec entities + API-contract hardening** — structured editing + `test_scenario` CRUD (closes the NC-9 bypass), `contract_type` discriminator + HTTP-method enum, granular per-requirement N/A for IR/OR/contract, and structured choice fields.
- **Analytics & telemetry** — IR/OR coverage calculator with a cancelled-card filter, Decision-coverage surfacing, and beacon-off metrics modes.

See `CHANGELOG.md` for the per-subsystem diff-level rationale.

### 0.2.2

Patch release rolling up the post-0.2.1 fixes. Same surface as `0.2.1` plus:

- **SDLC E2E gate polish (4 issues from the 2026-05-17 ceremonial run)** —
  - `submit_spec_validation` now runs the AC → test-scenario coverage check as its first pre-requisite, so a spec with uncovered ACs fails BEFORE the validation locks the content (previously the move → done gate raised the same error but only after the spec was already locked).
  - The "FR has no linked business rule" error now uses an `[i]` index marker rather than the duplicated `FR{i}:` prefix that collided with the author's own `FRN:` label and produced strings like `"FR1: FR2: ..."`.
  - `okto_pulse_link_task target_type='decision'` now spreads the saturation envelope into its success JSON, in parity with the other six target types. A parametrised dispatcher test pins the contract so a future eighth helper can't regress it silently.
  - `okto_pulse_evaluate_ideation` docstring now states the `status='evaluating'` pre-requisite and the full `draft → review → approved → evaluating → done` flow up front. The tool deliberately does not auto-promote — each transition is an explicit gate decision.
- **Agent instructions split + reference catalogue** — `agent_instructions.md` trimmed by extracting the static reference material into three new MCP resources (`okto-pulse://reference/list_tools`, `tools_catalog`, `transitions`). Workflow docs (`refinements`, `specs`, `stories`) refreshed in lockstep.
- **MCP server module slim-down** — `core/mcp/server.py` lost ~970 lines of helpers that now live in supporting modules. Public tool surface is unchanged; this is purely an organisational refactor on top of `0.2.1`.

Anti-regression tests added for each of the four E2E fixes (`tests/test_spec_validation_gate.py::TestAcScenarioPrecheck`, `TestFrCoverageMessageFormat`, `tests/test_link_task_dispatcher.py::test_link_helper_returns_saturation_envelope`). The `submit_spec_validation` baseline hash in `tests/.cache/validation_gates_baseline.txt` was bumped to reflect the intentional addition of the new AC → scenario pre-check.

See `CHANGELOG.md` for the diff-level rationale and the per-fix bug card references.

### 0.2.1

#### Branch changelog (`feature/0.2.1`)

This branch turns 0.2.1 into the IR/OR, telemetry, resource-propagation and MCP-surface optimization release.

- Added local-first telemetry and metrics infrastructure: product event schema, settings model, local event store, sender, privacy-aware service layer, metrics REST API and tests for local-only, disabled and anonymous-beacon modes.
- Added first-class Integration Requirements (IR) and Observability Requirements (OR) across database models, Pydantic schemas, REST responses, MCP handlers, permission registry and presets.
- Extended spec context, sprint context and coverage summaries so agents and UI callers can see IR/OR items alongside technical requirements, business rules, API contracts, decisions and test scenarios.
- Added service-layer spec resource propagation. Knowledge Base entries, architecture designs and mockups can be copied from specs to cards automatically when board settings enable auto-derive resources.
- Hardened propagation triggers on card creation/linking, spec resource edits and architecture updates so downstream task cards stay self-contained without relying on "see the spec" references.
- Added granular IR/OR permissions (`read`, `create`, `link_task`) and enforced them consistently across API, MCP and permission presets.
- Added four consolidated MCP list handlers: `okto_pulse_list_by_board`, `okto_pulse_list_qa`, `okto_pulse_list_knowledge` and `okto_pulse_list_snapshots`.
- Added server-side MCP filter validation and JSON-string filter decoding so tool transports can pass either dict objects or JSON-encoded filter strings.
- Split the large root MCP agent instructions into a compact pre-flight plus 12 lazy MCP resources under `okto-pulse://workflows/...` and `okto-pulse://reference/...`.
- Added a runtime MCP schema-generation pilot for card CRUD tools, backed by Pydantic v2 model schemas and a snapshot fixture.
- Added minimal-envelope response modes and token-optimization refinements for agent workflows, including tighter list/context payloads and refreshed workflow documentation.
- Added cursor-based keyset pagination to `okto_pulse_get_activity_log`, including opaque `next_cursor`, invalid-cursor structured errors and SQLite timestamp normalization for microsecond-safe pagination.
- Improved activity-log summaries and card-move logging by covering more action shapes and de-duplicating noisy `card_moved` entries.
- Hardened architecture services with semantic normalization and additional validation coverage used by the community Architecture UI and Excalidraw import flow.
- Added focused regression coverage for spec resource propagation, telemetry, IR/OR requirements, consolidated MCP handlers, MCP resources, schema generation, activity-log pagination and story/refinement regressions.

#### Post-release polish already on the branch

- Aligned authoritative handler signatures for the four consolidated MCP list handlers.
- Unified list handler defaults to `limit=100` for consistency across old and consolidated paths.
- Clarified that the implementation keeps `board_id` for ACL/auth and uses `entity_type/entity_id` naming for knowledge listings.

#### SDLC E2E gate polish (4 issues from end-to-end run 2026-05-17)

A full ceremonial E2E run (Story → Ideation → Refinement → Spec → Sprint → Cards → Sprint closeout) on the `E2E` board surfaced four small but recurring issues across the spec validation gates, error messages and tool response shapes. All four were fixed in the same `Unreleased` cycle and validated in-vivo against the live MCP server. See `CHANGELOG.md` for the full diff and rationale; the short summary:

- `submit_spec_validation` now runs the AC → test-scenario coverage check as the first pre-requisite, so a spec with uncovered ACs fails BEFORE the validation locks it. The error message also reminds the caller that the spec is locked after a successful validation.
- The "FR has no linked business rule" error message now uses an `[i]` index marker instead of `FR{i}:`, removing the confusing `FR1: FR2: ...` duplication that occurred whenever the FR text already started with its own label.
- `okto_pulse_link_task` with `target_type='decision'` now returns the same `saturation` envelope as the other six target types. Previously only the decision branch returned the bare `{success, decision_id, card_id, linked_tasks}` shape, breaking agents that drive "continue linking vs submit validation" off the saturation signal.
- The `okto_pulse_evaluate_ideation` MCP docstring now states the `status='evaluating'` pre-requisite and the full `draft → review → approved → evaluating → done` flow up front, so agents stop discovering the requirement by trial and error.

Anti-regression tests were added for each fix (`test_spec_validation_gate.py::TestAcScenarioPrecheck`, `TestFrCoverageMessageFormat`, and `test_link_task_dispatcher.py::test_link_helper_returns_saturation_envelope`). The `submit_spec_validation` baseline hash in `tests/.cache/validation_gates_baseline.txt` was bumped to reflect the intentional addition of the new coverage call.

### 0.2.0

#### Branch changelog (`feature/0.2.0`)

This branch turns 0.2.0 into the governed SDLC + Knowledge Graph release.

- Added Stories and Topics as pre-ideation intake primitives, including REST/MCP services, permissions, lifecycle rules, story-to-ideation traceability and the rule that a Story can reference only one Ideation while an Ideation can reference many Stories.
- Added Resource Gate readiness across Architecture, Mockups and Knowledge Base, with reversible N/A justification, entity-level readiness summaries and MCP guardrails that keep deterministic resource checks out of ad-hoc agent judgement.
- Hardened agent instructions for ambiguity handling: agents are directed to ask more clarification questions, prefer multiple-choice questions with recommendations when possible, and preserve an additional comment path for user nuance.
- Added Ideation Knowledge Base support and propagation, plus lineage/reporting improvements so specs, sprints, tasks, tests and bugs remain traceable even when a flow intentionally starts at Spec without a root Ideation.
- Expanded deterministic KG ingestion for specs, cards, bugs, tests, outcomes, requirements, criteria, constraints, API contracts and decisions, including resolved Bug `originates_from` and `covered_by` edges and schema migration coverage for those relationship tables.
- Strengthened KG schema lifecycle and graph runtime resilience: per-board schema bootstrap/migration, edge metadata migration, entity dedup support, Kuzu memory/runtime settings, vector-extension loading on hot-path graph connections and richer health/dead-letter diagnostics.
- Improved KG query/display contracts: `/kg/boards/{board_id}/graph` now accepts a node `type` filter, `/nodes` total hints remain filter-aware, graph stats expose node/edge histograms and tests cover pagination, type filtering and schema edge counts.
- Fixed guideline creation/parsing paths that could reject inline guideline additions with 422 responses.
- Preserved test scenario evidence in REST response schemas, including `latest_evidence` fallback data, so UI audit surfaces can expose recorded execution proof for Test cards.
- Added and expanded focused tests for Stories, Topic permissions, Resource Gate, Ideation KB, guidelines, deterministic KG workers, graph pagination, schema migration, traceability reports, presets and MCP registration contracts.

#### Fix C: single-process, dual-port serve (Kùzu lock contention)

`okto-pulse serve` now runs API/UI **and** MCP from a **single Python process** but on **two different ports** (`--api-port` defaults to 8100, `--mcp-port` defaults to 8101). Two `uvicorn.Server` instances run concurrently inside one `asyncio.gather` — the embedded Kùzu DB is owned by exactly one OS process (no inter-process lock contention), and the two listeners share the module-level state (the registered session factory, the `_global_db` Kùzu cache, the `_active_api_key` `ContextVar`).

What you get:
- **No Kùzu file-lock thrash** — the embedded DB does not support multiple writers, so a single Python process is the only safe topology. The `kg.db_open.lock_retry path=... attempt=N/5` warnings disappear.
- **Independent ports** — keep `:8100` for the SPA fetches and `:8101` for the MCP HTTP transport, unchanged from earlier releases.
- **One lifespan** — `init_db`, KG worker startup, scheduler boot, and `register_session_factory` all run once on the API listener; the MCP sub-app picks up the registered factory automatically.

Public surface:
- `okto_pulse.core.mcp.build_mcp_asgi_app()` — returns the MCP ASGI app wrapped in the `ApiKeySessionMiddleware` (handles `?api_key=` / `X-API-Key` / `Authorization: Bearer` and binds the key to the request `ContextVar`).
- `okto_pulse.core.mcp.mount_mcp(app, mount_path="/mcp")` — mounts the same ASGI app onto an existing FastAPI app at the given path (community does this when an embedded mount is needed).
- `okto_pulse.core.mcp.register_session_factory(factory)` — call from the API lifespan so the MCP sub-app finds the DB. Idempotent.

#### Spec Skills entity removed in its entirety

The experimental "skills" feature on the spec entity is gone. Adoption was zero in real boards and knowledge entries already cover the reusable-context use case more naturally — the dedicated tab, MCP tools, REST endpoints and ORM table were paying recurring maintenance cost without return.

What goes away:
- **5 MCP tools removed** — `okto_pulse_create_spec_skill`, `okto_pulse_delete_spec_skill`, `okto_pulse_spec_skill_retrieve`, `okto_pulse_spec_skill_inspect`, `okto_pulse_spec_skill_load`.
- **4 REST endpoints removed** — `GET / POST / PATCH / DELETE /api/v1/specs/{spec_id}/skills` (and the `{skill_id}` variants).
- **5 permission flags removed** — `spec.skills.{read,load,create,delete,recall}` from the registry and from every preset.
- **Database table dropped** — `spec_skills`. Migration is idempotent (`DROP TABLE IF EXISTS`); no downgrade — the data is gone.
- **Pydantic schemas removed** — `SkillSectionSchema`, `SpecSkillCreate`, `SpecSkillUpdate`, `SpecSkillResponse`, `SpecSkillSummary`. The `skills` field is gone from `SpecResponse`.
- **`agent_instructions.md` scrubbed** — Quick Navigation, the dedicated Spec Skills section, the spec-authoring workflow step and the destructive-operations row no longer reference skills.

Reader-side defensive handling: `BaseSchema` now sets `extra="ignore"` so historical payloads still carrying a `skills` field validate silently — no warning, no log, no error. There is nothing to migrate; the field is dropped on read.

Use **knowledge entries** (`spec_knowledge`, `card_knowledge`) and **decisions** for the same use case.

#### Agent instructions overhaul

`agent_instructions.md` was reviewed end-to-end. Three behavioural sections were added in response to repeated drift patterns observed across production sessions:

- **§ 2.1a Ambiguity-killer protocol** — at ideation, the agent must scan the user's request against a table of ambiguity symptoms (vague verbs, undefined nouns, multiple plausible interpretations, implicit success criteria, implicit scope) and post Q&A items for every gap before advancing the ideation. "Just make a reasonable choice" is permission, not silence — it must be recorded explicitly.
- **§ 2.2a Investigação profunda obrigatória (refinement)** — refinement is research, not paraphrasing. The agent must exhaust all applicable sources (project files, source code, KE, Knowledge Graph, mockups, web docs, online discussions, runtime evidence, stakeholder context) and the refinement body must cite each finding with `path:line`, KE titles, KG node ids or URLs.
- **§ 2.8 Card-level artifact attachment (MANDATORY)** — every card must be self-contained. KE/mockup dependencies must be attached **directly to the card** via `copy_knowledge_to_card` / `copy_mockups_to_card` / `add_card_knowledge` / `add_screen_mockup(entity_type="card")`. Vague references to "see the spec" are a protocol violation.

Cleanup:
- Quick Navigation header *Multi-value Parameters — Two Accepted Formats* corrected to *Three Input Shapes* (the section was extended to native `list[str]` in 0.1.4).
- Obsolete `delete_task_validation` reference removed from the *Available Tools → Evaluations & Validations* table (the tool never shipped).
- `okto_pulse_create_sprint` parameter list aligned with the schema (`objective?` and `expected_outcome?` were missing).
- Duplicate "Startup Protocol" subsection deleted — Pre-Flight Checklist is the single source of truth.

#### Other improvements

- **MCP `ApiKeySessionMiddleware`** rewritten on top of `ContextVar` — required because the FastAPI process serves multiple concurrent requests and the previous module-level global would leak identities across requests. Token-based set/reset pattern protects against exception leaks.
- **`run_mcp_server`** retained for legacy callers, now defers to `build_mcp_asgi_app` for ASGI-mode embeds.

To upgrade an existing install: `pip install -U okto-pulse okto-pulse-core` and then `okto-pulse init --agents` to regenerate `.mcp.json` (the URL still points at port 8101 by default; override with `--mcp-port` if you remapped). No downstream contract changes for MCP clients — the wire protocol and tool catalog (sans 5 skills tools) are unchanged.

### 0.1.3 — previous stable (PyPI)

First hardening pass on the card lifecycle, the analytics contract, and the MCP instruction set.

- **`CardService.delete_card` cascades** through every spec-side JSON list (`test_scenarios[].linked_task_ids`, `business_rules[]`, `api_contracts[]`, `technical_requirements[]`, `decisions[]`) and through bug cards' `linked_test_task_ids`. The transactional cascade unblocks the delete→recreate flow that previously tripped `_validate_spec_linked_refs`.
- **Analytics card-type classifier** uses enum identity instead of `str(card.card_type).endswith(...)`. `total_cards_impl/test/bug`, `task_validation_gate.total_submitted`, `velocity[].test/bug`, and `bug_rate_per_spec` now report real counts.
- **`parse_multi_value` helper** consolidated the scattered `.split("|")` pattern; pipe-separated and JSON-array inputs are autodetected.
- **MCP agent instructions** rewritten (1830 → 2050 lines) with new sections for Multi-value Parameters, Destructive Operations, Versioning & Concurrent Edits, Security, Analytics-Driven Closure.

### 0.1.1 — initial PyPI release

26+1 SQLAlchemy models, 17+1 service classes, 11 API route modules, 119 MCP tools, embedded Kùzu Knowledge Graph with deterministic workers. (Spec Skills shipped here and was removed in 0.2.0.)

(Version 0.1.2 was published to TestPyPI only as a release candidate for 0.1.3.)

## License

[Elastic License 2.0](./LICENSE) — free for personal and commercial use. Cannot be offered as a hosted/managed service.
