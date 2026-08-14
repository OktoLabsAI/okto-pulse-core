---
version: "1.1"
---

Knowledge Base placement, authority, and safe promotion are governed by
`okto-pulse://reference/knowledge-governance`. KB content cannot substitute for
FR/TR/BR/AC, decisions, contracts, architecture, or test scenarios.
Semantic guideline assessment follows
`okto-pulse://reference/policy-compliance`.

# Specs Workflow — Saturation, Gate, Evaluation & Coverage Progress

## 2.3 Specs — CRITICAL: Analysis Before Populating

> **MANDATORY — Query the KG before moving the spec out of `draft`.** Run the Stage 3 query set: `okto_pulse_kg_get_related_context(artifact_id="spec:<uuid>")` (the `spec:` discriminator is required; a raw UUID is rejected), board-wide `okto_pulse_kg_find_contradictions()`, per-major-FR/BR `okto_pulse_kg_find_similar_decisions`, and `okto_pulse_kg_explain_constraint` for every constraint cited. Its `constraint_id` is the canonical graph node id, not a TR/worker-candidate id; resolve it by `source_artifact_ref` with the parameterized `okto_pulse_kg_query_cypher(..., include_working=true)` recipe in `okto-pulse://workflows/kg`. A spec that proceeds to `review` without this sweep will fail validation audit and is a protocol violation.

**A spec is NOT a copy of the ideation.** When populating a spec's structured fields, you MUST:

1. **Read the ideation/refinement context**: `okto_pulse_get_spec` returns the compiled context. Read it carefully.
2. **Analyze the codebase**: Before writing requirements, explore the actual codebase to understand:
   - What already exists (don't re-specify existing functionality)
   - Current architecture and patterns (requirements must be compatible)
   - Technical constraints (language, frameworks, dependencies)
   - File structure and naming conventions
3. **Check knowledge bases**: Use `okto_pulse_list_knowledge(entity_type="spec")` and `okto_pulse_get_spec_knowledge` to read attached reference documents
4. **Review Q&A history**: Read all Q&A on the spec AND on the parent ideation/refinement — decisions made during Q&A are binding context
5. **Then write requirements**:
   - **Functional requirements**: Specific, testable behaviors. Reference real components, endpoints, or modules from the codebase when applicable.
   - **Technical requirements**: Constraints derived from actual codebase analysis — not generic "best practices" but specific to this project's stack, patterns, and architecture.
   - **Acceptance criteria**: Verifiable conditions that reference real test scenarios, endpoints, or user flows.

## 2.3a Detail Saturation — DO NOT Push Forward With Gaps

**This is a hard behavioral rule, not a suggestion.** Coverage gates (existing tests/rules/TRs/contracts counts) tell you that content *exists*, not that it is *good enough*. Your job as spec author is to iterate on detail until an evaluator can assess **confidence**, **clarity**, **assertiveness**, **decidability**, and **ambiguity** with concrete evidence — not to race to the next stage.

**Before you call any tool that promotes a spec forward**, you MUST assess the spec on five dimensions:

| Dimension | Self-assessment question | Raise the bar when... |
|-----------|-------------------------|-----------------------|
| **Confidence** | How reliable is the evaluator's inspection of the whole current Spec edition? | Important sections were not inspected, current identity was not confirmed, or conclusions are not traceable to evidence. Confidence measures the evaluation, not Spec quality. |
| **Clarity** | Does the Spec state the problem, solution and requirements clearly? | Scope, actors, behavior or terminology require contextual inference. |
| **Assertiveness** | Does every normative statement provide an observable pass/fail oracle, with units, bounds or tolerances when applicable? | You find words like "should", "appropriate", "reasonable", "if needed", "quickly", or "etc." without objective criteria. |
| **Decidability** | Does every material requirement direct an implementation/operational choice, or deliberately delegate it with explicit constraints and decision criteria? | A requirement names a quality such as "high availability" without topology, bounds, SLOs, defaults, ownership, or other decision criteria. |
| **Ambiguity** (lower is better) | Are two or more materially different interpretations plausible? | A requirement can be read in ways that change behavior, security, data, testing, or operations, or a domain term is undefined. |

The calibrated bands, evidence caps, dimension-boundary rules, examples, and
pinpoint anchors are canonical in `okto-pulse://reference/spec_gates` under
**Canonical Spec Validation scoring rubric**. Read that section before scoring;
the summary above is not sufficient to choose a number.

**The required loop — iterate until saturation:**

1. **Draft** — populate ACs, FRs, BRs, TRs, contracts, test scenarios.
2. **Read the full current edition** with
   `okto_pulse_get_spec_context(profile="full")`, not only its description and
   requirements. Cross-check Q&A, rules, contracts, scenarios, decisions,
   dependencies, and every applicable artifact. Confirm the current `edition`
   and technical `version` before evaluating.
3. **Inspect the effective thresholds** from
   `okto_pulse_get_board(board_id).settings`; do not assume defaults.
4. **Select a rubric band from evidence first**, then choose a score inside the
   band. Score confidence / clarity / assertiveness / decidability / ambiguity,
   justify every score, and pinpoint concrete problems. Do not start at the
   configured threshold or make scores compensate for one another.
5. **If any dimension is below your bar, KEEP DETAILING.** Specific actions:
   - Add more test scenarios (edge cases, error flows, boundary conditions)
   - Rewrite vague ACs into measurable, verifiable statements (numbers, specific endpoints, concrete inputs/outputs)
   - Add BRs to capture invariants you've been assuming implicitly
   - Add TRs for architectural constraints you derived from codebase analysis
   - Add API contracts with concrete request/response shapes
6. **Ask, don't assume.** When you hit a genuine ambiguity, **use `okto_pulse_ask_spec_question` to ask the user**.
7. **Re-read and re-score.** Repeat until all five dimensions clear the
   configured bar and no blocker remains.
8. **On a retry after failed validation**, first read the Current result and
   its `threshold_violations`, all five justifications, and pinpoints. Modify
   the Spec rather than only changing scores; then refresh edition, version,
   head revision, prerequisite results, and full context before resubmitting.
9. **Only then promote.**

### Spec Quality — Canonical Agent Flow

Use this section for **status-to-action routing**. Result lifecycle rules live in
`okto-pulse://reference/quality-assessments`, signatures in
`okto-pulse://reference/tool-docs/quality`, and validation rules in
`okto-pulse://reference/spec_gates`.

#### Surface responsibilities

- **Requirement Lint is external agent evidence.** Pulse Core and Community do
  not inspect a local repository or run lint cognition. At `approved`, the
  agent records it with `okto_pulse_record_requirement_lint`.
- **Validation is actionable at `approved`.** It owns the configured checklist
  and `okto_pulse_submit_spec_validation`.
- Never call `okto_pulse_record_ambiguity_assessment` for a Spec. Requirement
  Lint requires an accepted result for the Current edition; finding count and
  severity are advisory and never independently authorize or block the move.

#### Agent flow by Spec status

| Status | Required behavior |
|---|---|
| `draft` | Author with semantic Spec tools. Previous validation results are history, not live gates. |
| `review` | Review the candidate without issuing a new Requirement Lint result. |
| `approved` | Run the external analysis, call `okto_pulse_record_requirement_lint`, then follow **The Spec Validation Gate** below. |
| `validated`, `in_progress`, `done` | Read Quality only for a decision or audit. Only the Current result for this lifecycle edition may support a decision; Previous results are history. Reopening in `draft` starts a new edition and reruns authoring and validation. |
| `cancelled` or archived | Audit only. After restore or reopen, follow the row for the resulting status and require a Current result for the new edition. |

#### Token-efficient read sequence

1. Read the mandatory full Spec context at `approved`.
2. Record Requirement Lint with exact version, edition, and head fences.
3. Call `okto_pulse_get_current_quality_assessment` for
   `subject_type="spec"` and `assessment_kind="requirement_lint"`.
4. If a Current result exists, inspect its lifecycle state; call
   `okto_pulse_list_quality_findings` only to diagnose an issue.
5. Read earlier results only for audit. For readiness, use
   live Validation and the current Spec context.

### The Spec Validation Gate

When the board has `require_spec_validation=true`, advancing a spec from `approved` to `validated` is gated by `okto_pulse_submit_spec_validation`.

**The order that actually works:**

1. Populate the spec in `draft` → move through `review` → `approved`.
2. **With the spec in `approved`, create the test cards** (`okto_pulse_create_card(card_type="test", test_scenario_ids=...)` — the server rejects test card creation before `approved`) and **link each scenario** to a test card via `okto_pulse_link_task(target_type="scenario", ...)` until `scenario_task_linkage_pct = 100`. The validation gate fails on any scenario without a linked `card_type="test"` card.
3. Iterate until ALL deterministic gates are green — the complete gate enumeration is the one in `okto-pulse://reference/spec_gates` (single source; it includes more gates than just AC/FR coverage).
4. Read the configured binding with
   `okto_pulse_get_checklist_binding(board_id)`. When mode is `advisory` or
   `blocking`, combine its identity with the current Spec version from the
   required full Spec context, then call `okto_pulse_start_checklist_execution`.
   Submit all ten ordered `/specify/v1` results with concrete anchors via
   `okto_pulse_submit_checklist_execution`, and read the submitted result with
   `okto_pulse_get_checklist_receipt` (the compatibility API name). Re-run the full Spec context or
   `okto_pulse_get_allowed_transitions` before validation; that canonical
   readiness check detects a missing or failing Current result.
5. Only then call `okto_pulse_submit_spec_validation` with the current fences,
   the five externally evaluated scores (confidence, clarity, assertiveness,
   decidability and ambiguity), a justification for each, optional
   metric-tagged pinpoints, and an approve/reject recommendation. Pulse records
   this evaluation; it does not generate the scores. For each accepted
   pinpoint Pulse resolves the authorized anchor and seals its human label,
   text/excerpt, source version and digest into validation history. Clients
   submit only the anchor selector and explanation; they cannot supply the
   sealed snapshot. Records created before this contract explicitly report the
   snapshot as `legacy_unavailable` and are never re-resolved from current
   mutable Spec content.

Checklist mode/template governance is human-owned in Board Config. Agents may
read the binding and validation results and execute the configured immutable
template, but must not attempt to mutate its binding or author checklist
items. The result belongs to the Spec lifecycle edition. Returning the Spec to
`draft` increments that edition, moves the earlier result to Previous, and
requires a new execution when the candidate reaches validation again. The
binding digest/template pin remains technical audit evidence; it does not
create a second human-facing lifecycle state.

**Thresholds** (defaults 70/80/80/80/30): `confidence`, `clarity`,
`assertiveness`, and `decidability` are higher-is-better minimums;
`ambiguity` is a LOWER-is-better maximum. All scores are 0-100 integers, not
1-5 — a value like `5` is read literally as 5/100 and will usually fail the
gate.

**Single source for gate mechanics** — canonical flow, atomic `outcome` computation, content lock, and the reopen path (`okto_pulse_move_spec` back to `draft`, starting a new edition and clearing `current_validation_id`): `okto-pulse://reference/spec_gates`.

## 2.3b Spec Evaluation — Quality Gate for Execution

After a spec reaches `validated`, it must undergo **qualitative evaluation** via `okto_pulse_submit_spec_evaluation` before moving to `in_progress`: 4 dimensions + `overall_score`, each 0-100 with mandatory justification, and a `recommendation` of `approve` | `request_changes` | `reject`.

**Single source for the evaluation gate** — signature, dimension definitions, the full recommendation semantics (including `request_changes` and the `skip_qualitative_validation` escape), and enforcement of validated → in_progress: `okto-pulse://reference/spec_gates`.

## 2.3c Coverage Progress — Zero-Friction Gate Tracking

Every tool that feeds into a coverage gate **automatically returns a `coverage` object** in its response. This eliminates the need for separate "check coverage" calls.

**Key fields to watch per operation:**

| Tool | Primary metric to track | Done when |
|------|------------------------|-----------|
| `okto_pulse_add_test_scenario` | `ac_coverage_pct` + `ac_uncovered_indices` | `ac_coverage_pct = 100` or `skip_test_coverage = true` |
| `okto_pulse_add_business_rule` | `fr_coverage_pct` + `fr_uncovered_indices` | `fr_coverage_pct = 100` or `skip_rules_coverage = true` |
| `okto_pulse_link_task(target_type="scenario", ...)` | `scenario_task_linkage_pct` | `scenario_task_linkage_pct = 100` |
| `okto_pulse_link_task(target_type="rule", ...)` | `br_task_linkage_pct` | `br_task_linkage_pct = 100` |

**The `skip_*` flags tell you if full coverage is mandatory:**
- `skip_test_coverage = false` → AC coverage MUST reach 100% before spec can advance
- `skip_test_coverage = true` → AC coverage is tracked but not enforced

**FR coverage source:** `fr_coverage_pct` is computed from `business_rules[].linked_requirements`. Direct links on `functional_requirements[].linked_task_ids` are useful task traceability, but they do not satisfy the FR→BR coverage gate.

## 2.3d Agent-mediated Code Traceability

When Code Traceability is enabled, the Spec remains normative and Code
Evidence remains factual, immutable, and historical. Before moving a Spec to
`review`:

1. Read `inherited_code_evidence` from the full Spec context.
2. Formalize the applicable FR, TR, BR, Decision, and Contract entities.
3. Link every inherited Evidence item to each entity it supports.
4. Record an explicit disposition for every inherited item that is not
   applicable to this Spec version.
5. Confirm `evidence_disposition_coverage_pct = 100`.

Each Evidence link/disposition mutates the technical Spec version. Use the
`spec_version` returned by one successful mutation as the
`expected_spec_version` of the next, or refetch the full Spec context. Link to
stable entity IDs (FR/TR/AC/etc.), never array positions. The canonical
resource contains a complete sequential TR/FR/AC example.

Do not move to `review` while inherited Evidence is pending. A link or
disposition never rewrites the Evidence snapshot, its accepted agent receipt,
or its observed source state. Pulse Core and Pulse Community do not inspect a
repository to establish these facts. An authenticated external agent first
performs the capability/access preflight and deterministic investigation in its
own environment, then submits the bounded receipt and Evidence.

Canonical protocol: `okto-pulse://reference/code-traceability`.
