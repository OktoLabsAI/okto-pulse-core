---
version: "1.0"
---

# Specs Workflow — Saturation, Gate, Evaluation & Coverage Progress

## 2.3 Specs — CRITICAL: Analysis Before Populating

> **MANDATORY — Query the KG before moving the spec out of `draft`.** Run the Stage 3 query set: `okto_pulse_kg_get_related_context(artifact_id=<spec_id>)`, board-wide `okto_pulse_kg_find_contradictions()`, per-major-FR/BR `okto_pulse_kg_find_similar_decisions`, and `okto_pulse_kg_explain_constraint` for every constraint cited. A spec that proceeds to `review` without this sweep will fail validation audit and is a protocol violation.

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

**This is a hard behavioral rule, not a suggestion.** Coverage gates (existing tests/rules/TRs/contracts counts) tell you that content *exists*, not that it is *good enough*. Your job as spec author is to iterate on detail until your own perception of **completeness**, **assertiveness**, and **ambiguity** is satisfactory — not to race to the next stage.

**Before you call any tool that promotes a spec forward**, you MUST self-assess the spec on three dimensions:

| Dimension | Self-assessment question | Raise the bar when... |
|-----------|-------------------------|-----------------------|
| **Completeness** | Have I covered every functional requirement with concrete ACs, BRs, TRs, test scenarios, and (where applicable) API contracts? Are there scenarios, edge cases, or error paths I haven't written down? | You can think of any plausible user flow, failure mode, or integration point that isn't yet documented in the spec. |
| **Assertiveness** | Is every statement in the spec **measurable and testable**? Would two independent engineers produce the same implementation from this text, or would they have to guess? | You find words like "should", "appropriate", "reasonable", "if needed", "etc." without objective criteria behind them. |
| **Ambiguity** (lower is better) | How many sentences in the spec admit more than one interpretation? How many terms are undefined, implicit, or rely on shared context that isn't written down? | Any requirement can be read two ways, or any domain term is used without a definition. |

**The required loop — iterate until saturation:**

1. **Draft** — populate ACs, FRs, BRs, TRs, contracts, test scenarios.
2. **Read your own spec out loud** (i.e., call `okto_pulse_get_spec` and re-read it in full). Look for weasel words, undefined terms, missing edge cases, and untested error paths.
3. **Score yourself** on completeness / assertiveness / ambiguity. Be honest.
4. **If any dimension is below your bar, KEEP DETAILING.** Specific actions:
   - Add more test scenarios (edge cases, error flows, boundary conditions)
   - Rewrite vague ACs into measurable, verifiable statements (numbers, specific endpoints, concrete inputs/outputs)
   - Add BRs to capture invariants you've been assuming implicitly
   - Add TRs for architectural constraints you derived from codebase analysis
   - Add API contracts with concrete request/response shapes
5. **Ask, don't assume.** When you hit a genuine ambiguity, **use `okto_pulse_ask_spec_question` to ask the user**.
6. **Re-read and re-score.** Repeat until all three dimensions clear your bar.
7. **Only then promote.**

### The Spec Validation Gate

When the board has `require_spec_validation=true`, advancing a spec from `approved` to `validated` is gated by `okto_pulse_submit_spec_validation`.

**The canonical flow:**

1. Populate the spec in `draft` → move through `review` → `approved`.
2. Iterate coverage until ALL deterministic gates are green (AC, FR, TR, contract).
3. When genuinely ready, call `okto_pulse_submit_spec_validation(board_id, spec_id, completeness, completeness_justification, assertiveness, assertiveness_justification, ambiguity, ambiguity_justification, general_justification, recommendation)`.
4. The gate runs coverage checks first. If any fail, you get a coverage error — fix the gap and retry.
5. If coverage passes, the gate computes `outcome` atomically:
   - `outcome=failed` if ANY threshold is violated OR `recommendation=reject`
   - `outcome=success` ONLY if all thresholds pass AND `recommendation=approve`
6. On `success`, the spec is atomically promoted to `validated` AND enters **content lock**.
7. To edit a locked spec, move it back to `draft` or `approved` via `okto_pulse_move_spec`. Both transitions clear `current_validation_id`.

**Thresholds** (default 80/80/30):
- `completeness` (0-100, higher is better): are all ACs concrete, every AC has a test scenario, BRs capture invariants, TRs are grounded in real code, contracts have request/response shapes, edge cases covered?
- `assertiveness` (0-100, higher is better): is every statement measurable and testable?
- `ambiguity` (0-100, LOWER is better, max threshold): how many sentences admit multiple interpretations?

Use a 0-100 scale, not a 1-5 scale. A value like `5` is treated literally as 5/100 and will usually fail the gate.

## 2.3b Spec Evaluation — Quality Gate for Execution

After a spec reaches `validated` status, it must undergo **qualitative evaluation** before moving to `in_progress`.

**Tool:** `okto_pulse_submit_spec_evaluation(board_id, spec_id, breakdown_completeness, breakdown_justification, granularity, granularity_justification, dependency_coherence, dependency_justification, test_coverage_quality, test_coverage_justification, overall_score, overall_justification, recommendation)`

**Evaluation dimensions (each scored 0-100 with mandatory justification):**

| Dimension | What to assess | Score guide |
|-----------|---------------|-------------|
| `breakdown_completeness` | Do derived cards fully cover the spec's scope? | 90+: every requirement traced to ≥1 card. |
| `granularity` | Are cards properly sized for independent execution? | 90+: each card is 1-3 days of focused work. |
| `dependency_coherence` | Do card dependencies reflect the real execution order? | 90+: clean DAG, parallelizable where possible. |
| `test_coverage_quality` | Do test scenarios cover happy paths AND edge cases? | 90+: every AC has meaningful tests with edge cases. |
| `overall_score` | Holistic assessment. | 90+: ready to go. 70-89: minor issues. |

**Gate enforcement (validated → in_progress):**
- At least 1 evaluation with `recommendation="approve"`
- Zero evaluations with `recommendation="reject"`
- Average `overall_score` of approvals ≥ `validation_threshold` (default 70)

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
