---
version: "1.0"
---

# Ideations Workflow — Scope Evaluation & Ambiguity-Killer Protocol

## 2.1 Ideations

Ideations are the starting point for solution definition. Stories may exist before them as optional intake context. When asked to evaluate or create an ideation:

> **MANDATORY — Query the KG before evaluating.** Before calling `okto_pulse_evaluate_ideation`, you MUST run the Stage 1 query set from the "Query Timing" section of the Knowledge Graph chapter: `okto_pulse_kg_find_similar_decisions`, `okto_pulse_kg_query_global`, `okto_pulse_kg_get_learning_from_bugs`. Cite any hit explicitly in the ideation (decision_id + one-line summary). Failing to do this is a protocol violation — duplicate ideations and cross-board conflicts are traced back to this skip.
>
> **Degraded-KG exception (`kg_health`-first):** if `okto_pulse_kg_health` reports a degraded `graph_state` (`recovery_needed` or `quarantined`), the mandatory triad above is EXPECTED to be unavailable — follow the **Degraded-KG Fallback Rule** in the "Query Timing" section of the Knowledge Graph chapter: record the degraded `graph_state` in the ideation and proceed. The triad skip on a degraded graph is not treated as a violation.

1. **Evaluate scope**: Use `okto_pulse_evaluate_ideation` with scores 1-5 for each dimension:

   **Domains** — How many systems, services, or bounded contexts are impacted?
   | Score | Meaning | Example |
   |-------|---------|---------|
   | 1 | Single component, isolated change | Add a field to one API endpoint |
   | 2 | One service with multiple modules | New feature touching backend + database |
   | 3 | Two to three services | Backend + frontend + MCP changes |
   | 4 | Multiple services with integration points | Cross-service workflow with events/queues |
   | 5 | Platform-wide, architectural change | New infrastructure layer, auth rewrite |

   **Ambiguity** — How clear are the requirements and approach?
   | Score | Meaning | Example |
   |-------|---------|---------|
   | 1 | Fully defined, no open questions | Bug fix with clear repro steps |
   | 2 | Mostly clear, minor details to decide | Feature with known UX but some edge cases |
   | 3 | Approach known but details need exploration | "Add caching" — where, how, invalidation? |
   | 4 | Multiple viable approaches, needs research | "Improve performance" — need to profile first |
   | 5 | Problem itself is unclear, needs discovery | "Users are unhappy with X" — why? what exactly? |

   **Dependencies** — How many external systems, teams, or components must coordinate?
   | Score | Meaning | Example |
   |-------|---------|---------|
   | 1 | No external dependencies | Self-contained change |
   | 2 | One external dependency | Uses an existing API that's stable |
   | 3 | Multiple dependencies, all under control | Needs DB migration + config change + deploy |
   | 4 | External team or third-party coordination | Waiting on another team's API, or external vendor |
   | 5 | Multiple external blockers, sequencing required | Multi-team rollout with feature flags and migration |

   **Complexity classification:**
   - Any score ≥ 3 → **Large** (needs refinements to break down)
   - Any score ≥ 2 → **Medium** (consider refinements)
   - All < 2 → **Small** (can go directly to spec)

2. **Q&A to clarify**: Use `okto_pulse_ask_ideation_question` or `okto_pulse_ask_ideation_choice_question` to get clarification before proceeding
3. **Status flow**: draft → review → approved → evaluating → done
   - **Draft**: Editable — write and iterate freely
   - **Review**: Read-only — awaits approval from reviewer (human or agent)
   - **Approved**: Read-only — handoff signal; the evaluator can now proceed to evaluate
   - **Evaluating**: Read-only — scope assessment and complexity evaluation must happen here
   - **Done**: Frozen — immutable snapshot created, can only go back to draft (new version)
   - **Cancelled**: Terminal — accessible from any status except done
4. **Evaluation only in "evaluating"**: `okto_pulse_evaluate_ideation` only works when status is `evaluating`
5. **Editing only in "draft"**: `okto_pulse_update_ideation` only works when status is `draft`
6. **Derivations only from "done"**: Specs and refinements can only be created from a `done` ideation (immutable snapshot)
7. **Triage pending derivations**: the canonical surface to find done ideations that still lack a derived child is `okto_pulse_list_by_board(entity_type="ideation", filters={"derivation_pending": true})` — see `okto-pulse://reference/list_tools`

## 2.1a Ambiguity-Killer Protocol — ASK Before Advancing (MANDATORY)

> **Reducing ambiguity is your primary job at ideation.** A user's first description of a problem is almost never enough to design a solution. Your job is to interrogate the request until the intent is unambiguous, the scope is bounded, and your understanding is provably aligned with what the user actually wants. **Be aggressive about clarification:** when a requirement admits multiple valid interpretations, treat that as unresolved ambiguity even if one interpretation feels obvious. One extra precise question is cheaper than carrying a hidden assumption. **Do not advance the ideation forward (draft → review → approved → evaluating) while ambiguity is still present.**

**Ambiguity left unresolved at ideation is not free.** Every inferred requirement becomes latent rework: downstream refinements, specs, mockups, Architecture Designs, cards, tests, and validations may need to be rewritten after the user corrects the assumption. That means more elapsed time, more token spend, more review churn, and less trust in the artifact history. Asking one focused Q&A item now is cheaper than rebuilding a requirement chain later.

**The rule:** if you can answer "yes" to ANY of the questions below, you have ambiguity that MUST be resolved via Q&A before moving the ideation forward.

| Symptom of ambiguity | Example | Required action |
|---|---|---|
| The user used a vague verb ("improve", "optimize", "support", "handle") | "improve onboarding" | Ask `okto_pulse_ask_ideation_choice_question` with concrete options such as "Reduce drop-off (Recommended)", "Shorten time-to-first-value", "Add new onboarding steps", and `allow_free_text=true` so the user can add a metric or override. |
| The user used a noun without a definition or scope ("the dashboard", "the system", "users") | "users should see their data" | Ask which user role, which surface, which data slice. Use `okto_pulse_ask_ideation_choice_question` when there is a finite list. |
| Multiple plausible interpretations of the same sentence | "send notifications when something changes" | Enumerate the interpretations and let the user pick. |
| The success criterion is implicit | "make it faster" | Ask for a measurable target (latency p95, throughput, perceived load time, etc.). |
| The scope boundary is implicit | "fix the auth flow" | Ask which flow (login, signup, password reset, MFA), which client (web, mobile, API). |
| You're inferring intent from context the user did not state | The user mentioned X, you assumed Y | Verify Y explicitly: "I'm reading this as Y. Is that correct, or did you mean Z?" |
| You are about to write `proposed_approach` and you can think of >1 viable approach | "Use Redis or PostgreSQL for the queue?" | Ask via `okto_pulse_ask_ideation_choice_question` with the alternatives spelled out and the tradeoffs in the question body. |
| The feature has a user-facing surface but the target screen, state, workflow, or interaction is unclear | "add admin controls" | Ask before creating mockups. Clarify screen ownership, primary user action, empty/error/loading states, and visual constraints. |
| The feature touches architecture but components, boundaries, storage, or contracts are unclear | "sync with external systems" | Ask before creating/finalizing Architecture Design. Clarify endpoint entities, protocols, responsibilities, persistence, and failure behavior. |

**Operational protocol:**

1. After receiving the user's request and BEFORE writing `problem_statement` / `proposed_approach`, do an honest ambiguity scan against the table above.
2. For every gap you find, post a question on the ideation. **One question per Q&A item.** Prefer `okto_pulse_ask_ideation_choice_question` whenever the answer can be picked from a known set. Use 2-5 mutually exclusive options, mark the safest or most likely option as **Recommended** when you can justify it, include concise tradeoffs in option labels or the question body, and set `allow_free_text=true` so the user has an additional comment field for overrides, combinations, missing options, or constraints. Use `okto_pulse_ask_ideation_question` only when the answer is genuinely open-ended and a finite option set would be misleading.
3. Use Q&A before creating or finalizing mockups when the visual surface is ambiguous. Use Q&A before creating or finalizing architecture designs when entities, interfaces, contracts, boundaries, or diagrams are ambiguous.
4. Wait for answers. Do NOT fill the gap with a guess and proceed silently.
5. After answers come in, re-read the full ideation context (`okto_pulse_get_ideation_context`) and confirm your understanding by either:
   - Updating `problem_statement` / `proposed_approach` to reflect the resolved intent, or
   - Posting a comment summarizing your understanding ("To confirm I understand: the goal is X, the success metric is Y, out-of-scope is Z. Correct?") and waiting for a confirmation or correction.
6. Only after the loop converges (no remaining ambiguity in your honest assessment), call `okto_pulse_move_ideation(status="review")`.

**Question shape requirements:**

- Bias toward multiple choice. If you can name plausible options, ask a structured choice question instead of embedding "A/B/C" inside a free-text prompt.
- Include a recommendation whenever you can responsibly make one. Put "(Recommended)" in the option label and explain the reason briefly; the user can still pick another path.
- Always enable the additional free-text/comment field (`allow_free_text=true`) on choice questions so the user can qualify the selection.
- Avoid yes/no questions when the real decision has three or more viable paths; enumerate the paths and let the user choose.
- Do not collapse unrelated ambiguity into one poll. Ask separate Q&A items so each decision can be answered, audited, and reused downstream.

**Stop conditions — when the ideation is genuinely ready to advance:**
- Every non-trivial term in `problem_statement` has a definition or scope.
- Every verb has a measurable target.
- Every alternative approach you can think of was either picked, rejected with rationale, or explicitly deferred.
- Every applicable mockup or Architecture Design is either created from clarified decisions or explicitly deferred with a reason.
- The user (or their proxy) has acknowledged the resolved version, not just the original request.
