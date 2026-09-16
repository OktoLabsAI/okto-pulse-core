# KG traceability gaps: v0.3.4 plan (Core + Community)

Status: **plan agreed on 2026-09-16, no code landed yet.** Branch `feature/v0.3.4`
in both repos. Community-side items live in
`okto-pulse/docs/KG_TRACEABILITY_GAPS_COMMUNITY.md`. Tracked in Pulse as ideation
`dfbdc0f4-9428-4f10-afd6-30dd39edda4d` on board Okto Pulse (Architecture Design
`2afc46e8`); the twenty decisions D1-D20 are recorded in its Q&A and closed on
2026-09-16 (section 8). This document mirrors the ideation; where an older
recommendation survives in sections 2-5, section 8 prevails.

## 1. Why

Agents leave the graph and re-read specs, refinements and ideations for questions
the graph was designed to answer. The two motivating questions:

1. Which features (cards, requirements, scenarios) are impacted by a change in a
   Decision?
2. Which problems (bugs) hit a board in the last N days, and what do they have in
   common?

Neither is answerable today. The root cause is not the schema: the schema
already declares every endpoint pair the first question needs. The root cause is
that the deterministic projection never reads the relational link fields, and the
read surface has no traversal for the shapes that exist.

A third question was added mid-analysis: why are `Learning` nodes almost never
recorded even on boards with bugs. That is a lifecycle and documentation defect,
covered in section 5.

## 2. Evidence base

Verified against `okto-pulse-core` and `okto-pulse` at v0.3.3 (`main`) and the
local `pulse.db` (three boards: Okto Pulse, Okto Grafx, Okto Neuron), read-only.

### 2.1 The relational links exist and are dense

Per spec-child collection, share of items carrying the link field (board Okto
Neuron, 5 specs; Okto Pulse is equivalent with 35 specs):

| Collection | items | `linked_task_ids` | `linked_criteria` | `linked_requirements` |
|---|---|---|---|---|
| functional_requirements | 26 | 26 | n/a | n/a |
| technical_requirements | 27 | 27 | n/a | n/a |
| acceptance_criteria | 30 | 0 | n/a | n/a |
| test_scenarios | 30 | 30 | 30 | n/a |
| business_rules | 27 | 27 | n/a | 27 |
| decisions | 14 | 14 | n/a | 14 |
| api_contracts / IR / OR | 18 | 18 | n/a | 18 |

Every `linked_criteria` value on all three boards is an `ac_xxxxxxxx` id (508 on
Okto Pulse alone). `card_dependencies` has 278 rows. `cards.test_scenario_ids` is
set on 223 cards.

### 2.2 What the worker reads today

| Relational field | Read by `deterministic_kg.py` / `consolidation.py` | Edge today |
|---|---|---|
| spec child `linked_task_ids` (8 collections) | **never** | none |
| `test_scenarios[].linked_criteria` | yes, but resolves only exact text or int index; the persisted form is `ac_<id>` | `tests` = **0 edges on every board** |
| `business_rules[].linked_requirements` | never (the comment at `deterministic_kg.py:1417` claims otherwise) | none |
| `api_contracts[].linked_requirements` | yes | `implements/fr_match` |
| `api_contracts[].linked_rules` | never | none |
| `decisions[].linked_requirements` | yes | `derives_from/explicit_link`, else co-occurrence fan-out at 0.6 |
| `decisions[].supersedes_decision_id` | never; superseded decisions are filtered out of the projection | none |
| `cards.test_scenario_ids` | never on the card itself (only transitively for a bug's linked test card) | none |
| `card_dependencies` | never | none (only `spec_dependencies` -> `precedes`) |
| `cards.origin_task_id` (bug) | resolved in consolidation | `originates_from` Bug -> origin card |
| `cards.linked_test_task_ids` (bug) | resolved in consolidation | `covered_by` Bug -> test card / TestScenario |
| bug -> Constraint (`violates`) | only a `MissingLinkCandidate`; never resolved, never persisted | **0 edges anywhere** |
| `cards.severity`, `cards.status`, `cards.created_at` | severity folded into `priority_boost`; status only selects `graph_layer`; timestamps not passed | node `created_at` = projection time |

Unresolved `MissingLinkCandidate`s are logged and dropped
(`consolidation.py:2637`). The "cognitive agent picks them up later" premise has
no durable store.

### 2.3 Stale edges

Deterministic edges are deduplicated on `(edge_type, from_id, to_id)` and never
removed, except two hard-coded active-set namespaces: `(refinement, rdl)` and
`(spec, dependencies)` (`primitives.py:585`). Any anti-join ("which criterion has
no scenario") over a projected edge family without an active set is unsafe.

### 2.4 Schema evolution cost (Community / Grafx)

Adding a node column or an endpoint pair changes the logical fingerprint pinned
in `grafx_schema_manifest.py`, trips the import-time assertion in
`grafx_schema_evolution.py`, and has no in-place ALTER path; existing boards fail
`table_shape_mismatch` / `versioned_partial_schema`. The only migration
machinery is the 0.3.12 -> 0.5.0 candidate rebuild. Schema changes are therefore
an expensive class of work; decision D5 puts them inside 0.3.4 as the first
technical phase, so every emitter is written once against the final schema.

## 3. Question catalog and verdicts

`yes` = one graph call today; `partial` = some hops exist; `no` = relational
fallback only. G-ids refer to section 4.

| # | Question | Today | Gaps |
|---|---|---|---|
| Q01 | Features impacted by a change in a Decision | partial (Decision->Requirement only) | G2, G6, G16 |
| Q02 | Cards implementing a requirement, and the reverse | no | G2, G13, G14 |
| Q03 | Scenarios covering a criterion; uncovered criteria | no (`tests` never emitted) | G1, G13 |
| Q04 | Test cards automating a scenario; unautomated scenarios | no | G2/G4, G9, G13 |
| Q05 | Bugs in the last N days and their common cause | partial (bugs by origin card only) | G5, G10, G16, L7 |
| Q06 | Bugs by origin card/spec; bug-prone areas | partial | G5, G9, G10 |
| Q07 | What a bug regresses; scenarios to re-run after the fix | partial (`covered_by` only) | G5, G4, G11 |
| Q08 | Decisions governing a requirement or a card | partial (requirement side) | G2, G6 |
| Q09 | Downstream effect of superseding a decision | no | G6, G2, G16 |
| Q10 | Constraints applying to a card; bugs that violated them | partial (spec granularity) | G2, G5, G9 |
| Q11 | Blocked specs/cards; impact of delaying a spec | partial (spec `precedes`, 1 hop) | G3, G9, G16 |
| Q12 | What changed on a board in the last N days | partial, wrong timestamps | G10, G16 |
| Q13 | Learnings applying to the area I will touch | partial (spec granularity) | G2, G5, L1..L7 |
| Q14 | Contracts implementing a requirement; cards touching them | partial (contract side) | G2, G7 |
| Q15 | Requirements delivered per sprint; requirements without a card | no | G2, G9, G13 |
| Q16 | Code paths implementing a requirement | partial (evidence side, raw Cypher, CT permission) | G16 |
| Q17 | Structural spec coverage (AC without scenario, FR without BR) | no (`tests` missing; BR->FR has no pair) | G1, G8, G13 |
| Q18 | Amendments that changed a spec and what they touched | partial (amendment -> spec only) | G11 |
| Q19 | Open bugs by severity/status, resolution time | no | phase 5 columns |
| Q20 | Full lineage of a card, up and down | partial (all edges exist, no tool, canonical layer truncates) | G9, G16 |

Added by the completeness critics (all `no` today): card dependency cycles and
execution order (G3); decisions of a spec with no card (G2); effective sprint
scope from cards (G2 + sprint explicit scope); all coverage gates of a spec in one
query (G1, G2, G8, G13); spec children with canonical ids (G16 center prefix);
BR->FR coverage (G8).

## 4. Gap inventory

Class: **E** emitter-only (zero DDL), **A** active set / enqueue, **R** read
surface, **D** docs, **S** schema evolution.

| Id | Class | Gap | Source of truth | Target | Notes |
|---|---|---|---|---|---|
| G1 | E | `tests` TestScenario->Criterion never emitted: resolver lacks the `ac_<id>` branch | `test_scenarios[].linked_criteria` -> `acceptance_criteria[].id` | `tests/ac_match@v2.0` (existing slot) | One resolver branch plus a regression test with id-form links; the only existing test uses text-form links. Highest-leverage fix in the whole plan. |
| G2 | E | Card -> spec child links never projected | spec child `linked_task_ids` on FR, TR, AC, TS, BR, API, IR, OR, Decision | `supports` Entity(card) -> Requirement / Constraint / Criterion / TestScenario / APIContract / Decision, rule `supports/spec_child_task_link@v2.0` | All seven Entity pairs exist; the six `(Bug, X)` pairs arrive with the 0.6.0 evolution (D4), so bug cards are projected by the same card-side emitter in phase 2. |
| G3 | E | Card dependencies never projected | `card_dependencies(card_id, depends_on_id)` | `precedes` Entity(card) -> Entity(card), rule `precedes/card_dependency/<dep_id>@v2.0` | Mirrors `precedes/spec_dependency`; direction prerequisite -> dependent. Enables cycle and execution-order queries. |
| G4 | E | Card's own `test_scenario_ids` never projected | `cards.test_scenario_ids` | `supports` Entity(card) -> TestScenario | Merged into the card-side emitter of G2 so one owner asserts card->scenario from both sources. |
| G5 | E | `violates` Bug->Constraint has zero producers; `explain_constraint.violations` is always `[]` | `cards.origin_task_id` -> origin card -> its spec -> TR/BR whose `linked_task_ids` contain the origin card | `violates/origin_task_constraint@v2.0`, confidence 0.8 | Resolve in `_resolve_missing_link_candidates` (already loads cards; add the spec load). Heuristic by construction; the rule slot says so. Decision D3 extends the same path to `violates (Bug, Requirement)` and `violates (Bug, Criterion)` on the 0.6.0 pairs. |
| G6 | E | Decision supersedence never projected; superseded decisions dropped from the projection | `decisions[].supersedes_decision_id`, `decisions[].status` | `supersedes` Decision->Decision, rule `supersedes/spec_decision@v2.0`; stamp `superseded_by`/`superseded_at` on the predecessor | `supersedes` is cognitive-owned; use the existing `system:` exemption (precedent `supersedes/code_traceability`). Keep superseded nodes as tombstoned-but-walkable. |
| G7 | E | `api_contracts[].linked_rules` ignored | `api_contracts[].linked_rules` | `implements` APIContract->Constraint (pair exists), rule `implements/api_rule_link@v2.0` | Sits beside the existing `implements/fr_match` loop. |
| G8 | S | BR->FR, OR->IR, IR->FR have no endpoint pair | `business_rules[].linked_requirements` etc. | `derives_from (Constraint, Requirement)` and `(Requirement, Requirement)`, added by the 0.6.0 evolution | Phase 2 on the final schema. This is the authoritative FR-coverage source in the relational gate. |
| G9 | E | `kind_of` is NULL on every projected node, so bare labels are over-broad (Entity = board/spec/sprint/card/story/...; Constraint = TR/BR/OR; Requirement = FR/IR; APIContract = spec contract / architecture interface) | artifact type dispatched by `process_artifact`; `cards.card_type`; spec collection name | system-declared subtypes in `NodeSubtypeRegistry`, e.g. Entity: `board`, `spec`, `sprint`, `card_task`, `card_test`, `story`, `ideation`, `refinement`, `amendment`, `architecture_design`, `architecture_entity`; Constraint: `technical_requirement`, `business_rule`, `observability_requirement`; Requirement: `functional_requirement`, `integration_requirement`; APIContract: `spec_contract`, `architecture_interface` | Data-only, no SCHEMA_VERSION bump. Do **not** put severity or status in `kind_of`; it is a single slot reserved for artifact kind. Verify `kind_of` is in `_NODE_UPDATEABLE_ATTRS` so existing nodes refresh. |
| G10 | S | Node `created_at` is projection time and is re-stamped by rebuild/DLQ replay; no relational timestamp reaches the graph | `cards.created_at/updated_at`, `specs.*`, activity log (last move to `done`) | dedicated columns `source_created_at`, `source_updated_at`, `resolved_at` (0.6.0); `created_at` keeps its projection-time semantics | Decision D2 (revised): no overload of `created_at`; `resolved_at` = last transition to `done`, cleared on reopen (D11). |
| G11 | E | Amendment lineage flattened into content text; no enqueue on amendment events | `amendment_hotfix_revisions.regression_scenario_ids`, `revision_spec_id`, `original_spec_id` | `supports` Entity(amendment)->TestScenario; `supersedes` Entity(revision spec)->Entity(original spec); `kind_of=amendment` | Also add the `amendment.*` events to the consolidation enqueuer. |
| G12 | A | Unresolved `MissingLinkCandidate`s are dropped | worker output (every unresolvable reference: `derives_from`, `tests`, `implements`, `violates`, `supports`, missing `kgref:`) | durable ledger keyed `(board, artifact, edge_type, from_ref, reason)` with `suggested_candidates` and `next_action`; KG Health row `missing_link_backlog`; tools `okto_pulse_kg_list_missing_links` and `okto_pulse_kg_resolve_missing_link` (writes a `layer=fallback` edge, confidence <= 0.85, `rule_id <edge>/agent_fallback@v2.0`, audited); board policy `missing_link_gate` = `advisory` (default) or `blocking` | Decisions D12 and D20: the ledger is the agent's work queue; fixing the relational source is the preferred path and closes the item; a later deterministic edge wins and tombstones the fallback; a removed link reopens the item; `blocking` refuses `done` with `missing_links_open`. |
| G13 | A | No stale-edge removal for any new family (or for `tests`) | relational link removal | generalise `RelationalProjectionActiveSetIntent`: `WorkerResult` carries a **list** of intents; `supported_scope` gains `(spec, spec, ac_coverage)`, `(card, card, spec_links)`, `(card, card, dependencies)`, `(spec, spec, decision_supersedence)`, `(bug, bug, violations)` | Edges-only namespaces (precedent: `dependencies`): hard delete with before-images and post-delete confirmation. Rebuild must produce the same active set. |
| G14 | A | Enqueue coverage: `link_task` / traceability events re-enqueue only the spec; `card_dependencies` and `cards.test_scenario_ids` changes re-enqueue nothing | domain events | `consolidation_enqueuer._map_targets` routes link events to **both** endpoints (spec and card); dependency and scenario-link events to the card | Also fix the existing `card.linked_to_spec` branch that re-enqueues only the spec. |
| G15 | A | Edge identity ignores `rule_id`: two rules over the same `(type, from, to)` collapse and active-set cleanup by one rule can delete the other's edge | n/a | one owner per edge family: all card->spec-child edges are owned by the card-side emitter; `supports` from code evidence keeps its own from-nodes | Design rule, enforced by test. |
| G16 | R | Read surface cannot express the new shapes | graph | (a) `get_related_context`: center by prefix (`spec:<uuid>` includes children) or `include_children=true`; (b) `ContextHop` gains `node_type`, `kind_of`, `source_artifact_ref`, `direction`, `rule_id`, `confidence`; (c) `RELATED_CONTEXT_DEPTHS` += 3 in its three enforcement points; (d) `TYPED_ARTIFACT_KINDS` accepts spec-child refs; (e) new curated tools: `okto_pulse_kg_get_decision_impact`, `okto_pulse_kg_get_spec_coverage`, `okto_pulse_kg_get_bug_clusters(board_id, since_days, group_by)`, `okto_pulse_kg_get_lineage`, plus `okto_pulse_kg_list_missing_links` / `okto_pulse_kg_resolve_missing_link`; (f) `since`/`until` on `get_decision_history` over `source_created_at`; (g) `get_supersedence_chain(direction=ancestors\|successors)`; (h) `explain_constraint` reports lane emptiness and marks proxy/fallback edges; (i) **no count-based rate limit anywhere** (D15/D18): the 30/min tier-power bucket is removed, the `RateLimiter` port stays with the Community default off, and cost/usage guidance replaces it; (j) per-query timeout enforced from the board setting `kg_query_timeout_seconds` (default 15s, max 30s) with a contextual `kg_query_timeout` error; row caps 200 default / 1000 hard with pagination; (k) `projection_freshness` block on every traversal response (D14) | Each new tool: permission registry entry, `tools_catalog.md` regeneration, tool-count gates in `okto-pulse/scripts/release_artifact_gate.py`. |
| G17 | D | Docs drift | n/a | `workflows/kg.md` (both edge-ownership tables), `tool-docs/kg.md` (`relates_to` lists only Decision->Alternative; the 21 cognitive pairs are missing), Learning ref grammar, Bug-id discovery recipe, temporal recipe (`timestamp($since)`), lineage and coverage recipes, `reference/errors.md` KG codes, README counters, `ska_resource_manifest.json` regeneration | Ships with each phase. |

### Design note for G2/G3/G4 (card-side ownership)

Emit card -> spec-child edges from `process_card`, not from `process_spec`:

- The card node is local to the session, so the Entity-vs-Bug ambiguity is
  decided by the emitter (bug cards skip `supports` until phase 5).
- Endpoints are spec children addressed by stable source ref through the
  existing `kgref:<NodeType>:spec:<spec_id>:<section>:<child_id>` grammar, which
  never creates or overwrites a node. Spec-before-card ordering already exists in
  `REBUILD_SOURCE_DEPENDENCY_RANK`.
- One active-set namespace per card, `(card, card, spec_links)`, converges the
  union of `linked_task_ids` (spec side) and `test_scenario_ids` (card side).
- `load_projection_inputs` (already used for ideation/refinement/spec) supplies
  the card's spec children that reference it, so the pure worker stays pure.

Open check before implementation: confirm that `kgref:` resolution accepts a
**working-layer** target (spec not yet `done`), or edges to in-progress specs are
silently dropped. If it is canonical-only, extend the resolver with an explicit
layer hint for deterministic writers.

## 5. Learning audit (why Learnings are almost never recorded)

Findings that survived adversarial verification (three refuters per hypothesis):

| Id | Finding | Evidence |
|---|---|---|
| L1 | No running component in the Community edition persists a Learning automatically. `CognitiveCloseoutWorker` has no `RuntimeWorkerSpec` in `build_community_worker_registry` (five families registered; `tests/test_r08c_worker_registry.py` pins them). 100% of Learnings depend on an agent's manual consolidation. | `okto-pulse/.../adapters/workers.py:141-176` |
| L2 | Nothing enforces a Learning. The bug->done cognitive gate is ledger-only (`cognitive_closeout_gate.py:526-534`); it never checks the graph. The obligation is documented as "Mandatory" only in `workflows/kg.md`, which is not among the resources the agent reads when it moves a bug card. `agent_instructions.md` contains zero occurrences of "Learning". | `workflows/kg.md:253`, `agent_instructions.md` |
| L3 | Even when wired, the automatic path is opt-in on `Board.settings.cognitive_llm_config` (default `None`, `openai` provider only) and needs all seven evidence categories including `lineage`; misses write terminal ledger statuses that are never re-drained. This contradicts "serve requires no LLM keys" only for Learning production, and nothing documents it. | `cognitive_closeout_production.py`, `workers/cognitive_closeout.py:112` |
| L4 | A rejected bug-derived Learning commit is not a retryable error: the R7 working-only violation persists a PENDING ledger hold keyed by the candidate's raw `source_artifact_ref`; only a human can clear it (`human_only_reason_code`). None of this is in `reference/errors.md`. | `kg_tools.py:707-716`, `rebuild_audit.py:2601-2660`, `cognitive_readiness.py:121-133` |
| L5 | Docs contradict the schema: the served `tool-docs/kg.md` says `relates_to` is Decision->Alternative only, omitting the 21 `{Learning, Alternative, Assumption} x 7` pairs that are the sole legal path for a non-bug Learning. No document gives the Learning `source_artifact_ref` grammar (`bug:<card_uuid>` is what the server's own producer emits) or a recipe to obtain the `kg:<bug_node_id>` the `validates` edge needs. | `kg_tools.py:411-416`, `cognitive_source_ref_resolver.py:160-188` |
| L6 | The trigger is timed before its prerequisite: the Bug node becomes canonical asynchronously (queue-driven re-projection at card `done`), but `kg.md` tells the agent to consolidate at the move. The race is bounded and self-healing, but the doc gives no "verify canonical Bug first" step, although it gives exactly that recipe for Constraint ids. | `workflows/kg.md:253`, `consolidation.py:3132-3141` |
| L7 | Local data: 12 Learnings exist, all from one day. The 6 bug-derived ones were committed at 16:08 while their bug cards reached `done` at 18:10; all 6 sit in `canonical_debt` with reason `canonical_learning_historical_working_only_bug_evidence_debt`, `retry_count=0`, never reconciled although the Bugs later became canonical. Learning->Bug is effectively 1:1, so Learnings cannot act as a shared-cause key. | `pulse.db` `canonical_debt`, `kg_cognitive_sources` |

Refuted (kept for the record): "a done bug without linked test task stays
working forever" (the minimal-evidence rule is satisfied through the normal
lifecycle); "`card:<uuid>` refs are misrouted" (the probe is graph-fed).

Fixes (phase 4):

- **L-A** Reach: add the bug-closeout obligation and a pointer to
  `okto-pulse://workflows/kg` to `agent_instructions.md` Quick Navigation, to the
  bug section of `workflows/cards.md`, and to `reference/transitions.md`.
- **L-B** Honest gate: board settings key `bug_learning_closeout` (human-written,
  mirrored in the default board config, decision D17) with values `advisory`
  (default, decision D6: KG Health `health_issues[]` row `bugs_without_learning`)
  and `blocking` (bug->done requires a canonical `Learning -validates-> Bug`).
  Downgrade "Mandatory" wording to match the effective policy.
- **L-C** Two-step trigger in `kg.md`: move to `done`; call
  `okto_pulse_kg_evaluate_bug_cognitive_closure` (extend its payload with
  `bug_node_id` and `bug_node_layer`) until the Bug is canonical; then
  consolidate. Add the Bug-id Cypher recipe.
- **L-D** Self-clearing R7 hold: when the card's re-projection promotes the Bug
  to canonical, re-evaluate PENDING holds whose source ref resolves to that bug
  and release them; document every Learning rejection code in `errors.md`.
- **L-E** Docs: list the real `relates_to` pairs and the canonical-layer
  requirement in the `add_edge_candidate` doc; define the Learning ref grammar
  (`bug:<card_uuid>` for bug-derived); either admit `bug_ref` on `NodeCandidate`
  or delete the five dead probes.
- **L-F** Historical debt: diagnose why the 6 debts did not close after the Bugs
  became canonical (likely the `validates` edge points at the superseded
  working Bug node id); add a reconcile pass on Bug promotion that re-points or
  re-validates, then close the debt.
- **L-G** Fan-in (decision D13): the closeout (manual and automatic) calls
  `get_similar_nodes` against canonical Learnings and reuses the reconciliation
  bands: >= 0.95 attaches `validates` to the existing Learning; 0.85-0.95 mints
  a new Learning that `supersedes` the old one and inherits its `validates`;
  < 0.85 mints an independent one. The supersedence chain is the cluster key
  for Q05 `group_by=learning`.
- **L-H** Community: register `cognitive_closeout_worker` behind a setting that
  defaults to enabled only when `cognitive_llm_config` is present; emit a
  startup warning and a health signal when closeout items terminate as
  `skipped_no_llm_config`.

## 6. Delivery plan

The schema evolution is the first technical phase (decision D5), right after the
test harness. Every emitter is written **once** against the final schema: no
interim zero-DDL phase, no provisional ledger for bug cards, no `created_at`
overload, one rebuild-affecting cutover. Execution order:
0 -> 1 (schema) -> 2 (emitters, active sets, enqueue, missing-link queue) ->
3 (rebuild/repair) -> 4 (read surface: MCP, REST, full UI) -> 5 (Learnings) ->
6 (docs). Each phase lands as its own PR pair (core first, then community)
targeting `develop`, with a census before/after on the three local boards; every
phase that touches projection exits with `okto_pulse_kg_orphan_report = 0`
(decision D4). No KG query tool counts calls (D15/D18); the analytical tools are
usage suggestions and remove no read step from the protocol (D14).

### Phase 0: prerequisites (core)

- P0.1 Port `tests/kg_schema_testing.py` and `kg_registry_testing.py` off the
  removed `kg_runtime` / `kuzu_graph_transaction` modules onto the Grafx
  adapters. ~89 core test files depend on it, and phase 1 changes the schema,
  which must not be validated with in-memory fakes only.
- P0.2 Baseline census on the three local boards (edge counts per type and
  rule_id, orphan report) under `docs/evidence/`.
- P0.3 Sequencing (D7): this initiative runs first; the WIP branch
  `feature/v0.3.4-kg-replay-only-cognitive-commit` is rebased afterwards and its
  conflicts in `primitives.py` / `consolidation.py` are resolved in that rebase.
- P0.4 Confirm that `kgref:` resolves a working-layer target (condition of D1);
  otherwise extend the resolver with a layer hint for `system:` writers.

### Phase 1: schema evolution 0.5.0 -> 0.6.0 (core + community, D5)

Columns (core `STABLE_NODE_PROPERTIES` + community `COMMON_NODE_COLUMNS`,
`embedding` last): `severity STRING`, `source_status STRING` (raw relational
status, D10), `source_created_at TIMESTAMP`, `source_updated_at TIMESTAMP`,
`resolved_at TIMESTAMP` (last transition to `done` from the activity log, NULL
outside `done`, cleared on reopen, D11). Added to `_NODE_UPDATEABLE_ATTRS` and
`_SEMANTIC_PROJECTION_NODE_ATTRS`; treated as semantic payload by the tombstone.
`created_at` keeps its projection-time semantics (D2).

Pairs (`MULTI_REL_TYPES`, no new relationship *name*): `supports (Bug,
Requirement | Constraint | Criterion | TestScenario | APIContract | Decision)`
(D4, 6 tables); `violates (Bug, Requirement)`, `violates (Bug, Criterion)` (D3,
2 tables); `derives_from (Constraint, Requirement)`, `derives_from (Requirement,
Requirement)` (G8); `derives_from (Decision, Constraint)` (origins for
`explain_constraint`). 69 -> 80 pairs, 44 -> 49 columns.

Mechanics: `SCHEMA_VERSION = "0.6.0"`; `grafx_schema_evolution` generalised into
a 0.3.12 -> 0.5.0 -> 0.6.0 chain (or `grafx_schema_evolution_0_6_0.py`) with
candidate rebuild and the introduced columns/pairs declared; manifest fingerprint
recomputed; pins updated in `test_grafx_schema_bootstrap.py` (49 columns),
`test_grafx_relationship_layout.py` (16 types / 80 pairs),
`test_grafx_auxiliary_indexes.py` and the core `SCHEMA_VERSION` allowlists;
structural hash flips once with `SCHEMA_VERSION_CHANGED` (one documented operator
override on promotion); `BoardMeta.schema_version` migrates at cutover; frontend
`constants/kg.ts`; README/GLOSSARY counters;
`docs/grafx-schema-evolution-0.5.0-to-0.6.0.md` and `docs/migrations/v0.6.0.md`.

Board migration (D8, D9): explicit per-board trigger by the operator
(`okto_pulse_kg_migrate_schema` / REST / CLI) with preflight and backup; a board
that has not been migrated fails closed with an actionable error. After the
candidate-rebuild cutover, a full deterministic re-consolidation of every board
source runs through the normal queue, preserving cognitive nodes, until
`orphan_report = 0`.

Exit criteria: the three local boards migrated; `okto_pulse_kg_schema_info`
reports 0.6.0 with 80 pairs; `okto_pulse_kg_orphan_report = 0`;
`okto_pulse_kg_health` without connectivity issues; edge census unchanged for
the pre-existing families.

### Phase 2: emitters on 0.6.0, active sets, enqueue and the missing-link queue (core)

| Id | Gap | Source | Edge / property / rule_id |
|---|---|---|---|
| G1 | `tests` never emitted | `test_scenarios[].linked_criteria` -> `acceptance_criteria[].id` | `ac_<id>` branch in the resolver; existing slot `tests/ac_match@v2.0` |
| G2+G4 | card -> spec children for task, test and bug cards | `linked_task_ids` on the 8 collections + `cards.test_scenario_ids` (+ `ImpactEvidenceTest.scenario_id`, dec_477af268) | `supports` Entity(card) -> X and Bug(card) -> X (D4), `supports/spec_child_task_link@v2.0`; card-side emission (D1) with `kgref:<Type>:spec:<spec_id>:<section>:<child_id>` endpoints; one active set per card `(card, card, spec_links)` |
| G3 | card dependencies | `card_dependencies` | `precedes` Entity -> Entity, `precedes/card_dependency/<dep_id>@v2.0`, prerequisite -> dependent, per-board fence (dec_16b56285); active set `(card, card, dependencies)` |
| G5 | `violates` has no producer | `origin_task_id` -> origin card -> spec -> TR/BR, FR/IR and AC (through scenarios) linked to the origin card | `violates/origin_task_constraint@v2.0`, `violates/origin_task_requirement@v2.0`, `violates/origin_task_criterion@v2.0` (D3), confidence 0.8, resolved in `_resolve_missing_link_candidates`; active set `(bug, bug, violations)` |
| G6 | decision supersedence | `decisions[].supersedes_decision_id`, `status` | `supersedes/spec_decision@v2.0` via the `system:` exemption; predecessor kept and stamped `superseded_by/at`; active set `(spec, spec, decision_supersedence)` |
| G7 | `linked_rules` ignored | `api_contracts[].linked_rules` | `implements` APIContract -> Constraint, `implements/api_rule_link@v2.0` |
| G8 | BR -> FR, OR -> IR, IR -> FR | `business_rules[].linked_requirements`, `observability_requirements[].linked_integration_requirements`, `integration_requirements[].linked_requirements` | `derives_from/br_requirement_link@v2.0` and siblings; active set `(spec, spec, requirement_links)` |
| G-origins | constraint origins and end of the fan-out (D12) | `decisions[].linked_requirements` resolving to FR or TR | `derives_from/explicit_link@v2.0` (new Decision -> Constraint pair included); `derives_from/cooccurrence` is no longer emitted: a decision without a resolved `linked_requirements` becomes a missing-link item; `explain_constraint.origins` stops being hard-coded `[]` |
| G9 | `kind_of` NULL on every node | artifact type + `card_type` + collection | subtypes in `NodeSubtypeRegistry` (Entity: board/spec/sprint/card_task/card_test/story/ideation/refinement/amendment/architecture_*; Bug: card_bug; Constraint: technical_requirement/business_rule/observability_requirement; Requirement: functional_requirement/integration_requirement; APIContract: spec_contract/architecture_interface). Never severity/status in `kind_of` |
| G-cols | `severity`, `source_status`, `source_created_at`, `source_updated_at`, `resolved_at` on every projected node | `cards.severity/status/created_at/updated_at`, `specs.*`, activity log of the last move to `done` | worker dicts carry the fields outside `raw_parts` (no `content_hash` change); `source_status` raw (D10); `resolved_at` = last `done`, cleared on reopen (D11) |
| G11 | amendment lineage flattened into text | `regression_scenario_ids`, `revision_spec_id`, `original_spec_id` | `supports` Entity(amendment) -> TestScenario; `supersedes` revision spec -> original; `kind_of=amendment`; `amendment.*` events in the enqueuer |
| G12 | missing links dropped -> **agent work queue** (D12, D20) | every reference the worker cannot resolve | durable ledger keyed `(board, artifact, edge_type, from_ref, reason)` with `suggested_candidates` and `next_action`; KG Health row `missing_link_backlog` with drill-down; tools `okto_pulse_kg_list_missing_links` and `okto_pulse_kg_resolve_missing_link(item_id, target_ref, justification)` (writes a `layer=fallback` edge, agent authorship, confidence <= 0.85, `rule_id <edge>/agent_fallback@v2.0`, audited); fixing the relational source is the preferred path and closes the item on re-consolidation; a later deterministic edge wins and tombstones the fallback; a removed link reopens the item; board policy `missing_link_gate` = `advisory` (default) or `blocking` (refuses `done` with `missing_links_open`), human-written, evaluated by the common transition evaluator, never switched on by rebuild/migration |
| G13 | stale edges | link removal | generalise `RelationalProjectionActiveSetIntent` (list of intents; `supported_scope` gains the namespaces above plus `(spec, spec, ac_coverage)`); edges-only namespaces with before-image, compensation and post-delete confirmation |
| G14 | one-sided enqueue | domain events | `_map_targets` routes `link_task`/traceability to spec AND card; dependency and scenario-link events to the card; fixes `card.linked_to_spec` |
| G15 | edge identity ignores `rule_id` | - | one owner per family: card-side for every card -> spec-child edge; `supports` from code evidence keeps its own from-nodes (guard test) |

Zero-orphan invariant (D4): `supports`, `violates`, `tests`, `precedes` and
`derives_from` never count as connectivity; every Bug/Entity keeps `belongs_to`
to spec/sprint/board root; the connectivity guard and the orphan report run in
the exit census.

### Phase 3: rebuild and repair (core + community)

`card_dependencies` and `test_scenario_ids` join `CARD_CONTENT_COLUMNS` (one-time
`content_hash` change, documented); `deterministic_projection_repair` accepts
cards and bugs; the structural hash keeps excluding edges (recorded decision);
rebuild and incremental reconciliation produce the same active sets and the same
missing-link queue; `orphan_report = 0` after a full rebuild of the three boards.

### Phase 4: read surface: MCP, REST and full UI (core + community)

API/MCP (core): (a) `get_related_context` with a prefix center (`spec:<uuid>`
includes children) or `include_children`; (b) `ContextHop` with `node_type`,
`kind_of`, `source_artifact_ref`, direction, `rule_id`, `confidence`;
(c) `RELATED_CONTEXT_DEPTHS` += 3 in its three enforcement points;
(d) `TYPED_ARTIFACT_KINDS` accepts child refs; (e) new curated, paginated tools
`okto_pulse_kg_get_decision_impact`, `okto_pulse_kg_get_spec_coverage`,
`okto_pulse_kg_get_bug_clusters(board_id, since_days, group_by=constraint|requirement|criterion|origin_spec|learning|severity)`,
`okto_pulse_kg_get_lineage`, `okto_pulse_kg_list_missing_links`,
`okto_pulse_kg_resolve_missing_link`; (f) `since`/`until` over
`source_created_at` on `get_decision_history`; (g) `get_supersedence_chain(direction)`;
(h) `explain_constraint` reports lane emptiness, returns real `origins` and marks
proxy/fallback edges; (i) **cost guidance instead of rate limits (D15/D18)**: the
30/min bucket leaves `kg_query_cypher` / `kg_query_natural` / `kg_query_reflective`
and no tool counts calls; the `RateLimiter` port stays with the Community default
off; expensive tools declare cost class, when to prefer a curated tool over raw
Cypher, how to narrow a query and what to do with an empty result;
`workflows/kg.md` gains a "cost and tool choice" section; (j) **per-query timeout
enforced (D18)**: setting `kg_query_timeout_seconds` (default 15, max 30) mirrored
in the default board config, applied with `asyncio.wait_for` to the power tools
and the new traversals, per-call `timeout_ms` clamped to it, contextual
`kg_query_timeout` error naming the current limit, that it can be raised in
Menu > Settings > Knowledge Graph (up to 30s) and how to narrow the query;
`min_confidence` of `query_natural` applied or removed; (k) **row caps (D18)**:
default 200, hard cap 1000, pagination; (l) usage and timeout telemetry per agent
in KG Health; (m) **`projection_freshness` block (D14)** on every traversal
response: board queue depth, `last_consolidated_at` of the touched artifacts,
`graph_layer` per node. Each new tool: permission registry entry,
`tools_catalog.md` regeneration, tool-count gates in `release_artifact_gate.py`;
`graph_layer` defaults to `all` for impact/coverage/lineage shapes.

Usage rule for the analytical tools (D14, confirmed): they are suggestions, not
gates; no read step is removed and no read is blocked. For the artifact under
construction (the spec in the `specs.md` saturation loop) the relational side
stays authoritative: `get_spec_context(profile="full")` and `coverage_summary`
decide saturation, because the projection is asynchronous and spec children are
canonical only at `done`. For already-consolidated external knowledge (other
specs, decisions elsewhere, bugs, Learnings, impact, clusters) the graph tools are
the recommended path instead of full re-reads. The full read before critical
transitions stays in both existing layers (server guard
`require_full_context_for_critical_actions` and the agent gate read). Promotion
to mandatory protocol steps is deferred to a later release, after measuring.

REST (community): paginated twins of the new tools (`kg_traceability.py`),
`since/until`, `direction`, and the `kg_query_timeout_seconds` /
`missing_link_gate` / `bug_learning_closeout` settings in the board-settings
contract and default board config.

Full UI (D16, D19), Community frontend, contextual screens in the existing modals:

- **"Impact" tab on each Decision inside the Spec modal**: derived requirements
  (`derives_from` with `rule_id`/`confidence`/`layer`), cards implementing them
  (`supports`, Entity and Bug), scenarios testing them, supersedence chain both
  ways; filters by `graph_layer`, `rule_id`, `confidence`; drill-down to card and
  scenario.
- **"Coverage" tab on the Spec modal**: every coverage gate in one view
  (AC -> scenario, FR -> BR, scenario -> test card, BR/TR/API/IR/OR/decision ->
  card), anti-joins highlighted with exact ids, counters aligned with the
  relational `coverage_summary`, graph-vs-relational divergences made explicit
  (stale edge or missing link), the spec's missing links listed and actionable;
  shows projection freshness and states that saturation authority is relational.
- **"Bug Clusters" panel in KG Health / Analytics** (board-wide): window over
  `source_created_at` (default 15 days), selectable grouping (constraint,
  requirement, criterion, origin spec, Learning, severity), `rule_id`/`confidence`
  visible, `resolved_at` and status for MTTR.
- **Settings**: `kg_query_timeout_seconds` (1-30, help text quoted by the
  `kg_query_timeout` error), `missing_link_gate` and `bug_learning_closeout`
  (advisory|blocking) under Menu > Settings > Knowledge Graph.
- **Cross-cutting**: `kind_of`/`rule_id`/`confidence`/`layer` badges on the
  existing relationship panels, `kind_of` filter on the graph page,
  `missing_link_backlog` and `bugs_without_learning` rows in KG Health with
  drill-down.
- **Process**: mockups authored in this phase's refinement/spec under the board's
  effective Design System (`MockupDesignSystemGate`) with prior Q&A on the still
  ambiguous visual points; vitest per component; lint ratchet respected;
  `frontend_dist` synchronised at release; `KG_SOURCE_NAVIGATION.md` and
  `KG_HEALTH_DASHBOARD.md` updated.

Acceptance queries (run with `include_working=true`; the local boards must return rows):

```cypher
MATCH (d:Decision) WHERE d.source_artifact_ref = $decision_ref AND d.superseded_by IS NULL
OPTIONAL MATCH (d)-[:derives_from]->(r:Requirement)
OPTIONAL MATCH (card)-[:supports]->(r)            // Entity or Bug
OPTIONAL MATCH (card)-[:supports]->(ts:TestScenario)
RETURN r.id AS requirement, collect(DISTINCT card.id) AS cards, collect(DISTINCT ts.id) AS scenarios
```

```cypher
MATCH (b:Bug) WHERE b.source_created_at >= timestamp($since)
OPTIONAL MATCH (b)-[v:violates]->(c)              // Constraint, Requirement or Criterion
OPTIONAL MATCH (l:Learning)-[:validates]->(b)
RETURN label(c) AS target_type, c.id AS target, c.kind_of AS kind, b.severity AS severity,
       collect(DISTINCT b.id) AS bugs, collect(DISTINCT l.id) AS learnings, collect(DISTINCT v.rule_id) AS rules
ORDER BY size(bugs) DESC
```

### Phase 5: Learning lifecycle (core + community)

L-A through L-H from section 5, with decisions D6 (advisory default), D13
(reconciliation bands for fan-in) and D17 (`board.settings` key). Order: L-E and
L-A (docs), L-C, L-D, L-F, L-G, L-B, L-H.

### Phase 6: documentation and release hygiene

`workflows/kg.md` (both edge-ownership tables, pair count, "cost and tool
choice" section, the rule "artifact under construction: relational; external
knowledge: graph; with projection lag: relational read", the "resolve the
missing links" step in the spec and bug sweeps, safety rails updated: configurable
timeout, no rate limit), `workflows/specs.md` (same rule in the saturation loop),
`tool-docs/kg.md`, recipes (Bug-id, temporal, lineage, coverage),
`reference/errors.md` (`kg_query_timeout`, `missing_links_open`), README/GLOSSARY,
`CHANGELOG.md` in both repos, `ska_resource_manifest.json` regeneration for every
touched resource, post-phase census against the baseline, 0.6.0 migration docs,
documentation of the three screens.

## 7. Invariants every change must respect

- `SCHEMA_VERSION` moves once, from `0.5.0` to `0.6.0`, at the phase 1 cutover;
  no new relationship *name* anywhere in the initiative.
- `created_at` keeps its projection-time semantics; `source_*` and `resolved_at`
  are the temporal authority.
- No KG query tool counts calls (D15/D18); the per-query timeout comes from a
  setting, never from a constant in core.
- The analytical tools remove no read step from the protocol (D14).
- New `rule_id`s follow `<edge_type>/<slot>@v2.0`; existing literals are pinned
  by tests and by the active-set scope matchers. Never rename an existing rule.
- Every deterministic edge carries `layer=deterministic`, `created_by=worker_layer1`,
  a non-empty `rule_id` and an empty `fallback_reason`; agent resolution edges
  always carry `layer=fallback`, confidence <= 0.85 and an audit record, never
  through raw `add_edge_candidate`.
- Every node passes the connectivity guard and **no node may be loose in the
  graph** (decision D4): `belongs_to` to an Entity/Bug; `supports`, `tests`,
  `precedes`, `violates` and `derives_from` do not count as connectivity; every
  projection phase exits with `okto_pulse_kg_orphan_report = 0`.
- Reference-only endpoints use `kgref:` so no partial root node is created.
- Deterministic writers are `system:`-prefixed agents; cognitive-owned names
  (`supersedes`) are emitted only through that exemption.
- Active-set removal ships with before-images, compensation and post-delete
  confirmation; rebuild and incremental reconciliation produce the same active
  set and the same missing-link queue.
- Retired columns never come back.
- Editing `agent_instructions.md` or anything under `mcp/resources/` regenerates
  `ska_resource_manifest.json` in the same commit; `workflows/kg.md` keeps its
  required headings, cross-links and pinned strings; `agent_instructions.md`
  stays under 500 lines and 10K tokens.
- New MCP tools update the tool-count gates in `okto-pulse/scripts/release_artifact_gate.py`
  and `tools_catalog.md`.
- Core never imports `okto_pulse.community`; traversals go through the graph
  store ports and are implemented in the Grafx adapter; the schema evolution
  follows the `grafx-schema-evolution-0.3.12-to-0.5.0.md` document format.
- Every new screen is born from a mockup approved under the board's effective
  Design System; new gates (`missing_link_gate`, `bug_learning_closeout`) are
  human-written board settings defaulting to `advisory`.

## 8. Decisions (recorded in the Pulse ideation Q&A, all closed on 2026-09-16)

| # | Decision | Recorded choice | Origin |
|---|---|---|---|
| D1 | Ownership of card -> spec-child edges | card-side (`process_card` + `load_projection_inputs`, `kgref:` to children, one active set per card) | agent recommendation, confirmed by the user |
| D2 | Source timestamp | dedicated `source_created_at` (+ `source_updated_at`, `resolved_at`) in 0.6.0; `created_at` not overloaded | revised agent recommendation, confirmed by the user |
| D3 | `violates` semantics | origin proxy for Bug -> Constraint, Bug -> Requirement and Bug -> Criterion, confidence 0.8 | user decision |
| D4 | Bug cards in `linked_task_ids` | `supports (Bug, X)` in 0.3.4 through 0.6.0; no loose node in the graph | user decision |
| D5 | Schema evolution 0.6.0 | inside 0.3.4, as the first technical phase | user decision |
| D6 | Learning gate default | `advisory`, `blocking` opt-in per board | agent recommendation, confirmed by the user |
| D7 | Sequencing | this initiative first, WIP replay-only rebased afterwards | user decision |
| D8 | 0.6.0 migration trigger | explicit per board, with preflight and backup | user decision |
| D9 | Post-cutover backfill | candidate rebuild + full deterministic re-consolidation | user decision |
| D10 | `source_status` | raw relational status value | user decision |
| D11 | `resolved_at` | last transition to `done`, cleared on reopen | user decision |
| D12 | `derives_from/cooccurrence` fan-out | stop emitting; a missing link becomes a durable item the agent analyses and resolves | user decision |
| D13 | Learning fan-in | reconciliation bands (>= 0.95 attach; 0.85-0.95 supersede; < 0.85 new) | user decision |
| D14 | Analytical tools in the protocol | usage suggestions, not mandatory; artifact under construction stays relational, external knowledge on the graph; freshness block | refined agent recommendation, confirmed by the user |
| D15 | Rate limit for the recall family | no new rate limit; usage guidance on the expensive tools | user decision |
| D16 | Frontend scope | API/REST + full UI: Decision Impact, Spec Coverage, Bug Clusters, Settings and badges | user decision |
| D17 | Where `bug_learning_closeout` lives | `board.settings` key, human-written, default advisory | user decision |
| D18 | Existing tier-power limit | rate limit removed from every flow (port kept, off by default); per-query timeout via Settings, default 15s, max 30s, contextual error; row caps 200/1000 with pagination | user decision |
| D19 | Placement of the new screens | contextual tabs in the existing modals (Decision -> Impact, Spec -> Coverage, KG Health/Analytics -> Clusters) | user decision |
| D20 | Does a pending missing link block `done`? | board policy `missing_link_gate` = `advisory` (default) or `blocking`, human-written, common transition evaluator | user decision |

## 9. Out of scope (deliberately)

- Quality receipts, checklists, requirement lint, ambiguity scores: relational
  by design (SK-A), never projected.
- Mockups, knowledge bases, Q&A items as graph nodes: would need `kind_of`
  slots that compete with artifact kinds; revisit after 0.6.0.
- Delivery-evidence records (implementation/test receipts, waivers): relational
  read model is authoritative; the graph gets only the resulting `supports`.
- Structural-hash inclusion of edges (rebuild promotion guard): unchanged.
