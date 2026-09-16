# KG traceability gaps: v0.3.4 plan (Core + Community)

Status: **plan agreed on 2026-09-16, no code landed yet.** Branch `feature/v0.3.4`
in both repos. Community-side items live in
`okto-pulse/docs/KG_TRACEABILITY_GAPS_COMMUNITY.md`. Tracked in Pulse as ideation
`dfbdc0f4-9428-4f10-afd6-30dd39edda4d` on board Okto Pulse; decisions D1-D7 are
recorded in its Q&A (D3, D4, D5 and D7 decided by the user on 2026-09-16, see
section 8).

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
a separate, expensive class of work (section 6, phase 5). Everything else in this
plan is **zero DDL**.

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
| G2 | E | Card -> spec child links never projected | spec child `linked_task_ids` on FR, TR, AC, TS, BR, API, IR, OR, Decision | `supports` Entity(card) -> Requirement / Constraint / Criterion / TestScenario / APIContract / Decision, rule `supports/spec_child_task_link@v2.0` | All seven pairs exist. Emit **card-side** (see design note below). Bug cards cannot carry `supports` until the `(Bug, X)` pairs land in phase 5 (same release, decision D4): log them in the ledger in phase 1, project them in phase 5b with the same emitter. |
| G3 | E | Card dependencies never projected | `card_dependencies(card_id, depends_on_id)` | `precedes` Entity(card) -> Entity(card), rule `precedes/card_dependency/<dep_id>@v2.0` | Mirrors `precedes/spec_dependency`; direction prerequisite -> dependent. Enables cycle and execution-order queries. |
| G4 | E | Card's own `test_scenario_ids` never projected | `cards.test_scenario_ids` | `supports` Entity(card) -> TestScenario | Merged into the card-side emitter of G2 so one owner asserts card->scenario from both sources. |
| G5 | E | `violates` Bug->Constraint has zero producers; `explain_constraint.violations` is always `[]` | `cards.origin_task_id` -> origin card -> its spec -> TR/BR whose `linked_task_ids` contain the origin card | `violates/origin_task_constraint@v2.0`, confidence 0.8 | Resolve in `_resolve_missing_link_candidates` (already loads cards; add the spec load). Heuristic by construction; the rule slot says so. Decision D3 extends the same path in phase 5b to `violates (Bug, Requirement)` and `violates (Bug, Criterion)`. |
| G6 | E | Decision supersedence never projected; superseded decisions dropped from the projection | `decisions[].supersedes_decision_id`, `decisions[].status` | `supersedes` Decision->Decision, rule `supersedes/spec_decision@v2.0`; stamp `superseded_by`/`superseded_at` on the predecessor | `supersedes` is cognitive-owned; use the existing `system:` exemption (precedent `supersedes/code_traceability`). Keep superseded nodes as tombstoned-but-walkable. |
| G7 | E | `api_contracts[].linked_rules` ignored | `api_contracts[].linked_rules` | `implements` APIContract->Constraint (pair exists), rule `implements/api_rule_link@v2.0` | Sits beside the existing `implements/fr_match` loop. |
| G8 | S | BR->FR, OR->IR, IR->FR have no endpoint pair | `business_rules[].linked_requirements` etc. | needs `derives_from (Constraint, Requirement)` and `(Requirement, Requirement)` | Phase 5b (in 0.3.4 by decision D5). This is the authoritative FR-coverage source in the relational gate. |
| G9 | E | `kind_of` is NULL on every projected node, so bare labels are over-broad (Entity = board/spec/sprint/card/story/...; Constraint = TR/BR/OR; Requirement = FR/IR; APIContract = spec contract / architecture interface) | artifact type dispatched by `process_artifact`; `cards.card_type`; spec collection name | system-declared subtypes in `NodeSubtypeRegistry`, e.g. Entity: `board`, `spec`, `sprint`, `card_task`, `card_test`, `story`, `ideation`, `refinement`, `amendment`, `architecture_design`, `architecture_entity`; Constraint: `technical_requirement`, `business_rule`, `observability_requirement`; Requirement: `functional_requirement`, `integration_requirement`; APIContract: `spec_contract`, `architecture_interface` | Data-only, no SCHEMA_VERSION bump. Do **not** put severity or status in `kind_of`; it is a single slot reserved for artifact kind. Verify `kind_of` is in `_NODE_UPDATEABLE_ATTRS` so existing nodes refresh. |
| G10 | E | Node `created_at` is projection time and is re-stamped by rebuild/DLQ replay; no relational timestamp reaches the graph | `cards.created_at`, `specs.created_at`, ... | stamp `created_at` from the source row at CREATE | Zero DDL, hash-neutral (`created_at` is in `EXCLUDED_HASH_FIELDS`). Semantic change for keyset ordering; documented. Phase 5 adds dedicated `source_*` columns if the overload proves confusing. |
| G11 | E | Amendment lineage flattened into content text; no enqueue on amendment events | `amendment_hotfix_revisions.regression_scenario_ids`, `revision_spec_id`, `original_spec_id` | `supports` Entity(amendment)->TestScenario; `supersedes` Entity(revision spec)->Entity(original spec); `kind_of=amendment` | Also add the `amendment.*` events to the consolidation enqueuer. |
| G12 | A | Unresolved `MissingLinkCandidate`s are dropped | worker output | persist per `(board, artifact, edge_type, reason)` in a small ledger and surface a KG Health `health_issues[]` row (`missing_link_backlog`) | Makes "no edge" distinguishable from "unresolvable reference". |
| G13 | A | No stale-edge removal for any new family (or for `tests`) | relational link removal | generalise `RelationalProjectionActiveSetIntent`: `WorkerResult` carries a **list** of intents; `supported_scope` gains `(spec, spec, ac_coverage)`, `(card, card, spec_links)`, `(card, card, dependencies)`, `(spec, spec, decision_supersedence)`, `(bug, bug, violations)` | Edges-only namespaces (precedent: `dependencies`): hard delete with before-images and post-delete confirmation. Rebuild must produce the same active set. |
| G14 | A | Enqueue coverage: `link_task` / traceability events re-enqueue only the spec; `card_dependencies` and `cards.test_scenario_ids` changes re-enqueue nothing | domain events | `consolidation_enqueuer._map_targets` routes link events to **both** endpoints (spec and card); dependency and scenario-link events to the card | Also fix the existing `card.linked_to_spec` branch that re-enqueues only the spec. |
| G15 | A | Edge identity ignores `rule_id`: two rules over the same `(type, from, to)` collapse and active-set cleanup by one rule can delete the other's edge | n/a | one owner per edge family: all card->spec-child edges are owned by the card-side emitter; `supports` from code evidence keeps its own from-nodes | Design rule, enforced by test. |
| G16 | R | Read surface cannot express the new shapes | graph | (a) `get_related_context`: center by prefix (`spec:<uuid>` includes children) or `include_children=true`; (b) `ContextHop` gains `node_type`, `kind_of`, `source_artifact_ref`, `direction`, `rule_id`, `confidence`; (c) `RELATED_CONTEXT_DEPTHS` += 3 in its three enforcement points; (d) `TYPED_ARTIFACT_KINDS` accepts spec-child refs; (e) new curated tools: `okto_pulse_kg_get_decision_impact`, `okto_pulse_kg_get_spec_coverage`, `okto_pulse_kg_get_bug_clusters(board_id, since_days, group_by)`, `okto_pulse_kg_get_lineage`; (f) `since`/`until` on `get_decision_history`; (g) `get_supersedence_chain(direction=ancestors\|successors)`; (h) `explain_constraint` reports lane emptiness instead of `[]`; (i) rate-limit parity for the recall family; (j) wire or remove the dead `timeout_ms` and `min_confidence` params | Each new tool: permission registry entry, `tools_catalog.md` regeneration, tool-count gates in `okto-pulse/scripts/release_artifact_gate.py`. |
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
- **L-B** Honest gate: board policy key `bug_learning_closeout` with values
  `advisory` (default: KG Health `health_issues[]` row `bugs_without_learning`)
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
- **L-G** Fan-in: the closeout (manual and automatic) must call
  `get_similar_nodes` against canonical Learnings and attach `validates` to an
  existing Learning at >= 0.85 instead of minting a duplicate, so a Learning can
  cluster bugs (feeds Q05 `group_by=learning`).
- **L-H** Community: register `cognitive_closeout_worker` behind a setting that
  defaults to enabled only when `cognitive_llm_config` is present; emit a
  startup warning and a health signal when closeout items terminate as
  `skipped_no_llm_config`.

## 6. Delivery plan

Order matters: emitters first (they make the graph true), then the schema
evolution (decision D5 puts it inside 0.3.4), then the emitters that depend on
it, then the read surface (it makes the graph useful), then lifecycle.
Execution order: 0 -> 1 -> 2 -> 5 -> 5b -> 3 -> 4 -> 6. Each phase lands as its
own PR pair (core first, then community) targeting `develop`. Every phase that
touches projection exits with `okto_pulse_kg_orphan_report = 0` (decision D4:
no node may be loose in the graph).

### Phase 0: prerequisites (core)

- P0.1 Port `tests/kg_schema_testing.py` and `kg_registry_testing.py` off the
  removed `kg_runtime` / `kuzu_graph_transaction` modules onto the Grafx
  adapters. ~89 core test files depend on it; without it no real-graph
  projection test runs, and phase 5 changes the schema, which must not be
  validated with in-memory fakes only.
- P0.2 Baseline census on the three local boards (counts per edge type, per
  rule_id, orphan report) saved under `docs/evidence/` so every later phase has
  a before/after.
- P0.3 Sequencing (decision D7, user): this initiative runs first (phases 1-3);
  the WIP branch `feature/v0.3.4-kg-replay-only-cognitive-commit` is rebased
  afterwards on the result, and its conflicts in `primitives.py` /
  `consolidation.py` are resolved in that rebase.
- P0.4 Confirm that `kgref:` resolves a working-layer target (condition of D1);
  if it is canonical-only, extend the resolver with a layer hint for `system:`
  writers.

### Phase 1: projection emitters (core), zero DDL

| Item | Files | Done when |
|---|---|---|
| G1 `tests` resolver | `application/processors/deterministic_kg.py` (AC loop, TS loop) | id-form, text-form and index-form links all resolve; `tests` edge count on Okto Neuron = 30 |
| G2+G4 card->spec-child `supports` | `deterministic_kg.py:process_card`, `consolidation.py:load_projection_inputs`, `_resolve_missing_link_candidates` | one edge per `(card, child)` from either source; bug cards logged, not emitted |
| G3 card `precedes` | `process_card` + projection inputs | 278 edges on the local DB; direction prerequisite -> dependent |
| G5 `violates` | `consolidation.py:_resolve_missing_link_candidates` | the 6 local bugs get `violates` edges; `explain_constraint.violations` non-empty |
| G6 decision `supersedes` | `process_spec` decisions loop; keep superseded nodes | chain walkable in both directions |
| G7 `implements` from `linked_rules` | `process_spec` api loop | |
| G9 `kind_of` | `process_*` emitters, registry seed in `code_traceability_kg.py`-style declaration module | every projected node carries `kind_of`; existing nodes refreshed on re-consolidation |
| G10 source `created_at` | `_card_to_dict` and siblings, `EmittedNode`, `primitives` CREATE path | rebuild/replay preserve the source time |
| G11 amendment lineage | `process_amendment`, enqueuer | |
| G13 active sets | `deterministic_kg.py:WorkerResult`, `primitives.py:585-698`, `grafx_graph_transaction.py` cleanup | removing a link removes the edge; rebuild reproduces the same active set |
| G14 enqueue | `consolidation_enqueuer.py:_map_targets` | link events reach both endpoints |
| G12 missing-link ledger | new small store + KG Health row | |
| G15 ownership rule | test in `test_kg_deterministic_worker.py` | |

Tests to extend: `test_kg_deterministic_worker.py` (edge counts, rule literals),
`test_kg_deterministic_identity.py`, `test_kg_rebuild_deterministic.py`,
`test_ska_kg_projection_active_set.py`, `test_c8_relational_projection_worker.py`,
`test_skm_spec_dependency_kg_projection.py`, `test_projection_active_set_session_cleanup.py`,
plus new `test_kg_card_link_projection.py` and `test_kg_bug_violates_projection.py`.

### Phase 2: rebuild and repair parity (core + community)

- Rebuild manifest: the source fields the new rules consume must be inside the
  content-hash column tuples (`CARD_CONTENT_COLUMNS`, `SPEC_CONTENT_COLUMNS_V*`) or
  a link change never re-enqueues. `card_dependencies` and `test_scenario_ids`
  join the card content hash. This changes every board's `content_hash` once;
  document it.
- `deterministic_projection_repair` accepts cards and bugs, not only specs
  (use case, REST DTO, admission tests).
- Structural hash: keep edges out of the hash in 0.3.4 (unchanged behaviour);
  record the decision.

### Phase 3: read surface (core MCP + community REST/UI)

- G16 (a)-(j). Templates in `cypher_templates.py`, tool DTOs in
  `tool_schemas.py`, tools in `kg_query_tools.py`, permission registry, catalog
  regeneration, tool-count gates.
- Acceptance queries (must return the expected rows on the local boards):

```cypher
// Q01: impact of a decision, one call
MATCH (d:Decision) WHERE d.source_artifact_ref = $decision_ref AND d.superseded_by IS NULL
OPTIONAL MATCH (d)-[:derives_from]->(r:Requirement)
OPTIONAL MATCH (card:Entity)-[:supports]->(r)
OPTIONAL MATCH (card)-[:supports]->(ts:TestScenario)
RETURN r.id AS requirement, collect(DISTINCT card.id) AS cards, collect(DISTINCT ts.id) AS scenarios
```

```cypher
// Q05: bugs in the last N days grouped by violated constraint
MATCH (b:Bug) WHERE b.created_at >= timestamp($since)
OPTIONAL MATCH (b)-[:violates]->(c:Constraint)
OPTIONAL MATCH (l:Learning)-[:validates]->(b)
RETURN c.id AS constraint, c.kind_of AS kind, collect(DISTINCT b.id) AS bugs, collect(DISTINCT l.id) AS learnings
ORDER BY size(bugs) DESC
```

Both must run with `include_working=true` because open bugs and in-progress
specs are working-layer; the curated tools default to `graph_layer="all"` for
these shapes and say so in their docs.

### Phase 4: Learning lifecycle (core + community)

L-A through L-H above. Order: L-E and L-A (docs, cheap, immediate effect),
L-C, L-D, L-F, L-G, L-B, L-H.

### Phase 5: schema evolution 0.5.0 -> 0.6.0 (inside 0.3.4, decision D5)

Runs after phase 2 and before phase 3, so the 0.6.0-dependent emitters and the
new read tools are built on the final schema.

Columns (core `STABLE_NODE_PROPERTIES` + community `COMMON_NODE_COLUMNS`,
`embedding` last): `severity STRING`, `source_status STRING`,
`source_created_at TIMESTAMP`, `source_updated_at TIMESTAMP`,
`resolved_at TIMESTAMP`; added to `_NODE_UPDATEABLE_ATTRS` and
`_SEMANTIC_PROJECTION_NODE_ATTRS`, treated as semantic payload by the tombstone.

Pairs (`MULTI_REL_TYPES`, no new relationship *name*): `supports (Bug,
Requirement | Constraint | Criterion | TestScenario | APIContract | Decision)`
(D4, 6 tables); `violates (Bug, Requirement)`, `violates (Bug, Criterion)` (D3,
2 tables); `derives_from (Constraint, Requirement)`, `derives_from (Requirement,
Requirement)` (G8); `derives_from (Decision, Constraint)` (origins for
`explain_constraint`). 69 -> 80 pairs, 44 -> 49 columns.

Mechanics: `SCHEMA_VERSION = "0.6.0"`; a new `grafx_schema_evolution` step
(generalise the module into a 0.3.12 -> 0.5.0 -> 0.6.0 chain or add
`grafx_schema_evolution_0_6_0.py`) with candidate rebuild and the introduced
columns/pairs declared; manifest fingerprint recomputed; pins updated in
`test_grafx_schema_bootstrap.py` (49 columns), `test_grafx_relationship_layout.py`
(16 types / 80 pairs), `test_grafx_auxiliary_indexes.py`, and the
`SCHEMA_VERSION` allowlists in core tests; structural hash flips once with
`SCHEMA_VERSION_CHANGED` (one documented operator override on promotion);
`BoardMeta.schema_version` migrates at cutover; frontend `constants/kg.ts`;
README/GLOSSARY counters; `docs/grafx-schema-evolution-0.5.0-to-0.6.0.md` and
`docs/migrations/v0.6.0.md`.

Exit criteria: the three local boards migrated; `okto_pulse_kg_schema_info`
reports 0.6.0 with 80 pairs; `okto_pulse_kg_orphan_report = 0`;
`okto_pulse_kg_health` without connectivity issues; edge census unchanged for
the pre-existing families.

### Phase 5b: emitters that depend on 0.6.0 (core)

| Item | Edge / property | Source | rule_id |
|---|---|---|---|
| G2-bug | `supports` Bug(card) -> spec child (D4) | `linked_task_ids` + `test_scenario_ids` of bug cards | same card-side emitter, `supports/spec_child_task_link@v2.0`; only the source node type differs |
| G5-ext | `violates` Bug -> Requirement, Bug -> Criterion (D3) | same origin path: FR/IR whose `linked_task_ids` contain the origin card; AC reached through the scenarios linked to the origin card | `violates/origin_task_requirement@v2.0`, `violates/origin_task_criterion@v2.0`, confidence 0.8, active set `(bug, bug, violations)` |
| G8 | `derives_from` Constraint(BR) -> Requirement(FR), Constraint(OR) -> Requirement(IR), Requirement(IR) -> Requirement(FR) | `business_rules[].linked_requirements`, `observability_requirements[].linked_integration_requirements`, `integration_requirements[].linked_requirements` | `derives_from/br_requirement_link@v2.0` and siblings; active set `(spec, spec, requirement_links)` |
| G-origins | `derives_from` Decision -> Constraint | `decisions[].linked_requirements` resolving to a TR | `derives_from/explicit_link@v2.0` (same slot, new pair); `explain_constraint.origins` stops being hard-coded `[]` |
| G-cols | `severity`, `source_status`, `source_created_at`, `source_updated_at`, `resolved_at` on every projected node | `cards.severity/status/created_at/updated_at`, `specs.*`, activity log for the move to `done` (`resolved_at`) | worker dicts carry the fields outside `raw_parts` |

Zero-orphan invariant (D4): `supports`, `violates`, `tests`, `precedes` and
`derives_from` never count as connectivity; every Bug/Entity keeps its
`belongs_to` to spec/sprint/board root; the connectivity guard and the orphan
report run in the exit census of 5b.

### Phase 6: documentation and release hygiene

G17 plus `CHANGELOG.md` entries in both repos (Keep a Changelog, prose bullets),
`ska_resource_manifest.json` regeneration for every touched resource, and a
`docs/evidence/` after-census comparing edge counts with the phase 0 baseline.

## 7. Invariants every change must respect

- `SCHEMA_VERSION` stays `0.5.0` through phases 0-2 and becomes `0.6.0` at the
  phase 5 cutover (a single bump); no new relationship *names* anywhere in the
  initiative.
- No node may be loose in the graph (decision D4): every node passes the
  connectivity guard and every projection phase exits with
  `okto_pulse_kg_orphan_report = 0`.
- New `rule_id`s follow `<edge_type>/<slot>@v2.0`; existing literals are pinned
  by tests and by the active-set scope matchers. Never rename an existing rule.
- Every emitted edge carries `layer=deterministic`, `created_by=worker_layer1`,
  non-empty `rule_id`, empty `fallback_reason`.
- Every emitted node must pass the connectivity guard (`belongs_to` to an
  Entity/Bug); `supports`, `tests`, `precedes` do not count as connectivity.
- Reference-only endpoints use `kgref:` so no partial root node is created.
- Deterministic writers are `system:`-prefixed agents; cognitive-owned names
  (`supersedes`) are emitted only through that exemption.
- Active-set removal ships with before-images, compensation and post-delete
  confirmation; rebuild and incremental reconciliation produce the same active
  set.
- Editing `agent_instructions.md` or anything under `mcp/resources/` regenerates
  `ska_resource_manifest.json` in the same commit; `workflows/kg.md` keeps its
  required headings, cross-links and the strings pinned by `test_mcp_resources.py`;
  `agent_instructions.md` stays under 500 lines and 10K tokens.
- New MCP tools update the tool-count gates in `okto-pulse/scripts/release_artifact_gate.py`
  and `tools_catalog.md`.
- Core never imports `okto_pulse.community`; traversals go through the graph
  store ports and are implemented in the Grafx adapter.

## 8. Decisions (recorded in the Pulse ideation Q&A, 2026-09-16)

| # | Decision | Recorded choice | Origin |
|---|---|---|---|
| D1 | Ownership of card -> spec-child edges | card-side (`process_card` + `load_projection_inputs`, `kgref:` to children, one active set per card) | agent recommendation, user confirmation pending |
| D2 | Source timestamp | overload `created_at` in phase 1; `source_created_at` becomes canonical in phase 5 | agent recommendation, user confirmation pending |
| D3 | `violates` semantics | origin proxy now (Bug -> Constraint) and, in 0.6.0, also Bug -> Requirement and Bug -> Criterion by the same path | **user decision** |
| D4 | Bug cards in `linked_task_ids` | bring `supports (Bug, X)` into 0.3.4 through 0.6.0; no node may be loose in the graph | **user decision** |
| D5 | Schema evolution 0.6.0 | inside 0.3.4, between phases 2 and 3 | **user decision** (agent had recommended 0.3.5) |
| D6 | Learning gate default | `advisory`, `blocking` opt-in per board | agent recommendation, user confirmation pending |
| D7 | Sequencing | this initiative first (phases 1-3), WIP replay-only rebased afterwards | **user decision** (agent had recommended harness -> WIP -> initiative) |

## 9. Out of scope (deliberately)

- Quality receipts, checklists, requirement lint, ambiguity scores: relational
  by design (SK-A), never projected.
- Mockups, knowledge bases, Q&A items as graph nodes: would need `kind_of`
  slots that compete with artifact kinds; revisit after 0.6.0.
- Delivery-evidence records (implementation/test receipts, waivers): relational
  read model is authoritative; the graph gets only the resulting `supports`.
- Structural-hash inclusion of edges (rebuild promotion guard): unchanged.
