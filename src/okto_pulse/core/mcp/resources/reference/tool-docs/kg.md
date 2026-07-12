---
version: "1.0"
---

# Tool docs — `kg`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_kg_abort_consolidation`

Covered fully by the live tool description.

## `okto_pulse_kg_add_edge_candidate`

Covered fully by the live tool description.

## `okto_pulse_kg_add_node_candidate`

Add a node candidate to an open consolidation session.

The candidate stays in-memory until commit_consolidation or expiry.
candidate_id must be unique within the session.

**Writer-path ownership (allowlist):** the cognitive consolidation path may only create
`Decision`, `Learning`, `Alternative`, `Assumption`. `Criterion` (from acceptance
criteria) and `Constraint` (from technical requirements / business rules) are
**deterministic-only** — materialized by the deterministic worker, not by this tool.
Reference an existing deterministic `Criterion`/`Constraint` node by id (or wait for the
deterministic worker); do not recreate it on the cognitive path. A `Criterion`/`Constraint`
candidate proposed here is rejected **before any graph mutation** with
`status=source_type_not_supported`, `reason=writer_not_connectivity_owner` (distinct from a
missing-connectivity failure); remediation: remove the candidate, abort/recreate the
session without it, or route through the deterministic owner.

For cognitive closeout, create a `Decision` only when the source artifact
contains a real choice and you can add a valid judgement edge for it. Do not add
an artificial Decision to satisfy connectivity. If the closeout only captures
uncertainty, rejected paths, risks, or contextual notes, prefer `Assumption` or
`Alternative` and include a precise `source_artifact_ref` such as
`spec:<spec_id>:assumption:<stable_id>` or
`spec:<spec_id>:alternative:<stable_id>`.

Args:
    session_id: Session from begin_consolidation
    candidate: Dict with candidate_id, node_type, title, content, etc.

Returns:
    JSON with accepted=true and node_count_in_session

## `okto_pulse_kg_begin_consolidation`

Covered fully by the live tool description.

## `okto_pulse_kg_commit_consolidation`

Covered fully by the live tool description.

## `okto_pulse_kg_canonical_debt_list`

List canonical-debt ledger rows for a board.

Use this when `okto_pulse_kg_health` reports `canonical_debt.open_count > 0`
and an agent needs to inspect which artifacts are pending, blocked, failed,
or retry-scheduled. The tool is read-only and mirrors the REST canonical-debt
list projection.

Args:
    board_id: Board UUID.
    artifact_type: Optional filter such as `spec`, `task`, `test`, or `bug`.
    state: Optional canonical_state filter such as `pending`, `failed`,
        `blocked`, or `retry_scheduled`.
    limit: Max rows to return (1-200, default 50).
    offset: Skip first N rows (>=0, default 0).

Returns:
    JSON `{board_id, items, counts, total, limit, offset}`. Each item includes
    artifact identity, source_ref, target_status, canonical_state, failure
    reason, last_error, retry metadata, queue/DLQ refs, and evidence_ref.

## `okto_pulse_kg_canonical_partition_integrity_list`

List canonical Learning partition-integrity signals for KG health drill-down
(R7). READ-ONLY: cognitive holds, canonical debt, mixed-evidence deferred and
provenance-only Learnings. Each item carries an S-KG-02 `classification`
(missing_source, unresolved_source, canonical_learning_resolved,
weak_provenance, invalid_orphan_learning) plus a `classification_counts`
census. Mirrors REST `GET /api/v1/kg/{board_id}/canonical-partition-integrity`
(same `classification` on the per-node detail).

This tool NEVER skips, clears or resolves an R7 hold/debt — that remains
human-only.

Args:
    board_id: Board ID.
    reason_code: Optional reason-code filter.
    graph_layer: Optional graph-layer filter.
    source_ref: Optional `<type>:<id>` source reference filter.
    node_id: Optional node ID filter.
    status: Optional status filter.
    limit: Max rows to return (default 50).
    offset: Page offset (default 0).

Returns:
    JSON with partition-integrity items, `classification_counts`, and bounded
    counts.

## `okto_pulse_kg_originates_from_contract_audit`

Read-only advisory audit for persisted `originates_from` KG edges whose endpoint
labels violate the Bug->Entity contract.

Use this when validating historical KG hygiene. Known endpoint pairs outside
Bug->Entity are returned as high-confidence advisory findings; missing endpoint
types are returned as low-confidence warnings. The tool never mutates, rebuilds,
reprocesses, skips, or remediates graph data.

Args:
    board_id: Board ID.
    limit: Max findings to return (1-200, default 50).
    offset: Page offset.
    include_ok: Include contract-satisfying edges in `items` when true.

Returns:
    JSON `{board_id, relationship_type, contract, status, items, counts, total,
    scanned, limit, offset, read_only, mutated}`. Each item includes relationship
    id, source/target ids, known endpoint types, classification, confidence,
    reason, path, and `mutated=false`.

## `okto_pulse_kg_dead_letter_list`

Prose covered by the live tool description. Delta:

Args:
    board_id: Board UUID
    limit: Max rows to return (1-200, default 50)
    offset: Skip first N rows (>=0, default 0)

Returns:
    JSON `{rows, total, limit, offset}` on success. `{error: "..."}`
    on auth or permission failure.

## `okto_pulse_kg_dead_letter_reprocess`

Prose covered by the live tool description. Delta:

Args:
    board_id: Board UUID.
    dead_letter_ids: Optional multi-value DLQ row IDs
        (formats: okto-pulse://reference/multivalue). Empty means "oldest
        rows for this board up to limit".
    limit: Max DLQ rows to requeue (1-200, default 50).
    process_now: "true" to immediately run one consolidation worker batch
        after requeueing; "false" to only mark rows pending.

Returns:
    JSON with selected/requeued/already_queued counts and, when
    process_now is true, the worker batch processed count.

## `okto_pulse_kg_connectivity_dlq_diagnose`

Diagnose the LIVE connectivity-guard `technical_dlq` class (RKG-04) before any
reprocess. Read-only.

The class is every dead-letter row whose terminal error is `KG node
connectivity guard rejected the commit before graph mutation` (the recurring
cognitive-closeout failure that RKG-02 fixes at the root). Returns each member's
`dead_letter_id`, `artifact_id`, `attempts`, `errors`, `last_error`, the
`source_artifact_ref` involved, the `probable_root_cause`, the `next_action` and
a `remediation` hint — the input you must feed to
`okto_pulse_kg_connectivity_dlq_reprocess`, which only accepts in-class ids.

Args:
    board_id: Board UUID.

Returns:
    JSON `{board_id, dlq_class, count, items, dead_letter_ids}`.

## `okto_pulse_kg_connectivity_dlq_reprocess`

Fail-closed reprocess of the connectivity-guard `technical_dlq` class (RKG-04).

Unlike the generic `okto_pulse_kg_dead_letter_reprocess`, this NEVER does a broad
reprocess: it requires EXPLICIT in-class `dead_letter_ids` (from
`okto_pulse_kg_connectivity_dlq_diagnose`) and blocks — removing NO DLQ — when the
selection is empty (`no_dlq_selected`), missing (`selected_dlq_missing`),
out-of-class (`selected_dlq_out_of_class`), the RKG-02/RKG-03 root-cause fixes are
absent (`rkg02_rkg03_not_applied`) or the KG is quarantined (`kg_quarantined`). On
success it reuses the idempotent DLQ→ConsolidationQueue path (queue dedup).

Args:
    board_id: Board UUID.
    dead_letter_ids: REQUIRED in-class DLQ row IDs (native list, JSON array
        string, or pipe-separated string). Empty is blocked, never "all".
    process_now: "true" to run one consolidation worker batch after requeueing.

Returns:
    JSON. When blocked: `{success: false, blocked: true, removed_dlq: false,
    reasons, preconditions}`. On success: selected/requeued/already_queued counts
    + optional worker batch info.

## `okto_pulse_kg_connectivity_dlq_verify`

Prose covered by the live tool description. Delta:

Args:
    board_id: Board UUID.
    artifact_refs: Optional `type:id` refs to scope the check (formats:
        okto-pulse://reference/multivalue). Empty checks the whole class.

Returns:
    JSON `{class_cleared, remaining_count, remaining_dlq}`.

## `okto_pulse_kg_health_readiness`

Canonical NON-MASKABLE health/readiness projection (RKG-05; gemelar do REST
`GET /api/v1/kg/health-readiness`). The single source the health/readiness/MCP/UI/
report surfaces share, so a technical blocker is never hidden by a summary view or
a cognitive skip.

Both `profile=summary` and `profile=full` expose:
- `technical_signals` — scalar counters `dead_letter_count`, `technical_dlq_count`,
  `canonical_debt_open_count`, `active_queue_count`. These are SEPARATE
  operational domains: one count is never inferred from another (e.g.
  `active_queue_count` is not derived from `dead_letter_count`).
- `readiness` — `blocking` (a technical problem IS visible) vs `would_block_done`
  (whether the gate would actually block; `false` under advisory enforcement),
  plus `reasons` and `policy_reason`.
- top-level `cognitive_enforcement_mode` (`advisory`/`blocking`) and
  `enforcement_active`.
- `non_maskable_items` — one entry per OPEN technical item with `artifact_ref`,
  `source_ref`, `signal`, `last_error`, `error_text`, `next_action`,
  `remediation` and `drill_down_tool`. A cognitive skip/no_action can never
  reduce this list (it is derived from health, not from the cognitive verdict).

`profile=full` ADDS the prose `health_issues` + `root_cause`. An invalid profile
returns `invalid_profile` (HTTP 400 on REST). Optional `artifact_ref` scopes
`non_maskable_items`.

Args:
    board_id: Board UUID.
    profile: "summary" (default) or "full".
    artifact_ref: Optional `type:id` ref to scope `non_maskable_items`.

Returns:
    JSON `{board_id, profile, overall_state, cognitive_enforcement_mode,
    enforcement_active, technical_signals, readiness, non_maskable_items,
    operational_domains, [health_issues, root_cause]}`.

## `okto_pulse_kg_explain_constraint`

Covered fully by the live tool description.

## `okto_pulse_kg_find_contradictions`

Covered fully by the live tool description.

## `okto_pulse_kg_find_similar_decisions`

Covered fully by the live tool description.

## `okto_pulse_kg_get_decision_history`

Trace decisions about a topic/module over time. Returns decisions
matching the topic with their supersedence chain.

Args:
    board_id: Board ID
    topic: Topic or keyword to search for. Accepts natural-language
        phrases when ``use_semantic=True`` (paraphrases like
        "cache strategy" vs "caching approach" surface related hits).
    min_confidence: Minimum confidence threshold (default 0.5)
    max_rows: Maximum results (default 100)
    use_semantic: When True (default), embed the topic and query the
        Decision HNSW index first, then backfill with title-CONTAINS
        matches. Set False for deterministic string-only search.
    min_similarity: Cosine similarity floor for semantic hits
        (default 0.3; range 0.0–1.0).

Returns:
    JSON with decisions list. Semantic hits are ordered by similarity
    (best first); title-CONTAINS fallbacks retain relevance_score
    ordering.

## `okto_pulse_kg_get_learning_from_bugs`

Covered fully by the live tool description.

## `okto_pulse_kg_get_related_context`

Given an artifact, return its neighborhood in the KG: prior
decisions, applicable criteria, similar bugs, discarded alternatives.
Supports impact-analysis filters so an agent can scope traversal to
a specific edge set or direction.

Args:
    board_id: Board ID
    artifact_id: Source artifact reference (source_artifact_ref)
    min_confidence: Minimum confidence (default 0.5)
    max_rows: Maximum results (default 100)
    rel_types: Comma- or pipe-separated edge types to restrict the
        first hop (e.g. ``"supersedes,contradicts"`` or
        ``"tests|relates_to"``). Empty = any type.
    direction: ``"both"`` (default), ``"outgoing"``, or ``"incoming"``.
        Applied to hop1 only; hop2 is always undirected.
    max_depth: ``1`` returns center+hop1 only (hop2 fields null);
        ``2`` (default) returns the full 2-hop context.

Returns:
    JSON with 2-hop neighborhood context

## `okto_pulse_kg_get_similar_nodes`

Covered fully by the live tool description.

## `okto_pulse_kg_get_supersedence_chain`

Covered fully by the live tool description (args: board_id, decision_id,
node_type — node_type defaults to Decision).

## `okto_pulse_kg_health`

Payload shape covered by the live tool description. Delta:

Use it before kicking off long consolidations (high queue_depth means
your enqueue may sit pending), after flagging contradictions (spike in
contradict_warn_count = curator should reconcile), or to debug flat
ranking (default_score_ratio > 0.7 = scoring not differentiating).

Args:
    board_id: Board ID (uuid)
    profile: "summary" (default, slim) or "full"/"legacy" (all diagnostics).

Returns:
    JSON health snapshot, or {"error": "..."} on auth/not-found.

## `okto_pulse_kg_digest_layer_mismatch_list`

List nodes whose digest/materialization layer metadata is inconsistent.

Args:
    board_id: Board ID.
    limit: Max rows to return.
    offset: Page offset.

Returns:
    JSON with mismatch rows, expected/actual layer fields, and counts.

## `okto_pulse_kg_stale_canonical_parity_list`

List canonical nodes whose parity with working/source materialization is stale.

Use this after migrations or rebuilds to inspect stale canonical parity without
mutating the graph.

Args:
    board_id: Board ID.
    limit: Max rows to return.
    offset: Page offset.

Returns:
    JSON with stale parity rows and diagnostic metadata.

## `okto_pulse_kg_orphan_report`

Return a bounded safe orphan-node report for a board KG.

The payload intentionally exposes safe identifiers and aggregate diagnostics
only: board_id, generation_id, orphan counts, safe samples, unresolved reasons,
backfill summary, and correlation_id. It does not return raw node text,
embeddings, prompts, or payload bodies.

Args:
    board_id: Board ID.
    generation_id: Optional KG generation id.
    limit: Max safe sample count, clamped by the server.

Returns:
    JSON safe orphan report, or a structured graph-unavailable payload.

## `okto_pulse_kg_orphan_backfill`

Run explicit orphan backfill for structurally resolvable nodes.

Defaults to dry_run=true. The tool refuses writes when KG Health is
`recovery_needed` or `quarantined`, so operators use the recovery flow instead
of mutating a degraded graph.

Args:
    board_id: Board ID.
    generation_id: Optional KG generation id.
    dry_run: true to preview, false to write resolvable edges.
    node_ids: Optional multi-value node IDs as a native list, JSON array, or
        pipe-separated string.
    limit: Max nodes to inspect, clamped by the server.

Returns:
    JSON backfill summary with dry_run, detected, connected, unresolved,
    ambiguous, semantic_pending, and correlation_id.

## `okto_pulse_kg_list_alternatives`

Covered fully by the live tool description.

## `okto_pulse_kg_list_cognitive_pending_items`

KG-03.2 — List cognitive pending items by board + generation.

Implements api_ae3a932a:

    request: board_id, kg_generation_id?, status?, limit?, offset?
    response (success): board_id, selected_kg_generation_id,
                        legacy_mode, counts, items
    errors: unauthorized | invalid_status | generation_not_found

Resolves to the latest recorded generation when ``kg_generation_id``
is omitted. When ``kg_generation_id`` is explicitly provided and
the record does not exist, returns a typed ``generation_not_found``
error (Codex audit val_ead80fbd).

Items use a strict API projection (``project_item_for_api``) that
exposes only the contract-defined fields. Storage-only fields
(board_id, kg_generation_id, event_ref, free-text ``reason``) are
never echoed.

Args:
    board_id: Target board id (required, non-empty).
    kg_generation_id: Optional KG generation UUID v4. When omitted
        the store's ``latest_generation(board_id)`` is used.
    status: Optional status filter from the bounded enum
        {pending, in_progress, consolidated, skipped, failed}.
    limit: Page size, 1..200, default 100.
    offset: Page offset, ≥ 0, default 0.
    status_filter: Deprecated compatibility alias for ``status``;
    ``status`` takes precedence when both are provided.

## `okto_pulse_kg_list_cognitive_readiness_items`

List board cognitive-readiness rows: cognitive items, canonical debt and
technical DLQ, reconciled by normalized `artifact_id`. Rows mirror the central
CognitiveReadinessService verdict; the cognitive `reason_code` stays distinct
from the technical `error_cause`. `would_block_done` is enforcement-aware
(false under advisory enforcement even when a blocker is visible).

Use this to inspect outstanding cognitive closeout work before advancing a bug,
spec, or refinement through a gate.

Args:
    board_id: Board ID.
    signal: Optional signal filter (default `all`).
    artifact_id: Optional normalized artifact ID filter.
    source_ref: Optional `<type>:<id>` source reference filter.
    reason_code: Optional bounded cognitive reason-code filter.
    status: Optional readiness status filter.
    search: Optional free-text search.
    limit: Max rows (<=200, default 50).
    offset: Page offset (default 0).
    kg_generation_id: Optional KG generation to scope the listing.

Returns:
    JSON with readiness items, counts, and source references.

## `okto_pulse_kg_evaluate_cognitive_readiness`

Evaluate ONE artifact's cognitive readiness via the central
CognitiveReadinessService. Returns the 6-tier verdict verbatim
(`readiness_effect`, `blocking`, `tier`, `readiness_signal`, `reason_code`,
`revisit_at`, `precedence_explanation` = the blocked-by source) — precedence
is NEVER recomputed here. `would_block_done` is enforcement-aware (see the
list tool).

Args:
    board_id: Board ID.
    source_ref: `<type>:<id>` reference of the artifact to evaluate. A
        `bug:<uuid>` reconciles to its `card:<uuid>`. task/test carry no
        reusable cognition -> advisory.
    kg_generation_id: Optional KG generation to evaluate against.

Returns:
    JSON with the readiness verdict, blockers, skip state, and remediation text.

## `okto_pulse_kg_evaluate_bug_cognitive_closure`

Read-only bug cognitive-closure evaluation. Mirrors the REST/UI classifier and
the central CognitiveReadinessService verdict; it does not recompute precedence.

Allowed agent actions: `evaluate` (default) and `create_learning`. Agent-facing
`skip`/`no_action` fails closed with `human_control_required` and never writes
the ledger — human skip/no_action stays on the authorized UI/REST path and
cannot mask technical debt.

Args:
    board_id: Board ID.
    bug_id: Bug card ID.
    evidence: Optional evidence payload for the evaluation.
    requested_action: `evaluate` (default) or `create_learning`; agent-facing
        `skip`/`no_action` fails closed.
    reason_code: Bounded reason code (only meaningful with a skip request,
        which is human-only here).
    justification: Optional justification text.
    evidence_refs: Optional evidence references.
    revisit_at: Optional revisit timestamp.

Returns:
    JSON with closure readiness, missing cognitive items, and gate outcome.

## `okto_pulse_kg_record_cognitive_skip`

Agent-facing cognitive skip/no_action control — HUMAN-only (R5-IMP1). This MCP
surface fails closed with `human_control_required` (mutation_allowed=false,
state_changed=false): it performs NO state change and never writes the ledger
or the KG. A human operator records the skip via the IDE control or the human
REST/UI surface, which keeps the canonical validations (invalid reason,
missing revisit date, technical-debt masking).

Args:
    board_id: Board ID.
    source_ref: `<type>:<id>` reference of the target artifact.
    reason_code: Bounded cognitive reason code.
    justification: Optional justification text.
    evidence_refs: Optional evidence references.
    revisit_at: Optional revisit timestamp.
    kg_generation_id: Optional KG generation.

Returns:
    JSON `human_control_required` envelope (read-only; no mutation).

## `okto_pulse_kg_clear_cognitive_skip`

Clear a cognitive skip / no_action, reopening the item to pending via the
central ledger path — but HUMAN-only (R5-IMP1): clearing/reopening a cognitive
skip is a human decision and is NOT applicable from the agent-facing MCP
surface. This tool fails closed with `human_control_required`
(mutation_allowed=false, state_changed=false) and never reopens the ledger
item. A human operator clears the skip via the IDE control or the human REST
surface (ledger-only — no KG mutation; the clearing actor + timestamp are
stamped and the stale reason_code / revisit_at are dropped).

Args:
    board_id: Board ID.
    source_ref: `<type>:<id>` reference of the target artifact.
    kg_generation_id: Optional KG generation.

Returns:
    JSON `human_control_required` envelope (read-only; no mutation).

## `okto_pulse_kg_list_cognitive_dlq`

List cognitive-readiness dead-letter or failed extraction items.

Args:
    board_id: Board ID.
    limit: Max rows.
    offset: Page offset.

Returns:
    JSON with cognitive DLQ rows, error reason codes, and counts.

## `okto_pulse_kg_queue_drilldown`

Drill down into the ACTIVE operational queue depth (R6-IMP2). Read-only.

Use this when `okto_pulse_kg_health` reports an `active_queue` backlog (a
health issue with `drill_down_tool='okto_pulse_kg_queue_drilldown'`) and you
need to know WHERE the queue depth comes from. This is the ACTIVE queue only:
dead-letter (DLQ), outbox dead_letter and canonical debt are TERMINAL and
intentionally NOT counted here — inspect those via
`okto_pulse_kg_dead_letter_list` / `okto_pulse_kg_canonical_debt_list`.

Args:
    board_id: Board ID.

Returns:
    JSON with `worker_mode`, `total_active_depth`, an overall `classification`
    (transient | stuck | backpressure | idle) and per-source breakdowns:
    `consolidation_queue` (pending/claimed by status + by artifact category +
    oldest_age_seconds) and `global_update_outbox` (pending retry-window depth
    + oldest_age_seconds).

## `okto_pulse_kg_migrate_schema`

Prose covered by the live tool description. Delta:

Args:
    board_id: Board UUID específico (mutuamente exclusivo com all_boards)
    all_boards: Se True, migra todos os boards conhecidos do server.
        Default False — exige board_id.

Returns:
    Single board: JSON `{board_id, migrated, columns_added, errors,
    duration_ms}`. All-boards: `{results: [<single>, ...]}`.
    Erro de input: `{error: "missing_board_or_all_boards"}`.

## `okto_pulse_kg_propose_reconciliation`

Covered fully by the live tool description.

## `okto_pulse_kg_query_cypher`

> **Equivalence fold exclusion (spec MKG-C-S1):** raw Cypher results are
> NOT folded by the equivalence ledger — rows may expose MEMBER node ids
> of active merges until the physical materialization inside the
> deterministic rebuild. The curated recall surfaces (related context,
> similar nodes, contradictions, natural query) fold members into their
> survivor automatically.


Execute a read-only Cypher query directly against a board's graph.

Safety rails applied automatically:
- Parser whitelist rejects write keywords (CREATE/DELETE/SET/etc)
- Comment stripping + unicode normalization
- Auto-inject LIMIT if missing; variable-length paths bounded to *..20
- Timeout 5s default, 30s max; rate limit 30 queries/min per agent
- Embedding/vector columns and nested embedding fields are STRIPPED
  from the response (RETURN n / RETURN n.embedding never dump 384-float
  vectors into your context); see response.sanitization.stripped_fields
- Rows are bounded to an agent-safe page; numeric scores are rounded

Args:
    board_id: Board ID
    cypher: Read-only Cypher query string
    params: Optional parameter dict for parameterized queries
    max_rows: 0 = agent-safe default (50). Pass 1..1000 for an explicit
        bounded page; >1000 is rejected (max_rows_exceeds_hard_cap).
    timeout_ms: Timeout in ms (default 5000, max 30000)
    include_working: Optional boolean. Default false enforces canonical-only
        visibility. Pass true to query working + canonical rows during working
        graph validation, rebuild checks, or E2E ingestion tests.

Layer contract:
    Node rows use `graph_layer` as the persisted node property. Do not query
    `kg_layer` on nodes; `kg_layer_counts` appears only in KG health payloads.
    This tool scopes layer visibility with `include_working` (boolean), NOT a
    `graph_layer` selector — the `graph_layer` canonical|working|all selector
    applies to `okto_pulse_kg_query_global` and `okto_pulse_kg_get_related_context`.

Schema-safe queries:
    Properties are NOT universal across labels in semantics — introspect with
    `okto_pulse_kg_schema_info` first and query ONLY the `stable_properties` it
    lists per label (e.g. `id`, `title`, `content`, `graph_layer`,
    `source_confidence`, `relevance_score`). There is no `name` property — use
    `title`. Never assume an ad-hoc property exists on a label.

Returns:
    JSON with rows, row_count, truncated, row_bounds, sanitization,
    execution_time_ms, query_state, canonical_filter_enforced,
    working_omitted_count

## `okto_pulse_kg_query_global`

Covered fully by the live tool description. Delta: the default
`graph_layer=canonical` never leaks working nodes; each result also carries
its own `graph_layer`.

## `okto_pulse_kg_query_natural`

Natural language search over the board's knowledge graph. Uses hybrid
search (embedding + HNSW + traversal). Falls back to string match if
embedding is unavailable.

Does NOT invoke any LLM — all processing is deterministic (embedding
model is a local embedding model or stub).

Args:
    board_id: Board ID
    nl_query: Natural language query
    limit: Max results (default 20)
    min_confidence: Min confidence threshold (default 0.5)
    since: Optional ISO-8601 timestamp — return only nodes with
        ``created_at >= since``. Empty string = no lower bound.
        Invalid timestamps are ignored (best-effort).
    until: Optional ISO-8601 timestamp — return only nodes with
        ``created_at <= until``. Empty string = no upper bound.

Returns:
    JSON with nodes, total_matches, optional warning. When a temporal
    filter is active the response also carries ``temporal_filter``
    metadata (candidates_before_filter, filtered_out).

## `okto_pulse_kg_query_reflective`

Covered fully by the live tool description.

## `okto_pulse_kg_schema_info`

Summary and Args covered by the live tool description. Delta:

Returns:
    JSON with schema_version, stable_node_types, stable_rel_types,
    vector_indexes, label_properties, optionally internal_*_types.

    `label_properties` (R6-IMP3) maps each canonical node label to its
    `stable_properties` (the schema-guaranteed scalar properties — the SAME set
    on every label, since all node tables share the common attributes) plus
    `has_vector_index`. Query ONLY these stable properties; never assume an
    ad-hoc/universal property. There is no `name` property — use `title`/`content`.
    Use this map to write schema-safe Cypher (okto_pulse_kg_query_cypher).

## `okto_pulse_kg_tick_run_now`

Prose covered by the live tool description. Delta:

Args:
    board_id: Optional board UUID. Empty string = global tick (all boards).
    force_full_rebuild: When true, resets last_recomputed_at to NULL
        for all nodes in scope before the tick — ignores staleness.

Returns:
    JSON with `{tick_id, status: "running", scheduled_at}` on 202 success.
    On 409 (lock held), `{error: "tick_already_running", message: "..."}`.
    On auth failure, `{error: "..."}`.

## `okto_pulse_kg_update_cognitive_pending_item`

KG-03.3 — Mutate exactly one cognitive consolidation item.

Implements api_525a25f1:

    request: board_id, kg_generation_id, item_id, status,
             consolidation_session_id?, reason?, summary_text?
    response (success): board_id, kg_generation_id, item,
                        counts, updated
    errors: unauthorized | item_not_found |
            consolidation_session_required | reason_required |
            invalid_status | unsafe_payload

Invariants enforced BEFORE the storage write:

* br_689bdf14 — ``status=consolidated`` requires a non-empty
  ``consolidation_session_id`` that references a prior
  ``commit_consolidation`` workflow session (ir_d52c3279). The
  MCP write tool only records the reference; the actual cognitive
  KG nodes still flow through the existing seven consolidation
  primitives (``begin_consolidation`` … ``commit_consolidation``).
* br_f9823bad — ``status=skipped`` or ``status=failed`` require a
  non-empty ``reason`` (human-readable, bounded length).
* br_858a0859 — Reject token shapes and oversized narrative
  fields as ``unsafe_payload`` so raw artifact bodies never
  land in the ledger.
* br_d544da65 — Single-item atomic update via
  ``CognitiveConsolidationItemStore.update_item``. Other items in
  the generation remain unchanged and aggregate counts are
  recomputed by the store.

Counter ``kg_cognitive_item_update_total`` (or_174f18d5) emits
exactly one bounded sample per call with labels
``(board_id, target_status, outcome, reason_code)``. Free-text
``reason`` is NEVER labelled; ``reason_code`` is bounded.

## `okto_pulse_kg_rebuild_preflight`

Run the KG rebuild preflight for a board — gemelar do REST POST /api/v1/kg/rebuild/preflight.

Executes the pre-rebuild check (read-only, TR13): enumerates real sources
via BoardSourceStore (the relational store), classifies the KG health state, and persists
the immutable manifest needed for /confirm.

**Admission gate (FR8):** refuses with `rebuild_refused_quarantined` when
`graph_state == 'quarantined'`. `recovery_needed` IS ADMITTED — rebuild is
the prescribed exit from that state (see the stop-rule exception in
`agent_instructions.md`).

**Flow:** call `okto_pulse_kg_health` first. If `overall_state == quarantined`
stop. Otherwise: preflight → confirm → run.

Args:
    board_id: UUID of the board to preflight.

Returns:
    JSON with `outcome`, `action_required`, `base_state`,
    `eligible_source_count`, `preflight_hash`, `manifest_ref`,
    `source_set_hash`. Pass `manifest_ref` + `preflight_hash` to
    `okto_pulse_kg_rebuild_confirm`.

Errors:
    `rebuild_refused_quarantined` — graph is quarantined; use KG reset flow first.
    `preflight_enumerate_failed` — source enumeration failed (detail in response).
    `preflight_service_failed` — preflight service error (detail in response).
    `preflight_manifest_failed` — manifest persistence failed (detail in response).

## `okto_pulse_kg_rebuild_confirm`

Emit the single-use confirmation token for a rebuild — gemelar do REST POST /api/v1/kg/rebuild/confirm.

Loads the manifest persisted in /preflight via `manifest_ref` (NEVER
re-enumerates), verifies that `preflight_hash` matches, and issues the
confirmation token. Pass the token to `okto_pulse_kg_rebuild_run`.

Args:
    board_id: UUID of the board (same used in /preflight).
    operation: Canonical operation (e.g. `'rebuild'`).
    preflight_hash: SHA-256 hex received from /preflight (64 chars).
    manifest_ref: Manifest identifier received from /preflight.

Returns:
    JSON with `confirmation_id`, `manifest_ref`, `source_set_hash`,
    `expires_at`. Pass `confirmation_id` to `okto_pulse_kg_rebuild_run`.

Errors:
    `unsupported_operation` — operation not in canonical set.
    `operation_pending_implementation` — operation valid but not yet implemented.
    `invalid_preflight_hash` — hash format invalid.
    `manifest_not_found` — manifest_ref does not exist on disk.
    `manifest_board_mismatch` — manifest_ref belongs to a different board.
    `preflight_hash_mismatch` — hash does not match manifest binding.
    `confirm_failed` — unexpected error (detail in response).

## `okto_pulse_kg_rebuild_run`

Execute the KG rebuild — gemelar do REST POST /api/v1/kg/rebuild/run.

Consumes the single-use token emitted by `okto_pulse_kg_rebuild_confirm`
and executes the full rebuild under the admin lane KG-01. NEVER mutates
the graph if the token is invalid, the manifest has changed, or the
exclusive lock cannot be acquired.

**Admission gate (FR8):** re-checks quarantine even before consuming the
token. `recovery_needed` IS ADMITTED.

Args:
    board_id: UUID of the board.
    confirmation_id: Token emitted by /confirm.
    operation: Canonical operation (must match /confirm).
    preflight_hash: SHA-256 hex (must match /confirm).
    manifest_ref: Manifest identifier (must match /confirm).
    reason: Human-readable description for audit (max 512 chars).

Returns:
    JSON with `run_id`, `outcome`, `reason`, `audit_ref`,
    `previous_kg_generation_id`, `current_kg_generation_id`,
    `started_at`, `finished_at`, `affected_files`, `report_ref`,
    `report_id`, `publishable_status`, `promotion_outcome`,
    `operator_action`, `event_emitted`.

Errors:
    `rebuild_refused_quarantined` — graph is quarantined; use KG reset flow first.
    `rebuild_run_failed` — unexpected error during rebuild (detail in response).

## `okto_pulse_kg_quarantine_restore`

KG quarantine restore — dry-run/apply with backup-swap (KGD-01 FR4/BR4).

`apply=false` (default) returns the auditable plan (files, destinations,
conflicts, sizes) with NO mutation. `apply=true` moves the board's live files
into a NEW quarantine with manifest (`backup_quarantine_id` in the result),
copies the snapshot back, validates the board open, and emits
`kg.quarantine.restore_dry_run` / `kg.quarantine.restored`.

Args:
    quarantine_id: Quarantine ID to restore from.
    apply: false (default) = dry-run plan only; true = execute the restore
        with backup-swap.

Returns:
    JSON `{plan, applied, backup_quarantine_id?}`.

Errors:
    `quarantine_not_found` — quarantine id does not exist.
    `board_locked` — require a maintenance window before applying.
    `partial_restore` — the manifest records the exact state for rollback;
    never a silent half-restored board.

## `okto_pulse_kg_export_jsonld`

Read-only JSON-LD export of a board graph (spec MKG-E-S1 / FR5-FR6).

Fixed PROV-O mapping: nodes → `prov:Entity` with `pulse:nodeType` /
`pulse:kindOf`; `source_artifact_ref` → `prov:wasDerivedFrom`; session →
`prov:wasGeneratedBy`; agent → `prov:wasAttributedTo`; supersedence →
`prov:wasRevisionOf` on the successor. Edges are typed `pulse:Edge`
entries. Deterministic: stable ordering + sorted keys — the same board
always serializes to the same bytes.

Paged by a stable `node_id` cursor: pass `next_cursor` until
`last_page=true`; the concatenation of pages is the full export. An
unreadable graph returns `kg_export_failed` and never a partial document.
The CLI twin (`okto-pulse kg export --output`) writes the full document
atomically offline. REST is deliberately absent (spec decision D7).

## `okto_pulse_kg_provenance_drift`

Read-only artifact→node drift report (spec MKG-B-S1 / FR7).

Compares each node's persisted `source_content_hash` (stamped at commit
with the session recipe) against the latest consolidation audit of the
same artifact and the artifact's current existence via the board source
reader. Reasons: `content_changed` (anchor stale vs last consolidated
state, or artifact edited after the last consolidation) and
`artifact_missing` (source deleted — terminal). The remedy is a normal
re-consolidation (the NC-8 provenance restamp clears the flag); the
tool never mutates the graph.

Args:
    board_id: Board ID.
    node_type: Optional — narrow the scan to one node table.

Returns:
    JSON: `checked_count`, `skipped_count`, `drifted_count`,
    `drifted_by_reason` (`content_changed` / `artifact_missing`),
    `drifted` (node_id, node_type, source_artifact_ref,
    persisted_hash, current_hash, reason; capped at 200 with
    `truncated` flag).

## `okto_pulse_kg_verify_grounding`

Verify that an agent answer is grounded in the retrieved KG nodes.

Deterministic entity check only in this V1 — matches entity names
against retrieved row titles via normalized exact match (NFKD +
strip diacritics + lowercase) with Jaccard fallback (threshold
0.7). Semantic grounding via LLM is available programmatically
via the Python API `verify_grounding(..., extractor_fn=,
grounder_fn=)` but not exposed over MCP (no LLM wired here).

Ideação d3dfdab8. Enforcement is decoupled — this tool returns
the verdict; the caller (agent, UI, critic loop) decides what to
do with it.

Args:
    board_id: Board ID for authorization (kg.query.global).
    answer_text: The agent's response to verify.
    retrieved_rows_json: JSON string — list of
        `{"node_id": ..., "title": ..., ...}` rows the answer
        was based on.
    pre_extracted_entities_json: Optional JSON array of strings
        listing the entity names the caller wants to check. If
        empty, falls back to heuristic extraction (quoted terms
        and capitalised multi-word phrases).

Returns:
    JSON with the GroundingResult fields: overall_grounded,
    confidence, hallucinated_entities, unsupported_claims,
    attribution_map.

Raises:
    ValueError: if retrieved_rows_json is not valid JSON.
