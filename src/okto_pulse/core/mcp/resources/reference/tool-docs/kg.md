---
version: "1.0"
---

# Tool docs — `kg`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## Authorization and board scope

Every board-scoped tool authenticates the request and resolves the caller's
effective `AgentContext` for that exact board before opening a graph, Unit of
Work, ledger, embedding provider, or writer lock. Session tools first resolve
session ownership, derive its board, and then apply that board's ACL. A denied
request is fail-closed and has no mutation side effect.

Required permission by affected family:

| Tool/family | `required_permission` |
|---|---|
| begin / add node / add edge / get similar / propose / commit / abort consolidation | `kg.session.begin` / `kg.session.add_node` / `kg.session.add_edge` / `kg.session.get_similar` / `kg.session.propose` / `kg.session.commit` / `kg.session.abort` |
| list cognitive pending items | `board.read` |
| update cognitive pending item | `kg.session.commit` |
| decision history / related context / supersedence / contradictions / similar decisions / constraint explanation / alternatives / learning from bugs | `board.read` plus the matching `kg.query.decision_history`, `kg.query.related_context`, `kg.query.supersedence_chain`, `kg.query.contradictions`, `kg.query.similar_decisions`, `kg.query.constraint_explain`, `kg.query.alternatives`, or `kg.query.learning_from_bugs` |
| global intent query | global `kg.query.global`; each included board also requires effective `board.read` and `kg.query.global` |
| Cypher / natural / reflective query | `kg.power.cypher` / `kg.power.natural` |
| schema info | `kg.power.schema_info` |
| schema info with `include_internal=true` | additionally `kg.admin.settings_read` |
| grounding / provenance drift / JSON-LD export | `board.read` |
| health / health-readiness / canonical debt / partition integrity / digest mismatch / stale parity | `board.read` |
| cognitive-readiness evaluations and lists / cognitive DLQ / bug cognitive closure evaluation | `board.read` |
| orphan report / dead-letter list | `board.read` |
| originates-from audit / takedown status / queue drill-down / connectivity DLQ diagnose and verify | `board.read` |
| orphan backfill | `board.read` for `dry_run=true`; `kg.admin.historical_consolidation` for apply |
| manual KG tick | `kg.admin.historical_consolidation` (board-effective for one board; global effective context for all boards) |
| rebuild preflight / confirm / run | `kg.admin.wipe_board` |
| quarantine restore plan / apply | global `kg.admin.wipe_board`, then the resolved destination board's effective `kg.admin.wipe_board` |

Board overrides are honored because checks use the resolved board context, not
the global agent object. Explicit legacy flat principals retain their historical
`board:read` fallback for non-admin KG operations. Administrative schema
introspection and every administrative mutation above have no legacy
`board:read` fallback. Global administrative operations authenticate through
the global effective context; a raw authenticated principal is not sufficient.
`okto_pulse_kg_schema_info` with an empty `board_id` returns static global
contract metadata and never opens, selects, or enumerates a board graph.

Missing authentication or board access returns the non-enumerating
`unauthorized` envelope. A resolved caller lacking a required flag receives
`permission_denied` plus `required_permission`; for example:

```json
{
  "error": {
    "code": "permission_denied",
    "message": "Permission denied: requires 'kg.power.cypher'",
    "required_permission": "kg.power.cypher"
  }
}
```

The JSON-LD export keeps its legacy flat error projection while carrying the
same information:
`{"error":"permission_denied","message":"...","required_permission":"board.read"}`.

## `okto_pulse_kg_abort_consolidation`

Covered fully by the live tool description.

## `okto_pulse_kg_add_edge_candidate`

Add an edge candidate to an open consolidation session.

`candidate` is a strict object with `candidate_id`, `edge_type`,
`from_candidate_id`, `to_candidate_id`, and optional `confidence`, `layer`,
`rule_id`, `created_by`, and `fallback_reason`. Unknown fields are rejected
with `invalid_candidate`; they are never silently ignored. Edge confidence is
named `confidence`, while node confidence is named `source_confidence`.

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
    candidate: Strict object with `candidate_id`, `node_type`, `title`, and
        optional `content`, `context`, `justification`,
        `source_artifact_ref`, `graph_layer`, `maturity_status`,
        `source_confidence`, `relevance_score`, `priority_boost`,
        extraction-provenance fields, and `kind_of`. Unknown fields are
        rejected with `invalid_candidate`; they are never silently ignored.
        Node confidence is named `source_confidence` (not `confidence`).

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
    dead_letter_ids: Multi-value DLQ row IDs
        (formats: okto-pulse://reference/multivalue). For `scope=generic`,
        empty means "oldest non-Code-Traceability rows for this board up to
        limit". For `scope=code_traceability`, exact IDs are required.
    limit: Max DLQ rows to requeue (1-200, default 50).
    process_now: "true" to immediately run one consolidation worker batch
        after requeueing; "false" to only mark rows pending.
    scope: `generic` (default) or `code_traceability`. The CT scope is
        all-or-nothing, board-scoped, requires all four Code Traceability read
        permissions, and only requeues persisted CT artifacts. It never reads
        a repository, filesystem, provider, or runtime from Community.

Returns:
    JSON with scope, selected/requeued/already_queued counts and, when a row
    mutated and process_now is true, the worker batch processed count. An
    invalid CT selection returns `blocked=true`, `mutated=false`, and removes
    no DLQ row.

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

Explain the origin, related constraints, and registered violations for one
canonical graph Constraint.

The live tool description remains canonical for the complete permission and
response-envelope contract; this reference adds the deterministic discovery recipe.

Args:
    board_id: Board ID.
    constraint_id: Canonical `Constraint.id` returned by the graph. This is not a
        technical-requirement id and not a deterministic-worker candidate id.

Discovery for the mandatory Stage 3 sweep is documented in
`okto-pulse://workflows/kg`: use the parameterized read-only Cypher lookup over
`source_artifact_ref`, with `include_working=true` while the spec is still a draft;
reject missing or duplicate refs rather than guessing an id.

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
    artifact_id: Typed source reference: ``spec:<uuid>`` or ``card:<uuid>``.
        A raw UUID is rejected as ``invalid_artifact_ref``; historical
        non-UUID source refs remain readable for compatibility.
    min_confidence: Minimum confidence (default 0.5)
    max_rows: Maximum results (default 100)
    rel_types: Comma- or pipe-separated edge types to restrict the
        first hop (e.g. ``"supersedes,contradicts"`` or
        ``"tests|relates_to"``). Empty = any type.
    direction: ``"both"`` (default), ``"outgoing"``, or ``"incoming"``.
        Applied to hop1 only; hop2 is always undirected.
    max_depth: Closed set ``1|2``. ``1`` returns center+hop1 only (hop2 fields null);
        ``2`` (default) returns the full 2-hop context.
    graph_layer: ``canonical`` (default), ``working``, or ``all``. Invalid
        values fail closed and the response echoes ``applied_graph_layer``.

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

## `okto_pulse_kg_digest_layer_reconcile`

Administrative board-scoped WRITE for the specific case where
`okto_pulse_kg_digest_layer_mismatch_list` still reports DecisionDigest layer
drift while `okto_pulse_kg_queue_drilldown` reports an idle queue. It enqueues a
durable `consolidation_committed` event with `nodes_added=0`; the event contains
no graph-node reference rows and does not require a consolidation-session audit
parent (its `session_id` is correlation metadata only). It reuses the normal
Global Discovery parity reconciler, does not rebuild the graph, and does not
change either read-only diagnostic tool.

The worker treats the per-board graph as authoritative and keyset-paginates
every publishable digest source type (`embedding IS NOT NULL`), grouped by ID;
any physical source count other than one fails closed. It rechecks that source
inventory after reconciliation and again after flush before ACK, so concurrent
insert/remove or embedding eligibility changes are retried rather than pruned
from an inconsistent snapshot. It prunes vanished/unembedded global rows only
after a complete guard proves no `DECISION_MENTIONS_ENTITY` or
`DECISION_DERIVES_FROM` relationship would be lost, repairs duplicate or corrupt
physical identities, and backfills missing identities.

A repair is acknowledged only after close/fsync/reopen and a fresh-handle read verifies
exactly one stable digest, one edge from the correct Board, and one total inbound `CONTAINS_DECISION`
edge per source; invalid cross-board links are
removed without deleting digest or clustering relationships. Verification is
isolated per board after the batch-global flush, so one corrupt board does not
retry healthy boards. Board `decision_count` is written from the absolute
authoritative inventory and remains idempotent across retries. Structured logs
report `duplicate_count`, `repaired_count`, `backfilled_count`,
`layer_corrected_count`, `link_repaired_count`,
`invalid_link_pruned_count` and `verified_count`.

Repeated calls are idempotent by effect: each request receives a distinct audit
event ID, while a converged source/global set produces no further graph change.
Requires `kg.admin.historical_consolidation`.

Args:
    board_id: Board ID. Authentication, board access, realm and command scope
        must all resolve to this same board.
    reason: Required 3-128 character audit code. Use lowercase letters, digits,
        `.`, `:`, `_` or `-`; do not put free-form prose or sensitive data here.

Returns:
    MCP Outcome V2 success with board_id, event_id, session_id, normalized
    reason, enqueued=true and effect_idempotent=true. Authentication, permission,
    board-scope and validation failures use structured error outcomes.

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

## `okto_pulse_kg_takedown_status`

Read-only timeline for one governed SOT/KG deletion. Use an identity returned
by the delete response to follow intent creation, graph convergence, durable
delivery and the fail-closed board/Global Discovery parity predicate.

Args:
    board_id: Required authorization and storage scope. Selectors are resolved
        inside this board before any timeline or aggregate is materialized.
    delete_event_id: Durable delete event identity. Mutually exclusive with
        delivery_key.
    delivery_key: Logical Global Discovery delivery identity. Mutually
        exclusive with delete_event_id.

Returns:
    JSON with found, immutable artifact identity, generation, ordered states,
    retry metadata, SLO aggregates and e2e_health. The tool never retries,
    rearms or mutates queue work. An unavailable parity probe is explicit and
    cannot be interpreted as healthy.

## `okto_pulse_kg_orphan_report`

Return a bounded safe orphan-node report for a board KG.
Requires the board's effective `board.read` permission.

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
`dry_run=true` requires effective `board.read` and acquires no writer lock.
Applying the backfill requires effective
`kg.admin.historical_consolidation`. The complete board batch runs under one
single-writer fence and safe-write barrier; checkpoint/flush/fsync must finish
even when a later row fails after an earlier edge was written. A lifecycle
failure returns an error and never a successful backfill summary.

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
    `consolidation_queue` (ready/scheduled_retry/claimed/overdue_claimed,
    work_kind, attempts, next_retry_at, last_progress_at and safe reason) and
    `global_update_outbox` (pending retry-window depth + oldest_age_seconds).
    Scheduled backoff is active depth but is not classified as stuck until it
    becomes retry-eligible; claimed work is stuck only after its claim expires.

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
- Canonical read subset accepts root operations MATCH, OPTIONAL, UNWIND,
  WITH, or RETURN. Other safe roots fail as ``unsupported_operation``.
- Parser rejects write keywords (CREATE/DELETE/SET/etc) as ``unsafe_cypher``
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
        graph validation, rebuild checks, or E2E ingestion tests. Governed
        source-deletion tombstones may remain visible for lineage/audit checks,
        but their semantic payload (`title`, `content`, `context`,
        `justification`, and source quote) is erased. The Community provider
        replaces indexed nodes and drops their embedding too; independently,
        vector fields are always response-boundary stripped by this tool.

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
    working_omitted_count. The legacy `working_omitted_count` name denotes the
    total rows suppressed by canonical projection, including invalid/unknown
    layer rows; it is derived from paired bounded query windows in the Community
    provider. `working_omitted_count_exact`, `working_omitted_count_source`,
    `working_omitted_count_scope=returned_window`, `layer_counts`,
    `working_row_count`, and `omitted_layer_counts` explain the measurement.
    Providers without paired-query observability return null rather than a
    misleading zero.

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

Run the real bounded ``retrieve → critic → corrective action`` loop. The
Community composition supplies both retrieval and deterministic critic ports;
Core only defines the public contracts and orchestrates them. No LLM is needed.

Args:
    board_id: Accessible board UUID. ACL is checked before embedding or graph IO.
    nl_query: Natural-language query (same size guard as natural query).
    limit: 1..100, default 20.
    min_confidence: 0.0..1.0, default 0.5.
    graph_layer: ``canonical|working|all``; default canonical.
    max_iterations: 1..8, default 3.
    deadline_ms: 50..30000, default 5000.
    budget_units: 1..10000, default 10.

Returns:
    A structured terminal result with ``accepted``, ``terminal_reason``,
    ``iterations``, ``rows``, bounded critic trace and
    ``applied_graph_layer``. Acceptance occurs only when the critic returns
    adequate+sufficient and action=accept. Other terminal reasons include
    ``rejected``, ``max_iterations``, ``deadline``, ``budget``,
    ``no_progress``, ``malformed_critic_output``, ``retrieval_error`` and
    ``critic_error``. Cache identity includes board, query, graph version,
    parameters, critic identity/version and the ACL scope hash.

## `okto_pulse_kg_schema_info`

Summary and Args covered by the live tool description. Delta:

Returns:
    JSON with schema_version, stable_node_types, stable_rel_types,
    vector_indexes, label_properties, query_contract, optionally
    internal_*_types.

    `label_properties` (R6-IMP3) maps each canonical node label to its
    `stable_properties` (the schema-guaranteed scalar properties — the SAME set
    on every label, since all node tables share the common attributes) plus
    `has_vector_index`. Query ONLY these stable properties; never assume an
    ad-hoc/universal property. There is no `name` property — use `title`/`content`.
    Use this map to write schema-safe Cypher (okto_pulse_kg_query_cypher).

    ``query_contract.version == "1.0"`` is the machine-readable canonical
    corpus shared by runtime validators and wire schemas. It declares typed
    artifact kinds, node/edge types and endpoint pairs, graph layers,
    related-context directions/depths, the 0..1 similarity range, cognitive
    outcome types, and the supported read-only Cypher subset. Safe operations
    outside that subset return ``unsupported_operation``; write keywords return
    ``unsafe_cypher``.

## `okto_pulse_kg_tick_run_now`

Prose covered by the live tool description. Delta:

Authorization:
    Requires `kg.admin.historical_consolidation`. A board tick checks the
    destination board's effective overrides before resolving the lease
    provider. A global tick authenticates through the global effective context;
    raw-principal authentication alone never authorizes it.

Args:
    board_id: Optional board UUID. Empty string = global tick (all boards).
    force_full_rebuild: When true, resets last_recomputed_at to NULL
        for all nodes in scope inside the governed per-board writer lifecycle
        before the tick — ignores staleness. This is persisted as the dedicated
        fail-closed event type `kg.tick.full_rebuild`.

Returns:
    JSON with
    `{tick_id, correlation_id, tick_ids, status: "running", scheduled_at}`
    on success. For a concrete board, the persisted event and tick run preserve
    `tick_id` exactly. A global request keeps that value as `correlation_id` and
    returns one deterministic child id per sorted board in `tick_ids`; every
    child receives the same `scheduled_at`.
    On 409 (lock held), `{error: "tick_already_running", message: "..."}`.
    On auth failure, `{error: "..."}`.

Rolling deployment:
    Deploy consumers that register `kg.tick.full_rebuild` before enabling its
    producers. Before downgrading, stop producers and drain pending executions
    of that type. An older consumer treats the unknown type as retryable pending
    work; it must never acknowledge it as an ordinary daily tick.

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

## `okto_pulse_kg_global_outbox_dead_letter_list`

List terminal Global Discovery outbox deliveries for global-admin recovery.
This read-only operation returns only rows whose delivery remains unprocessed
and whose retry state is the dead-letter sentinel or has exhausted the shared
retry ceiling. Errors are bounded/redacted.

Args:
- `limit`: 1-100, default 50.
- `cursor`: optional opaque keyset cursor. Ordering is deterministic by
  `(created_at, dead_letter_id)`; never construct or edit the cursor.
- `classification`: optional `global_open_failure`, `board_source_failure`, or
  `unclassified_failure`.

The classification filter is evaluated over each bounded physical page. A
filtered page can therefore return `count=0` together with a non-null
`next_cursor`; continue from that cursor until it is null. Returns
`{items, count, next_cursor}`. Each item includes the immutable
`dead_letter_id`, event/board identity, retry count, classification, redacted
last error and creation time.

## `okto_pulse_kg_global_outbox_dead_letter_reprocess`

Atomically requeue an explicit terminal Global Discovery outbox selection.
There is no broad or implicit "all" mode.

This legacy recovery surface never reuses a governed delivery key. Rows whose
event identity is `gd_parity:...:attempt:n` are owned by tick machinery.
`kg.tick.daily` starts recovery; each durable `kg.tick.delivery_redrive`
continuation consumes one global budget with oldest-first queues and
round-robin board fairness, advances its persisted checkpoint by CAS, and
inserts a new `attempt:n+1`. Remaining due debt schedules the next bounded run
in the same transaction. A governed or mixed selection is rejected atomically
with `governed_delivery_attempt_tick_owned` and `mutated=false`.
The tick watchdog uses a separate persisted cursor per board; every bounded
page advances transactionally even when attempts are still active, preventing
an active prefix from starving a later orphaned attempt.
The redrive receipt reports the age of the oldest remaining due debt as
`oldest_debt_age_seconds`; the gauge is `0.0` once the due backlog is drained.
For historical terminal rows only, the circuit probe can resolve a delivered
ledger through the payload delivery key, unique delete event, or physical
attempt-key prefix. Missing or conflicting non-delivered identities remain
fail-closed; normal attempt consumption still requires the exact envelope.

Args:
- `dead_letter_ids`: required native list of 1-100 unique immutable IDs from
  the list operation.
- `reason`: required bounded operator audit reason.
- `process_now`: optional boolean; after a successful commit, signal the owned
  outbox worker. The signal is never sent before commit.

The entire selection is validated before its guarded update in one dedicated
transaction. Empty, duplicate, over-limit, unknown, non-terminal,
superseded, or mixed selections fail closed with `mutated=false`; a row changed
between validation and update returns `selection_changed`, and relational-store
lock contention returns `global_outbox_busy` without backend details. Replaying a
valid request is idempotent. Success returns `selected_ids`, `requeued_ids`,
`already_queued_ids`, `already_applied_ids`, `rejected_ids`, and
`worker_signaled`.

## `okto_pulse_kg_global_outbox_dead_letter_verify`

Read the authoritative post-reprocess state for 1-100 explicit immutable IDs.
Returns one item per requested ID with state `absent`, `still_dead_lettered`,
`queued`, `processing`, `applied`, or `superseded`, plus event identity,
`authoritative_id`, ordered `supersedence_chain`, and a bounded `reason_code`.
Broken lineage never invents authority: a missing successor yields
`authoritative_id=null` and `supersedence_target_absent`; cycles and the bounded
chain ceiling likewise return typed reason codes.

## `okto_pulse_kg_global_discovery_recovery_preflight`

Global-admin admission for an unreadable Global Discovery cache. The request
has a closed, empty input schema. It atomically reserves the single global
recovery slot and durably dispatches preparation; it never scans boards, opens
graph files, or materializes the candidate inside the MCP request. Replaying
the incumbent prepared reservation returns that same run. Another active run
is refused with the typed global-slot conflict.

Returns within the bounded control-plane window with `run_id`, `state`,
`phase`, `preparation_state`, monotonic `progress_seq`, and `action_required`.
Poll the status tool while `phase=preparing`. When `phase=prepared`, status
also exposes the immutable `manifest_ref` and `preflight_hash` required by
confirm. A prepared run remains `state=pending` and is not worker-adoptable
until it is confirmed.

## `okto_pulse_kg_global_discovery_recovery_confirm`

Issue a TTL-bound, single-use token for the exact prepared run, manifest and
preflight hash. The call rechecks preparation expiry and the persisted source
fingerprint and fails closed with `manifest_stale` if either changed. It never
rescans all boards or performs graph/filesystem mutation.

Args: `run_id`, `manifest_ref`, and `preflight_hash` from the prepared status.
Returns `outcome=confirmation_issued`, `action_required`, `confirmation_id`,
and `expires_at`.

## `okto_pulse_kg_global_discovery_recovery_run`

Consume the exact confirmation binding, recheck the prepared manifest's TTL
and fingerprint, and durably dispatch the already-prepared run. It performs no
all-board inventory, health, source, or candidate-seed scan in the MCP request.
The call returns without waiting for native work, cutover, or delivery drain.
Use the status tool to follow monotonic checkpoints and the closed terminal
state. A stale binding fails closed with `manifest_stale` and requires a new
preflight.

Args: `confirmation_id`, `manifest_ref`, `preflight_hash`, and bounded audit
`reason` (1-512 characters). The accepted response contains `run_id`,
`attempt_id`, epoch, current state, `preparation_state`, progress/heartbeat
fields, `idempotent_replay=false|true`, `status_tool`, and
`action_required=call_okto_pulse_kg_global_discovery_recovery_status`. Replaying
the exact immutable binding returns the existing run and never dispatches a
second attempt; a different manifest/hash/reason for that run id fails closed.

## `okto_pulse_kg_global_discovery_recovery_status`

Return the authoritative durable control-plane projection for one explicit
`run_id`. The response includes the current epoch, closed lifecycle state,
`attempt_id`, `preparation_state`, monotonic `progress_seq`, phase, progress
counts, heartbeat/deadline timestamps, active and cumulative budget
consumption, cancellation time, `terminal_outcome`, reason code, retryability,
and `status_tool`. Any authorized global admin may inspect the global run;
admitting, confirming, cancelling, and resuming actors remain immutable audit
facts on their respective transitions.

## `okto_pulse_kg_global_discovery_recovery_cancel`

Request durable cancellation for one explicit `run_id` and its current
`expected_epoch`. A stale epoch returns `recovery_epoch_conflict` with the
expected/actual epoch and progress sequence and performs no mutation. The
bounded request acknowledges the durable intent; the fenced worker reports the
terminal `cancelled` state only after native work drains safely. `reason` is
optional and, when supplied, is limited to 512 characters. Cancelling a
prepared run terminalizes it and releases the global slot without dispatching
physical work. The authenticated caller is persisted separately as
`cancel_requested_by_actor_id`; the original admitting actor never changes.
Prepared manifests and staged inputs remain immutable audit evidence and are
revoked by append-only evidence rather than deletion.

## `okto_pulse_kg_global_discovery_recovery_resume`

Explicitly resume a resumable terminal attempt or take over an expired worker
lease for one `run_id` and `expected_epoch`. Admission preserves the same run
identity, increments the epoch exactly once and enqueues owned work off-request.
Typed denials include active lease, non-retryable terminal outcome and exhausted
attempt/cumulative budgets; timeout and success are never resumable. `reason`
is optional and, when supplied, is limited to 512 characters. Successful
admission persists `resume_requested_at`, `resume_requested_by_actor_id`, and
the optional resume reason without changing the original actor binding.

## `okto_pulse_kg_rebuild_preflight`

Run the KG rebuild preflight for a board — gemelar do REST POST /api/v1/kg/rebuild/preflight.

Executes a diagnostic pre-rebuild check (read-only against graph storage):
enumerates real sources via BoardSourceStore and classifies KG health. It does
not persist a manifest or confirmation artifact. The local one-shot executor
performs its own fresh preflight, manifest build, confirmation, and run, or
resumes the one verified active receipt before a governed fresh run, after
proving Pulse and SDLC writers are offline.
Requires the board's effective `kg.admin.wipe_board` permission before opening
the Unit of Work or source provider.

**Admission gate (FR8):** refuses with `rebuild_refused_quarantined` when
`graph_state == 'quarantined'`. Board `graph_state=recovery_needed` is admitted.
When the board graph is healthy and only Global Discovery requires recovery,
this tool refuses with `board_rebuild_wrong_recovery_scope` and points to the
global discovery recovery preflight; a board rebuild cannot repair that cache.

**Flow:** call `okto_pulse_kg_health` first and branch on the component state,
not generic `overall_state`. For board-graph recovery, stop Pulse and surface the
governed local one-shot executor; do not call or loop online confirm/run.
Discovery-only recovery uses the global flow above.

**Offline execution:** stop Pulse/API/MCP and SDLC writers and keep them offline.
First inspect the installed executor against the live home:

```powershell
okto-pulse-kg-recovery-only --data-home <ABS_LIVE_HOME> --board-id <UUID> --inspect-install
```

Review the SHA-256 installation fingerprint, make a physical isolated copy of
the stopped live home, and run the rehearsal while writing a new receipt:

```powershell
okto-pulse-kg-recovery-only --data-home <ABS_COPY_HOME> --board-id <UUID> --rehearsal-copy-of <ABS_LIVE_HOME> --rehearsal-receipt-out <NEW_ABS_RECEIPT.json> --expected-install-fingerprint <SHA256>
```

If rehearsal succeeds, execute against that exact live home within 2 hours:

```powershell
okto-pulse-kg-recovery-only --data-home <ABS_LIVE_HOME> --board-id <UUID> --execute --rehearsal-receipt <ABS_RECEIPT.json> --expected-install-fingerprint <SHA256>
```

The 7200-second receipt is single-use and bound to its exact receipt path,
board, installation fingerprint, live data-home path/storage hashes and
terminal rehearsal evidence. The physical-copy relationship is verified at
rehearsal; the copy path is not persisted as a binding. Never copy, rename or reuse it. The one-shot
creates its own fresh preflight/manifest/confirmation or resumes and
reconciles the one verified active receipt before a governed fresh run. Online
`preflight_hash`, `manifest_ref`, and `confirmation_id` values are not executor
inputs.

Args:
    board_id: UUID of the board to preflight.

Returns:
    JSON with `outcome=diagnostic_complete`, the original classification in
    `preflight_outcome`, `action_required`, `base_state`,
    `eligible_source_count`, `preflight_hash`, null `manifest_ref` and
    `source_set_hash`, `execution_mode=recovery_only_offline`, remediation,
    and `operator_action=run_local_offline_kg_recovery_executor`.

Errors:
    `board_rebuild_wrong_recovery_scope` — graph healthy, discovery-only failure.
    `rebuild_refused_quarantined` — graph is quarantined; use KG reset flow first.
    `preflight_enumerate_failed` — source enumeration failed (detail in response).
    `preflight_service_failed` — preflight service error (detail in response).

## `okto_pulse_kg_rebuild_confirm`

Validate online request syntax, then deny confirmation — REST twin POST /api/v1/kg/rebuild/confirm.

Validates the operation and SHA-256 shape, but never loads/persists a manifest
or issues a token online. The one-shot local executor owns fresh internal
authorization and exact active-receipt reconciliation. Requires the board's effective
`kg.admin.wipe_board` before returning the typed denial.

Args:
    board_id: UUID of the board (same used in /preflight).
    operation: Canonical operation (e.g. `'rebuild'`).
    preflight_hash: Compatibility SHA-256 syntax field (64 chars); not an
        executor input.
    manifest_ref: Compatibility-only legacy field; online preflight emits null
        and the executor does not accept this value.

Returns:
    `recovery_execution_required`, `execution_mode=recovery_only_offline`,
    remediation and `operator_action=run_local_offline_kg_recovery_executor`.

Errors:
    `unsupported_operation` — operation not in canonical set.
    `operation_pending_implementation` — operation valid but not yet implemented.
    `invalid_preflight_hash` — hash format invalid.

## `okto_pulse_kg_rebuild_run`

Reject online KG rebuild execution — REST twin POST /api/v1/kg/rebuild/run.

Returns `recovery_execution_required` without consuming legacy/existing tokens.
Request data cannot carry or mint the opaque board/lifetime-bound capability.
Stop Pulse and use the governed local one-shot executor; never retry in a loop.
Requires the board's effective `kg.admin.wipe_board` before admission, token
consumption, provider resolution, or lock acquisition. Online run stops before
all of those effects.

Args:
    board_id: UUID of the board.
    confirmation_id: Compatibility-only legacy token; it is not consumed.
    operation: Compatibility operation field.
    preflight_hash: Compatibility SHA-256 field.
    manifest_ref: Compatibility manifest field.
    reason: Human-readable description for audit (max 512 chars).

Returns:
    `recovery_execution_required`, `execution_mode=recovery_only_offline`,
    remediation and `operator_action=run_local_offline_kg_recovery_executor`.

Errors:
    `recovery_execution_required` — online execution is disabled; use the local
    one-shot executor with Pulse and SDLC writers offline.

## `okto_pulse_kg_quarantine_restore`

KG quarantine restore — dry-run/apply with backup-swap (KGD-01 FR4/BR4).
Both plan and apply require `kg.admin.wipe_board`. Because `quarantine_id`
does not reveal its owning board, the handler first checks the global effective
admin context, resolves the minimum plan, and then re-checks the destination
board's effective override before returning any plan path or applying files.

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
