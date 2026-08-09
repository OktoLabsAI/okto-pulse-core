---
version: "2.0"
contract: "semantic-guideline-protocol/v1"
---

# Semantic guideline protocol

This is the single agent protocol for versioned guidelines, semantic metrics,
board adoption, assessment evidence and transition gates. Workflow resources
only route here; read this resource before authoring metrics, changing a
revision, adopting a guideline, assessing an entity or relying on assessment
evidence at a transition.

Pulse never judges semantic adherence. The external agent produces the
cognitive analysis; Pulse deterministically validates exact authority,
completeness, score bounds, thresholds, independence and currentness, then
seals immutable evidence.

## Revision and adoption

- Guideline prose is contextual. A metric makes one semantic criterion
  assessable. Never convert a semantic guideline into predicates or facts.
- Every metric requires a stable `metric_id`, readable `code`, title,
  description, evaluation rubric, targets, direction (`minimum|maximum`) and a
  default threshold in `0..100`. `confidence` is reserved and compulsory at
  assessment level.
- An empty metric set is valid context-only guidance and creates no gate.
- Revision SemVer minimums are deterministic: adding a metric is minor;
  removing one, changing identity/direction/targets/rubric, or tightening its
  default threshold is major; relaxing a threshold is minor; editorial-only
  changes are patch.
- A partial revision patch preserves omitted fields. `metrics=[]` explicitly
  removes all metrics. A no-op creates no revision.
- Preview every board adoption with the proposed priority, enforcement,
  minimum confidence and threshold overrides. Overrides use metric `code`,
  never `metric_id`.
- Inspect paginated impact items, then adopt only with the exact
  server-returned receipt ID and digest. A stale preview must be recreated.
- `guideline_impact_no_changes` means the proposed adoption configuration is
  already authoritative; do not retry it as a mutation.

## Recording an assessment

Before recording:

1. Read the entity's full current context and the exact adopted guideline
   revision. Use the current subject version, binding revision and guideline
   revision ID as explicit fences.
2. Assess every metric targeting that entity type. Do not omit difficult or
   failed metrics and do not submit non-applicable metrics.
3. For each metric submit one integer score in `0..100`, a concrete rationale,
   at least one immutable evidence reference and at least one stable pinpoint.
   Follow the metric rubric when translating analysis into a score.
4. Submit compulsory confidence in `0..100` plus the model identifier when
   known. Confidence describes reliability of the whole assessment, not the
   score of a particular metric.
5. Use a caller-stable idempotency key only for byte-equivalent retries.

Pinpoints use `whole_artifact`, `field`, `structured_child` or `qa`. Whole
artifact forbids `anchor_ref`; all other kinds require a stable semantic
reference. Never use mutable list indexes. Evidence references require source
type, source ID, positive source version and exact content hash.

Pulse rejects the assessment before persistence when a fence is stale, a
metric is missing/unknown/non-applicable, confidence is below the board
minimum, or an agent that last changed the subject attempts a blocking
assessment. Receipt and all metric results are written atomically.

`policy_assessment_inadmissible` is non-retryable without remediation. Inspect
`details.inadmissibility_cause`: `confidence_below_minimum` requires a new
assessment with sufficient confidence; `assessor_separation_required` requires
an independent assessor. The response deliberately exposes no editor or
assessor identity.

### Canonical agent journey

1. Load the entity with the full context tool named by its workflow and use its
   policy-compliance bindings; do not reconstruct the subject from list output.
2. Read each exact adopted revision with
   `okto_pulse_get_guideline_revision(profile="detail")`.
3. Analyze every applicable metric as described above, then call
   `okto_pulse_record_semantic_guideline_assessment` with the context fences.
4. Confirm the sealed result with
   `okto_pulse_get_current_semantic_guideline_assessment(profile="full")`.
5. If the current read is missing, or a listed/full receipt reports stale
   currentness, reload context and revision, analyze the new content, and
   record again with refreshed fences and a new stable idempotency key. Never
   reuse or edit the stale receipt.

<!-- semantic-assessment-rollout:start -->
## Semantic assessment contract rollout

The legacy writer remains available as contract v1. The explicit v2 writer is
`okto_pulse_record_semantic_guideline_assessment_v2`; its REST twin is
`POST /boards/{board_id}/semantic-guideline-assessments/v2`. Current reads are
dual-read and return the newest live-current receipt with an outer v1/v2
discriminator. Versioned request examples ship at
`reference/examples/semantic-guideline-assessment-v1.json` and
`reference/examples/semantic-guideline-assessment-v2.json`.

Roll out forward-only, in this order:

1. deploy dual-read readers;
2. apply the idempotent v2 tables and immutability/idempotency triggers;
3. deploy both v2 transports;
4. set `SEMANTIC_ASSESSMENT_V2_READERS_READY=true`;
5. set `SEMANTIC_ASSESSMENT_V2_WRITER_ENABLED=true`.

The writer activates only when both flags and all runtime probes agree. A
disabled writer fails with `unsupported_contract_version`. A requested writer
with a missing reader, table, trigger, REST or MCP capability fails with
`v2_writer_not_ready`. Operational rollback disables only the writer flag;
schema and readers remain forward-compatible. Never drop v2 data or triggers
as a rollback action.
<!-- semantic-assessment-rollout:end -->

## Gate and currentness

- Evidence presence is mandatory regardless of enforcement: every applicable
  binding requires a current, admissible assessment receipt before a governed
  transition. A missing, stale, unavailable or inadmissible assessment rejects
  the transition even when the binding is advisory (an active human skip is
  the only bypass).
- Advisory bindings never block on metric SCORES: with a current receipt
  recorded, failed advisory metrics stay visible evidence only.
- Blocking bindings are conjunctive: every applicable metric must pass its
  effective threshold and the compulsory confidence minimum must pass.
- A board threshold override replaces only that metric's default for that
  binding. `minimum` passes at score >= threshold; `maximum` passes at score <=
  threshold.
- Blocking assessment requires an assessor independent from the subject's last
  semantic editor. Advisory evidence does not require separation.
- A receipt is current only for the exact subject content/version, binding
  configuration and revision, guideline revision, board binding head and
  semantic policy set it sealed. Any relevant drift requires reassessment.
- Native ambiguity, Resource, checklist, test, validation and evaluation gates
  remain independent. Semantic guidelines do not duplicate or replace them.
- Human skip is deliberately absent from MCP and agent permissions. Only the
  explicit human UI authority may create a governed skip.

## Lists, pagination and errors

Revision and impact lists use `summary|detail`; semantic assessment, finding,
waiver and skip lists use `summary|detail|full`. Use `full` when an agent must
inspect sealed digests, authority fences or idempotency metadata. All lists
use signed opaque keyset cursors. Preserve every filter, projection and
`evaluated_at` across pages; never decode or edit a cursor.
`next_cursor=null` with `has_more=false` ends traversal.

Use the semantic assessment reads to inspect immutable receipts and derived
currentness; use the finding list to address one failed metric without
reconstructing it from free text. Currentness filters apply only to assessment
lists. Finding and waiver results expose currentness but deliberately do not
accept a currentness filter.

Waiver collection and singular reads evaluate expiry as of their required
`evaluated_at`. Reuse the exact collection snapshot when opening one result.
An approved ledger head whose deadline has elapsed is returned as effective
status `expired`, while its immutable head and event history remain unchanged.
Revalidation appends an independent decision and never rebinds or reactivates
an expired, stale or revoked waiver. A requester or the assessor who sealed the
anchored receipt cannot review or revalidate that waiver.

On failure inspect the MCP outcome code, retryability, next action and bounded
details. Refresh exact authority on conflict/staleness; increase SemVer on
`under_bump`; restart without a cursor on `invalid_cursor`; request the named
capability on permission denial. Never repair evidence or authority by editing
digests, database rows or KG nodes.

## Capabilities and KG

Dedicated leaves are:

- revisions: `guidelines.revisions.read|create|retire`;
- metric authoring: `guidelines.metrics.author`;
- impact/adoption: `guidelines.impact.preview`,
  `guidelines.adoption.manage`;
- assessments: `guidelines.assessments.read|record`;
- governed exceptions: `guidelines.waiver.read|request|review|revoke|revalidate`.

Reporter is read-only. Full Control receives introduced non-human
capabilities automatically; role presets receive only their documented
defaults. Missing custom authority fails closed.

Relational revisions, bindings, assessments and exception events are
canonical. After their transaction commits, durable idempotent events project
board-local revision, metric, binding-configuration, assessment, metric-result,
waiver and skip lineage. These are semantic `Entity` projections, never
deterministic `Constraint` nodes and never gate authority. Unlink, retirement,
supersedence and exception closure terminate the corresponding active
projection with a tombstone; rebuild must converge to the same relational
state and explicitly terminate legacy rule nodes. Diagnose KG health and use
the normal rebuild workflow; never edit the graph directly.

Related resources: `okto-pulse://reference/transitions`,
`okto-pulse://reference/errors`, `okto-pulse://reference/projection-profiles`
and `okto-pulse://workflows/kg`.
