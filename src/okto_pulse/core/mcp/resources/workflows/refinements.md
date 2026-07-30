---
version: "1.0"
---

Knowledge Base placement and promotion are governed by
`okto-pulse://reference/knowledge-governance`. Put consequential findings in
the appropriate first-class artifact and retain only supporting evidence in KB.
Executable guideline evaluation follows
`okto-pulse://reference/policy-compliance`.

# Refinements Workflow — Deep Investigation

## 2.2 Refinements

Refinements break down a complex ideation into focused areas. Each refinement covers one specific aspect.

> **MANDATORY — Query the KG before moving to `approved`.** Run the Stage 2 query set: `okto_pulse_kg_find_similar_decisions` for the refinement topic, `okto_pulse_kg_find_contradictions` on anchor decisions the refinement depends on, and `okto_pulse_kg_list_alternatives` on those anchors. Use `okto_pulse_kg_get_related_context` only for an existing formalized spec/card and pass its typed reference (`spec:<uuid>` or `card:<uuid>`); raw UUIDs and ideation IDs are not valid anchors. Ideations are lightweight lineage Entity nodes, not cognitive knowledge containers. Every decision referenced in the refinement body must either (a) cite an existing node_id or (b) declare explicitly that it is new knowledge. Silent reuse or silent contradiction is rejected.

- **Governance**: Refinements can only be created from a **"done" ideation** — the ideation must be fully reviewed and snapshotted first. This ensures refinements are based on a stable, agreed-upon version of the ideation.
- **Context compilation**: When creating a refinement without a description, context is automatically compiled from the ideation (problem statement, approach, scope assessment, Q&A decisions).
- **Status flow**: draft → review → approved → done
  - **Draft**: Editable — write and iterate freely
  - **Review**: Read-only — awaits approval from reviewer (human or agent)
  - **Approved**: Read-only — handoff signal; the responsible can proceed to finalize (move to done)
  - **Done**: Frozen — immutable snapshot created, can only go back to draft (new version)
  - **Cancelled**: Terminal — accessible from any status except done
- **Editing only in "draft"**: `okto_pulse_update_refinement` only works when status is `draft`
- **Use Q&A** to clarify scope and decisions with the user. If investigation reveals two or more valid interpretations, ask before inferring; refinement assumptions become expensive rework in specs, tasks, mockups, Architecture Designs, tests, and validations.
- **Spec creation from refinement**: Only from **"done"** status — a spec draft can be created from a done refinement
- **Triage pending derivations**: the canonical surface to find done refinements that still lack a derived spec is `okto_pulse_list_by_board(entity_type="refinement", filters={"ideation_id": "...", "derivation_pending": true})` — see `okto-pulse://reference/list_tools`. `entity_type="refinement"` **REQUIRES** `filters.ideation_id` (omitting it fails with `missing_required_filter`); `entity_type="sprint"` likewise requires `filters.spec_id`.

## Receipt-Backed Ambiguity and Pinpointing

Refinement ambiguity is a version-bound Quality assessment, separate from a
general prose judgment. It is written only in `approved` and gates
`approved → done` when the board enables
`require_refinement_ambiguity_gate`.

Use this sequence:

1. Complete the deep investigation, RDL entries, semantic edits, and ordinary
   Q&A in `draft`; resolve review feedback before returning to `approved`.
2. Re-read the full Refinement context. Read the current Quality head with
   `okto_pulse_get_current_quality_assessment(subject_type="refinement",
   assessment_kind="ambiguity")`; use head revision 0 if none exists.
3. Call `okto_pulse_record_ambiguity_assessment` with the current Refinement
   version/head revision and a 1–5 lower-is-better score. Pinpoint every issue
   to a stable field, structured-child ID, Q&A ID, or the whole artifact.
   Mutable list positions are forbidden anchors.
4. Proposed questions are created atomically with the receipt. If they are
   answered or the Refinement changes, the clarification/content identity can
   change: re-read context and record a successor assessment.
5. Immediately before `done`, read the current assessment again. It must be
   current and at or below `max_refinement_ambiguity` when the gate is
   required. Use `okto_pulse_list_quality_findings` for the complete pinpoint
   set; never infer freshness from "latest" alone.

A missing, stale, or excessive assessment fails closed. The per-Refinement
skip is human-owned; agents may report the blocker or request a human skip but
cannot set it. Full contracts:
`okto-pulse://reference/tool-docs/quality`.

## Operational Research Decision Ledger

Use the Refinement Research Decision Ledger (RDL) for investigated unknowns
whose alternatives, evidence, and decision must remain auditable. It is not a
replacement for Q&A: ask the user when authority or intent is missing; use RDL
to preserve the research trail and resulting decision.

- RDL writes are allowed only while the Refinement is in `draft`.
  `okto_pulse_append_research_decision` creates a new thread with
  `expected_head_revision=0`; omit `ledger_id` and
  `supersedes_entry_id`.
- Entries are immutable. To progress or revise a thread, append a successor
  using its current `ledger_id`, `supersedes_entry_id`, and positive head
  revision. Reuse the same idempotency key only for an exact retry.
- Every append/supersede bumps the Refinement version. Refresh the full
  context and pass the resulting `expected_refinement_version` before the
  next write.
- Anchor to a stable Functional Requirement, Acceptance Criterion, Technical
  Requirement, or Q&A ID; never an array index. Progress through `open`,
  `investigating`, `resolved`, or explicitly `deferred`.
- A `resolved` entry requires a decision, rationale, confidence, and either
  evidence references or an explicit evidence-absence justification.
- Use `okto_pulse_list_research_decisions` with its opaque keyset cursor and
  stable filters; do not decode the cursor or substitute offset pagination.

Before moving to review, account for every material research unknown as
`resolved` or deliberately `deferred` with rationale. At `done`, the current
RDL heads are frozen into the Refinement snapshot. Spec derivation carries
references to resolved heads without copying them into `Spec.decisions`, so
keep the ledger provenance precise and independently readable.

### 2.2a Selective Knowledge propagation when deriving a spec

`okto_pulse_derive_spec_from_refinement` has two intentionally separate
Knowledge paths:

- Omit `knowledge_propagation` to preserve the legacy v1 derivation exactly.
  Legacy `kb_ids` keeps its existing meaning on this path.
- Supply `knowledge_propagation` to opt into contract v2. In this case,
  `kb_ids` and `knowledge_propagation` are mutually exclusive; passing both
  fails with `conflicting_propagation_parameters`.

The v2 envelope has `contract_version=2`, a caller-stable
`idempotency_key`, and one coherent tri-state selection:

| `selection_state` | Required shape | Effect |
|---|---|---|
| `omitted` | no `mode`; empty `knowledge_ids`; justification optional | Records an authoritative v2 omission. It does not fall back to v1. |
| `explicit_empty` | `mode="drop"`; empty `knowledge_ids`; non-empty `justification` | Derives the spec with an authoritative empty Knowledge selection. |
| `explicit_ids` | non-empty `knowledge_ids`; `mode="reference"`, `"snapshot"`, or `"drop"`; non-empty `justification` | Derives the spec with only the selected stable roots, or explicitly drops the named roots. |

Creation accepts `expected_revision` omitted or `0`. Preflight validates the
done refinement, parent ownership, source IDs, and request identity before the
spec is inserted. An exact retry with the same `idempotency_key` returns the
original `spec_id`, operation, selection, and assignments with
`replayed=true`; never reuse the key for changed derivation content or
selection. A rare `knowledge_creation_race` is retryable and the MCP surface
already performs one retry in a fresh unit of work before exposing it.

Selective Knowledge v2 does not change mockup or Architecture Design
parameters. Continue to pass `mockup_ids`, `architecture_design_ids`, and
`architecture_propagation_mode` independently.

## 2.2b Mandatory Deep Investigation — Refinement is Research, Not Paraphrasing

> **A refinement is NOT a copy of the ideation with prettier wording.** Its purpose is to convert a vetted idea into a concrete blueprint by gathering EVERY piece of factual evidence required to design the solution. The depth of the investigation here directly determines whether the downstream spec is implementable or speculative. Skipping this step compounds — every gap here becomes a question at spec time, every wrong assumption here becomes a bug at implementation time.

**Refinement Q&A is mandatory when evidence is incomplete.** If code, KG, KEs, mockups, architecture, or stakeholder context leaves a requirement open to interpretation, create a refinement Q&A item and wait for the answer before approval. Do not "average" the sources into a plausible answer. The consequence of skipping clarification is downstream rework: the spec may encode the wrong requirement, mockups may visualize the wrong flow, Architecture Design may model the wrong boundary, and cards/tests may spend extra time and tokens implementing and validating the wrong thing.

**MANDATORY scope of investigation — before moving the refinement to `approved` you MUST exhaust ALL of the following sources that apply to the topic:**

| Source | Tools / actions | When it applies |
|---|---|---|
| **Project files** | Use the host agent's local file-read and file-search capabilities on the working directory; surface relevant configs, manifests, package files, IaC, env templates. | Always when a codebase is accessible — the refinement must reflect the real shape of the codebase, not a generic mental model. **When the board is not coupled to a codebase (decoupled mode — frontend out-of-repo, conceptual/doc-only board), declare this source N/A with an explicit justification** (same pattern already used for Mockups/Architecture when they do not apply). |
| **Source code** | Open the modules, classes, functions, endpoints, schemas, migrations directly impacted. Read enough to know existing contracts, naming, patterns, error handling, and edge cases already covered. | Always when a codebase is accessible — anything the refinement claims about behaviour must be verifiable against current code. **In decoupled mode (no repository), declare this source N/A with an explicit justification** instead of fabricating code mappings. |
| **Knowledge bases (KE)** | `okto_pulse_list_knowledge(entity_type="spec")` on related specs; `okto_pulse_add_spec_knowledge` once the knowledge is formalized in a spec. | Whenever there is documented domain knowledge — never paraphrase a KE; cite it and attach at spec/card level if missing. |
| **Knowledge Graph** | The Stage 2 query set (`okto_pulse_kg_find_similar_decisions`, `okto_pulse_kg_find_contradictions`, `okto_pulse_kg_list_alternatives`, plus `okto_pulse_kg_get_related_context` only for existing formalized specs/cards using `spec:<uuid>`/`card:<uuid>`) — see "Query Timing — MANDATORY at every stage". | Always — institutional memory MUST be checked for prior decisions on the same topic. |
| **Mockups & visual artifacts** | `okto_pulse_list_screen_mockups` on the parent ideation; create new mockups via `okto_pulse_add_screen_mockup` when the refinement implies a UI surface; ask Q&A first when screen, state, workflow, or visual behavior is ambiguous. | Whenever a user-facing behaviour is in scope. |
| **Architecture Design** | `okto_pulse_list_architecture_designs` on the parent ideation/refinement/spec; create or update Architecture Design through the architecture tools; ask Q&A first when entities, boundaries, interfaces, contracts, storage, or diagrams are ambiguous. | Whenever services, data flow, integrations, persistence, runtime/deployment boundaries, or interface contracts are in scope. |
| **Web research** | External docs of every dependency the refinement touches: framework docs, library API references, RFCs, vendor changelogs, public issue trackers. Use the host agent's web-search or web-fetch capability when available, and cite authoritative sources. | Whenever the refinement depends on third-party behaviour, version constraints, protocol details, or industry conventions. |
| **Runtime evidence** | Logs, telemetry, existing analytics, prior bug cards (`okto_pulse_kg_get_learning_from_bugs`), DLQ rows. | Whenever the refinement touches an area with prior production behaviour. |
| **Stakeholder context** | Open Q&A on the parent ideation/refinement, related spec evaluations, and `okto_pulse_list_my_mentions`. | Always — never re-litigate a decision the user already made. |

**Mandatory deliverables in the refinement body** — once the investigation is done, the refinement MUST cite the evidence:

1. **`analysis`** — written narrative of what you found in each applicable source above. Cite file paths with `path:line`, KE titles, KG node ids, URLs, mockup titles. Quote the relevant snippet for each citation. If a source did not apply, state that explicitly — this includes **Project files** and **Source code**, which are eligible for an N/A-with-justification when the board has no accessible codebase (decoupled mode); a silent omission is never acceptable.
2. **`in_scope`** / **`out_of_scope`** — the boundary MUST be derived from the investigation, not from intuition. Each scope item should be traceable back to a source or decision.
3. **`decisions`** — every architectural choice the refinement locks in. Each decision must reference (a) the alternatives considered, (b) the source that informed the pick, (c) the prior art it extends or supersedes (KG node id when applicable).
4. **Attached KEs / mockups / Architecture Designs** — when the investigation produced new reference material, UI decisions, or structural design, attach it via `okto_pulse_add_refinement_knowledge`, `okto_pulse_add_screen_mockup`, or the architecture tools. Do not leave findings in chat — make them addressable for the downstream spec.

**Anti-patterns — NEVER do these:**

| Anti-pattern | Why it's wrong | What to do instead |
|---|---|---|
| Writing the refinement from the ideation text alone, without opening any source file | The refinement claims behaviour the code may not implement | Open the modules in scope; cite `path:line` for every claim about code behaviour when a codebase is accessible. **In a decoupled board (no repository), anchor every claim to the applicable sources (KG node_id, KE, Q&A, URL) — never fabricate a `path:line` for code that does not exist.** |
| "I think the library handles this" | Speculation that becomes a bug at impl time | Read the library's official docs or its source; cite the page or commit. |
| Skipping `okto_pulse_kg_find_contradictions` because the topic "feels new" | Silent contradiction with prior decisions in the KG | Always run the Stage 2 query set. |
| Marking the refinement `approved` while open Q&A items exist | Pushes ambiguity downstream | Resolve every Q&A item first; ambiguity at refinement = exponentially worse at spec/impl. |
| Drawing mockups or architecture from unresolved assumptions | The visual or structural artifact becomes a false source of truth | Ask Q&A first, then create or update the first-class artifact. |

**Stop condition — the refinement is genuinely ready when:**
- A reviewer reading the `analysis` field alone (without opening the codebase) can verify every claim against the cited sources. In decoupled mode (no repository), verifiability rests on the sources that DO apply (KG, KEs, Q&A, stakeholder, web); a justified N/A of Project files / Source code satisfies this condition.
- Every decision in the refinement traces to either a source, a KG node, a Q&A answer, or an explicit user instruction.
- There are zero unresolved Q&A items on the refinement.
- New evidence discovered during investigation has been attached as a KE, mockup, or Architecture Design, not buried in prose.
