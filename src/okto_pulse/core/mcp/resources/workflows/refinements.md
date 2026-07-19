---
version: "1.0"
---

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

## 2.2a Mandatory Deep Investigation — Refinement is Research, Not Paraphrasing

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
