# Delivery Evidence: original implementation record

## Current implementation authority (v0.4.0 initiative)

This document preserves the earlier DE-1–DE-4 implementation/validation record.
Its spec-scoped proof workflow and statements that no advisory/skip exists are
superseded by the local v0.3.4 card-ledger implementation and the consolidated
v1.3 specification. Do not use the historical sections below as current API
instructions or as validation performed in this initiative.

Implementation/test bindings belong to the card ledger. Record implementation
before completing the task/bug; the Spec reads the rollup. Spec POST accepts only
human waivers/revocations. Legacy proof writes fail with
`delivery_card_scope_required`; historical records remain preserved. Existing
`delivery_evidence_gate` and `skip_delivery_evidence` policies do not change the
factual verdict. Current contracts are in the served Code Traceability resources
and the paired Community `docs/DELIVERY_EVIDENCE.md`.

Follow [the integrated ledger](pulse-simplification/IMPLEMENTATION_LEDGER.md)
for current decisions, reproductions, changes and outstanding work. Progress,
batch, specialized verification and the other v1.3 features are not implemented
merely because the original phases below were completed.

## Historical implementation record

Status: **DE-1–DE-4 implemented and validated in isolation on 2026-09-14; not installed in the active user runtime**.

## Agreed product contract

Planning context and delivered implementation are distinct. Existing Code Evidence
Matrix Skip settings apply only to their current context coverage obligation.
They do not waive delivery proof. Greenfield does not exempt completed software.

- Tasks/bug-fix cards record delivered code: source/repository, immutable result
  revision, file/symbol, explanation, author and accepted target-execution receipt.
- **Only test cards** provide verification: linked executable scenario, recorded
  passing result and authenticated current evidence. Task completion cannot stand
  in for a test. A done test card without valid execution evidence is insufficient.
- The Spec aggregates explicit many-to-many links to its active obligations.
  One implementation or test may cover multiple obligations; no duplicate manual
  filling is required solely to repeat those associations.
- Evidence is bound to Board, Spec edition and each obligation's semantic digest.
  Only affected bindings become stale when obligations change. Verification must
  also correspond to the implementation receipt tested, not an older delivery.
- Implementation and test dispensations are separate, exact-obligation, authorized,
  justified and audited records. They must display as waived, not tested/passed.
- Future transitions to `done` require both sides covered or explicitly waived.
  No extra mandatory AI approval round. Existing done Specs are not automatically
  reopened: a read projection will expose missing delivery proof for later repair.
- Recorded/authenticated evidence is not automatically independently verified.

## Fixed implementation sequence and completion criteria

| Block | Status | Scope / completion criteria |
|---|---|---|
| DE-1 | Implemented, domain tests | Provider-neutral immutable facts, exact scope and binding checks, separate task/test coverage, explicit waiver facts and fail-closed incomplete projections. Read-port contract. |
| DE-2 | Implemented | Community `sqlalchemy_delivery_evidence.py`, immutable `delivery_evidence_records` schema, current execution/signed-test projection, exact obligation associations, human waivers/revocations, authorized shared REST/MCP use cases and actor-scoped idempotency. |
| DE-3 | Implemented | Same evaluator for allowed transitions and authoritative `done`, with a second transaction-fenced check. New independent implementations require test coverage too. Old done Specs remain done; no Skip/advisory/greenfield bypass. |
| DE-4 | Implemented and validated | Spec Delivery evidence UI with receipt selection, phase-specific coverage/waivers/audit; QA-specific test authority; workflow/tool resources and generated catalogs; Core, signed-receipt Community, REST/MCP and Chromium tests. |

The gate is now wired in source. The domain module remains **not** an HTTP schema
or receipt verifier. Closed request models never accept its trusted booleans.
Deployment requires the paired Core/Community build and schema initialization;
the running installation has not been replaced or restarted.

## Existing contracts to reuse

- `ImplementationTargetExecutionRecord` and target/Spec links in
  `domain/code_traceability.py` / `ports/code_traceability.py`.
- `ports/test_evidence.py` verifier/issuer and the authenticated scenario evidence
  write path in `services/main.py`; its currentness checks cannot be skipped.
- Real `CardType.TEST` records and `test_scenario_ids` define test ownership.
- The application's relational unit of work, authorization and immutable audit
  patterns; no Grafx-specific contract or direct engine access enters Core.

The projection must select current heads: an old passing run must not survive a
new failure or revocation. It must not silently truncate the obligation inventory,
read another board, treat an empty inventory as 100%, or classify context evidence
as delivery proof. Fact construction is an internal trusted boundary.

## DE-1 validation

`tests/test_delivery_evidence_domain.py` covers separate ownership, missing receipt
and result fields, failed/automated/in-progress tests, cancelled cards, wrong board,
Spec or edition, stale semantic bindings, verification of an older implementation,
shared evidence, selective invalidation, unauthorized/revoked or wrong-phase waivers,
duplicate identities, incomplete and empty snapshots and side-effect-free diagnosis.

The evaluator does not mutate persistence or issue executions. Domain success
alone is not the final delivery criterion; validation also exercises lifecycle
fences, real SQL, signed receipt verification, REST/MCP and the UI.

## Implemented contracts and policies

- `models/delivery_evidence.py`: closed read/write inputs, strict editions and
  versions, phase-specific shapes, maximum 1,000 refs/implementation IDs and 20,000
  justification characters. Actor, digests and trust are server-owned.
- `ports/delivery_evidence.py`: transaction-scoped storage/projection contract;
  `application/use_cases/delivery_evidence.py`: authorization + shared commit.
- `services/delivery_evidence.py`: semantic FR/TR/BR/AC/API/IR/OR/decision inventory
  and the terminal gate. Empty structured content becomes an explicit Spec-root
  obligation. Operational links/state/timestamps do not change the digest.
- Community REST GET/POST `/api/v1/boards/{board_id}/specs/{spec_id}/delivery-evidence`;
  MCP `okto_pulse_get_delivery_evidence` and `okto_pulse_record_delivery_evidence`.
- Implementation associations require `code_traceability.target.execution_submit`;
  tests require `spec.tests.execute`; waivers/revocations require a human plus the
  corresponding Code Traceability waiver permission. No new configuration flags.
- Accepted clean immutable result revisions are required. Source-observation TTL
  does not erase a historical committed delivery; explicit receipt revocation and
  current Target execution/revision are still checked. Signed tests cannot predate
  their implementation observation or survive newer failure/tampering.
- Different test cards may jointly cover all current implementation records. An
  implementation waiver without code does not invent testable code: explicitly
  resolve both phases for a wholly non-code obligation.
- Associations are authenticated actor claims about coverage, not independent
  source inspection. This distinction is documented in the UI and agent resources.

Full Community consumer/deployment contract: `docs/DELIVERY_EVIDENCE.md` in the
paired Community repository. The initial instruction bundle, Specs/Cards workflows,
Code Traceability reference and exact tool docs all point agents to this protocol.
Tools catalog and resource manifest are regenerated, not hand-maintained.

## Completed validation

Core selections: 125 delivery/coverage/lifecycle regressions; 302 MCP/permission/
resource/Spec-validation regressions; 61 import-boundary and repeated delivery
tests (the latter overlaps the first selection). All passed.

Community selections: 98 delivery/signed-Test-Evidence/REST/MCP transport tests,
1 idempotent schema-upgrade test preserving existing done Specs, and 10 existing
Code Traceability persistence regressions. Frontend: 37 unit tests and 2 Chromium
flows; the final receipt-detail rendering also passed a focused rerun. Build and
verification passed for 78 packaged frontend files, tree SHA-256
`5fd0b95e7ca08e287c51722fd7166328604d24cdf5770a653a49cb2461f60535`.

New Python/frontend files pass lint. The existing repository-wide frontend lint
ratchet remains over its baseline (399 warnings versus 393), with no warnings
introduced by the delivery files; changed existing SpecModal/API files retain
their HEAD counts. This pre-existing lint debt is not reported as a green gate.
The initial agent-instruction character budget remains unchanged; the two new
closed MCP tools raise the explicitly reviewed inventory from 338 to 340.

No real board/card/Spec records were changed, and the active Pulse was not stopped,
reinstalled or restarted. Deploy Core and Community together when updating it.
# Publication contract follow-up (2026-09-14)

The Community import audit explicitly publishes only `delivery_digest` and
`delivery_inventory` from the delivery service for adapter use. The paired
Community contract expectation must match the Core manifest. Graph exploration
also consumes the storage-neutral `application_kg` facade for guarded writes and
registry access; no private-import exception or Grafx coupling is introduced.
