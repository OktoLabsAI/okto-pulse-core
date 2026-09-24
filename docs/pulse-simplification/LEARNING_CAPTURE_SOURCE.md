# Learning capture source — F6 integration

`learning-capture/v1` is a closed semantic payload stored in the existing
`kg_cognitive_sources` revision ledger. It is not a graph property map. The
existing v3 source fingerprint covers every field and the evidence references;
the enclosing record retains node identity and generation. No second memory
store or graph schema expansion is introduced.

The payload contains `capture_format`, `capture_id`, `author_id`, `captured_at`,
`content`, `context`, `applicability`, `source`, and `intent`. Unknown fields and
formats are rejected. `source` binds Board, Bug, positive policy version, the
relational source digest and a nonempty list of unique evidence references.
Those references must exactly match the enclosing source record. Timestamp
requires a timezone. Text sections have a 65,536-character ceiling; the entire
payload has a 256 KiB UTF-8 ceiling. Up to 128 evidence references are accepted.
These are representation limits, not proof of adequate evidence.

`intent` is closed to create, reuse and supersede. Create carries no target.
Reuse explicitly names the same Learning identity and generation; supersede
names a different identity. Both carry the expected target fingerprint and the
author's reason. Validation never performs similarity matching, merges content,
inherits associations or changes a source head. The authorized writer must
resolve the target and perform the appropriate CAS in its transaction.

## Authority and recovery

Structural validity and a matching fingerprint do not establish authenticated
authorship, current source facts, evidence admission, applicability, permission,
independent review or Bug completion. A caller-supplied `admitted` flag is not
part of the format. Metadata outside the fingerprint cannot supply that proof.
Public transports and the governed transition binding are still pending.

The projection inventory validates every revision, including older captures,
before selecting heads. Captures are reported as
`capture_pending_materialization`, even if a graph node with the identity exists.
Literal node decoding refuses them. Restoration planners preserve the pending
observation and select no literal node. Replay qualification does not promote
them; legacy replay reports `learning_capture_materialization_required` without
probing or writing the graph. This explicit pending outcome does not satisfy a
canonical replay acceptance gate.

Legacy payloads without `capture_format` retain literal replay semantics. A
capture must not overwrite historical payloads or erase prior associations.
Actual materialization must follow source/evidence/transition reconciliation,
record its governed binding and preserve the existing substantive holds.

## Transaction boundary for the capture writer

The optional public `BugSemanticWriteSnapshotReader` capability serializes the
relational sources before reading them. The application must authorize first,
then retain the same UOW through source qualification, evidence admission,
conditional append and commit. The ordinary semantic reader does not offer
that serialization guarantee.

Community implements this boundary with the SQLite writer slot, covering
related Test Cards, Spec scenarios, comments and lineage as well as the Bug.
It refreshes ORM entities inside the SQL snapshot and leaves commit/rollback
to the caller. Unsupported database mechanisms and failed snapshot upgrades
are refused without falling back to an unguarded read. This is a concurrency
guarantee, not evidence approval or permission to write.

## Remaining integrated work

The creation use case now stages an authored capture through the same UOW. It
requires Board/source read authority together with the existing KG begin,
add-node, add-edge and commit permissions, and checks Card/Board/realm access
before the serialized read. No new permission or read-to-write fallback exists.
Author comes from the authenticated actor; timestamp comes from the application
clock. The source digest and policy version must match freshly qualified facts.

This first creation path accepts explicit IDs of scenarios already linked to
the Bug's context. Each must have a receipt authenticated by the existing
scenario evidence consumer. It does not equate a narrative or structural legacy
test reference to an authenticated receipt, require the author to be the receipt
issuer, or turn an authenticated observation into passing implementation credit.
Reference grammar follows the existing `spec:<id>:test_scenario:<id>` convention.

The identity is deterministic per Board, author and capture ID. A transactional
source reader sees staged records as well as committed history. Repeating the
same content and still-current basis returns the original record and timestamp;
reusing the ID with different content fails. A changed basis requires a new
evaluation and does not silently refresh the prior capture. The caller owns
commit/rollback; successful staging does not mean the transaction committed.

Complete other evidence-reference paths, explicit reuse/supersedence operations,
policy and preview, Done binding/outbox, deterministic materializer, reopen
currentness, transports and frontend. Qualify those paths with concurrency,
upgrade/replay/rollback and installed-pair tests. Neither this format nor the
staging use case completes F6 or changes a gate.
