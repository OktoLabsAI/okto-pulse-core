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
The public capture writer and its governed transition binding are still pending.

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

## Remaining integrated work

Implement the authorized capture use case and idempotency, fresh source and
evidence admission, explicit reuse/supersedence CAS, policy and preview, Done
binding/outbox, deterministic materializer, reopen currentness, transports and
frontend. Qualify those paths with concurrency, upgrade/replay/rollback and
installed-pair tests. This format alone does not complete F6 or change any gate.
