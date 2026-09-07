# Bounded Global digest visibility assignments

`GlobalOutboxProcessor._set_board_digest_source_visibility` sends at most 512
source IDs per graph call, in deterministic sorted order, independently for
active and revoked sources. This is application-side payload bounding, not a
Grafx dependency or a change to the generic Global Discovery port.

Only NULL or differing `source_revoked` values are assigned. A repeated successful
reconciliation returns zero changes instead of rewriting every digest. Board
ownership remains in every predicate. The count is changed digests, not matched
source IDs; sources without a digest do not inflate it.

The batches are not atomic as a group. Each backend call retains its transaction
contract. A failed batch propagates to the existing delivery error/retry path;
earlier committed idempotent assignments may remain and the next attempt safely
converges. No ACK, retry ledger, writer lease or post-flush verification policy
is bypassed. This must not be confused with chunking negative membership tests:
`NOT IN` cannot be split this way without changing which rows are deleted.

Validation of an isolated Git-index export: 19 tests passed in 26.71 s across
visibility payload boundaries, failure/retry, blocking execution and the worker
processor boundary. The companion Community native integration exercises 2414
source IDs, NULL and already-correct values, both flags, and another board.
It remains fail-closed if a deployment configures a backend parameter limit below
this application batch size; the batch is not a promise to bypass native quotas.

No production redrive or cognitive consolidation is part of this source change.
