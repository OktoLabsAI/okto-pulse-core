# Global delivery: linear edge-count projection

Checkpoint: 2026-09-08. This is a backend-agnostic Core optimization, not a
Grafx-specific API or a change to delivery authority.

## Implementation and guarantees

`GlobalOutboxProcessor._correct_board_digest_edge_counts` aggregates each freshly
observed edge inventory once. Reconciliation and verification then look up the
exact `(digest_id, original_node_id)` pair. This replaces an inventory scan for
each expected digest: O(S * E) becomes O(S + E), with O(E) additional bounded-by-
inventory map space. It does not remove the required inventory reads.

Both source and destination board ownership remain mandatory. Counts are summed,
not deduplicated, and identities are not coerced to strings. Missing links,
duplicate multiplicity, wrong identities, foreign ownership and extra inbound
links still refuse verification. Fresh verification independently rereads the
graph and rebuilds the map; pre/post-flush checks, source-inventory revalidation,
durability and ACK ordering are unchanged. No authority cache was introduced.

## Evidence

- `tests/test_global_outbox_edge_inventory_cost.py`: differential parity with the
  old predicate; 256/1,024-source traversal counts; changed multiplicity/identity
  between verification calls; foreign inbound links.
- Combined new tests, blocking execution, visibility batching, Global discovery
  and R1 integration coverage: **73 passed in 50.84 s**. Ruff and diff checks pass.
- One isolated measurement over a preserved, read-only Global fixture with
  **2,253** edge rows/identities: **5,076,009 -> 2,253** row visits;
  **1.2187701 s -> 0.0034425 s**, identical counts. This excludes graph reads,
  writes, certification, scheduling and delivery; it is not an end-to-end speedup.

The preceding authorized one-spec live run took 22.981 s for the full commit and
131.489 s from outbox creation to ACK. This local counting improvement explains
only a small portion of that delivery latency. Further attribution remains open.
A subsequent private read-phase diagnostic's terminal output was not retained;
no timings or successful completion are claimed from it.

## Deployment boundary

This checkpoint is source-only, after the one-spec measurement. Pulse PID 2124
continues using the previously loaded Core implementation and installed Grafx
0.0.4 at 2db169d. No live consolidation was replayed, no additional pending spec
was consumed and no reset, rebuild or redrive was performed for this patch.

Accumulated deployment update, 2026-09-08: Core **9e91ea9** is now loaded by
Pulse 0.3.3 PID **34048**, with installed Grafx 0.0.4@a82d3bf and Community7158383.
The old process became terminal and ports were free before restart. Twenty pending
cognitive items are exactly unchanged; MCP readback of the preceding single-spec
commit passed. Board/Global report healthy, while historical policy DLQ keeps the
overall status at_risk. Detailed deployment evidence is in the Grafx repository's
`docs/GLOBAL_DESTINATION_BATCHING_0_0_4.md`. No new consolidation was performed.
