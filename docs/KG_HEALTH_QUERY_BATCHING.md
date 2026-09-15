# Optional Health read groups — 2026-09-07

Health now groups its node relevance reads and layer/maturity census separately
through the existing optional `execute_read_only_batch` capability. Core owns only
a backend-neutral optional protocol, not engine paths, transaction internals or
Grafx settings. Existing scalar executors remain compatible.

## Semantics and limits

- Preserve all 11 queries in each family, their order, parameters and 10000-row
  per-statement limit. This change does not solve the pre-existing row limit.
- Accept only a complete aligned list of dictionary results. On exception,
  incomplete prefix or malformed element, discard the whole batch and retry the
  original authorized read-only scalar operations.
- Preserve per-type partial/unavailable behavior, relevance arithmetic, and layer
  classification. Failure can cost a batch plus scalar retries; no retry loop or
  new success fallback is introduced.
- Community may use one read transaction per group. This does not guarantee one
  common snapshot across all Health probes. Independent readers/writers, WAL,
  snapshot validation and fail-closed storage checks remain unchanged.
- No probe is skipped or cached; historical fingerprint validation is unchanged.

## Evidence

Read-only routed comparison on board 15877207-c147-4805-96d7-d53a625571df:
22 scalar calls become 2 batches. All four scalar/batch/batch/scalar arms produce
the same result digest:
`84cb19918ec555abe634ffc7da0c5e1c4c5c38e52a5c0f06d3fa7c3b12096d59`.
Results: 2960 nodes, 2555 default scores, mean relevance 0.4939;
2779 canonical / 181 working, no failed node types.

Cold scalar 5.532 s; batches 1.012 and 0.985 s; warm scalar 1.243 s.
The warm samples suggest approximately 19–21% less time for these two families,
not a fivefold end-to-end improvement. Ordering/cache effects are not controlled.

A preceding 35-second stack-only live Refresh profile captured 1553 samples,
zero capture errors: 1079 Health, 101 census, 95 graph, 278 other stacks. These
are not additive wall-time or CPU percentages. Refresh graph/stats completed
in 8.206 s before this change; historical source auditing remains substantial.

## Validation

`tests/test_kg_health_query_batching.py tests/test_kg_health.py`:
61 passed in 29.52 s. Covers scalar compatibility, batch query/limit/order parity,
prefix rejection, complete failure and per-type partial/unavailable results.
Community `tests/test_health_schema_1_1_contract.py`: 3 passed in 21.00 s.
Ruff passed for the changed source and tests.

The existing response-shape test failed identically with the original HEAD
aggregators: Community adds `graph_storage` after the Core service returns.
The test now requires exact parity for every Core field and separately pins
that sole Community extension to its default unavailable snapshot. Core does
not acquire physical storage details to satisfy a presentation-layer test.

No reserved spec was consolidated, no rebuild/redrive/reset performed. The 21
pending specs remain reserved for later write benchmarks. Live deployment and
UI observations are recorded in Grafx's evolution/census documentation.
