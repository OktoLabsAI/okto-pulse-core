# Consolidation phase timing

2026-09-08. Complements the existing full-write attribution task; it introduces
no performance threshold, graph backend dependency or business-state transition.

## Contract

The real `commit_consolidation` path now emits at INFO one structured
`kg.consolidation.phase` observation per completed/failed awaited phase:

| Phase | Measured boundary | Not implied |
| --- | --- | --- |
| `health_admission` | Existing pre-graph health resolution | A returned health state is not necessarily healthy |
| `graph_dispatch` | Executor dispatch, graph callback, embeddings, scoring and graph commit | Not exclusively native write time; includes executor waiting |
| `cognitive_source_append` | Existing source append or caller-UOW staging | Caller-owned relational durability is not proved |
| `audit_outbox_stage` | Existing audit/outbox operation | Not Global delivery time or caller-UOW commit acknowledgement |
| `session_finalize` | Non-deferred process-local finalization | Deferred caller finalization is outside this observation |

Fields are `event`, `session_id`, `phase`, `outcome`, `elapsed_s`. Outcome is
`returned`, `raised` or `cancelled`, deliberately not `committed`. Timing uses
the monotonic performance counter. No candidate body, graph query, parameters,
embeddings, result payload or exception message/traceback is logged here.

The helper awaits the existing operation exactly once, in the existing task;
it creates no task, shield, executor, lock, retry or timeout. Cancellation and
the owning saga's compensation/finalization boundaries remain unchanged. A
deferred retry has fresh health and relational phases but no graph phase.
The total is bounded by phases per invocation, not by node/edge/history count.
If INFO is disabled, the helper does not read the clock. Ordinary clock/logger
failures suppress only this observation, never turn an applied write into an
apparent failure or replace its original error. Missing events are therefore
unknown measurements, not zero elapsed time or evidence of skipped work.

These phase times omit session-lock waiting, validations outside the marked
awaits, caller-owned relational commit, transport and later Global delivery.
Use the surrounding MCP/API duration as the full-call denominator; do not
assume summing phase observations accounts for every millisecond. Concurrent
sessions must be correlated separately by session ID. Repeated phase events
for the same session can be legitimate relational retries, not duplicate graph
commits. There is no new persistent audit format or metrics label cardinality.

## Validation and deployment

Focused tests exercise identity-preserving return/exception/cancellation,
ordinary clock/logger failures before and after the await, disabled telemetry,
no payload/error-message exposure, cancellable waiting, and the actual deferred
retry/compensation orchestration. The existing cancellation, provenance and
connectivity slices remain the business-behavior oracle.

Final combined slice: **38 passed in 15.37 s**, strict markers and pytest's
60-second thread timeout enabled. Ruff and `git diff --check` pass. An initial
new observation assertion expected two relational stages; inspection confirmed
the existing cancellation fixture intentionally executes a third. The assertion
was corrected to three without changing production retry or cancellation logic.

Accumulated deployment completed: Core 0a38312 is loaded by Pulse 0.3.3 PID 4212
with installed Grafx 0.0.4@fa8f188 and Community 7158383. Previous PID 15940 exited
before restart. HTTP root and authenticated canonical readback passed; all 40
ledger entries are identical, including the 20 pending reserved specs. No live
consolidation, DLQ redrive, reset or rebuild was used. The new observations have
been exercised by tests, not by consuming another live spec. Full deployment
evidence is in Grafx `docs/UNSTAGED_WRITE_PREFLIGHT_0_0_4.md`.

The previous real benchmark remains 22.981 s full commit and 131.489 s Global ACK.
This instrumentation does not itself improve or remeasure those latencies.
