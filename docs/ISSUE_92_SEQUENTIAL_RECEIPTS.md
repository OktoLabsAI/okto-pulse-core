# Issue #92 — sequential source observations

The single-agent implementation flow no longer treats a new revision in the same
Card/Target selector scope as an automatic contradiction. See the canonical
[agent contract](../src/okto_pulse/core/mcp/resources/reference/code_traceability.md#sequential-observations-and-conflicts).

Only the authenticated attestor of the current receipt may use this sequential
advance. Source identity must match, the head must be current, both workspace
observations must carry revisions, and observation time must not go backwards.
A new revision or explicitly distinct dirty workspace may advance the head with
`single_attestation`, never inherited corroboration. Existing conflicted heads
still require independent corroboration; a changed revision does not self-heal one.

The old receipt and its resolutions remain immutable and become outdated. The
agent can submit a current result receipt and Execution Disposition, refreshing
Target Resolution when required by policy. Board policies requiring corroboration
or committed state still apply. There is no migration of stored receipts, reset of
existing conflicts, new settings, protocol field, graph-engine dependency or call
to Git/the filesystem in Core. Legitimate reverts are observations too: opaque
revision IDs are not treated as an ordered ancestry graph.

`tests/test_issue_92_sequential_receipts.py` covers V1/V2 real service flows,
resolution/execution/replay, V2 blocking transition gates, same-revision conflicts,
actor/source/time fences, dirty-state advancement, inability to self-clear an
existing conflict, independent corroboration and trust reset. Existing application,
domain, gate and MCP tests form the grouped regression. Tests use in-memory ports;
edition persistence/REST regression checks the concrete Community boundary.

Final qualification and source commits are linked in the
[issue response](https://github.com/OktoLabsAI/okto-pulse-core/issues/92).

Source checkpoint: **103 passed**, zero failures/errors/skips, 8.78 seconds:
`test_issue_92_sequential_receipts`, `test_code_traceability_application`,
`test_code_traceability_gate`, `test_code_traceability_contracts` and
`test_contextual_code_traceability_mcp`. The new service flow covers actual
resolution/execution submission and exact replay; the V2 blocking gate allows
both validation and done after the current resolution is refreshed.
Community's affected CLI/persistence/REST/transport selection additionally passed
169 checks. These are affected regressions, not a claim of a complete repository
or live production lifecycle run.

The initial two failures were an invalid test fixture: its resolution observation
time did not match the accepted receipt workspace. Correcting that input preserved
the production exact-time check. Original failed receipts remain under Grafx
`.grafx-tmp/`; final Core receipt is `pulse-issues-core-qualified.xml`.
No existing conflicted lineage, production receipt or stored data was rewritten.

The exact staged-index Core/Community exports also passed **103 Core checks**
and **46 CLI/metrics checks**, independently of other uncommitted adoption work.
These overlap the source regression, not additional distinct tests. Staged Core
receipt: `pulse-issues-core-staged.xml` (12.71 seconds, zero failures/errors/skips).
No main merge, PyPI release or global installation is implied by issue closure.
