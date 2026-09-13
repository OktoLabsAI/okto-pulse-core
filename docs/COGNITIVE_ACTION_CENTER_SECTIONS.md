# Cognitive Action Center section contract

The existing read model accepts `signal=attention` and `signal=deferred` in
addition to all existing signal filters. These group records before pagination
without computing or overriding the readiness service's verdict.

- `attention`: cognitive pending/failed/in-progress, technical DLQ, open canonical
  debt and review-required waivers with elapsed, missing or invalid review dates.
- `deferred`: non-time-limited waivers and review-required waivers with valid
  future dates. Uses the readiness service clock; equality with now is overdue.
- `terminal_history` and `all` retain their existing behavior. The three UI
  sections partition the current closed signal vocabulary. A source artifact
  may still have records in multiple sections; counts are not distinct artifacts.

Filtering precedes `limit`/`offset`. Each returned row retains its service-derived
`readiness_effect`, `blocking` and precedence. Enforcement annotations remain the
responsibility of the existing application boundary. No new store, engine
dependency, transition policy or write path is introduced.

Optional `justification` and `actor` fields project existing cognitive ledger
details. Technical rows default to null. Neither field is used as a metric label.
The existing cognitive-read access checks still govern this projection.

Core and Community must be deployed together for the new UI sections. Community
documents the human workflow, confirmation effects, permissions and navigation
in `docs/COGNITIVE_ACTION_CENTER.md`. Existing consumers using old filters are
unaffected. Covered by the section, S3.1 read-model and S3.3 REST suites.
