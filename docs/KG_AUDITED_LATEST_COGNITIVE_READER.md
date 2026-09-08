# Optional audited latest cognitive-source reader

`LatestVerifiedCognitiveSourceReader` is a backend-neutral optional capability
of a registered cognitive source store. It does not replace `CognitiveSourceStore`
or change its full-history `enumerate` method.

`enumerate_latest_verified(board_id)` must audit the complete scoped history,
including every superseded revision's canonical fingerprint and divergent
duplicate revision identity, then return the same ordered heads as
`latest_cognitive_source_records(await store.enumerate(board_id))`.

The rebuild-source durable digest uses this operation when structurally
available; otherwise it uses full enumeration. An operation error propagates:
there is no error-to-legacy fallback that could hide corruption. The final digest
still revalidates the returned payloads. Frozen DTOs do not guarantee immutability
of their nested mappings, so their fingerprint is not a trusted cache.

Use this capability when an adapter can avoid constructing and repeatedly hashing
superseded DTOs while retaining all historical checks. Do not implement it using
MAX(revision)-only queries, stored digest trust, cached prior authority, or partial
enumeration. Consumers needing history continue using `enumerate`.

Community implements the capability with raw normalized relational mappings and
the unchanged Core canonical selector. This is not a Grafx-specific port and adds
no engine dependency, transaction policy, schema change or new fingerprint format.

Validation: 107 Core port and rebuild-source tests passed in 33.46 s, including
capability dispatch, legacy store fallback and error propagation. Community's
`docs/KG_HEALTH_COGNITIVE_ENUMERATION_COST.md` documents adapter adversarial tests
and live-source read-only digest parity. No production consolidation was started.
