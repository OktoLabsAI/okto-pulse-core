# Provenance candidate-batch membership

## 2026-09-07 checkpoint

Consolidation preparation formerly searched the whole candidate edge dictionary
for every node in both automatic root attachment and supersedence inheritance.
The growing auto-generated edge tail made this O(C*E+C²) membership work.

Each phase now constructs a local set of outgoing source candidate IDs for
`belongs_to`, then updates it after adding an edge. Inheritance constructs its
own set from the current batch, including automatic attachments from the prior
phase. This is O(C+E) membership preparation with O(E) local memory; it is not
a claim about the complete consolidation algorithm. An exceptional generated-ID
collision rebuilds membership after replacement: the displaced edge may have
been the only provenance of a later candidate. That fallback preserves the old
behavior and is excluded from the ordinary linear-work claim.

No graph result, source-root resolution, node identity, authority or health
decision is cached. Source resolution still executes for each relevant candidate;
self-loop and allowed-pair guards, curated override, generation/successor checks
and connectivity validation are untouched. This is backend-neutral Core work,
not a Grafx-specific query shortcut. Empty candidate batches still return without
inspecting edge values.

Validation: `test_kg_provenance_batch_cost.py`, `test_kg_self_loop_connectivity.py`,
`test_kg_primitives_connectivity_guard.py` and `test_kg_nc8_supersede_trail.py`
passed 24 tests in 20.29 s in the final slice (the prior overlapping slice had
23 passes). The structural 250-node case counts exactly one scan
of 250 original edges, then one scan of the 500-edge resulting batch. All 250 root
lookups remain. Missing/Entity/Bug-root cases match repeated-scan membership;
a failure resolving the second candidate of the same source still propagates.
The generated-ID collision test also proves displaced provenance is withdrawn.
Ruff and whitespace checks passed.

The change is prepared for accumulated source deployment, not yet loaded by the
running Pulse PID 23228. No live consolidation/redrive/rebuild or deterministic
repair replay was performed. The 21 pending specs remain reserved. Related native
write attribution is recorded in Grafx `docs/SOURCE_REFERENCE_WRITE_COST_0_0_4.md`;
its isolated commit samples are not full Pulse consolidation latency.
