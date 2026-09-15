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

Initially prepared for accumulated deployment without consuming the 21 reserved
specs. Subsequently, the operator authorized exactly one real consolidation.
Core `e1e9d08` is now loaded by Pulse 0.3.3 PID 2124 with installed Grafx 0.0.4
`2db169d`. Session `kgses_43a75608efd34f80` committed 5 Alternatives and 10 edges
in 22.981 s through the full MCP commit call; automatic provenance and explicit
judgement pairs were read back. Global event `evt_07f41318afbf4fa7` acknowledged
without retry after 131.489 s. The ledger is now 20 pending/20 consolidated,
all remaining pending items unchanged. No redrive/rebuild or deterministic repair
replay was performed. These end-to-end observations do not isolate this local
membership change. Full evidence and remaining write-cost priorities are in Grafx
`docs/PULSE_SINGLE_SPEC_INSTALLED_0_0_4.md`.
